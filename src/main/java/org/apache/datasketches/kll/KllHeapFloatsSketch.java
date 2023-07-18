/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.kll;

import static org.apache.datasketches.common.ByteArrayUtil.putFloatLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllSketch.Error.EMPTY;
import static org.apache.datasketches.kll.KllSketch.Error.NOT_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;

import java.util.Arrays;
import java.util.Objects;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class implements an on-heap floats KllSketch.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
final class KllHeapFloatsSketch extends KllFloatsSketch {
  private final int k; // configured size of K.
  private final int m; // configured size of M.
  private long n;      // number of items input into this sketch.
  private int minK;    // dynamic minK for error estimation after merging with different k.
  private boolean isLevelZeroSorted;
  private float minFloatItem;
  private float maxFloatItem;
  private float[] floatItems;

  /**
   * New instance heap constructor with a given parameters <em>k</em> and <em>m</em>.
   *
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * <em>k</em> can be between <em>m</em> and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about 1.65%.
   * Larger <em>k</em> will have smaller error but the sketch will be larger (and slower).
   * @param m parameter controls the minimum level width in items. It can be 2, 4, 6 or 8.
   * The DEFAULT_M, which is 8 is recommended. Other sizes of <em>m</em> should be considered
   * experimental as they have not been as well characterized.
   */
  KllHeapFloatsSketch(final int k, final int m) {
    super(UPDATABLE, null, null);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    this.k = k;
    this.m = m;
    this.n = 0;
    this.minK = k;
    this.isLevelZeroSorted = false;
    this.levelsArr = new int[] {k, k};
    this.minFloatItem = Float.NaN;
    this.maxFloatItem = Float.NaN;
    this.floatItems = new float[k];
  }

  /**
   * Heapify constructor.
   * @param srcMem Memory object that contains data serialized by this sketch.
   * @param memValidate the MemoryValidate object
   */
  private KllHeapFloatsSketch(
      final Memory srcMem,
      final KllMemoryValidate memValidate) {
    super(SketchStructure.UPDATABLE, null, null);
    final SketchStructure memStructure = memValidate.sketchStructure;
    this.k = memValidate.k;
    this.m = memValidate.m;
    n = memValidate.n;
    minK = memValidate.minK;
    levelsArr = memValidate.levelsArr; //normalized to full
    isLevelZeroSorted = memValidate.level0SortedFlag;

    if (memStructure == COMPACT_EMPTY) {
      minFloatItem = Float.NaN;
      maxFloatItem = Float.NaN;
      floatItems = new float[k];
    }
    else if (memStructure == COMPACT_SINGLE) {
      final float item = srcMem.getFloat(DATA_START_ADR_SINGLE_ITEM);
      minFloatItem = maxFloatItem = item;
      floatItems = new float[k];
      floatItems[k - 1] = item;
    }
    else if (memStructure == COMPACT_FULL) {
      int offsetBytes = DATA_START_ADR;
      offsetBytes += (levelsArr.length - 1) * Integer.BYTES; //shortened levelsArr
      minFloatItem = srcMem.getFloat(offsetBytes);
      offsetBytes += Float.BYTES;
      maxFloatItem = srcMem.getFloat(offsetBytes);
      offsetBytes += Float.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      final int garbageItems = levelsArr[0];
      final int retainedItems = capacityItems - garbageItems;
      floatItems = new float[capacityItems];
      srcMem.getFloatArray(offsetBytes, floatItems, garbageItems, retainedItems);
    }
    else { //(memStructure == UPDATABLE)
      int offsetBytes = DATA_START_ADR;
      offsetBytes += levelsArr.length * Integer.BYTES; //full levelsArr
      minFloatItem = srcMem.getFloat(offsetBytes);
      offsetBytes += Float.BYTES;
      maxFloatItem = srcMem.getFloat(offsetBytes);
      offsetBytes += Float.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      floatItems = new float[capacityItems];
      srcMem.getFloatArray(offsetBytes, floatItems, 0, capacityItems);
    }
  }

  static KllHeapFloatsSketch heapifyImpl(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, FLOATS_SKETCH, null);
    return new KllHeapFloatsSketch(srcMem, memVal);
  }

  @Override
  public int getK() { return k; }

  @Override
  public float getMaxItem() {
    if (isEmpty()) { kllSketchThrow(EMPTY); }
    return maxFloatItem;
  }

  @Override
  public float getMinItem() {
    if (isEmpty()) { kllSketchThrow(EMPTY); }
    return minFloatItem;
  }

  @Override
  public long getN() { return n; }

  //restricted

  @Override
  float[] getFloatItemsArray() { return floatItems; }

  @Override
  float getFloatSingleItem() {
    if (n != 1L) { kllSketchThrow(NOT_SINGLE_ITEM); return Float.NaN; }
    return floatItems[k - 1];
  }

  @Override
  int getM() { return m; }

  @Override
  int getMinK() { return minK; }

  @Override
  byte[] getMinMaxByteArr() {
    final byte[] bytesOut = new byte[2 * Float.BYTES];
    putFloatLE(bytesOut, 0, minFloatItem);
    putFloatLE(bytesOut, Float.BYTES, maxFloatItem);
    return bytesOut;
  }

  @Override
  byte[] getRetainedDataByteArr() {
    if (isEmpty()) { return new byte[0]; }
    final byte[] bytesOut;
    if (isSingleItem()) {
      bytesOut = new byte[Float.BYTES];
      putFloatLE(bytesOut, 0, getFloatSingleItem());
      return bytesOut;
    }
    final int retained = getNumRetained();
    final int bytes = retained * Float.BYTES;
    bytesOut = new byte[bytes];
    final WritableMemory wmem = WritableMemory.writableWrap(bytesOut);
    wmem.putFloatArray(0, floatItems, levelsArr[0], retained);
    return bytesOut;
  }

  @Override
  byte[] getTotalItemDataByteArr() {
    final byte[] byteArr = new byte[floatItems.length * Float.BYTES];
    final WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    wmem.putFloatArray(0, floatItems, 0, floatItems.length);
    return byteArr;
  }

  @Override
  int getTotalItemDataBytes() {
    return floatItems.length * Float.BYTES;
  }

  @Override
  void incN() { n++; }

  @Override
  void incNumLevels() {
    //the heap sketch computes num levels from the array itself, so this is not used on-heap
  }

  @Override
  boolean isLevelZeroSorted() { return this.isLevelZeroSorted; }

  @Override
  void setFloatItemsArray(final float[] floatItems) { this.floatItems = floatItems; }

  @Override
  void setFloatItemsArrayAt(final int index, final float item) { this.floatItems[index] = item; }

  @Override
  void setLevelZeroSorted(final boolean sorted) { this.isLevelZeroSorted = sorted; }

  @Override
  void setMaxItem(final float item) { this.maxFloatItem = item; }

  @Override
  void setMinItem(final float item) { this.minFloatItem = item; }

  @Override
  void setMinK(final int minK) { this.minK = minK; }

  @Override
  void setN(final long n) { this.n = n; }

  @Override
  void setNumLevels(final int numLevels) {
  //the heap sketch computes num levels from the array itself, so this is not used on-heap
  }

  @Override
  float[] getFloatRetainedItemsArray() {
    return Arrays.copyOfRange(floatItems, levelsArr[0], levelsArr[getNumLevels()]);
  }

}
