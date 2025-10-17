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

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static org.apache.datasketches.common.ByteArrayUtil.putFloatLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_FLOATS_SKETCH;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Objects;

import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.SketchesArgumentException;

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
   * @param m parameter controls the minimum level width in items. It can be 2, 4, 6 or 8.
   * The DEFAULT_M, which is 8 is recommended. Other sizes of <em>m</em> should be considered
   * experimental as they have not been as well characterized.
   */
  KllHeapFloatsSketch(final int k, final int m) {
    super(UPDATABLE);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    levelsArr = new int[] {k, k};
    readOnly = false;
    this.k = k;
    this.m = m;
    n = 0;
    minK = k;
    isLevelZeroSorted = false;
    minFloatItem = Float.NaN;
    maxFloatItem = Float.NaN;
    floatItems = new float[k];
  }

  /**
   * Used for creating a temporary sketch for use with weighted updates.
   */
  KllHeapFloatsSketch(final int k, final int m, final float item, final long weight) {
    super(UPDATABLE);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    levelsArr = KllHelper.createLevelsArray(weight);
    readOnly = false;
    this.k = k;
    this.m = m;
    n = weight;
    minK = k;
    isLevelZeroSorted = false;
    minFloatItem = item;
    maxFloatItem = item;
    floatItems = KllFloatsHelper.createItemsArray(item, weight);
  }

  /**
   * Heapify constructor.
   * @param srcSeg MemorySegment object that contains data serialized by this sketch.
   * @param segValidate the MemoryValidate object
   */
  private KllHeapFloatsSketch(
      final MemorySegment srcSeg,
      final KllMemorySegmentValidate segValidate) {
    super(UPDATABLE);
    final SketchStructure segStructure = segValidate.sketchStructure;
    k = segValidate.k;
    m = segValidate.m;
    n = segValidate.n;
    minK = segValidate.minK;
    levelsArr = segValidate.levelsArr; //normalized to full
    isLevelZeroSorted = segValidate.level0SortedFlag;

    if (segStructure == COMPACT_EMPTY) {
      minFloatItem = Float.NaN;
      maxFloatItem = Float.NaN;
      floatItems = new float[k];
    }
    else if (segStructure == COMPACT_SINGLE) {
      final float item = srcSeg.get(JAVA_FLOAT_UNALIGNED, DATA_START_ADR_SINGLE_ITEM);
      minFloatItem = maxFloatItem = item;
      floatItems = new float[k];
      floatItems[k - 1] = item;
    }
    else if (segStructure == COMPACT_FULL) {
      int offsetBytes = DATA_START_ADR;
      offsetBytes += (levelsArr.length - 1) * Integer.BYTES; //shortened levelsArr
      minFloatItem = srcSeg.get(JAVA_FLOAT_UNALIGNED, offsetBytes);
      offsetBytes += Float.BYTES;
      maxFloatItem = srcSeg.get(JAVA_FLOAT_UNALIGNED, offsetBytes);
      offsetBytes += Float.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      final int freeSpace = levelsArr[0];
      final int retainedItems = capacityItems - freeSpace;
      floatItems = new float[capacityItems];
      MemorySegment.copy(srcSeg, JAVA_FLOAT_UNALIGNED, offsetBytes, floatItems, freeSpace, retainedItems);
    }
    else { //(segStructure == UPDATABLE)
      int offsetBytes = DATA_START_ADR;
      offsetBytes += levelsArr.length * Integer.BYTES; //full levelsArr
      minFloatItem = srcSeg.get(JAVA_FLOAT_UNALIGNED, offsetBytes);
      offsetBytes += Float.BYTES;
      maxFloatItem = srcSeg.get(JAVA_FLOAT_UNALIGNED, offsetBytes);
      offsetBytes += Float.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      floatItems = new float[capacityItems];
      MemorySegment.copy(srcSeg, JAVA_FLOAT_UNALIGNED, offsetBytes, floatItems, 0, capacityItems);
    }
  }

  static KllHeapFloatsSketch heapifyImpl(final MemorySegment srcSeg) {
    Objects.requireNonNull(srcSeg, "Parameter 'srcSeg' must not be null");
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(srcSeg, KLL_FLOATS_SKETCH);
    return new KllHeapFloatsSketch(srcSeg, segVal);
  }

  //End of constructors

  @Override
  String getItemAsString(final int index) {
    if (isEmpty()) { return "NaN"; }
    return Float.toString(floatItems[index]);
  }

  @Override
  public int getK() { return k; }

  //MinMax Methods

  @Override
 float getMaxItemInternal() { return maxFloatItem; }

  @Override
  public float getMaxItem() {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    return maxFloatItem;
  }

  @Override
  String getMaxItemAsString() {
    return Float.toString(maxFloatItem);
  }

  @Override
  float getMinItemInternal() { return minFloatItem; }

  @Override
  public float getMinItem() {
    if (isEmpty() || Float.isNaN(minFloatItem)) { throw new SketchesArgumentException(EMPTY_MSG); }
    return minFloatItem;
  }

  @Override
  String getMinItemAsString() {
    return Float.toString(minFloatItem);
  }

  @Override
  byte[] getMinMaxByteArr() {
    final byte[] bytesOut = new byte[2 * Float.BYTES];
    putFloatLE(bytesOut, 0, minFloatItem);
    putFloatLE(bytesOut, Float.BYTES, maxFloatItem);
    return bytesOut;
  }

  @Override
  void setMaxItem(final float item) { maxFloatItem = item; }

  @Override
  void setMinItem(final float item) { minFloatItem = item; }

  //END MinMax Methods

  @Override
  public long getN() { return n; }

  //other restricted

  @Override
  float[] getFloatItemsArray() { return floatItems; }

  @Override
  float getFloatSingleItem() {
    if (n != 1L) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    return floatItems[k - 1];
  }

  @Override
  int getM() { return m; }

  @Override
  int getMinK() { return minK; }

  @Override
  byte[] getRetainedItemsByteArr() {
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
    final MemorySegment wseg = MemorySegment.ofArray(bytesOut);
    MemorySegment.copy(floatItems, levelsArr[0], wseg, JAVA_FLOAT_UNALIGNED, 0, retained);
    return bytesOut;
  }

  @Override
  byte[] getTotalItemsByteArr() {
    final byte[] byteArr = new byte[floatItems.length * Float.BYTES];
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    MemorySegment.copy(floatItems, 0, wseg, JAVA_FLOAT_UNALIGNED, 0, floatItems.length);
    return byteArr;
  }

  @Override
  MemorySegment getMemorySegment() {
    return null;
  }

  @Override
  void incN(final int increment) { n += increment; }

  @Override
  void incNumLevels() {
    //the heap sketch computes num levels from the array itself, so this is not used on-heap
  }

  @Override
  boolean isLevelZeroSorted() { return isLevelZeroSorted; }

  @Override
  void setFloatItemsArray(final float[] floatItems) { this.floatItems = floatItems; }

  @Override
  void setFloatItemsArrayAt(final int index, final float item) { floatItems[index] = item; }

  @Override
  void setFloatItemsArrayAt(final int dstIndex, final float[] srcItems, final int srcOffset, final int length) {
    System.arraycopy(srcItems, srcOffset, floatItems, dstIndex, length);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) { isLevelZeroSorted = sorted; }

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

  @Override
  void setMemorySegment(final MemorySegment wseg) { /* heap does not have MemorySegment */ }

  @Override
  public boolean hasMemorySegment() {
    return false;
  }

  @Override
  public boolean isOffHeap() {
    return false;
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return false;
  }

  @Override
  MemorySegmentRequest getMemorySegmentRequest() {
    return null;
  }

}
