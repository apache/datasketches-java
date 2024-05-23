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

import static org.apache.datasketches.common.ByteArrayUtil.putLongLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.LONGS_SKETCH;

import java.util.Arrays;
import java.util.Objects;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class implements an on-heap longs KllSketch.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
final class KllHeapLongsSketch extends KllLongsSketch {
  private final int k; // configured size of K.
  private final int m; // configured size of M.
  private long n;      // number of items input into this sketch.
  private int minK;    // dynamic minK for error estimation after merging with different k.
  private boolean isLevelZeroSorted;
  private long minLongItem;
  private long maxLongItem;
  private long[] longItems;

  /**
   * New instance heap constructor with a given parameters <em>k</em> and <em>m</em>.
   *
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * <em>k</em> can be between <em>m</em> and 65535, inclusive.
   * @param m parameter controls the minimum level width in items. It can be 2, 4, 6 or 8.
   * The DEFAULT_M, which is 8 is recommended. Other sizes of <em>m</em> should be considered
   * experimental as they have not been as well characterized.
   */
  KllHeapLongsSketch(final int k, final int m) {
    super(UPDATABLE);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    this.levelsArr = new int[] {k, k};
    this.readOnly = false;
    this.k = k;
    this.m = m;
    this.n = 0;
    this.minK = k;
    this.isLevelZeroSorted = false;
    this.minLongItem = Long.MAX_VALUE;
    this.maxLongItem = Long.MIN_VALUE;
    this.longItems = new long[k];
  }

  /**
   * Used for creating a temporary sketch for use with weighted updates.
   */
  KllHeapLongsSketch(final int k, final int m, final long item, final long weight) {
    super(UPDATABLE);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    this.levelsArr = KllHelper.createLevelsArray(weight);
    this.readOnly = false;
    this.k = k;
    this.m = m;
    this.n = weight;
    this.minK = k;
    this.isLevelZeroSorted = false;
    this.minLongItem = item;
    this.maxLongItem = item;
    this.longItems = KllLongsHelper.createItemsArray(item, weight);
  }

  /**
   * Heapify constructor.
   * @param srcMem Memory object that contains data serialized by this sketch.
   * @param memValidate the MemoryValidate object
   */
  private KllHeapLongsSketch(
      final Memory srcMem,
      final KllMemoryValidate memValidate) {
    super(UPDATABLE);
    final SketchStructure memStructure = memValidate.sketchStructure;
    this.k = memValidate.k;
    this.m = memValidate.m;
    this.n = memValidate.n;
    this.minK = memValidate.minK;
    this.levelsArr = memValidate.levelsArr; //normalized to full
    this.isLevelZeroSorted = memValidate.level0SortedFlag;

    if (memStructure == COMPACT_EMPTY) {
      minLongItem = Long.MAX_VALUE;
      maxLongItem = Long.MIN_VALUE;
      longItems = new long[k];
    }
    else if (memStructure == COMPACT_SINGLE) {
      final long item = srcMem.getLong(DATA_START_ADR_SINGLE_ITEM);
      minLongItem = maxLongItem = item;
      longItems = new long[k];
      longItems[k - 1] = item;
    }
    else if (memStructure == COMPACT_FULL) {
      int offsetBytes = DATA_START_ADR;
      offsetBytes += (levelsArr.length - 1) * Integer.BYTES; //shortened levelsArr
      minLongItem = srcMem.getLong(offsetBytes);
      offsetBytes += Long.BYTES;
      maxLongItem = srcMem.getLong(offsetBytes);
      offsetBytes += Long.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      final int freeSpace = levelsArr[0];
      final int retainedItems = capacityItems - freeSpace;
      longItems = new long[capacityItems];
      srcMem.getLongArray(offsetBytes, longItems, freeSpace, retainedItems);
    }
    else { //(memStructure == UPDATABLE)
      int offsetBytes = DATA_START_ADR;
      offsetBytes += levelsArr.length * Integer.BYTES; //full levelsArr
      minLongItem = srcMem.getLong(offsetBytes);
      offsetBytes += Long.BYTES;
      maxLongItem = srcMem.getLong(offsetBytes);
      offsetBytes += Long.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      longItems = new long[capacityItems];
      srcMem.getLongArray(offsetBytes, longItems, 0, capacityItems);
    }
  }

  static KllHeapLongsSketch heapifyImpl(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, LONGS_SKETCH);
    return new KllHeapLongsSketch(srcMem, memVal);
  }

  //End of constructors

  @Override
  String getItemAsString(final int index) {
    if (isEmpty()) { return "Null"; }
    return Long.toString(longItems[index]);
  }

  @Override
  public int getK() { return k; }

  //MinMax Methods

  @Override
 long getMaxItemInternal() { return maxLongItem; }

  @Override
  public long getMaxItem() {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    return maxLongItem;
  }

  @Override
  String getMaxItemAsString() {
    return Long.toString(maxLongItem);
  }

  @Override
  long getMinItemInternal() { return minLongItem; }

  @Override
  public long getMinItem() {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    return minLongItem;
  }

  @Override
  String getMinItemAsString() {
    return Long.toString(minLongItem);
  }

  @Override
  byte[] getMinMaxByteArr() {
    final byte[] bytesOut = new byte[2 * Long.BYTES];
    putLongLE(bytesOut, 0, minLongItem);
    putLongLE(bytesOut, Long.BYTES, maxLongItem);
    return bytesOut;
  }

  @Override
  void setMaxItem(final long item) { this.maxLongItem = item; }

  @Override
  void setMinItem(final long item) { this.minLongItem = item; }

  //END MinMax Methods

  @Override
  public long getN() { return n; }

  //other restricted

  @Override
  long[] getLongItemsArray() { return longItems; }

  @Override
  long getLongSingleItem() {
    if (n != 1L) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    return longItems[k - 1];
  }

  @Override
  int getM() { return m; }

  @Override
  MemoryRequestServer getMemoryRequestServer() { return null; }

  @Override
  int getMinK() { return minK; }

  @Override
  byte[] getRetainedItemsByteArr() {
    if (isEmpty()) { return new byte[0]; }
    final byte[] bytesOut;
    if (isSingleItem()) {
      bytesOut = new byte[Long.BYTES];
      putLongLE(bytesOut, 0, getLongSingleItem());
      return bytesOut;
    }
    final int retained = getNumRetained();
    final int bytes = retained * Long.BYTES;
    bytesOut = new byte[bytes];
    final WritableMemory wmem = WritableMemory.writableWrap(bytesOut);
    wmem.putLongArray(0, longItems, levelsArr[0], retained);
    return bytesOut;
  }

  @Override
  byte[] getTotalItemsByteArr() {
    final byte[] byteArr = new byte[longItems.length * Long.BYTES];
    final WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    wmem.putLongArray(0, longItems, 0, longItems.length);
    return byteArr;
  }

  @Override
  WritableMemory getWritableMemory() {
    return null;
  }

  @Override
  void incN(final int increment) { n += increment; }

  @Override
  void incNumLevels() {
    //the heap sketch computes num levels from the array itself, so this is not used on-heap
  }

  @Override
  boolean isLevelZeroSorted() { return this.isLevelZeroSorted; }

  @Override
  void setLongItemsArray(final long[] longItems) { this.longItems = longItems; }

  @Override
  void setLongItemsArrayAt(final int index, final long item) { this.longItems[index] = item; }

  @Override
  void setLongItemsArrayAt(final int dstIndex, final long[] srcItems, final int srcOffset, final int length) {
    System.arraycopy(srcItems, srcOffset, longItems, dstIndex, length);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) { this.isLevelZeroSorted = sorted; }

  @Override
  void setMinK(final int minK) { this.minK = minK; }

  @Override
  void setN(final long n) { this.n = n; }

  @Override
  void setNumLevels(final int numLevels) {
    //the heap sketch computes num levels from the array itself, so this is not used on-heap
  }

  @Override
  long[] getLongRetainedItemsArray() {
    return Arrays.copyOfRange(longItems, levelsArr[0], levelsArr[getNumLevels()]);
  }

  @Override
  void setWritableMemory(final WritableMemory wmem) { }

}
