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

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.ByteArrayUtil.putLongLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.LONGS_SKETCH;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Objects;

import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.SketchesArgumentException;

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
    levelsArr = new int[] {k, k};
    readOnly = false;
    this.k = k;
    this.m = m;
    n = 0;
    minK = k;
    isLevelZeroSorted = false;
    minLongItem = Long.MAX_VALUE;
    maxLongItem = Long.MIN_VALUE;
    longItems = new long[k];
  }

  /**
   * Internally used for creating a temporary sketch for use with weighted updates.
   */
  KllHeapLongsSketch(final int k, final int m, final long item, final long weight) {
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
    minLongItem = item;
    maxLongItem = item;
    longItems = KllLongsHelper.createItemsArray(item, weight);
  }

  /**
   * Heapify constructor.
   * @param srcSeg Memory object that contains data serialized by this sketch.
   * @param segValidate the MemoryValidate object
   */
  private KllHeapLongsSketch(
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
      minLongItem = Long.MAX_VALUE;
      maxLongItem = Long.MIN_VALUE;
      longItems = new long[k];
    }
    else if (segStructure == COMPACT_SINGLE) {
      final long item = srcSeg.get(JAVA_LONG_UNALIGNED, DATA_START_ADR_SINGLE_ITEM);
      minLongItem = maxLongItem = item;
      longItems = new long[k];
      longItems[k - 1] = item;
    }
    else if (segStructure == COMPACT_FULL) {
      int offsetBytes = DATA_START_ADR;
      offsetBytes += (levelsArr.length - 1) * Integer.BYTES; //shortened levelsArr
      minLongItem = srcSeg.get(JAVA_LONG_UNALIGNED, offsetBytes);
      offsetBytes += Long.BYTES;
      maxLongItem = srcSeg.get(JAVA_LONG_UNALIGNED, offsetBytes);
      offsetBytes += Long.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      final int freeSpace = levelsArr[0];
      final int retainedItems = capacityItems - freeSpace;
      longItems = new long[capacityItems];
      MemorySegment.copy(srcSeg, JAVA_LONG_UNALIGNED, offsetBytes, longItems, freeSpace, retainedItems);
    }
    else { //(segStructure == UPDATABLE)
      int offsetBytes = DATA_START_ADR;
      offsetBytes += levelsArr.length * Integer.BYTES; //full levelsArr
      minLongItem = srcSeg.get(JAVA_LONG_UNALIGNED, offsetBytes);
      offsetBytes += Long.BYTES;
      maxLongItem = srcSeg.get(JAVA_LONG_UNALIGNED, offsetBytes);
      offsetBytes += Long.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      longItems = new long[capacityItems];
      MemorySegment.copy(srcSeg, JAVA_LONG_UNALIGNED, offsetBytes, longItems, 0, capacityItems);
    }
  }

  static KllHeapLongsSketch heapifyImpl(final MemorySegment srcSeg) {
    Objects.requireNonNull(srcSeg, "Parameter 'srcSeg' must not be null");
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(srcSeg, LONGS_SKETCH);
    return new KllHeapLongsSketch(srcSeg, segVal);
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
  void setMaxItem(final long item) { maxLongItem = item; }

  @Override
  void setMinItem(final long item) { minLongItem = item; }

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
    final MemorySegment wseg = MemorySegment.ofArray(bytesOut);
    MemorySegment.copy(longItems, levelsArr[0], wseg, JAVA_LONG_UNALIGNED, 0, retained);
    return bytesOut;
  }

  @Override
  byte[] getTotalItemsByteArr() {
    final byte[] byteArr = new byte[longItems.length * Long.BYTES];
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    MemorySegment.copy(longItems, 0, wseg, JAVA_LONG_UNALIGNED, 0, longItems.length);
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
  void setLongItemsArray(final long[] longItems) { this.longItems = longItems; }

  @Override
  void setLongItemsArrayAt(final int index, final long item) { longItems[index] = item; }

  @Override
  void setLongItemsArrayAt(final int dstIndex, final long[] srcItems, final int srcOffset, final int length) {
    System.arraycopy(srcItems, srcOffset, longItems, dstIndex, length);
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
  long[] getLongRetainedItemsArray() {
    return Arrays.copyOfRange(longItems, levelsArr[0], levelsArr[getNumLevels()]);
  }

  @Override
  void setMemorySegment(final MemorySegment wseg) { }

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
