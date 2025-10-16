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

import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static org.apache.datasketches.common.ByteArrayUtil.putDoubleLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_DOUBLES_SKETCH;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Objects;

import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.SketchesArgumentException;

/**
 * This class implements an on-heap doubles KllSketch.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
final class KllHeapDoublesSketch extends KllDoublesSketch {
  private final int k; // configured size of K.
  private final int m; // configured size of M.
  private long n;      // number of items input into this sketch.
  private int minK;    // dynamic minK for error estimation after merging with different k.
  private boolean isLevelZeroSorted;
  private double minDoubleItem;
  private double maxDoubleItem;
  private double[] doubleItems;

  /**
   * New instance heap constructor with a given parameters <em>k</em> and <em>m</em>.
   *
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * <em>k</em> can be between <em>m</em> and 65535, inclusive.
   * @param m parameter controls the minimum level width in items. It can be 2, 4, 6 or 8.
   * The DEFAULT_M, which is 8 is recommended. Other sizes of <em>m</em> should be considered
   * experimental as they have not been as well characterized.
   */
  KllHeapDoublesSketch(final int k, final int m) {
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
    minDoubleItem = Double.NaN;
    maxDoubleItem = Double.NaN;
    doubleItems = new double[k];
  }

  /**
   * Used for creating a temporary sketch for use with weighted updates.
   */
  KllHeapDoublesSketch(final int k, final int m, final double item, final long weight) {
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
    minDoubleItem = item;
    maxDoubleItem = item;
    doubleItems = KllDoublesHelper.createItemsArray(item, weight);
  }

  /**
   * Heapify constructor.
   * @param srcSeg MemorySegment object that contains data serialized by this sketch.
   * @param segValidate the MemoryValidate object
   */
  private KllHeapDoublesSketch(
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
      minDoubleItem = Double.NaN;
      maxDoubleItem = Double.NaN;
      doubleItems = new double[k];
    }
    else if (segStructure == COMPACT_SINGLE) {
      final double item = srcSeg.get(JAVA_DOUBLE_UNALIGNED, DATA_START_ADR_SINGLE_ITEM);
      minDoubleItem = maxDoubleItem = item;
      doubleItems = new double[k];
      doubleItems[k - 1] = item;
    }
    else if (segStructure == COMPACT_FULL) {
      int offsetBytes = DATA_START_ADR;
      offsetBytes += (levelsArr.length - 1) * Integer.BYTES; //shortened levelsArr
      minDoubleItem = srcSeg.get(JAVA_DOUBLE_UNALIGNED, offsetBytes);
      offsetBytes += Double.BYTES;
      maxDoubleItem = srcSeg.get(JAVA_DOUBLE_UNALIGNED, offsetBytes);
      offsetBytes += Double.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      final int freeSpace = levelsArr[0];
      final int retainedItems = capacityItems - freeSpace;
      doubleItems = new double[capacityItems];
      MemorySegment.copy(srcSeg, JAVA_DOUBLE_UNALIGNED, offsetBytes, doubleItems, freeSpace, retainedItems);
    }
    else { //(segStructure == UPDATABLE)
      int offsetBytes = DATA_START_ADR;
      offsetBytes += levelsArr.length * Integer.BYTES; //full levelsArr
      minDoubleItem = srcSeg.get(JAVA_DOUBLE_UNALIGNED, offsetBytes);
      offsetBytes += Double.BYTES;
      maxDoubleItem = srcSeg.get(JAVA_DOUBLE_UNALIGNED, offsetBytes);
      offsetBytes += Double.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      doubleItems = new double[capacityItems];
      MemorySegment.copy(srcSeg, JAVA_DOUBLE_UNALIGNED, offsetBytes, doubleItems, 0, capacityItems);
    }
  }

  static KllHeapDoublesSketch heapifyImpl(final MemorySegment srcSeg) {
    Objects.requireNonNull(srcSeg, "Parameter 'srcSeg' must not be null");
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(srcSeg, KLL_DOUBLES_SKETCH);
    return new KllHeapDoublesSketch(srcSeg, segVal);
  }

  //End of constructors

  @Override
  String getItemAsString(final int index) {
    if (isEmpty()) { return "NaN"; }
    return Double.toString(doubleItems[index]);
  }

  @Override
  public int getK() { return k; }

  //MinMax Methods

  @Override
 double getMaxItemInternal() { return maxDoubleItem; }

  @Override
  public double getMaxItem() {
    if (isEmpty() || Double.isNaN(maxDoubleItem)) { throw new SketchesArgumentException(EMPTY_MSG); }
    return maxDoubleItem;
  }

  @Override
  String getMaxItemAsString() {
    return Double.toString(maxDoubleItem);
  }

  @Override
  double getMinItemInternal() { return minDoubleItem; }

  @Override
  public double getMinItem() {
    if (isEmpty() || Double.isNaN(minDoubleItem)) { throw new SketchesArgumentException(EMPTY_MSG); }
    return minDoubleItem;
  }

  @Override
  String getMinItemAsString() {
    return Double.toString(minDoubleItem);
  }

  @Override
  byte[] getMinMaxByteArr() {
    final byte[] bytesOut = new byte[2 * Double.BYTES];
    putDoubleLE(bytesOut, 0, minDoubleItem);
    putDoubleLE(bytesOut, Double.BYTES, maxDoubleItem);
    return bytesOut;
  }

  @Override
  void setMaxItem(final double item) { maxDoubleItem = item; }

  @Override
  void setMinItem(final double item) { minDoubleItem = item; }

  //END MinMax Methods

  @Override
  public long getN() { return n; }

  //other restricted

  @Override
  double[] getDoubleItemsArray() { return doubleItems; }

  @Override
  double getDoubleSingleItem() {
    if (n != 1L) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    return doubleItems[k - 1];
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
      bytesOut = new byte[Double.BYTES];
      putDoubleLE(bytesOut, 0, getDoubleSingleItem());
      return bytesOut;
    }
    final int retained = getNumRetained();
    final int bytes = retained * Double.BYTES;
    bytesOut = new byte[bytes];
    final MemorySegment wseg = MemorySegment.ofArray(bytesOut);
    MemorySegment.copy(doubleItems,  levelsArr[0], wseg, JAVA_DOUBLE_UNALIGNED, 0, retained);
    return bytesOut;
  }

  @Override
  byte[] getTotalItemsByteArr() {
    final byte[] byteArr = new byte[doubleItems.length * Double.BYTES];
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    MemorySegment.copy(doubleItems, 0, wseg, JAVA_DOUBLE_UNALIGNED, 0, doubleItems.length);
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
  void setDoubleItemsArray(final double[] doubleItems) { this.doubleItems = doubleItems; }

  @Override
  void setDoubleItemsArrayAt(final int index, final double item) { doubleItems[index] = item; }

  @Override
  void setDoubleItemsArrayAt(final int dstIndex, final double[] srcItems, final int srcOffset, final int length) {
    System.arraycopy(srcItems, srcOffset, doubleItems, dstIndex, length);
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
  double[] getDoubleRetainedItemsArray() {
    return Arrays.copyOfRange(doubleItems, levelsArr[0], levelsArr[getNumLevels()]);
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
