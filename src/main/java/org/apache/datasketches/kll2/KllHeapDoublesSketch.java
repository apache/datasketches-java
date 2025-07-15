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

package org.apache.datasketches.kll2;

import static org.apache.datasketches.common.ByteArrayUtil.putDoubleLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;

import java.util.Arrays;
import java.util.Objects;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

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
    this.levelsArr = new int[] {k, k};
    this.readOnly = false;
    this.k = k;
    this.m = m;
    this.n = 0;
    this.minK = k;
    this.isLevelZeroSorted = false;
    this.minDoubleItem = Double.NaN;
    this.maxDoubleItem = Double.NaN;
    this.doubleItems = new double[k];
  }

  /**
   * Used for creating a temporary sketch for use with weighted updates.
   */
  KllHeapDoublesSketch(final int k, final int m, final double item, final long weight) {
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
    this.minDoubleItem = item;
    this.maxDoubleItem = item;
    this.doubleItems = KllDoublesHelper.createItemsArray(item, weight);
  }

  /**
   * Heapify constructor.
   * @param srcMem Memory object that contains data serialized by this sketch.
   * @param memValidate the MemoryValidate object
   */
  private KllHeapDoublesSketch(
      final Memory srcMem,
      final KllMemorSegmentValidate memValidate) {
    super(UPDATABLE);
    final SketchStructure memStructure = memValidate.sketchStructure;
    this.k = memValidate.k;
    this.m = memValidate.m;
    this.n = memValidate.n;
    this.minK = memValidate.minK;
    this.levelsArr = memValidate.levelsArr; //normalized to full
    this.isLevelZeroSorted = memValidate.level0SortedFlag;

    if (memStructure == COMPACT_EMPTY) {
      minDoubleItem = Double.NaN;
      maxDoubleItem = Double.NaN;
      doubleItems = new double[k];
    }
    else if (memStructure == COMPACT_SINGLE) {
      final double item = srcMem.getDouble(DATA_START_ADR_SINGLE_ITEM);
      minDoubleItem = maxDoubleItem = item;
      doubleItems = new double[k];
      doubleItems[k - 1] = item;
    }
    else if (memStructure == COMPACT_FULL) {
      int offsetBytes = DATA_START_ADR;
      offsetBytes += (levelsArr.length - 1) * Integer.BYTES; //shortened levelsArr
      minDoubleItem = srcMem.getDouble(offsetBytes);
      offsetBytes += Double.BYTES;
      maxDoubleItem = srcMem.getDouble(offsetBytes);
      offsetBytes += Double.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      final int freeSpace = levelsArr[0];
      final int retainedItems = capacityItems - freeSpace;
      doubleItems = new double[capacityItems];
      srcMem.getDoubleArray(offsetBytes, doubleItems, freeSpace, retainedItems);
    }
    else { //(memStructure == UPDATABLE)
      int offsetBytes = DATA_START_ADR;
      offsetBytes += levelsArr.length * Integer.BYTES; //full levelsArr
      minDoubleItem = srcMem.getDouble(offsetBytes);
      offsetBytes += Double.BYTES;
      maxDoubleItem = srcMem.getDouble(offsetBytes);
      offsetBytes += Double.BYTES;
      final int capacityItems = levelsArr[getNumLevels()];
      doubleItems = new double[capacityItems];
      srcMem.getDoubleArray(offsetBytes, doubleItems, 0, capacityItems);
    }
  }

  static KllHeapDoublesSketch heapifyImpl(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemorSegmentValidate memVal = new KllMemorSegmentValidate(srcMem, DOUBLES_SKETCH);
    return new KllHeapDoublesSketch(srcMem, memVal);
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
  void setMaxItem(final double item) { this.maxDoubleItem = item; }

  @Override
  void setMinItem(final double item) { this.minDoubleItem = item; }

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
  MemoryRequestServer getMemoryRequestServer() { return null; }

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
    final WritableMemory wmem = WritableMemory.writableWrap(bytesOut);
    wmem.putDoubleArray(0, doubleItems, levelsArr[0], retained);
    return bytesOut;
  }

  @Override
  byte[] getTotalItemsByteArr() {
    final byte[] byteArr = new byte[doubleItems.length * Double.BYTES];
    final WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    wmem.putDoubleArray(0, doubleItems, 0, doubleItems.length);
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
  void setDoubleItemsArray(final double[] doubleItems) { this.doubleItems = doubleItems; }

  @Override
  void setDoubleItemsArrayAt(final int index, final double item) { this.doubleItems[index] = item; }

  @Override
  void setDoubleItemsArrayAt(final int dstIndex, final double[] srcItems, final int srcOffset, final int length) {
    System.arraycopy(srcItems, srcOffset, doubleItems, dstIndex, length);
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
  double[] getDoubleRetainedItemsArray() {
    return Arrays.copyOfRange(doubleItems, levelsArr[0], levelsArr[getNumLevels()]);
  }

  @Override
  void setWritableMemory(final WritableMemory wmem) { }

}
