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

import org.apache.datasketches.memory.Memory;
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
  private final int k;    // configured value of K.
  private final int m;    // configured value of M.
  private long n_;        // number of items input into this sketch.
  private int minK_;    // dynamic minK for error estimation after merging with different k.
  private int numLevels_; // one-based number of current levels.
  private int[] levels_;  // array of index offsets into the items[]. Size = numLevels + 1.
  private boolean isLevelZeroSorted_;
  private double[] doubleItems_;
  private double minDoubleValue_;
  private double maxDoubleValue_;

  /**
   * Heap constructor with a given parameters <em>k</em> and <em>m</em>.
   *
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * <em>k</em> can be any value between <em>m</em> and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about 1.65%.
   * Higher values of <em>k</em> will have smaller error but the sketch will be larger (and slower).
   * @param m parameter controls the minimum level width in items. It can be 2, 4, 6 or 8.
   * The DEFAULT_M, which is 8 is recommended. Other values of <em>m</em> should be considered
   * experimental as they have not been as well characterized.
   */
  KllHeapDoublesSketch(final int k, final int m) {
    super(SketchType.DOUBLES_SKETCH, null, null);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    this.k = k;
    this.m = m;
    n_ = 0;
    minK_ = k;
    numLevels_ = 1;
    levels_ = new int[] {k, k};
    isLevelZeroSorted_ = false;
    doubleItems_ = new double[k];
    minDoubleValue_ = Double.NaN;
    maxDoubleValue_ = Double.NaN;
  }

  /**
   * Heapify constructor.
   * @param mem Memory object that contains data serialized by this sketch.
   * @param memVal the MemoryCheck object
   */
  KllHeapDoublesSketch(final Memory mem, final KllMemoryValidate memVal) {
    super(SketchType.DOUBLES_SKETCH,null, null );
    k = memVal.k;
    m = memVal.m;
    KllHelper.buildHeapKllSketchFromMemory(this, memVal);
  }

  @Override
  public int getK() {
    return k;
  }

  @Override
  public long getN() {
    return n_;
  }

  @Override
  public void reset() {
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelsArray(new int[] {k, k});
    setLevelZeroSorted(false);
    doubleItems_ = new double[k];
    minDoubleValue_ = Double.NaN;
    maxDoubleValue_ = Double.NaN;
  }

  @Override
  double[] getDoubleItemsArray() { return doubleItems_; }

  @Override
  double getDoubleItemsArrayAt(final int index) { return doubleItems_[index]; }

  @Override
  int[] getLevelsArray() {
    return levels_;
  }

  @Override
  int getLevelsArrayAt(final int index) { return levels_[index]; }

  @Override
  int getM() {
    return m;
  }

  @Override
  double getMaxDoubleValue() { return maxDoubleValue_; }

  @Override
  double getMinDoubleValue() { return minDoubleValue_; }

  @Override
  int getMinK() {
    return minK_;
  }

  @Override
  int getNumLevels() {
    return numLevels_;
  }

  @Override
  void incN() {
    n_++;
  }

  @Override
  void incNumLevels() {
    numLevels_++;
  }

  @Override
  boolean isLevelZeroSorted() {
    return isLevelZeroSorted_;
  }

  @Override
  void setDoubleItemsArray(final double[] doubleItems) { doubleItems_ = doubleItems; }

  @Override
  void setDoubleItemsArrayAt(final int index, final double value) { doubleItems_[index] = value; }

  @Override
  void setItemsArrayUpdatable(final WritableMemory itemsMem) { } //dummy

  @Override
  void setLevelsArray(final int[] levelsArr) {
    levels_ = levelsArr;
  }

  @Override
  void setLevelsArrayAt(final int index, final int value) { levels_[index] = value; }

  @Override
  void setLevelsArrayAtMinusEq(final int index, final int minusEq) {
    levels_[index] -= minusEq;
  }

  @Override
  void setLevelsArrayAtPlusEq(final int index, final int plusEq) {
    levels_[index] += plusEq;
  }

  @Override
  void setLevelsArrayUpdatable(final WritableMemory levelsMem) { } //dummy

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    this.isLevelZeroSorted_ = sorted;
  }

  @Override
  void setMaxDoubleValue(final double value) { maxDoubleValue_ = value; }

  @Override
  void setMinDoubleValue(final double value) { minDoubleValue_ = value; }

  @Override
  void setMinK(final int minK) {
    minK_ = minK;
  }

  @Override
  void setMinMaxArrayUpdatable(final WritableMemory minMaxMem) { } //dummy

  @Override
  void setN(final long n) {
    n_ = n;
  }

  @Override
  void setNumLevels(final int numLevels) {
    numLevels_ = numLevels;
  }

}
