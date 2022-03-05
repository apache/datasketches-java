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

abstract class KllHeapSketch extends KllSketch {

  /*
   * Data is stored in items_.
   * The data for level i lies in positions levels_[i] through levels_[i + 1] - 1 inclusive.
   * Hence, levels_ must contain (numLevels_ + 1) indices.
   * The valid portion of items_ is completely packed, except for level 0,
   * which is filled from the top down.
   *
   * Invariants:
   * 1) After a compaction, or an update, or a merge, all levels are sorted except for level zero.
   * 2) After a compaction, (sum of capacities) - (sum of items) >= 1,
   *  so there is room for least 1 more item in level zero.
   * 3) There are no gaps except at the bottom, so if levels_[0] = 0,
   *  the sketch is exactly filled to capacity and must be compacted.
   * 4) Sum of weights of all retained items == N.
   * 5) curTotalCap = items_.length = levels_[numLevels_].
   */

  private int dyMinK_;    // dynamic minK for error estimation after merging with different k
  private long n_;        // number of items input into this sketch
  private int numLevels_; // one-based number of current levels,
  private int[] levels_;  // array of index offsets into the items[]. Size = numLevels + 1.
  private boolean isLevelZeroSorted_;

  /**
   * Heap constructor.
   * @param k configured size of sketch. Range [m, 2^16]
   */
  KllHeapSketch(final int k, final SketchType sketchType) {
    super(k, sketchType);
    KllHelper.checkK(k);
    dyMinK_ = k;
    numLevels_ = 1;
    levels_ = new int[] {k, k};
    isLevelZeroSorted_ = false;
  }

  @Override
  int getDyMinK() {
    return dyMinK_;
  }

  @Override
  void setDyMinK(final int dyMinK) {
    dyMinK_ = dyMinK;
  }

  @Override
  int getNumLevels() {
    return numLevels_;
  }

  @Override
  void setNumLevels(final int numLevels) {
    numLevels_ = numLevels;
  }

  @Override
  void incNumLevels() {
    numLevels_++;
  }

  @Override
  int[] getLevelsArray() {
    return levels_;
  }

  @Override
  int getLevelsArrayAt(final int index) {
    return levels_[index];
  }

  @Override
  void setLevelsArray(final int[] levels) {
    this.levels_ = levels;
  }

  @Override
  void setLevelsArrayAt(final int index, final int value) {
    this.levels_[index] = value;
  }

  @Override
  void setLevelsArrayAtPlusEq(final int index, final int plusEq) {
    this.levels_[index] += plusEq;
  }

  @Override
  void setLevelsArrayAtMinusEq(final int index, final int minusEq) {
    this.levels_[index] -= minusEq;
  }

  @Override
  boolean isLevelZeroSorted() {
    return isLevelZeroSorted_;
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    this.isLevelZeroSorted_ = sorted;
  }

  @Override
  void setN(final long n) {
    n_ = n;
  }

  @Override
  void incN() {
    n_++;
  }

  // public functions

  @Override
  public long getN() {
    return n_;
  }

}
