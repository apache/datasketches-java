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

import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static org.apache.datasketches.kll.PreambleUtil.DEFAULT_M;
import static org.apache.datasketches.kll.PreambleUtil.MAX_K;
import static org.apache.datasketches.kll.PreambleUtil.MIN_K;

import java.util.Random;

abstract class BaseKllSketch {

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

  static final int M = DEFAULT_M; // configured minimum buffer "width", Must always be 8 for now.
  private final int k_; // configured value of K
  private int dyMinK_;      // dynamic minK for error estimation after merging with different k
  private long n_; // number of items input into this sketch
  private int numLevels_; // one-based number of current levels,
  private int[] levels_;  // array of index offsets into the items[]. Size = numLevels + 1.
  private boolean isLevelZeroSorted_;

  private final boolean compatible; //compatible with quantiles sketch treatment of rank 0.0 and 1.0.
  static final Random random = new Random();

  /**
   * Heap constructor.
   * @param k configured size of sketch. Range [m, 2^16]
   * @param m minimum level size. Default is 8.
   * @param compatible if true, compatible with quantiles sketch treatment of rank 0.0 and 1.0.
   */
  BaseKllSketch(final int k, final int m, final boolean compatible) {
    KllHelper.checkK(k);
    k_ = k;
    dyMinK_ = k;
    numLevels_ = 1;
    levels_ = new int[] {k, k};
    isLevelZeroSorted_ = false;
    this.compatible = compatible;
  }

  int getDyMinK() {
    return dyMinK_;
  }

  void setDyMinK(final int dyMinK) {
    dyMinK_ = dyMinK;
  }

  int getNumLevels() {
    return numLevels_;
  }

  void setNumLevels(final int numLevels) {
    numLevels_ = numLevels;
  }

  void incNumLevels() {
    numLevels_++;
  }

  int[] getLevelsArray() {
    return levels_;
  }

  int getLevelsArrayAt(final int index) {
    return levels_[index];
  }

  void setLevelsArray(final int[] levels) {
    this.levels_ = levels;
  }

  void setLevelsArrayAt(final int index, final int value) {
    this.levels_[index] = value;
  }

  void setLevelsArrayAtPlusEq(final int index, final int plusEq) {
    this.levels_[index] += plusEq;
  }

  void setLevelsArrayAtMinusEq(final int index, final int minusEq) {
    this.levels_[index] -= minusEq;
  }

  boolean isLevelZeroSorted() {
    return isLevelZeroSorted_;
  }

  void setLevelZeroSorted(final boolean sorted) {
    this.isLevelZeroSorted_ = sorted;
  }

  boolean isCompatible() {
    return this.compatible;
  }

  void setN(final long n) {
    n_ = n;
  }

  void incN() {
    n_++;
  }

  // public functions

  /**
   * Returns the parameter k
   * @return parameter k
   */
  public int getK() {
    return k_;
  }

  /**
   * Returns the length of the input stream.
   * @return stream length
   */
  public long getN() {
    return n_;
  }

  /**
   * Gets the approximate value of <em>k</em> to use given epsilon, the normalized rank error.
   * @param epsilon the normalized rank error between zero and one.
   * @param pmf if true, this function returns the value of <em>k</em> assuming the input epsilon
   * is the desired "double-sided" epsilon for the getPMF() function. Otherwise, this function
   * returns the value of <em>k</em> assuming the input epsilon is the desired "single-sided"
   * epsilon for all the other queries.
   * @return the value of <i>k</i> given a value of epsilon.
   * @see KllDoublesSketch
   */
  // constants were derived as the best fit to 99 percentile empirically measured max error in
  // thousands of trials
  public static int getKFromEpsilon(final double epsilon, final boolean pmf) {
    //Ensure that eps is >= than the lowest possible eps given MAX_K and pmf=false.
    final double eps = max(epsilon, 4.7634E-5);
    final double kdbl = pmf
        ? exp(log(2.446 / eps) / 0.9433)
        : exp(log(2.296 / eps) / 0.9723);
    final double krnd = round(kdbl);
    final double del = abs(krnd - kdbl);
    final int k = (int) (del < 1E-6 ? krnd : ceil(kdbl));
    return max(MIN_K, min(MAX_K, k));
  }

  /**
   * Gets the approximate rank error of this sketch normalized as a fraction between zero and one.
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, returns the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @see KllDoublesSketch
   */
  public double getNormalizedRankError(final boolean pmf) {
    return KllHelper.getNormalizedRankError(dyMinK_, pmf);
  }

  /**
   * Gets the normalized rank error given k and pmf.
   * Static method version of the <i>getNormalizedRankError(boolean)</i>.
   * @param k the configuration parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   */
  public static double getNormalizedRankError(final int k, final boolean pmf) {
    return KllHelper.getNormalizedRankError(k, pmf);
  }

  /**
   * Returns the number of retained items (samples) in the sketch.
   * @return the number of retained items (samples) in the sketch
   */
  public int getNumRetained() {
    return KllHelper.getNumRetained(numLevels_, levels_);
  }

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  public boolean isEmpty() {
    return n_ == 0;
  }

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  public boolean isEstimationMode() {
    return numLevels_ > 1;
  }

  /**
   * Returns serialized sketch in a compact byte array form.
   * @return serialized sketch in a compact byte array form.
   */
  public abstract byte[] toByteArray();

  /**
   * Returns serialized sketch in an updatable byte array form.
   * @return serialized sketch in an updatable byte array form.
   */
  public abstract byte[] toUpdatableByteArray();

  @Override
  public String toString() {
    return toString(false, false);
  }

  /**
   * Returns a summary of the sketch as a string.
   * @param withLevels if true include information about levels
   * @param withData if true include sketch data
   * @return string representation of sketch summary
   */
  public abstract String toString(final boolean withLevels, final boolean withData);

}
