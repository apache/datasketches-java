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
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static org.apache.datasketches.kll.PreambleUtil.MAX_K;
import static org.apache.datasketches.kll.PreambleUtil.MIN_K;

import java.util.Random;

import org.apache.datasketches.SketchesArgumentException;

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

  final int k_; // configured value of K
  final int m_; // configured minimum buffer "width", Must always be DEFAULT_M for now.

  int minK_;      // for error estimation after merging with different k
  long n_;        // number of items input into this sketch
  int numLevels_; // one-based number of current levels,
  int[] levels_;  // array of index offsets into the items[]. Size = numLevels + 1.
  boolean isLevelZeroSorted_;

  final boolean compatible; //compatible with quantiles sketch treatment of rank 0.0 and 1.0.
  static final Random random = new Random();

  /**
   * Heap constructor.
   * @param k configured size of sketch. Range [m, 2^16]
   * @param m minimum level size. Default is 8.
   * @param compatible if true, compatible with quantiles sketch treatment of rank 0.0 and 1.0.
   */
  BaseKllSketch(final int k, final int m, final boolean compatible) {
    checkK(k);
    k_ = k;
    minK_ = k;
    m_ = m;
    numLevels_ = 1;
    levels_ = new int[] {k, k};
    isLevelZeroSorted_ = false;
    this.compatible = compatible;
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
   * Returns the length of the input stream.
   * @return stream length
   */
  public long getN() {
    return n_;
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
    return getNormalizedRankError(minK_, pmf);
  }

  /**
   * Gets the normalized rank error given k and pmf.
   * Static method version of the <i>getNormalizedRankError(boolean)</i>.
   * @param k the configuration parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @see KllDoublesSketch
   */
  // constants were derived as the best fit to 99 percentile empirically measured max error in
  // thousands of trials
  public static double getNormalizedRankError(final int k, final boolean pmf) {
    return pmf
        ? 2.446 / pow(k, 0.9433)
        : 2.296 / pow(k, 0.9723);
  }

  /**
   * Returns the number of retained items (samples) in the sketch.
   * @return the number of retained items (samples) in the sketch
   */
  public int getNumRetained() {
    return levels_[numLevels_] - levels_[0];
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

  // Restricted Methods

  /**
   * Checks the validity of the given value k
   * @param k must be greater than 7 and less than 65536.
   */
  static void checkK(final int k) {
    if (k < MIN_K || k > MAX_K) {
      throw new SketchesArgumentException(
          "K must be >= " + MIN_K + " and <= " + MAX_K + ": " + k);
    }
  }

  /**
   * Finds the first level starting with level 0 that exceeds its nominal capacity
   * @return level to compact
   */
  int findLevelToCompact() { //
    int level = 0;
    while (true) {
      assert level < numLevels_;
      final int pop = levels_[level + 1] - levels_[level];
      final int cap = KllHelper.levelCapacity(k_, numLevels_, level, m_);
      if (pop >= cap) {
        return level;
      }
      level++;
    }
  }

  int currentLevelSize(final int level) {
    if (level >= numLevels_) { return 0; }
    return levels_[level + 1] - levels_[level];
  }

  int getNumRetainedAboveLevelZero() {
    if (numLevels_ == 1) { return 0; }
    return levels_[numLevels_] - levels_[1];
  }

  // for testing

  int[] getLevels() {
    return levels_;
  }

  int getNumLevels() {
    return numLevels_;
  }

}

