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
import static org.apache.datasketches.kll.KllHelper.getAllLevelStatsGivenN;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_DOUBLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.DEFAULT_M;
import static org.apache.datasketches.kll.KllPreambleUtil.MAX_K;
import static org.apache.datasketches.kll.KllPreambleUtil.MIN_K;
import static org.apache.datasketches.kll.KllPreambleUtil.N_LONG_ADR;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLE_SKETCH;

import java.util.Random;

import org.apache.datasketches.kll.KllHelper.LevelStats;

abstract class KllSketch {
  static final Random random = new Random();
  static final int M = DEFAULT_M; // configured minimum buffer "width", Must always be 8 for now.
  static final boolean compatible = true; //rank 0.0 and 1.0. compatible with classic Quantiles Sketch
  private final int k; //configured value of K

  enum SketchType { FLOAT_SKETCH, DOUBLE_SKETCH }

  static SketchType sketchType;

  KllSketch(final int k, final SketchType sketchType) {
    this.k = k;
    KllSketch.sketchType = sketchType;
  }

  abstract int getDyMinK();

  abstract void setDyMinK(int dyMinK);

  abstract int getNumLevels();

  abstract void setNumLevels(int numLevels);

  abstract void incNumLevels();

  abstract int[] getLevelsArray();

  abstract int getLevelsArrayAt(int index);

  abstract void setLevelsArray(int[] levels);

  abstract void setLevelsArrayAt(int index, int value);

  abstract void setLevelsArrayAtPlusEq(int index, int plusEq);

  abstract void setLevelsArrayAtMinusEq(int index, int minusEq);

  abstract boolean isLevelZeroSorted();

  abstract void setLevelZeroSorted(boolean sorted);

  boolean isCompatible() {
    return compatible;
  }

  abstract void setN(long n);

  abstract void incN();

  static int getSerializedSizeBytes(final int numLevels, final int numRetained, final SketchType sketchType,
      final boolean updatable) {
    int levelsBytes = 0;
    if (!updatable) {
      if (numRetained == 0) { return N_LONG_ADR; }
      if (numRetained == 1) {
        return DATA_START_ADR_SINGLE_ITEM + (sketchType == DOUBLE_SKETCH ? Double.BYTES : Float.BYTES);
      }
      levelsBytes = numLevels * Integer.BYTES;
    } else {
      levelsBytes = (numLevels + 1) * Integer.BYTES;
    }
    if (sketchType == DOUBLE_SKETCH) {
      return DATA_START_ADR_DOUBLE + levelsBytes + (numRetained + 2) * Double.BYTES; //+2 is for min & max
    } else {
      return DATA_START_ADR_FLOAT + levelsBytes + (numRetained + 2) * Float.BYTES;
    }
  }

  //Public Methods

  /**
   * Returns upper bound on the compact serialized size of a sketch given a parameter <em>k</em> and stream
   * length. This method can be used if allocation of storage is necessary beforehand.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @return upper bound on the compact serialized size
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n) {
    final LevelStats lvlStats = getAllLevelStatsGivenN(k, M, n, false, false, sketchType);
    return lvlStats.getCompactBytes();
  }

  /**
   * Returns the current compact number of bytes this sketch would require to store.
   * @return the current compact number of bytes this sketch would require to store.
   */
  public int getCurrentCompactSerializedSizeBytes() {
    return KllSketch.getSerializedSizeBytes(getNumLevels(), getNumRetained(), sketchType, false);
  }

  /**
   * Returns the current updatable number of bytes this sketch would require to store.
   * @return the current updatable number of bytes this sketch would require to store.
   */
  public int getCurrentUpdatableSerializedSizeBytes() {
    final int itemCap = KllHelper.computeTotalItemCapacity(k, M, getNumLevels());
    return KllSketch.getSerializedSizeBytes(getNumLevels(), itemCap, sketchType, true);
  }

  /**
   * Returns the number of bytes this sketch would require to store.
   * @return the number of bytes this sketch would require to store.
   * @deprecated use <i>getCurrentCompactSerializedSizeBytes()</i>
   */
  @Deprecated
  public int getSerializedSizeBytes() {
    return getCurrentCompactSerializedSizeBytes();
  }


  /**
   * Returns the parameter k
   * @return parameter k
   */
  public int getK() {
    return k;
  }

  /**
   * Returns the length of the input stream.
   * @return stream length
   */
  public abstract long getN();

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
    return KllHelper.getNormalizedRankError(getDyMinK(), pmf);
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
    return getLevelsArrayAt(getNumLevels()) - getLevelsArrayAt(0);
  }

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  public boolean isEmpty() {
    return getN() == 0;
  }

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  public boolean isEstimationMode() {
    return getNumLevels() > 1;
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
