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

package org.apache.datasketches.req;

import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;
import org.apache.datasketches.QuantileSearchCriteria;

/**
 * This abstract class provides a single place to define and document the public API
 * for the Relative Error Quantiles Sketch.
 *
 * @see <a href="https://datasketches.apache.org/docs/Quantiles/SketchingQuantilesAndRanksTutorial.html">
 * Sketching Quantiles and Ranks Tutorial</a>
 *
 * @author Lee Rhodes
 */
abstract class BaseReqSketch {

  /**
   * Same as {@link #getCDF(float[], QuantileSearchCriteria) getCDF(float[] splitPoints, INCLUSIVE)}
   * @param splitPoints splitPoints
   * @return CDF
   */
  public double[] getCDF(float[] splitPoints) {
    return getCDF(splitPoints, INCLUSIVE);
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of splitPoint (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that be obtained, a priori,
   * from the <i>getRSE(int, double, boolean, long)</i> function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing double values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the largest value retained by the sketch.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @param searchCrit if INCLUSIVE, the weight of a given value is included into its rank.
   *
   * @return an array of m+1 double values, which are a consecutive approximation to the CDF
   * of the input stream given the splitPoints. The value at array position j of the returned
   * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
   * array.
   */
  public abstract double[] getCDF(float[] splitPoints, QuantileSearchCriteria searchCrit);

  /**
   * If true, the high ranks are prioritized for better accuracy. Otherwise
   * the low ranks are prioritized for better accuracy.  This state is chosen during sketch
   * construction.
   * @return the high ranks accuracy state.
   */
  public abstract boolean getHighRankAccuracyMode();

  /**
   * Returns the user configured parameter k
   * @return the user configured parameter k
   */
  public abstract int getK();

  /**
   * Gets the largest value seen by this sketch
   * @return the largest value seen by this sketch
   */
  public abstract float getMaxValue();

  /**
   * Gets the smallest value seen by this sketch
   * @return the smallest value seen by this sketch
   */
  public abstract float getMinValue();

  /**
   * Returns an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]).
   * Derived from Lemma 12 in https://arxiv.org/abs/2004.01668v2, but the constant factors were
   * modified based on empirical measurements.
   *
   * @param k the given value of k
   * @param rank the given normalized rank, a number in [0,1].
   * @param hra if true High Rank Accuracy mode is being selected, otherwise, Low Rank Accuracy.
   * @param totalN an estimate of the total number of values submitted to the sketch.
   * @return an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]).
   */
  public abstract double getRSE(int k, double rank, boolean hra, long totalN);

  /**
   * Gets the total number of values offered to the sketch.
   * @return the total number of values offered to the sketch.
   */
  public abstract long getN();

  /**
   * Same as {@link #getPMF(float[], QuantileSearchCriteria) getPMF(float[] splitPoints, INCLUSIVE)}
   * @param splitPoints splitPoints
   * @return PMF
   */
  public double[] getPMF(float[] splitPoints) {
    return getPMF(splitPoints, INCLUSIVE);
  }

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of splitPoints (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that be obtained, a priori,
   * from the <i>getRSE(int, double, boolean, long)</i> function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing double values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these splitpoints.
   *
   * @param searchCrit if INCLUSIVE the weight of a given value is included into its rank.
   *
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream values (the mass) that fall into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint, with the exception that the last interval will include the largest value retained by the sketch.
   */
  public abstract double[] getPMF(float[] splitPoints, QuantileSearchCriteria searchCrit);

  /**
   * Same as {@link #getQuantile(double, QuantileSearchCriteria) getQuantile(double fraction, INCLUSIVE)}
   * @param normRank fractional rank
   * @return quantile
   */
  public float getQuantile(double normRank) {
    return getQuantile(normRank, INCLUSIVE);
  }

  /**
   * Gets the approximate quantile of the given normalized rank based on the given criterion.
   * The normalized rank must be in the range [0.0, 1.0].
   * @param normRank the given normalized rank.
   * @param searchCrit is INCLUSIVE, the given rank includes all values &le; the value directly
   * corresponding to the given rank.
   * @return the approximate quantile given the normalized rank.
   */
  public abstract float getQuantile(double normRank, QuantileSearchCriteria searchCrit);

  /**
   * Same as {@link #getQuantiles(double[], QuantileSearchCriteria) getQuantiles(double[] fractions, INCLUSIVE)}
   * @param normRanks normalized ranks
   * @return quantiles
   */
  public float[] getQuantiles(double[] normRanks) {
    return getQuantiles(normRanks, INCLUSIVE);
  }

  /**
   * Gets an array of quantiles that correspond to the given array of normalized ranks.
   * @param normRanks the given array of normalized ranks.
   * @param searchCrit if INCLUSIVE, the given ranks are considered inclusive.
   * @return the array of quantiles that correspond to the given array of normalized ranks.
   * See <i>getQuantile(double)</i>
   */
  public abstract float[] getQuantiles(double[] normRanks, QuantileSearchCriteria searchCrit);

  /**
   * Same as {@link #getQuantiles(int, QuantileSearchCriteria) getQuantiles(numEvenlySpaced, INCLUSIVE)}
   * @param numEvenlySpaced number of evenly spaced normalied ranks
   * @return array of quantiles.
   * @see org.apache.datasketches.QuantileSearchCriteria QuantileSearchCriteria
   */
  public float[] getQuantiles(int numEvenlySpaced) {
    return getQuantiles(numEvenlySpaced, INCLUSIVE);
  }

  /**
   * This is a multiple-query version of getQuantile() and allows the caller to
   * specify the number of evenly spaced normalized ranks.
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param numEvenlySpaced an integer that specifies the number of evenly spaced normalized ranks.
   * This must be a positive integer greater than 0. A value of 1 will return the min value.
   * A value of 2 will return the min and the max value. A value of 3 will return the min,
   * the median and the max value, etc.
   *
   * @param searchCrit if INCLUSIVE, the given ranks include all values &le; the value directly corresponding to each rank.
   * @return array of quantiles.
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public float[] getQuantiles(int numEvenlySpaced, QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.Util.evenlySpaced(0.0, 1.0, numEvenlySpaced), searchCrit);
  }

  /**
   * Same as {@link #getRank(float, QuantileSearchCriteria) getRank(float value, INCLUSIVE)}
   * @param value value to be ranked
   * @return normalized rank
   */
  public double getRank(float value) {
    return getRank(value, INCLUSIVE);
  }

  /**
   * Computes the normalized rank of the given value in the stream.
   * The normalized rank is the fraction of values less than the given value;
   * or if searchCrit is INCLUSIVE, the fraction of values less than or equal to the given value.
   * @param value the given value.
   * @param searchCrit if INCLUSIVE the weight of the given value is included into its rank.
   * @return the normalized rank of the given value in the stream.
   */
  public abstract double getRank(float value, QuantileSearchCriteria searchCrit);

  /**
   * returns an approximate lower bound rank of the given normalized rank.
   * @param rank the given rank, a value between 0 and 1.0.
   * @param numStdDev the number of standard deviations. Must be 1, 2, or 3.
   * @return an approximate lower bound rank.
   */
  public abstract double getRankLowerBound(double rank, int numStdDev);

  /**
   * Same as {@link #getRanks(float[], QuantileSearchCriteria) getRanks(float[] values, INCLUSIVE)}
   * @param values the given array of values to be ranked
   * @return array of normalized ranks
   */
  public double[] getRanks(float[] values) {
    return getRanks(values, INCLUSIVE);
  }

  /**
   * Gets an array of normalized ranks that correspond to the given array of values.
   * @param values the given array of values.
   * @param searchCrit if INCLUSIVE the weight of the given value is included into its rank.
   * @return the array of normalized ranks that correspond to the given array of values.
   * See <i>getRank(float)</i>
   */
  public abstract double[] getRanks(float[] values, QuantileSearchCriteria searchCrit);

  /**
   * Returns an approximate upper bound rank of the given rank.
   * @param rank the given rank, a value between 0 and 1.0.
   * @param numStdDev the number of standard deviations. Must be 1, 2, or 3.
   * @return an approximate upper bound rank.
   */
  public abstract double getRankUpperBound(double rank, int numStdDev);

  /**
   * Returns the number of retained values (samples) in the sketch.
   * @return the number of retained values (samples) in the sketch
   */
  public abstract int getNumRetained();

  /**
   * Returns the current number of bytes this sketch would require to store in the updatable Memory Format.
   * This is the same as {@link #getSerializedSizeBytes }
   * @return the current number of bytes this sketch would require to store in the updatable Memory Format.
   */
  public int getCurrentUpdatableSerializedSizeBytes() {
    return getSerializedSizeBytes();
  }

  /**
   * Gets the number of bytes when serialized.
   * @return the number of bytes when serialized.
   */
  public abstract int getSerializedSizeBytes();

  /**
   * Gets the sorted view of the current state of this sketch
   * @return the sorted view of the current state of this sketch
   */
  public abstract ReqSketchSortedView getSortedView();

  /**
   * Returns true if this sketch's data structure is backed by Memory or WritableMemory.
   * @return true if this sketch's data structure is backed by Memory or WritableMemory.
   */
  public boolean hasMemory() {
    return false;
  }

  /**
   * Returns true if this sketch is off-heap (direct)
   * @return true if this sketch is off-heap (direct)
   */
  public boolean isDirect() {
    return false;
  }

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  public abstract boolean isEmpty();

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  public abstract boolean isEstimationMode();

  /**
   * Returns true if this sketch is read only.
   * @return true if this sketch is read only.
   */
  public boolean isReadOnly() {
    return false;
  }

  /**
   * Returns an iterator for all the values in this sketch.
   * @return an iterator for all the values in this sketch.
   */
  public abstract ReqIterator iterator();

  /**
   * Merge other sketch into this one. The other sketch is not modified.
   * @param other sketch to be merged into this one.
   * @return this
   */
  public abstract ReqSketch merge(final ReqSketch other);

  /**
   * Resets this sketch by removing all data and setting all data related variables to their
   * virgin state.
   * The parameters k, highRankAccuracy, reqDebug and LessThanOrEqual will not change.
   * @return this
   */
  public abstract ReqSketch reset();

  /**
   * Returns a byte array representation of this sketch.
   * @return a byte array representation of this sketch.
   */
  public abstract byte[] toByteArray();

  /**
   * Returns a summary of the key parameters of the sketch.
   * @return a summary of the key parameters of the sketch.
   */
  @Override
  public abstract String toString();

  /**
   * Updates this sketch with the given value.
   * @param value the given value
   */
  public abstract void update(final float value);

  /**
   * A detailed, human readable view of the sketch compactors and their data.
   * Each compactor string is prepended by the compactor lgWeight, the current number of retained
   * values of the compactor and the current nominal capacity of the compactor.
   * @param fmt the format string for the data values; example: "%4.0f".
   * @param allData all the retained values for the sketch will be output by
   * compactor level.  Otherwise, just a summary will be output.
   * @return a detailed view of the compactors and their data
   */
  public abstract String viewCompactorDetail(String fmt, boolean allData);

}
