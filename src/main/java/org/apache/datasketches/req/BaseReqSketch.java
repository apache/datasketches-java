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

/**
 * This abstract class provides a single place to define and document the public API
 * for the Relative Error Quantiles Sketch.
 *
 * @author Lee Rhodes
 */
abstract class BaseReqSketch {

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
   * the maximum value.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @return an array of m+1 double values, which are a consecutive approximation to the CDF
   * of the input stream given the splitPoints. The value at array position j of the returned
   * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
   * array.
   */
  public abstract double[] getCDF(final float[] splitPoints);

  /**
   * If true, the high ranks are prioritized for better accuracy. Otherwise
   * the low ranks are prioritized for better accuracy.  This state is chosen during sketch
   * construction.
   * @return the high ranks accuracy state.
   */
  public abstract boolean getHighRankAccuracy();

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
   * @param totalN an estimate of the total number of items submitted to the sketch.
   * @return an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]).
   */
  public abstract double getRSE(int k, double rank, boolean hra, long totalN);

  /**
   * Gets the total number of items offered to the sketch.
   * @return the total number of items offered to the sketch.
   */
  public abstract long getN();

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
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream values (the mass) that fall into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint, with the exception that the last interval will include maximum value.
   */
  public abstract double[] getPMF(final float[] splitPoints);

  /**
   * Gets the approximate quantile of the given normalized rank based on the lteq criterion.
   * The normalized rank must be in the range [0.0, 1.0] (inclusive, inclusive).
   * @param normRank the given normalized rank
   * @return the approximate quantile given the normalized rank.
   */
  public abstract float getQuantile(final double normRank);

  /**
   * Gets an array of quantiles that correspond to the given array of normalized ranks.
   * @param normRanks the given array of normalized ranks.
   * @return the array of quantiles that correspond to the given array of normalized ranks.
   * See <i>getQuantile(double)</i>
   */
  public abstract float[] getQuantiles(final double[] normRanks);

  /**
   * Computes the normalized rank of the given value in the stream.
   * The normalized rank is the fraction of values less than the given value;
   * or if lteq is true, the fraction of values less than or equal to the given value.
   * @param value the given value
   * @return the normalized rank of the given value in the stream.
   */
  public abstract double getRank(final float value);

  /**
   * returns an approximate lower bound rank of the given noramalized rank.
   * @param rank the given rank, a value between 0 and 1.0.
   * @param numStdDev the number of standard deviations. Must be 1, 2, or 3.
   * @return an approximate lower bound rank.
   */
  public abstract double getRankLowerBound(double rank, int numStdDev);

  /**
   * Gets an array of normalized ranks that correspond to the given array of values.
   * @param values the given array of values.
   * @return the  array of normalized ranks that correspond to the given array of values.
   * See <i>getRank(float)</i>
   */
  public abstract double[] getRanks(final float[] values);

  /**
   * Returns an approximate upper bound rank of the given rank.
   * @param rank the given rank, a value between 0 and 1.0.
   * @param numStdDev the number of standard deviations. Must be 1, 2, or 3.
   * @return an approximate upper bound rank.
   */
  public abstract double getRankUpperBound(double rank, int numStdDev);

  /**
   * Gets the number of retained items of this sketch
   * @return the number of retained entries of this sketch
   */
  public abstract int getRetainedItems();

  /**
   * Gets the number of bytes when serialized.
   * @return the number of bytes when serialized.
   */
  public abstract int getSerializationBytes();

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
   * Returns the current comparison criterion. If true the value comparison criterion is
   * &le;, otherwise it will be the default, which is &lt;.
   * @return the current comparison criterion
   */
  public abstract boolean isLessThanOrEqual();

  /**
   * Returns an iterator for all the items in this sketch.
   * @return an iterator for all the items in this sketch.
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
   * Sets the chosen criterion for value comparison
   *
   * @param ltEq (Less-than-or Equals) If true, the sketch will use the &le; criterion for comparing
   * values.  Otherwise, the criterion is strictly &lt;, the default.
   * This can be set anytime prior to a <i>getRank(float)</i> or <i>getQuantile(double)</i> or
   * equivalent query.
   * @return this
   */
  public abstract ReqSketch setLessThanOrEqual(final boolean ltEq);

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
   * Updates this sketch with the given item.
   * @param item the given item
   */
  public abstract void update(final float item);

  /**
   * A detailed, human readable view of the sketch compactors and their data.
   * Each compactor string is prepended by the compactor lgWeight, the current number of retained
   * items of the compactor and the current nominal capacity of the compactor.
   * @param fmt the format string for the data items; example: "%4.0f".
   * @param allData all the retained items for the sketch will be output by
   * compactory level.  Otherwise, just a summary will be output.
   * @return a detailed view of the compactors and their data
   */
  public abstract String viewCompactorDetail(String fmt, boolean allData);

}
