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

package org.apache.datasketches.quantilescommon;

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

/**
 * The API for item type <i>float</i>.
 * @see QuantilesAPI
 * @author Lee Rhodes
 */
public interface QuantilesFloatsAPI extends QuantilesAPI {

  /**
   * Same as {@link #getCDF(float[], QuantileSearchCriteria) getCDF(splitPoints, INCLUSIVE)}
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing quantiles
   * @return a CDF array of m+1 double ranks (or probabilities) on the interval [0.0, 1.0).
   */
  default double[] getCDF(float[] splitPoints) {
    return getCDF(splitPoints, INCLUSIVE);
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF) of the input stream
   * as a monotonically increasing array of double ranks (or probabilities) on the interval [0.0, 1.0],
   * given a set of splitPoints.
   * The last rank in the returned array is always 1.0.
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(false) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing quantiles
   * that divide the real number line into <i>m+1</i> consecutive non-overlapping intervals.
   * It is not necessary to include either the minimum or maximum quantiles
   * of the input stream in these split points.
   *
   * <p>If searchCrit is INCLUSIVE, the definition of an "interval" is
   * exclusive of the left splitPoint and
   * inclusive of the right splitPoint.</p>
   *
   * <p>If searchCrit is EXCLUSIVE, the definition of an "interval" is
   * inclusive of the left splitPoint and
   * exclusive of the right splitPoint.</p>
   *
   *<p>The left "splitPoint" for the lowest interval is the
   * lowest quantile from the input stream retained by the sketch.
   * The right "splitPoint" for the highest interval is the
   * highest quantile from the input stream retained by the sketch.
   * </p>
   *
   * @param searchCrit if INCLUSIVE, the weight of a given splitPoint quantile,
   * if it also exists as retained quantile by the sketch,
   * is included into the interval below.
   * Otherwise, it is included into the interval above.
   *
   * @return an CDF array of m+1 double ranks (or probabilities) on the interval [0.0, 1.0).
   */
  double[] getCDF(float[] splitPoints, QuantileSearchCriteria searchCrit);

  /**
   * Returns the maximum quantile of the stream. This is provided for convenience, but is distinct from the largest
   * quantile retained by the sketch algorithm.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @return the maximum quantile of the stream
   */
  float getMaxQuantile();

  /**
   * Returns the minimum quantile of the stream. This is provided for convenience, but is distinct from the smallest
   * quantile retained by the sketch algorithm.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @return the minimum quantile of the stream
   */
  float getMinQuantile();

  /**
   * Same as {@link #getPMF(float[], QuantileSearchCriteria) getPMF(splitPoints, INCLUSIVE)}
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing quantiles
   * @return a PDF array of m+1 densities as doubles on the interval [0.0, 1.0).
   */
  default double[] getPMF(float[] splitPoints) {
    return getPMF(splitPoints, INCLUSIVE);
  }

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * as an array of densities as doubles on the interval [0.0, 1.0],
   * given a set of splitPoints.
   * The sum of the densities in the returned array is always 1.0.
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(true) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing quantiles
   * that divide the real number line into <i>m+1</i> consecutive non-overlapping intervals.
   * It is not necessary to include either the minimum or maximum quantiles
   * of the input stream in these split points.
   *
   * <p>If searchCrit is INCLUSIVE, the definition of an "interval" is
   * exclusive of the left splitPoint and
   * inclusive of the right splitPoint.</p>
   *
   * <p>If searchCrit is EXCLUSIVE, the definition of an "interval" is
   * inclusive of the left splitPoint and
   * exclusive of the right splitPoint.</p>
   *
   *<p>The left "splitPoint" for the lowest interval is the
   * lowest quantile from the input stream retained by the sketch.
   * The right "splitPoint" for the highest interval is the
   * highest quantile from the input stream retained by the sketch.
   * </p>
   *
   * @param searchCrit if INCLUSIVE, the weight of a given splitPoint quantile,
   * if it also exists as retained quantile by the sketch,
   * is included into the interval below.
   * Otherwise, it is included into the interval above.
   *
   * @return a PDF array of m+1 densities as doubles on the interval [0.0, 1.0).
   */
  double[] getPMF(float[] splitPoints, QuantileSearchCriteria searchCrit);

  /**
   * Same as {@link #getQuantile(double, QuantileSearchCriteria) getQuantile(rank, INCLUSIVE)}
   * @param rank the given normalized rank, a double in the range [0.0, 1.0].
   * @return the approximate quantile given the normalized rank.
   */
  default float getQuantile(double rank) {
    return getQuantile(rank, INCLUSIVE);
  }

  /**
   * Gets the approximate quantile of the given normalized rank and the given search criterion.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @param rank the given normalized rank, a double in the range [0.0, 1.0].
   * @param searchCrit is INCLUSIVE, the given rank includes all quantiles &le;
   * the quantile directly corresponding to the given rank.
   * @return the approximate quantile given the normalized rank.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  float getQuantile(double rank, QuantileSearchCriteria searchCrit);

  /**
   * Gets the lower bound of the quantile confidence interval in which the quantile of the
   * given rank exists.
   *
   * <p>Although it is possible to estimate the probablity that the true quantile
   * exists within the quantile confidence interval specified by the upper and lower quantile bounds,
   * it is not possible to guarantee the width of the quantile confidence interval
   * as an additive or multiplicative percent of the true quantile.</p>
   *
   * @param rank the given normalized rank
   * @return the lower bound of the quantile confidence interval in which the quantile of the
   * given rank exists.
   */
  float getQuantileLowerBound(double rank);

  /**
   * Gets the upper bound of the quantile confidence interval in which the true quantile of the
   * given rank exists.
   *
   * <p>Although it is possible to estimate the probablity that the true quantile
   * exists within the quantile confidence interval specified by the upper and lower quantile bounds,
   * it is not possible to guarantee the width of the quantile interval
   * as an additive or multiplicative percent of the true quantile.</p>
   *
   * @param rank the given normalized rank
   * @return the upper bound of the quantile confidence interval in which the true quantile of the
   * given rank exists.
   */
  float getQuantileUpperBound(double rank);

  /**
   * Same as {@link #getQuantiles(double[], QuantileSearchCriteria) getQuantiles(ranks, INCLUSIVE)}
   * @param ranks the given array of normalized ranks, each of which must be
   * in the interval [0.0,1.0].
   * @return an array of quantiles corresponding to the given array of normalized ranks.
   */
  default float[] getQuantiles(double[] ranks) {
    return getQuantiles(ranks, INCLUSIVE);
  }

  /**
   * Gets an array of quantiles from the given array of normalized ranks.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param ranks the given array of normalized ranks, each of which must be
   * in the interval [0.0,1.0].
   * @param searchCrit if INCLUSIVE, the given ranks include all quantiles &le;
   * the quantile directly corresponding to each rank.
   * @return an array of quantiles corresponding to the given array of normalized ranks.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  float[] getQuantiles(double[] ranks, QuantileSearchCriteria searchCrit);

  /**
   * Same as {@link #getQuantiles(int, QuantileSearchCriteria) getQuantiles(numEvenlySpaced, INCLUSIVE)}
   * @param numEvenlySpaced number of evenly spaced normalied ranks
   * @return an array of quantiles that are evenly spaced by their ranks.
   */
  default float[] getQuantiles(int numEvenlySpaced) {
    return getQuantiles(numEvenlySpaced, INCLUSIVE);
  }

  /**
   * This is a version of getQuantiles() where the caller only specifies the number of of desired evenly spaced,
   * normalized ranks, and returns an array of the corresponding quantiles.
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param numEvenlySpaced an integer that specifies the number of evenly spaced normalized ranks.
   * This must be a positive integer greater than 0.
   * <ul><li>Let <i>Smallest</i> and <i>Largest</i> be the smallest and largest quantiles
   * retained by the sketch algorithm, respectively.
   * (This should not to be confused with {@link #getMinQuantile} and {@link #getMaxQuantile},
   * which are the smallest and largest quantiles of the stream.)</li>
   * <li>A 1 will return the Smallest quantile.</li>
   * <li>A 2 will return the Smallest and Largest quantiles.</li>
   * <li>A 3 will return the Smallest, the Median, and the Largest quantiles.</li>
   * <li>Etc.</li>
   * </ul>
   *
   * @param searchCrit if INCLUSIVE, the given ranks include all quantiles &le; the quantile directly corresponding to
   * each rank.
   * @return an array of quantiles that are evenly spaced by their ranks.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  float[] getQuantiles(int numEvenlySpaced, QuantileSearchCriteria searchCrit);

  /**
   * Same as {@link #getRank(float, QuantileSearchCriteria) getRank(quantile, INCLUSIVE)}
   * @param quantile the given quantile
   * @return the normalized rank corresponding to the given quantile
   */
  default double getRank(float quantile) {
    return getRank(quantile, INCLUSIVE);
  }

  /**
   * Gets the normalized rank corresponding to the given a quantile.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @param quantile the given quantile
   * @param searchCrit if INCLUSIVE the given quantile is included into the rank.
   * @return the normalized rank corresponding to the given quantile
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  double getRank(float quantile, QuantileSearchCriteria searchCrit);

  /**
   * Same as {@link #getRanks(float[], QuantileSearchCriteria) getRanks(quantiles, INCLUSIVE)}
   * @param quantiles the given array of quantiles
   * @return an array of normalized ranks corresponding to the given array of quantiles.
   */
  default double[] getRanks(float[] quantiles) {
    return getRanks(quantiles, INCLUSIVE);
  }

  /**
   * Gets an array of normalized ranks corresponding to the given array of quantiles and the given
   * search criterion.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param quantiles the given array of quantiles
   * @param searchCrit if INCLUSIVE, the given quantiles include the rank directly corresponding to each quantile.
   * @return an array of normalized ranks corresponding to the given array of quantiles.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  double[] getRanks(float[] quantiles, QuantileSearchCriteria searchCrit);

  /**
   * Returns the current number of bytes this Sketch would require if serialized.
   * @return the number of bytes this sketch would require if serialized.
   */
  int getSerializedSizeBytes();

  /**
   * Gets the sorted view of this sketch
   * @return the sorted view of this sketch
   */
  FloatsSortedView getSortedView();

  /**
   * Gets the iterator for this sketch, which is not sorted.
   * @return the iterator for this sketch
   */
  QuantilesFloatsSketchIterator iterator();

  /**
   * Returns a byte array representation of this sketch.
   * @return a byte array representation of this sketch.
   */
  byte[] toByteArray();

  /**
   * Updates this sketch with the given quantile.
   * @param quantile from a stream of quantiles. NaNs are ignored.
   */
  void update(float quantile);
}

