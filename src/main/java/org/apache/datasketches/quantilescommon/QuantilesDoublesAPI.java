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
 * The Quantiles API for item type <i>double</i>.
 * @see QuantilesAPI
 *
 * @author Lee Rhodes
 */
public interface QuantilesDoublesAPI extends QuantilesAPI {

  /**
   * This is equivalent to {@link #getCDF(double[], QuantileSearchCriteria) getCDF(splitPoints, INCLUSIVE)}
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing items.
   * @return a discrete CDF array of m+1 double ranks (or cumulative probabilities) on the interval [0.0, 1.0].
   */
  default double[] getCDF(double[] splitPoints) {
    return getCDF(splitPoints, INCLUSIVE);
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF) of the input stream
   * as a monotonically increasing array of double ranks (or cumulative probabilities) on the interval [0.0, 1.0],
   * given a set of splitPoints.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(false) function.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing items
   * (of the same type as the input items)
   * that divide the item input domain into <i>m+1</i> overlapping intervals.
   *
   * <p>The start of each interval is below the lowest item retained by the sketch
   * corresponding to a zero rank or zero probability, and the end of the interval
   * is the rank or cumulative probability corresponding to the split point.</p>
   *
   * <p>The <i>(m+1)th</i> interval represents 100% of the distribution represented by the sketch
   * and consistent with the definition of a cumulative probability distribution, thus the <i>(m+1)th</i>
   * rank or probability in the returned array is always 1.0.</p>
   *
   * <p>If a split point exactly equals a retained item of the sketch and the search criterion is:</p>
   *
   * <ul>
   * <li>INCLUSIVE, the resulting cumulative probability will include that item.</li>
   * <li>EXCLUSIVE, the resulting cumulative probability will not include the weight of that split point.</li>
   * </ul>
   *
   * <p>It is not recommended to include either the minimum or maximum items of the input stream.</p>
   *
   * @param searchCrit the desired search criteria.
   * @return a discrete CDF array of m+1 double ranks (or cumulative probabilities) on the interval [0.0, 1.0].
   */
  double[] getCDF(double[] splitPoints, QuantileSearchCriteria searchCrit);

  /**
   * Returns the maximum item of the stream. This is provided for convenience, but may be different from the largest
   * item retained by the sketch algorithm.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @return the maximum item of the stream
   */
  double getMaxItem();

  /**
   * Returns the minimum item of the stream. This is provided for convenience, but is distinct from the smallest
   * item retained by the sketch algorithm.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @return the minimum item of the stream
   */
  double getMinItem();

  /**
   * This is equivalent to {@link #getPMF(double[], QuantileSearchCriteria) getPMF(splitPoints, INCLUSIVE)}
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing items.
   * @return a PMF array of m+1 probability masses as doubles on the interval [0.0, 1.0].
   */
  default double[] getPMF(double[] splitPoints) {
    return getPMF(splitPoints, INCLUSIVE);
  }

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * as an array of probability masses as doubles on the interval [0.0, 1.0],
   * given a set of splitPoints.
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(true) function.</p>
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing items
   * (of the same type as the input items)
   * that divide the item input domain into <i>m+1</i> consecutive, non-overlapping intervals.
   *
   * <p>Each interval except for the end intervals starts with a split point and ends with the next split
   * point in sequence.</p>
   *
   * <p>The first interval starts below the lowest item retained by the sketch
   * corresponding to a zero rank or zero probability, and ends with the first split point</p>
   *
   * <p>The last <i>(m+1)th</i> interval starts with the last split point and ends after the last
   * item retained by the sketch corresponding to a rank or probability of 1.0. </p>
   *
   * <p>The sum of the probability masses of all <i>(m+1)</i> intervals is 1.0.</p>
   *
   * <p>If the search criterion is:</p>
   *
   * <ul>
   * <li>INCLUSIVE, and the upper split point of an interval equals an item retained by the sketch, the interval
   * will include that item. If the lower split point equals an item retained by the sketch, the interval will exclude
   * that item.</li>
   * <li>EXCLUSIVE, and the upper split point of an interval equals an item retained by the sketch, the interval
   * will exclude that item. If the lower split point equals an item retained by the sketch, the interval will include
   * that item.</li>
   * </ul>
   *
   * <p>It is not recommended to include either the minimum or maximum items of the input stream.</p>
   *
   * @param searchCrit the desired search criteria.
   * @return a PMF array of m+1 probability masses as doubles on the interval [0.0, 1.0].
   */
  double[] getPMF(double[] splitPoints, QuantileSearchCriteria searchCrit);

  /**
   * This is equivalent to {@link #getQuantile(double, QuantileSearchCriteria) getQuantile(rank, INCLUSIVE)}
   * @param rank the given normalized rank, a double in the range [0.0, 1.0].
   * @return the approximate quantile given the normalized rank.
   */
  default double getQuantile(double rank) {
    return getQuantile(rank, INCLUSIVE);
  }

  /**
   * Gets the approximate quantile of the given normalized rank and the given search criterion.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @param rank the given normalized rank, a double in the range [0.0, 1.0].
   * @param searchCrit If INCLUSIVE, the given rank includes all quantiles &le;
   * the quantile directly corresponding to the given rank.
   * If EXCLUSIVE, he given rank includes all quantiles &lt;
   * the quantile directly corresponding to the given rank.
   * @return the approximate quantile given the normalized rank.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  double getQuantile(double rank, QuantileSearchCriteria searchCrit);

  /**
   * Gets the lower bound of the quantile confidence interval in which the quantile of the
   * given rank exists.
   *
   * <p>If the sketch is empty this returns NaN.</p>
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
  double getQuantileLowerBound(double rank);

  /**
   * Gets the upper bound of the quantile confidence interval in which the true quantile of the
   * given rank exists.
   *
   * <p>If the sketch is empty this returns NaN.</p>
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
  double getQuantileUpperBound(double rank);

  /**
   * This is equivalent to {@link #getQuantiles(double[], QuantileSearchCriteria) getQuantiles(ranks, INCLUSIVE)}
   * @param ranks the given array of normalized ranks, each of which must be
   * in the interval [0.0,1.0].
   * @return an array of quantiles corresponding to the given array of normalized ranks.
   * @deprecated Use {@link #getQuantile(double, QuantileSearchCriteria)
   * getQuantile(rank, searchCrit) in a loop.}
   */
  @Deprecated
  default double[] getQuantiles(double[] ranks) {
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
   * @deprecated Use {@link #getQuantile(double, QuantileSearchCriteria)
   * getQuantile(rank, searchCrit) in a loop.}
   */
  @Deprecated
  double[] getQuantiles(double[] ranks, QuantileSearchCriteria searchCrit);

  /**
   * This is equivalent to {@link #getQuantiles(int, QuantileSearchCriteria) getQuantiles(numEvenlySpaced, INCLUSIVE)}
   * @param numEvenlySpaced number of evenly spaced normalied ranks
   * @return an array of quantiles that are evenly spaced by their ranks.
   * @deprecated Use {@link #getQuantile(double, QuantileSearchCriteria)
   * getQuantile(rank, searchCrit) in a loop.}
   */
  @Deprecated
  default double[] getQuantiles(int numEvenlySpaced) {
    return getQuantiles(numEvenlySpaced, INCLUSIVE);
  }

  /**
   * This is a version of getQuantiles() where the caller only specifies the number of of desired evenly spaced,
   * normalized ranks, and returns an array of the corresponding quantiles.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param numEvenlySpaced an integer that specifies the number of evenly spaced normalized ranks.
   * This must be a positive integer greater than 0.
   * <ul><li>Let <i>Smallest</i> and <i>Largest</i> be the smallest and largest quantiles
   * retained by the sketch algorithm, respectively.
   * (This should not to be confused with {@link #getMinItem} and {@link #getMaxItem},
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
   * @deprecated Use {@link #getQuantile(double, QuantileSearchCriteria)
   * getQuantile(rank, searchCrit) in a loop.}
   */
  @Deprecated
  double[] getQuantiles(int numEvenlySpaced, QuantileSearchCriteria searchCrit);

  /**
   * This is equivalent to {@link #getRank(double, QuantileSearchCriteria) getRank(quantile, INCLUSIVE)}
   * @param quantile the given quantile
   * @return the normalized rank corresponding to the given quantile
   */
  default double getRank(double quantile) {
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
  double getRank(double quantile, QuantileSearchCriteria searchCrit);

  /**
   * This is equivalent to {@link #getRanks(double[], QuantileSearchCriteria) getRanks(quantiles, INCLUSIVE)}
   * @param quantiles the given array of quantiles
   * @return an array of normalized ranks corresponding to the given array of quantiles.
   * @deprecated Use {@link #getRank(double, QuantileSearchCriteria)
   * getRank(quantile, searchCrit) in a loop.}
   */
  @Deprecated
  default double[] getRanks(double[] quantiles) {
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
   * @deprecated Use {@link #getRank(double, QuantileSearchCriteria)
   * getRank(quantile, searchCrit) in a loop.}
   */
  @Deprecated
  double[] getRanks(double[] quantiles, QuantileSearchCriteria searchCrit);

  /**
   * Returns the current number of bytes this Sketch would require if serialized.
   * @return the number of bytes this sketch would require if serialized.
   */
  int getSerializedSizeBytes();

  /**
   * Gets the sorted view of this sketch
   * @return the sorted view of this sketch
   */
  DoublesSortedView getSortedView();

  /**
   * Gets the iterator for this sketch, which is not sorted.
   * @return the iterator for this sketch
   */
  QuantilesDoublesSketchIterator iterator();

  /**
   * Returns a byte array representation of this sketch.
   * @return a byte array representation of this sketch.
   */
  byte[] toByteArray();

  /**
   * Updates this sketch with the given item.
   * @param item from a stream of quantiles. NaNs are ignored.
   */
  void update(double item);
}

