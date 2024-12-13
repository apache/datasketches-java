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

import static java.lang.Math.min;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

import java.util.Comparator;

/**
 * The Quantiles API for item type <i>generic</i>.
 * @see QuantilesAPI
 * @param <T> The given item type
 * @author Lee Rhodes
 */
public interface QuantilesGenericAPI<T> extends QuantilesAPI, PartitioningFeature<T>, SketchPartitionLimits {

  /**
   * This is equivalent to {@link #getCDF(Object[], QuantileSearchCriteria) getCDF(splitPoints, INCLUSIVE)}
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing items.
   * @return a discrete CDF array of m+1 double ranks (or cumulative probabilities) on the interval [0.0, 1.0].
   * @throws IllegalArgumentException if sketch is empty.
   */
  default double[] getCDF(T[] splitPoints) {
    return getCDF(splitPoints, INCLUSIVE);
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF) of the input stream
   * as a monotonically increasing array of double ranks (or cumulative probabilities) on the interval [0.0, 1.0],
   * given a set of splitPoints.
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(false) function.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing items
   * (of the same type as the input items)
   * that divide the item input domain into <i>m+1</i> overlapping intervals.
   * <blockquote>
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
   * </blockquote>
   * @param searchCrit the desired search criteria.
   * @return a discrete CDF array of m+1 double ranks (or cumulative probabilities) on the interval [0.0, 1.0].
   * @throws IllegalArgumentException if sketch is empty.
   */
  double[] getCDF(T[] splitPoints, QuantileSearchCriteria searchCrit);

  /**
   * @return the sketch item class
   */
  Class<T> getClassOfT();

  /**
   * Returns the Comparator of T
   * @return Comparator of the sketch
   */
  Comparator<? super T> getComparator();

  /**
   * Returns the maximum item of the stream. This may be distinct from the largest item retained by the
   * sketch algorithm.
   *
   * @return the maximum item of the stream
   * @throws IllegalArgumentException if sketch is empty.
   */
  T getMaxItem();

  @Override
  default int getMaxPartitions() {
    return (int) min(1.0 / getNormalizedRankError(true), getNumRetained() / 2.0);
  }

  /**
   * Returns the minimum item of the stream. This may be distinct from the smallest item retained by the
   * sketch algorithm.
   *
   * @return the minimum item of the stream
   * @throws IllegalArgumentException if sketch is empty.
   */
  T getMinItem();

  /**
   * This is equivalent to {@link #getPMF(Object[], QuantileSearchCriteria) getPMF(splitPoints, INCLUSIVE)}
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing items.
   * @return a PMF array of m+1 probability masses as doubles on the interval [0.0, 1.0].
   * @throws IllegalArgumentException if sketch is empty.
   */
  default double[] getPMF(T[] splitPoints) {
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
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing items
   * (of the same type as the input items)
   * that divide the item input domain into <i>m+1</i> consecutive, non-overlapping intervals.
   * <blockquote>
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
   * </blockquote>
   * @param searchCrit the desired search criteria.
   * @return a PMF array of m+1 probability masses as doubles on the interval [0.0, 1.0].
   * @throws IllegalArgumentException if sketch is empty.
   */
  double[] getPMF(T[] splitPoints, QuantileSearchCriteria searchCrit);

  /**
   * This is equivalent to {@link #getQuantile(double, QuantileSearchCriteria) getQuantile(rank, INCLUSIVE)}
   * @param rank the given normalized rank, a double in the range [0.0, 1.0].
   * @return the approximate quantile given the normalized rank.
   * @throws IllegalArgumentException if sketch is empty.
   */
  default T getQuantile(double rank) {
    return getQuantile(rank, INCLUSIVE);
  }

  /**
   * Gets the approximate quantile of the given normalized rank and the given search criterion.
   *
   * @param rank the given normalized rank, a double in the range [0.0, 1.0].
   * @param searchCrit If INCLUSIVE, the given rank includes all quantiles &le;
   * the quantile directly corresponding to the given rank.
   * If EXCLUSIVE, he given rank includes all quantiles &lt;
   * the quantile directly corresponding to the given rank.
   * @return the approximate quantile given the normalized rank.
   * @throws IllegalArgumentException if sketch is empty.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  T getQuantile(double rank, QuantileSearchCriteria searchCrit);

  /**
   * Gets the lower bound of the quantile confidence interval in which the quantile of the
   * given rank exists.
   *
   * <p>Although it is possible to estimate the probability that the true quantile
   * exists within the quantile confidence interval specified by the upper and lower quantile bounds,
   * it is not possible to guarantee the width of the quantile confidence interval
   * as an additive or multiplicative percent of the true quantile.</p>
   *
   * @param rank the given normalized rank
   * @return the lower bound of the quantile confidence interval in which the quantile of the
   * given rank exists.
   * @throws IllegalArgumentException if sketch is empty.
   */
  T getQuantileLowerBound(double rank);

  /**
   * Gets the upper bound of the quantile confidence interval in which the true quantile of the
   * given rank exists.
   *
   * <p>Although it is possible to estimate the probability that the true quantile
   * exists within the quantile confidence interval specified by the upper and lower quantile bounds,
   * it is not possible to guarantee the width of the quantile interval
   * as an additive or multiplicative percent of the true quantile.</p>
   *
   * @param rank the given normalized rank
   * @return the upper bound of the quantile confidence interval in which the true quantile of the
   * given rank exists.
   * @throws IllegalArgumentException if sketch is empty.
   */
  T getQuantileUpperBound(double rank);

  /**
   * This is equivalent to {@link #getQuantiles(double[], QuantileSearchCriteria) getQuantiles(ranks, INCLUSIVE)}
   * @param ranks the given array of normalized ranks, each of which must be
   * in the interval [0.0,1.0].
   * @return an array of quantiles corresponding to the given array of normalized ranks.
   * @throws IllegalArgumentException if sketch is empty.
   */
  default T[] getQuantiles(double[] ranks) {
    return getQuantiles(ranks, INCLUSIVE);
  }

  /**
   * Gets an array of quantiles from the given array of normalized ranks.
   *
   * @param ranks the given array of normalized ranks, each of which must be
   * in the interval [0.0,1.0].
   * @param searchCrit if INCLUSIVE, the given ranks include all quantiles &le;
   * the quantile directly corresponding to each rank.
   * @return an array of quantiles corresponding to the given array of normalized ranks.
   * @throws IllegalArgumentException if sketch is empty.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  T[] getQuantiles(double[] ranks, QuantileSearchCriteria searchCrit);

  /**
   * This is equivalent to {@link #getRank(Object, QuantileSearchCriteria) getRank(T quantile, INCLUSIVE)}
   * @param quantile the given quantile
   * @return the normalized rank corresponding to the given quantile.
   * @throws IllegalArgumentException if sketch is empty.
   */
  default double getRank(T quantile) {
    return getRank(quantile, INCLUSIVE);
  }

  /**
   * Gets the normalized rank corresponding to the given a quantile.
   *
   * @param quantile the given quantile
   * @param searchCrit if INCLUSIVE the given quantile is included into the rank.
   * @return the normalized rank corresponding to the given quantile.
   * @throws IllegalArgumentException if sketch is empty.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  double getRank(T quantile, QuantileSearchCriteria searchCrit);

  /**
   * This is equivalent to {@link #getRanks(Object[], QuantileSearchCriteria) getRanks(quantiles, INCLUSIVE)}
   * @param quantiles the given array of quantiles
   * @return an array of normalized ranks corresponding to the given array of quantiles.
   * @throws IllegalArgumentException if sketch is empty.
   */
  default double[] getRanks(T[] quantiles) {
    return getRanks(quantiles, INCLUSIVE);
  }

  /**
   * Gets an array of normalized ranks corresponding to the given array of quantiles and the given
   * search criterion.
   *
   * @param quantiles the given array of quantiles
   * @param searchCrit if INCLUSIVE, the given quantiles include the rank directly corresponding to each quantile.
   * @return an array of normalized ranks corresponding to the given array of quantiles.
   * @throws IllegalArgumentException if sketch is empty.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  double[] getRanks(T[] quantiles, QuantileSearchCriteria searchCrit);

  /**
   * Gets the sorted view of this sketch
   * @return the sorted view of this sketch
   */
  GenericSortedView<T> getSortedView();

  /**
   * Gets the iterator for this sketch, which is not sorted.
   * @return the iterator for this sketch
   */
  QuantilesGenericSketchIterator<T> iterator();

  /**
   * Updates this sketch with the given item.
   * @param item from a stream of items. Nulls are ignored.
   */
  void update(T item);

}

