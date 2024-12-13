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

import static org.apache.datasketches.quantilescommon.QuantilesAPI.EMPTY_MSG;

import java.util.Comparator;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * The Sorted View for quantiles of generic type.
 * @param <T> The generic quantile type.
 * @see SortedView
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public interface GenericSortedView<T>  extends PartitioningFeature<T>, SketchPartitionLimits, SortedView  {

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
  default double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    GenericSortedView.validateItems(splitPoints, getComparator());
    final int len = splitPoints.length + 1;
    final double[] buckets = new double[len];
    for (int i = 0; i < len - 1; i++) {
      buckets[i] = getRank(splitPoints[i], searchCrit);
    }
    buckets[len - 1] = 1.0;
    return buckets;
  }

  /**
   * Gets the Comparator for this generic type.
   * @return the Comparator for this generic type.
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

  /**
   * Returns the minimum item of the stream. This may be distinct from the smallest item retained by the
   * sketch algorithm.
   *
   * @return the minimum item of the stream
   * @throws IllegalArgumentException if sketch is empty.
   */
  T getMinItem();

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
  default double[] getPMF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    GenericSortedView.validateItems(splitPoints, getComparator());
    final double[] buckets = getCDF(splitPoints, searchCrit);
    for (int i = buckets.length; i-- > 1; ) {
      buckets[i] -= buckets[i - 1];
    }
    return buckets;
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
   * Returns the full array of quantiles.
   * @return the full array of quantiles.
   */
  T[] getQuantiles();

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

  @Override
  GenericSortedViewIterator<T> iterator();

  /**
   * Checks the sequential validity of the given array of generic items.
   * They must be unique, monotonically increasing and not null.
   * @param <T> the data type
   * @param items given array of generic items
   * @param comparator the comparator for generic item data type T
   */
  static <T> void validateItems(final T[] items, final Comparator<? super T> comparator) {
    final int len = items.length;
    if (len == 1 && items[0] == null) {
      throw new SketchesArgumentException(
          "Items must be unique, monotonically increasing and not null.");
    }
    for (int j = 0; j < len - 1; j++) {
      if ((items[j] != null) && (items[j + 1] != null)
          && (comparator.compare(items[j], items[j + 1]) < 0)) {
        continue;
      }
      throw new SketchesArgumentException(
          "Items must be unique, monotonically increasing and not null.");
    }
  }

}
