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

/**
 * The Sorted View for quantiles of generic type.
 *
 * @param <T> The generic quantile type.
 * @see SortedView
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public interface GenericSortedView<T> extends SortedView {

  /**
   * Gets the quantile based on the given normalized rank, and the given search criterion.
   * @param normalizedRank the given normalized rank, which must be in the range [0.0, 1.0].
   * @param searchCrit the given search criterion to use.
   * @return the associated quantile.
   */
  T getQuantile(double normalizedRank, QuantileSearchCriteria searchCrit);

  /**
   * Gets the normalized rank based on the given generic quantile.
   * @param quantile the given quantile.
   * @param searchCrit the given search criterion to use.
   * @return the normalized rank, which is a number in the range [0.0, 1.0].
   */
  double getRank(T quantile, QuantileSearchCriteria searchCrit);

  /**
   * Returns an array of ranks in the range [0.0, 1.0].
   * The size of this array is one larger than the size of the input splitPoints array because it will always include
   * 1.0 at the top.
   *
   * <p>The points in the returned array are monotonically increasing and end with 1.0.
   * Each point represents a cumulative probability or cumulative fractional density along a cumulative distribution
   * function (CDF) that approximates the CDF of the input data stream. For example, if one of the returned points is
   * 0.5, then the splitPoint corresponding to that point would be the median of the distribution and its center
   * of mass.</p>
   *
   * @param splitPoints the given array of quantiles or splitPoints. This is a sorted, monotonic array of unique
   * quantiles in the range of (minQuantile, maxQuantile). This array does not need to include either the minQuantile
   * or the maxQuantile. The returned array will have one extra interval representing the very top of the distribution.
   * @param searchCrit if INCLUSIVE, each interval within the distribution will include its top quantile and exclude its
   * bottom quantile. Otherwise, it will be the reverse.  The only exception is that the top portion will always include
   * the top quantile retained by the sketch.
   * @return an array of points that correspond to the given splitPoints, and represents the input data distribution
   * as a CDF.
   */
  double[] getCDF(T[] splitPoints, QuantileSearchCriteria searchCrit);

  /**
   * Returns an array of doubles where each double is in the range [0.0, 1.0].
   * The size of this array is one larger than the size of the input splitPoints array.
   *
   * <p>The points in the returned array are not monotonic and represent the discrete derivative of the CDF,
   * which is also called the Probability Mass Function (PMF). Each returned point represents the fractional
   * area of the total distribution which lies between the previous point (or zero) and the given point, which
   * corresponds to the given splitPoint.</p>
   *
   * @param splitPoints the given array of quantiles or splitPoints. This is a sorted, monotonic array of unique
   * quantiles in the range of (minQuantile, maxQuantile). This array does not need to include either the minQuantile
   * or the maxQuantile. The returned array will have one extra interval representing the very top of the distribution.
   * @param searchCrit if INCLUSIVE, each interval within the distribution will include its top quantile and exclude its
   * bottom quantile. Otherwise, it will be the reverse.  The only exception is that the top portion will always include
   * the top quantile retained by the sketch.
   * @return an array of points that correspond to the given splitPoints, and represents the input data distribution
   * as a PMF.
   */
  double[] getPMF(T[] splitPoints,  QuantileSearchCriteria searchCrit);

  /**
   * Returns the array of quantiles.
   * @return the array of quantiles.
   */
  T[] getQuantiles();

  @Override
  GenericSortedViewIterator<T> iterator();

}

