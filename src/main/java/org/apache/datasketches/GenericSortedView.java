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

package org.apache.datasketches;

/**
 * The Sorted View for generic items.
 * @param <T> The generic quantile item type
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public interface GenericSortedView<T> extends SortedView {

  /**
   * Gets the quantile based on the given normalized rank, and the given search criterion.
   * @param normalizedRank the given normalized rank, which must be in the range [0.0, 1.0].
   * @param searchCrit the given search criterion to use.
   * @return the associated quantile item.
   */
  T getQuantile(double normalizedRank, QuantileSearchCriteria searchCrit);

  /**
   * Gets the normalized rank based on the given quantile item.
   * @param item the given quantile item
   * @param searchCrit the given search criterion to use.
   * @return the normalized rank, which is a number in the range [0.0, 1.0].
   */
  double getRank(T item, QuantileSearchCriteria searchCrit);

  /**
   * Returns an array of values where each value is a number in the range [0.0, 1.0].
   * The size of this array is one larger than the size of the input splitPoints array.
   *
   * <p>The points in the returned array are monotonically increasing and end with the
   * value 1.0. Each value represents a point along the cumulative distribution function that approximates
   * the CDF of the input data stream. Therefore, each point represents the fractional density of the distribution
   * from zero to the given point. For example, if one of the returned values is 0.5, then the splitPoint corresponding
   * to that value would be the median of the distribution.</p>
   *
   * @param splitPoints the given array of quantile values or splitPoints. This is a sorted, monotonic array of unique
   * values in the range of (minValue, maxValue). This array should not include either the minValue or the maxValue.
   * The returned array will have one extra interval representing the very top of the distribution.
   * @param searchCrit if INCLUSIVE, each interval within the distribution will include its top value and exclude its
   * bottom value. Otherwise, it will be the reverse.  The only exception is that the top portion will always include
   * the top value retained by the sketch.
   * @return an array of points that correspond to the given splitPoints, and represents the input data distribution
   * as a CDF.
   */
  double[] getCDF(T[] splitPoints, QuantileSearchCriteria searchCrit);

  /**
   * Returns an array of values where each value is a number in the range [0.0, 1.0].
   * The size of this array is one larger than the size of the input splitPoints array.
   *
   * <p>The points in the returned array are not monotonic and represent the discrete derivative of the CDF,
   * which is also called the Probability Mass Function (PMF). Each returned point represents the fractional
   * area of the total distribution which lies between the previous point (or zero) and the given point, which
   * corresponds to the given splitPoint.</p>
   *
   * @param splitPoints the given array of quantile values or splitPoints. This is a sorted, monotonic array of unique
   * values in the range of (minValue, maxValue). This array should not include either the minValue or the maxValue.
   * The returned array will have one extra interval representing the very top of the distribution.
   * @param searchCrit if INCLUSIVE, each interval within the distribution will include its top value and exclude its
   * bottom value. Otherwise, it will be the reverse.  The only exception is that the top portion will always include
   * the top value retained by the sketch.
   * @return an array of points that correspond to the given splitPoints, and represents the input data distribution
   * as a PMF.
   */
  double[] getPMF(T[] splitPoints,  QuantileSearchCriteria searchCrit);

  /**
   * Returns the array of items.
   * @return the array of items.
   */
  T[] getItems();

  @Override
  GenericSortedViewIterator<T> iterator();

}

