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
 * This abstract class provides a single place to define and document the common public API
 * for the ReqSketch.
 *
 * @author Lee Rhodes
 */
abstract class BaseReqSketch {

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of splitPoint (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that be obtained from the
   * getNormalizedRankError(false) function.
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
   * @return an array of m+1 double values, which are a consecutive approximation to the CDF
   * of the input stream given the splitPoints. The value at array position j of the returned
   * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
   * array.
   */
  public abstract double[] getCDF(final float[] splitPoints);

  /**
   * Gets the smallest value seen by this sketch
   * @return the smallest value seen by this sketch
   */
  public abstract float getMin();

  /**
   * Gets the largest value seen by this sketch
   * @return the largest value seen by this sketch
   */
  public abstract float getMax();

  /**
   * Gets the total number of items offered to the sketch.
   * @return the total number of items offered to the sketch.
   */
  public abstract long getN();

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of splitPoints (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that be obtained from the
   * getNormalizedRankError(true) function.
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
   * Gets the quantile of the largest normalized rank that is less-than the given normalized rank;
   * or, if lteq is true, this gets the quantile of the largest normalized rank that is less-than or
   * equal to the given normalized rank.
   * The normalized rank must be in the range [0.0, 1.0] (inclusive, inclusive).
   * A given normalized rank of 0.0 will return the minimum value from the stream.
   * A given normalized rank of 1.0 will return the maximum value from the stream.
   * @param normRank the given normalized rank
   * @return the largest quantile less than the given normalized rank.
   */
  public abstract float getQuantile(final float normRank);

  /**
   * Gets an array of quantiles that correspond to the given array of normalized ranks.
   * @param normRanks the given array of normalized ranks.
   * @return the array of quantiles that correspond to the given array of normalized ranks.
   * @see #getQuantile(float)
   */
  public abstract float[] getQuantiles(final float[] normRanks);

  /**
   * Computes the normalized rank of the given value in the stream.
   * The normalized rank is the fraction of values less than the given value;
   * or if lteq is true, the fraction of values less than or equal to the given value.
   * @param value the given value
   * @return the normalized rank of the given value in the stream.
   */
  public abstract float getRank(final float value);

  /**
   * Gets an array of normalized ranks that correspond to the given array of values.
   * @param values the given array of values.
   * @return the  array of normalized ranks that correspond to the given array of values.
   * @see #getRank(float)
   */
  public abstract float[] getRanks(final float[] values);

  /**
   * Gets the number of retained entries of this sketch
   * @return the number of retained entries of this sketch
   */
  public abstract int getRetainedEntries();

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
   * Returns a summary of the sketch and the horizontal lists for all compactors.
   * @param fmt The format for each printed item.
   * @param dataDetail show all the retained data from all the compactors.
   * @return a summary of the sketch and the horizontal lists for all compactors.
   */
  public abstract String toString(final String fmt, final boolean dataDetail);

  /**
   * Updates this sketch with the given item.
   * @param item the given item
   */
  public abstract void update(final float item);
}
