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
 * <p>This is a stochastic streaming sketch that enables near-real time analysis of the
 * approximate distribution of items from a very large stream in a single pass, requiring only
 * that the items are comparable.
 * The analysis is obtained using the <i>getQuantile()</i> function or the
 * inverse functions getRank(), getPMF() (the Probability Mass Function), and getCDF()
 * (the Cumulative Distribution Function).</p>
 *
 * <p>Given an input stream of <i>N</i> items, the <i>natural rank</i> of any specific
 * item is defined as its index <i>(1 to N)</i> in the hypothetical sorted stream of all
 * <i>N</i> input items.</p>
 *
 * <p>The <i>normalized rank</i> (<i>rank</i>) of any specific item is defined as its
 * <i>natural rank</i> divided by <i>N</i>, which is a number in the interval [0.0, 1.0].
 * In the Javadocs for all the quantile sketches <i>natural rank</i> is seldom used
 * so any reference to just <i>rank</i> should be interpreted as <i>normalized rank</i>.</p>
 *
 * <p>Inputs into a quantile sketch are called "items" that can be either generic or specific
 * primitives, like <i>float</i> or <i>double</i> depending on the sketch implementation.
 * In order to keep its size small, sketches don't retain all the items offered and retain only
 * a small fraction of all the items, thus purging most of the items. The items retained are
 * then sorted and associated with a rank.  At this point we call the retained items <i>quantiles</i>.
 * Thus, all quantiles are items, but only a few items become quantiles. Depending on the context
 * the two terms can be interchangeable.</p>
 *
 * <p>All quantile sketches are configured with a parameter <i>k</i>, which affects the size of
 * the sketch and its estimation error.</p>
 *
 * <p>In the research literature, the estimation error is commonly called <i>epsilon</i>
 * (or <i>eps</i>) and is a fraction between zero and one.
 * Larger sizes of <i>k</i> result in a smaller epsilon, but also a larger sketch.
 * The epsilon error is always with respect to the rank domain. Estimating the confidence interval
 * in the quantile domain can be done by first computing the error in the rank domain and then
 * translating that to the quantile domain. The sketch provides methods to assist with that.</p>
 *
 * <p>The relationship between the normalized rank and the corresponding quantiles can be viewed
 * as a two dimensional monotonic plot with the normalized rank on one axis and the
 * corresponding quantiles on the other axis. Let <i>q := quantile</i> and <i>r := rank</i> then both
 * <i>q = getQuantile(r)</i> and <i>r = getRank(q)</i> are monotonically increasing functions.
 * If the y-axis is used for the rank domain and the x-axis for the quantile domain,
 * then <i>y = getRank(x)</i> is also the single point Cumulative Distribution Function (CDF).</p>
 *
 * <p>The functions <i>getQuantile()</i> translate ranks into corresponding quantiles.
 * The functions <i>getRank(), getCDF(), and getPMF() (Probability Mass Function)</i>
 * perform the opposite operation and translate quantiles into ranks (or cumulative probabilities,
 * or probability masses, depending on the context).</p>
 *
 * <p>As an example, consider a large stream of one million items such as packet sizes coming into a network node.
 * The absolute rank of any specific item size is simply its index in the hypothetical sorted
 * array of such items.
 * The normalized rank is the natural rank divided by the stream size, or <i>N</i>,
 * in this case one million.
 * The quantile corresponding to the normalized rank of 0.5 represents the 50th percentile or median
 * of the distribution, obtained from getQuantile(0.5). Similarly, the 95th percentile is obtained from
 * getQuantile(0.95).</p>
 *
 * <p>From the min and max quantiles, for example, say 1 and 1000 bytes,
 * you can obtain the PMF from getPMF(100, 500, 900) that will result in an array of
 * 4 probability masses such as {.4, .3, .2, .1}, which means that
 * <ul>
 * <li>40% of the mass was &lt; 100,</li>
 * <li>30% of the mass was &ge; 100 and &lt; 500,</li>
 * <li>20% of the mass was &ge; 500 and &lt; 900, and</li>
 * <li>10% of the mass was &ge; 900.</li>
 * </ul>
 * A frequency histogram can be obtained by simply multiplying these probability masses by getN(),
 * which is the total count of items received.
 * The <i>getCDF()</i> works similarly, but produces the cumulative distribution instead.
 *
 * <p>The accuracy of this sketch is a function of the configured <i>k</i>, which also affects
 * the overall size of the sketch. Accuracy of this quantile sketch is always with respect to
 * the normalized rank.
 *
 * <p>The <i>getPMF()</i> function has about 13 to 47% worse rank error (depending
 * on <i>k</i>) than the other queries because the mass of each "bin" of the PMF has
 * "double-sided" error from the upper and lower edges of the bin as a result of a subtraction
 * of random variables where the errors from the two edges can sometimes add.</p>
 *
 * <p>A <i>getQuantile(rank)</i> query has the following probabilistic guarantees:</p>
 * <ul>
 * <li>Let <i>q = getQuantile(r)</i> where <i>r</i> is the rank between zero and one.</li>
 * <li>The quantile <i>q</i> will be a quantile from the input stream.</li>
 * <li>Let <i>trueRank</i> be the true rank of <i>q</i> derived from the hypothetical sorted
 * stream of all <i>N</i> quantiles.</li>
 * <li>Let <i>eps = getNormalizedRankError(false)</i>[*].</li>
 * <li>Then <i>r - eps &le; trueRank &le; r + eps</i>.
 * Note that the error is on the rank, not the quantile.</li>
 * </ul>
 *
 * <p>A <i>getRank(quantile)</i> query has the following probabilistic guarantees:</p>
 * <ul>
 * <li>Let <i>r = getRank(q)</i> where <i>q</i> is a quantile between the min and max quantiles of
 * the input stream.</li>
 * <li>Let <i>trueRank</i> be the true rank of <i>q</i> derived from the hypothetical sorted
 * stream of all <i>N</i> quantiles.</li>
 * <li>Let <i>eps = getNormalizedRankError(false)</i>[*].</li>
 * <li>Then <i>r - eps &le; trueRank &le; r + eps</i>.</li>
 * </ul>
 *
 * <p>A <i>getPMF()</i> query has the following probabilistic guarantees:</p>
 * <ul>
 * <li>Let <i>{r<sub>1</sub>, r<sub>2</sub>, ..., r<sub>m+1</sub>}
 * = getPMF(v<sub>1</sub>, v<sub>2</sub>, ..., v<sub>m</sub>)</i> where
 * <i>q<sub>1</sub>, q<sub>2</sub>, ..., q<sub>m</sub></i> are monotonically increasing quantiles
 * supplied by the user that are part of the monotonic sequence
 * <i>q<sub>0</sub> = min, q<sub>1</sub>, q<sub>2</sub>, ..., q<sub>m</sub>, q<sub>m+1</sub> = max</i>,
 * and where <i>min</i> and <i>max</i> are the actual minimum and maximum quantiles of the input
 * stream automatically included in the sequence by the <i>getPMF(...)</i> function.
 *
 * <li>Let <i>r<sub>i</sub> = mass<sub>i</sub></i> = estimated mass between
 * <i>v<sub>i-1</sub></i> and <i>q<sub>i</sub></i> where <i>q<sub>0</sub> = min</i>
 * and <i>q<sub>m+1</sub> = max</i>.</li>
 *
 * <li>Let <i>trueMass</i> be the true mass between the quantiles of <i>q<sub>i</sub>,
 * q<sub>i+1</sub></i> derived from the hypothetical sorted stream of all <i>N</i> quantiles.</li>
 * <li>Let <i>eps = getNormalizedRankError(true)</i>[*].</li>
 * <li>Then <i>mass - eps &le; trueMass &le; mass + eps</i>.</li>
 * <li><i>r<sub>1</sub></i> includes the mass of all points between <i>min = q<sub>0</sub></i> and
 * <i>q<sub>1</sub></i>.</li>
 * <li><i>r<sub>m+1</sub></i> includes the mass of all points between <i>q<sub>m</sub></i> and
 * <i>max = q<sub>m+1</sub></i>.</li>
 * </ul>
 *
 * <p>A <i>getCDF(...)</i> query has the following probabilistic guarantees:</p>
 * <ul>
 * <li>Let <i>{r<sub>1</sub>, r<sub>2</sub>, ..., r<sub>m+1</sub>}
 * = getCDF(q<sub>1</sub>, q<sub>2</sub>, ..., q<sub>m</sub>)</i> where
 * <i>q<sub>1</sub>, q<sub>2</sub>, ..., q<sub>m</sub>)</i> are monotonically increasing quantiles
 * supplied by the user that are part of the monotonic sequence
 * <i>{q<sub>0</sub> = min, q<sub>1</sub>, q<sub>2</sub>, ..., q<sub>m</sub>, q<sub>m+1</sub> = max}</i>,
 * and where <i>min</i> and <i>max</i> are the actual minimum and maximum quantiles of the input
 * stream automatically included in the sequence by the <i>getCDF(...)</i> function.
 *
 * <li>Let <i>r<sub>i</sub> = mass<sub>i</sub></i> = estimated mass between
 * <i>q<sub>0</sub> = min</i> and <i>q<sub>i</sub></i>.</li>
 *
 * <li>Let <i>trueMass</i> be the true mass between the true ranks of <i>q<sub>i</sub>,
 * q<sub>i+1</sub></i> derived from the hypothetical sorted stream of all <i>N</i> quantiles.</li>
 * <li>Let <i>eps = getNormalizedRankError(true)</i>[*].</li>
 * <li>then <i>mass - eps &le; trueMass &le; mass + eps</i>.</li>
 * <li><i>r<sub>1</sub></i> includes the mass of all points between <i>min = q<sub>0</sub></i> and
 * <i>q<sub>1</sub></i>.</li>
 * <li><i>r<sub>m+1</sub></i> includes the mass of all points between <i>min = q<sub>0</sub></i> and
 * <i>max = q<sub>m+1</sub></i>.</li>
 * </ul>
 *
 * <p>Because errors are independent, we can make some estimates of the size of the confidence bounds
 * for the <em>quantile</em> returned from a call to <em>getQuantile()</em>, but not error bounds.
 * These confidence bounds may be quite large for certain distributions.</p>
 *
 * <ul>
 * <li>Let <i>q = getQuantile(r)</i>, the estimated quantile of rank <i>r</i>.</li>
 * <li>Let <i>eps = getNormalizedRankError(false)</i>[*].</li>
 * <li>Let <i>q<sub>lo</sub></i> = estimated quantile of rank <i>(r - eps)</i>.</li>
 * <li>Let <i>q<sub>hi</sub></i> = estimated quantile of rank <i>(r + eps)</i>.</li>
 * <li>Then <i>q<sub>lo</sub> &le; q &le; q<sub>hi</sub></i>.</li>
 * </ul>
 *
 * <p>This sketch is order and distribution insensitive</p>
 *
 * <p>This algorithm intentionally inserts randomness into the sampling process for items that
 * ultimately get retained in the sketch. Thus, the results produced by this algorithm are not
 * deterministic. For example, if the same stream is inserted into two different instances of this
 * sketch, the answers obtained from the two sketches should be close, but may not be be identical.</p>
 *
 * <p>Similarly, there may be directional inconsistencies. For example, if a quantile obtained
 * from  getQuantile(rank) is input into the reverse query
 * getRank(quantile), the resulting rank should be close, but may not exactly equal the original rank.</p>
 *
 * <p>Please visit our website: <a href="https://datasketches.apache.org">DataSketches Home Page</a>
 * and specific Javadocs for more information.</p>
 *
 * <p>[*] Note that obtaining epsilon may require using a similar function but with more parameters
 * based on the specific sketch implementation.</p>
 *
 * @see <a href="https://datasketches.apache.org/docs/Quantiles/SketchingQuantilesAndRanksTutorial.html">
 * Sketching Quantiles and Ranks, Tutorial</a>
 * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
public interface QuantilesAPI {

  static String EMPTY_MSG = "The sketch must not be empty for this operation. ";
  static String UNSUPPORTED_MSG = "Unsupported operation for this Sketch Type. ";
  static String NOT_SINGLE_ITEM_MSG = "Sketch does not have just one item. ";
  static String MEM_REQ_SVR_NULL_MSG = "MemoryRequestServer must not be null. ";
  static String TGT_IS_READ_ONLY_MSG = "Target sketch is Read Only, cannot write. ";

  /**
   * Gets the user configured parameter k, which controls the accuracy of the sketch
   * and its memory space usage.
   * @return the user configured parameter k, which controls the accuracy of the sketch
   * and its memory space usage.
   */
  int getK();

  /**
   * Gets the length of the input stream offered to the sketch..
   * @return the length of the input stream offered to the sketch.
   */
  long getN();

  /**
   * Gets the number of quantiles retained by the sketch.
   * @return the number of quantiles retained by the sketch
   */
  int getNumRetained();

  /**
   * Gets the lower bound of the rank confidence interval in which the true rank of the
   * given rank exists.
   * @param rank the given normalized rank.
   * @return the lower bound of the rank confidence interval in which the true rank of the
   * given rank exists.
   */
  double getRankLowerBound(double rank);

  /**
   * Gets the upper bound of the rank confidence interval in which the true rank of the
   * given rank exists.
   * @param rank the given normalized rank.
   * @return the upper bound of the rank confidence interval in which the true rank of the
   * given rank exists.
   */
  double getRankUpperBound(double rank);

  /**
   * Returns true if this sketch's data structure is backed by Memory or WritableMemory.
   * @return true if this sketch's data structure is backed by Memory or WritableMemory.
   */
  boolean hasMemory();

  /**
   * Returns true if this sketch's data structure is off-heap (a.k.a., Direct or Native memory).
   * @return true if this sketch's data structure is off-heap (a.k.a., Direct or Native memory).
   */
  boolean isDirect();

  /**
   * Returns true if this sketch is empty.
   * @return true if this sketch is empty.
   */
  boolean isEmpty();

  /**
   * Returns true if this sketch is in estimation mode.
   * @return true if this sketch is in estimation mode.
   */
  boolean isEstimationMode();

  /**
   * Returns true if this sketch is read only.
   * @return true if this sketch is read only.
   */
  boolean isReadOnly();

  /**
   * Resets this sketch to the empty state.
   * If the sketch is <i>read only</i> this does nothing.
   *
   * <p>The parameter <i>k</i> will not change.</p>
   */
  void reset();

  /**
   * Returns a summary of the key parameters of the sketch.
   * @return a summary of the key parameters of the sketch.
   */
  @Override
  String toString();

}

