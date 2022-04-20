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

/**
 * Implementation of a very compact quantiles sketch with lazy compaction scheme
 * and nearly optimal accuracy per retained item.
 * See <a href="https://arxiv.org/abs/1603.05346v2">Optimal Quantile Approximation in Streams</a>.
 *
 * <p>This is a stochastic streaming sketch that enables near-real time analysis of the
 * approximate distribution of values from a very large stream in a single pass, requiring only
 * that the values are comparable.
 * The analysis is obtained using <i>getQuantile()</i> or <i>getQuantiles()</i> functions or the
 * inverse functions getRank(), getPMF() (Probability Mass Function), and getCDF()
 * (Cumulative Distribution Function).
 *
 * <p>Given an input stream of <i>N</i> numeric values, the <i>absolute rank</i> of any specific
 * value is defined as its index <i>(0 to N-1)</i> in the hypothetical sorted stream of all
 * <i>N</i> input values.
 *
 * <p>The <i>normalized rank</i> (<i>rank</i>) of any specific value is defined as its
 * <i>absolute rank</i> divided by <i>N</i>.
 * Thus, the <i>normalized rank</i> is a value in the interval [0.0, 1.0).
 * In the documentation and Javadocs for this sketch <i>absolute rank</i> is never used so any
 * reference to just <i>rank</i> should be interpreted to mean <i>normalized rank</i>.
 *
 * <p>This sketch is configured with a parameter <i>k</i>, which affects the size of the sketch
 * and its estimation error.
 *
 * <p>In the research literature, the estimation error is commonly called <i>epsilon</i>
 * (or <i>eps</i>) and is a fraction between zero and one.
 * Larger values of <i>k</i> result in smaller values of epsilon.
 * The epsilon error is always with respect to the rank and cannot be applied to the
 * corresponding values.
 *
 * <p>The relationship between the normalized rank and the corresponding values can be viewed
 * as a two dimensional monotonic plot with the normalized rank on one axis and the
 * corresponding values on the other axis. If the y-axis is specified as the value-axis and
 * the x-axis as the normalized rank, then <i>y = getQuantile(x)</i> is a monotonically
 * increasing function.
 *
 * <p>The functions <i>getQuantile(rank)</i> and getQuantiles(...) translate ranks into
 * corresponding values. The functions <i>getRank(value),
 * getCDF(...) (Cumulative Distribution Function), and getPMF(...)
 * (Probability Mass Function)</i> perform the opposite operation and translate values into ranks.
 *
 * <p>The <i>getPMF(...)</i> function has about 13 to 47% worse rank error (depending
 * on <i>k</i>) than the other queries because the mass of each "bin" of the PMF has
 * "double-sided" error from the upper and lower edges of the bin as a result of a subtraction,
 * as the errors from the two edges can sometimes add.
 *
 * <p>The default <i>k</i> of 200 yields a "single-sided" epsilon of about 1.33% and a
 * "double-sided" (PMF) epsilon of about 1.65%.
 *
 * <p>A <i>getQuantile(rank)</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>v = getQuantile(r)</i> where <i>r</i> is the rank between zero and one.</li>
 * <li>The value <i>v</i> will be a value from the input stream.</li>
 * <li>Let <i>trueRank</i> be the true rank of <i>v</i> derived from the hypothetical sorted
 * stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = getNormalizedRankError(false)</i>.</li>
 * <li>Then <i>r - eps &le; trueRank &le; r + eps</i> with a confidence of 99%. Note that the
 * error is on the rank, not the value.</li>
 * </ul>
 *
 * <p>A <i>getRank(value)</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>r = getRank(v)</i> where <i>v</i> is a value between the min and max values of
 * the input stream.</li>
 * <li>Let <i>trueRank</i> be the true rank of <i>v</i> derived from the hypothetical sorted
 * stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = getNormalizedRankError(false)</i>.</li>
 * <li>Then <i>r - eps &le; trueRank &le; r + eps</i> with a confidence of 99%.</li>
 * </ul>
 *
 * <p>A <i>getPMF(...)</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>{r<sub>1</sub>, r<sub>2</sub>, ..., r<sub>m+1</sub>}
 * = getPMF(v<sub>1</sub>, v<sub>2</sub>, ..., v<sub>m</sub>)</i> where
 * <i>v<sub>1</sub>, v<sub>2</sub>, ..., v<sub>m</sub></i> are monotonically increasing values
 * supplied by the user that are part of the monotonic sequence
 * <i>v<sub>0</sub> = min, v<sub>1</sub>, v<sub>2</sub>, ..., v<sub>m</sub>, v<sub>m+1</sub> = max</i>,
 * and where <i>min</i> and <i>max</i> are the actual minimum and maximum values of the input
 * stream automatically included in the sequence by the <i>getPMF(...)</i> function.
 *
 * <li>Let <i>r<sub>i</sub> = mass<sub>i</sub></i> = estimated mass between
 * <i>v<sub>i-1</sub></i> and <i>v<sub>i</sub></i> where <i>v<sub>0</sub> = min</i>
 * and <i>v<sub>m+1</sub> = max</i>.</li>
 *
 * <li>Let <i>trueMass</i> be the true mass between the values of <i>v<sub>i</sub>,
 * v<sub>i+1</sub></i> derived from the hypothetical sorted stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = getNormalizedRankError(true)</i>.</li>
 * <li>Then <i>mass - eps &le; trueMass &le; mass + eps</i> with a confidence of 99%.</li>
 * <li><i>r<sub>1</sub></i> includes the mass of all points between <i>min = v<sub>0</sub></i> and
 * <i>v<sub>1</sub></i>.</li>
 * <li><i>r<sub>m+1</sub></i> includes the mass of all points between <i>v<sub>m</sub></i> and
 * <i>max = v<sub>m+1</sub></i>.</li>
 * </ul>
 *
 * <p>A <i>getCDF(...)</i> query has the following guarantees;
 * <ul>
 * <li>Let <i>{r<sub>1</sub>, r<sub>2</sub>, ..., r<sub>m+1</sub>}
 * = getCDF(v<sub>1</sub>, v<sub>2</sub>, ..., v<sub>m</sub>)</i> where
 * <i>v<sub>1</sub>, v<sub>2</sub>, ..., v<sub>m</sub>)</i> are monotonically increasing values
 * supplied by the user that are part of the monotonic sequence
 * <i>{v<sub>0</sub> = min, v<sub>1</sub>, v<sub>2</sub>, ..., v<sub>m</sub>, v<sub>m+1</sub> = max}</i>,
 * and where <i>min</i> and <i>max</i> are the actual minimum and maximum values of the input
 * stream automatically included in the sequence by the <i>getCDF(...)</i> function.
 *
 * <li>Let <i>r<sub>i</sub> = mass<sub>i</sub></i> = estimated mass between
 * <i>v<sub>0</sub> = min</i> and <i>v<sub>i</sub></i>.</li>
 *
 * <li>Let <i>trueMass</i> be the true mass between the true ranks of <i>v<sub>i</sub>,
 * v<sub>i+1</sub></i> derived from the hypothetical sorted stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = getNormalizedRankError(true)</i>.</li>
 * <li>then <i>mass - eps &le; trueMass &le; mass + eps</i> with a confidence of 99%.</li>
 * <li><i>r<sub>1</sub></i> includes the mass of all points between <i>min = v<sub>0</sub></i> and
 * <i>v<sub>1</sub></i>.</li>
 * <li><i>r<sub>m+1</sub></i> includes the mass of all points between <i>min = v<sub>0</sub></i> and
 * <i>max = v<sub>m+1</sub></i>.</li>
 * </ul>
 *
 * <p>From the above, it might seem like we could make some estimates to bound the
 * <em>value</em> returned from a call to <em>getQuantile()</em>. The sketch, however, does not
 * let us derive error bounds or confidences around values. Because errors are independent, we
 * can approximately bracket a value as shown below, but there are no error estimates available.
 * Additionally, the interval may be quite large for certain distributions.
 * <ul>
 * <li>Let <i>v = getQuantile(r)</i>, the estimated quantile value of rank <i>r</i>.</li>
 * <li>Let <i>eps = getNormalizedRankError(false)</i>.</li>
 * <li>Let <i>v<sub>lo</sub></i> = estimated quantile value of rank <i>(r - eps)</i>.</li>
 * <li>Let <i>v<sub>hi</sub></i> = estimated quantile value of rank <i>(r + eps)</i>.</li>
 * <li>Then <i>v<sub>lo</sub> &le; v &le; v<sub>hi</sub></i>, with 99% confidence.</li>
 * </ul>
 *
 * <p>Please visit our website: <a href="https://datasketches.apache.org">DataSketches Home Page</a> and
 * the Javadocs for more information.</p>
 *
 * @author Kevin Lang
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */

package org.apache.datasketches.kll;
