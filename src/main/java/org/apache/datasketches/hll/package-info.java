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
 * <H1>The DataSketches&trade; HLL sketch family package</H1>
 * {@link org.apache.datasketches.hll.HllSketch HllSketch} and {@link org.apache.datasketches.hll.Union Union}
 * are the public facing classes of this high performance implementation of Phillipe Flajolet's
 * HyperLogLog algorithm[1] but with significantly improved error behavior and important features that can be
 * essential for large production systems that must handle massive data.
 *
 * <h2>Key Features of the DataSketches&trade; HLL Sketch and its companion Union</h2>
 *
 * <h3>Advanced Estimation Algorithms for Optimum Accuracy</h3>
 *
 * <h4>Zero error at low cardinalities</h4>
 * The HLL sketch leverages highly compact arrays and hash tables to keep exact counts until the transition to
 * dense mode is required for space reasons. The result is perfect accuracy for very low cardinalities.
 *
 * <p>Accuracy for very small streams can be important because Big Data is often fragmented into millions of smaller
 * streams (or segments) that inevitably are power-law distributed in size. If you are sketching all these fragments,
 * as a general rule, more than 80% of your sketches will be very small, 20% will be much larger, and only a few very
 * large in cardinality.
 *
 * <h4>HIP / Martingale Estimator</h4>
 * When obtaining a cardinality estimate, the sketch automatically determines if it was the result of the capture of
 * a single stream, or if was the result of certain qualifying union operations. If this is the case the sketch will
 * take advantage of Edith Cohen's Historical Inverse Probability (HIP) estimation algorithm[2], which was
 * also independently developed by Daniel Ting as the Martingale estimation algorithm[3].
 * This will result in a 20% improvement in accuracy over the standard Flajolet estimator.
 * If it is not a single stream or if the specific union operation did not qualify,
 * the estimator will default to the Composite Estimator.
 *
 * <h4>Composite Estimator</h4>
 * This advanced estimator is a blend of several algorithms including new algorithms developed by Kevin Lang for his
 * Compressed Probabilistic Counting (CPC) sketch[4]. These algorithms provide near optimal estimation accuracy
 * for cases that don't qualify for  HIP / Martingale estimation.
 *
 * <p>As a result of all of this work on accuracy, one will get a very smooth curve of the underlying accuracy of the
 * sketch once the statistical randomness is removed through multiple trials. This can be observed in the
 * following graph.</p>
 *
 * <p><img src="doc-files/HLL_HIP_K12T20U20.png" width="500" alt="HLL Accuracy">[6]</p>
 *
 * <p>The above graph has 7 curves. At y = 0, is the median line that hugs the x-axis so closely that it can't be seen.
 * The two curves, just above and just below the x-axis, correspond to +/- 1 standard deviation (SD) of error.
 * The distance between either one of this pair and the x-axis is also known as the Relative Standard Error (RSE).
 * This type of graph for illustrating sketch error we call a "pitchfork plot".</p>
 *
 * <p>The next two curves above and below correspond to +/- 2 SD, and
 * the top-most and bottom-most curves correspond to +/- 3 SD.
 * The chart grid lines are set at +/- multiples of Relative Standard Error (RSE) that correspond to +/- 1,2,3 SD.
 * Below the cardinality of about 512 there is no error at all. This is the point where this particular
 * sketch transitions from sparse to dense (or estimation) mode.</p>
 *
 * <h3>Three HLL Types</h3>
 * This HLL implementation offers three different types of HLL sketch, each with different
 * trade-offs with accuracy, space and performance. These types are selected with the
 * {@link org.apache.datasketches.hll.TgtHllType TgtHllType} parameter.
 *
 * <p>In terms of accuracy, all three types, for the same <i>lgConfigK</i>, have the same error
 * distribution as a function of cardinality.</p>
 *
 * <p>The configuration parameter <i>lgConfigK</i> is the log-base-2 of <i>K</i>,
 * where <i>K</i> is the number of buckets or slots for the sketch. <i>lgConfigK</i> impacts both accuracy and
 * the size of the sketch in memory and when stored.</p>
 *
 * <h4>HLL 8</h4>
 * This uses an 8-bit byte per HLL bucket. It is generally the
 * fastest in terms of update time but has the largest storage footprint of about <i>K</i> bytes.
 *
 * <h4>HLL 6</h4>
 * This uses a 6-bit field per HLL bucket. It is the generally the next fastest
 * in terms of update time with a storage footprint of about <i>3/4 * K</i> bytes.
 *
 * <h4>HLL 4</h4>
 * This uses a 4-bit field per HLL bucket and for large counts may require
 * the use of a small internal auxiliary array for storing statistical exceptions, which are rare.
 * For the values of <i>lgConfigK &gt; 13</i> (<i>K</i> = 8192),
 * this additional array adds about 3% to the overall storage. It is generally the slowest in
 * terms of update time, but has the smallest storage footprint of about <i>K/2 * 1.03</i> bytes.
 *
 * <h3>Off-Heap Operation</h3>
 * This HLL sketch also offers the capability of operating off-heap. Given a <i>WritableMemory[5]</i> object
 * created by the user, the sketch will perform all of its updates and internal phase transitions
 * in that object, which can actually reside either on-heap or off-heap based on how it was
 * configured. In large systems that must update and union many millions of sketches, having the
 * sketch operate off-heap avoids the serialization and deserialization costs of moving sketches from heap to
 * off-heap and back, and reduces the need for garbage collection.
 *
 * <h3>Merging sketches with different configured <i>lgConfigK</i></h3>
 * This enables a user to union a HLL sketch that was configured with, say, <i>lgConfigK = 12</i>
 * with another loaded HLL sketch that was configured with, say, <i>lgConfigK = 14</i>.
 *
 * <p>Why is this important?  Suppose you have been building a history of sketches of your customer's
 * data that go back a full year (or 5 or 10!) that were all configured with <i>lgConfigK = 12</i>. Because sketches
 * are so much smaller than the raw data it is possible that the raw data was discarded keeping only the sketches.
 * Even if you have the raw data, it might be very expensive and time consuming to reload and rebuild all your
 * sketches with a larger more accurate size, say, <i>lgConfigK = 14</i>.
 * This capability enables you to merge last year's data with this year's data built with larger sketches and still
 * have meaningful results.</p>
 *
 * <p>In other words, you can change your mind about what size sketch you need for your application at any time and
 * will not lose access to the data contained in your older historical sketches.</p>
 *
 * <p>This capability does come with a caveat: The resulting accuracy of the merged sketch will be the accuracy of the
 * smaller of the two sketches. Without this capability, you would either be stuck with the configuration you first
 * chose forever, or you would have to rebuild all your sketches from scratch, or worse, not be able to recover your
 * historical data.</p>
 *
 * <h3>Multi-language, multi-platform.</h3>
 * The binary structures for our sketch serializations are language and platform independent.
 * This means it is possible to generate an HLL sketch on a C++ Windows platform and it can be used on a
 * Java or Python Unix platform.
 *
 * <p>[1] Philippe Flajolet, et al, <a href="https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf">
<i>HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm.</i></a>
 * DMTCS proc. <b>AH</b>, 2007, 127-146.
 *
 * <p>[2] Edith Cohen, <a href="https://arxiv.org/pdf/1306.3284.pdf">
<i>All-Distances Sketches, Revisited: HIP Estimators for Massive Graphs Analysis.</i></a>
 * PODS'14, June 22-27, Snowbird, UT, USA.
 *
 * <p>[3] Daniel Ting,
 * <a href="https://research.facebook.com/publications/streamed-approximate-counting-of-distinct-elements">
<i>Streamed Approximate Counting of Distinct Elements, Beating Optimal Batch Methods.</i></a>
 * KDD'14 August 24, 2014 New York, New York USA.
 *
 * <p>[4] Kevin Lang,
 * <a href="https://arxiv.org/abs/1708.06839">
<i>Back to the Future: an Even More Nearly Optimal Cardinality Estimation Algorithm.</i></a>
 * arXiv 1708.06839, August 22, 2017, Yahoo Research.
 *
 * <p>[5] Memory Component,
 * <a href="https://datasketches.apache.org/docs/Memory/MemoryComponent.html">
<i>DataSketches Memory Component</i></a>
 *
 * <p>[6] MacBook Pro 2.3 GHz 8-Core Intel Core i9
 *
 * @see org.apache.datasketches.cpc.CpcSketch
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
package org.apache.datasketches.hll;
