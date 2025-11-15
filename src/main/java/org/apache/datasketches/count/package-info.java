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
 * This package in intended for implementations of the the Count Sketch and the Count-min Sketch both of which can be used to estimate
 * frequency-moments of a stream of distinct elements. They are different from the unique counting sketches (HLL, Theta, CPC, etc.)
 * and different from the Frequent-Items Sketch.
 *
 * <p>A Count-Min sketch is a probabilistic data structure that estimates the frequency of items in a large data stream using a
 * fixed amount of memory. It uses a 2D array of counters and multiple hash functions to map each item to a position in every row.
 * To estimate an item's frequency, it takes the minimum count from all the positions it hashes to, which provides an overestimate
 * that is guaranteed to be greater than or equal to the true count.</p>
 *
 * <p>A Count sketch is a type of dimensionality reduction that is particularly efficient in statistics, machine learning and
 * algorithms. It was invented by Moses Charikar, Kevin Chen and Martin Farach-Colton in an effort to speed up the AMS Sketch by
 * Alon, Matias and Szegedy for approximating the frequency moments of streams[4] (these calculations require counting of the
 * number of occurrences for the distinct elements of the stream).</p>
 *
 * @see <a href="https://en.wikipedia.org/wiki/Count_sketch">https://en.wikipedia.org/wiki/Count_sketch</a>
 * @see <a href="https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch">https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch</a>
 *
 */

package org.apache.datasketches.count;
