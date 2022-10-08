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
 * This is the base interface for the Sorted View interface hierarchy.
 *
 * <p>The Sorted View provides a view of the data retained by a quantiles-type sketch
 * that would be cumbersome to get any other way.
 * One can iterate over the contents of the sketch using the sketch's iterator, but the result is not sorted.</p>
 *
 * <p>Once this sorted view has been created, it provides not only a sorted view of the data retained by the sketch
 * but also the basic queries, such as getRank(), getQuantile(), and getCDF() and getPMF().
 * In addition, the iterator obtained from this sorted view provides useful detailed information about each entry.</p>
 *
 * <p>The data from a Sorted view is an unbiased sample of the input stream that can be used for other kinds of
 * analysis not directly provided by the sketch.  For example, comparing two sketches using the Kolmogorov-Smirnov
 * test.</p>
 *
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public interface SortedView {

  /**
   * Returns an iterator for this Sorted View.
   * @return an iterator for this Sorted View.
   */
  SortedViewIterator iterator();

  /**
   * Returns the array of cumulative weights
   * @return the array of cumulative weights
   */
  long[] getCumulativeWeights();
}

