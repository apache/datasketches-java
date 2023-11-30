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
 * This is the base interface for the Sorted View interface hierarchy and defines the methods that are type independent.
 *
 * <p>The SortedView interface hierarchy provides a sorted view of the data retained by a quantiles-type sketch that
 * would be cumbersome to get any other way.
 * One could use the sketch's iterator to iterate over the contents of the sketch,
 * but the result would not be sorted.</p>
 *
 * <p>The data from a Sorted view is an unbiased random sample of the input stream that can be used for other kinds of
 * analysis not directly provided by the sketch.</p>
 *
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public interface SortedView {

  /**
   * Returns the array of cumulative weights from the sketch.
   * Also known as the natural ranks, which are the Natural Numbers on the interval [1, N].
   * @return the array of cumulative weights (or natural ranks).
   */
  long[] getCumulativeWeights();

  /**
   * Returns the total number of items presented to the sourcing sketch.
   * @return the total number of items presented to the sourcing sketch.
   */
  long getN();

  /**
   * Returns true if this sorted view is empty.
   * @return true if this sorted view is empty.
   */
  boolean isEmpty();

  /**
   * Returns an iterator for this Sorted View.
   * @return an iterator for this Sorted View.
   */
  SortedViewIterator iterator();

}

