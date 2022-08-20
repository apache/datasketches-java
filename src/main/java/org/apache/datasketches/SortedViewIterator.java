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
 * This is the base interface for the SortedViewIterator hierarchy used with a SortedView obtained
 * from a quantile-type sketch. This provides an ordered iterator over the retained values of
 * the associated quantile-type sketch.
 *
 * <p>Prototype example of the recommended iteration loop:</p>
 * <pre>{@code
 *   SortedViewIterator itr = sketch.getSortedView().iterator();
 *   while (itr.next()) {
 *     long weight = itr.getWeight();
 *     ...
 *   }
 * }</pre>
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public interface SortedViewIterator {

  /**
   * Gets the cumulative weight at the current index (or previous index) based on the chosen search criterion.
   * This is also referred to as the "Natural Rank".
   *
   * <p>Don't call this before calling next() for the first time
   * or after getting false from next().</p>
   *
   * @param searchCrit if INCLUSIVE, includes the weight at the current index in the cumulative sum.
   * Otherwise, it will return the cumulative weight of the previous index.
   * @return cumulative weight at the current index on the chosen search criterion.
   */
  long getCumulativeWeight(QuantileSearchCriteria searchCrit);

  /**
   * Gets the total count of all values (or items) presented to the sketch.
   * @return the total count of all values (or items) presented to the sketch.
   */
  long getN();

  /**
   * Gets the normalized rank at the current index (or previous index)
   * based on the chosen search criterion.
   *
   * <p>Don't call this before calling next() for the first time
   * or after getting false from next().</p>
   *
   * @param searchCrit if INCLUSIVE, includes the normalized rank at the current index.
   * Otherwise, returns the normalized rank of the previous index.
   * @return the normalized rank at the current index (or previous index)
   * based on the chosen search criterion.
   */
  double getNormalizedRank(QuantileSearchCriteria searchCrit);

  /**
   * Gets the natural weight at the current index.
   *
   * <p>Don't call this before calling next() for the first time
   * or after getting false from next().</p>
   *
   * @return the natural weight at the current index.
   */
  long getWeight();

  /**
   * Advances the index and checks if it is valid.
   * The state of this iterator is undefined before the first call of this method.
   * @return true if the next index is valid.
   */
  boolean next();

}

