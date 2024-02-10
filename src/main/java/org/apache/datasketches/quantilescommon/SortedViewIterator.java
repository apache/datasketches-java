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

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

/**
 * This is the base interface for the SortedViewIterator hierarchy used with a SortedView obtained
 * from a quantile-type sketch. This provides an ordered iterator over the retained quantiles of
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
public class SortedViewIterator {
  protected final long[] cumWeights;
  protected long totalN;
  protected int index;

  SortedViewIterator(final long[] cumWeights) {
    this.cumWeights = cumWeights; //SpotBugs EI_EXPOSE_REP2 suppressed by FindBugsExcludeFilter
    this.totalN = (cumWeights.length > 0) ? cumWeights[cumWeights.length - 1] : 0;
    index = -1;
  }

  /**
   * Gets the natural rank at the current index.
   * This is equivalent to <i>getNaturalRank(INCLUSIVE)</i>.
   *
   * <p>Don't call this before calling next() for the first time or after getting false from next().</p>
   *
   * @return the natural rank at the current index.
   */
  public long getNaturalRank() {
    return cumWeights[index];
  }

  /**
   * Gets the natural rank at the current index (or previous index) based on the chosen search criterion.
   * This is also referred to as the "cumulative weight". The natural rank is a number in the range <i>[1, N]</i>,
   * where <i>N</i> ({@link #getN()}) is the total number of items fed to the sketch.
   *
   * <p>Don't call this before calling next() for the first time or after getting false from next().</p>
   *
   * @param searchCrit if INCLUSIVE, includes the weight of the item at the current index in the computation of
   * the natural rank.
   * Otherwise, it will return the natural rank of the previous index.
   *
   * @return the natural rank at the current index (or previous index) based on the chosen search criterion.
   */
  public long getNaturalRank(final QuantileSearchCriteria searchCrit) {
    if (searchCrit == INCLUSIVE) { return cumWeights[index]; }
    return (index == 0) ? 0 : cumWeights[index - 1];
  }

  /**
   * Gets the total count of all items presented to the sketch.
   * @return the total count of all items presented to the sketch.
   */
  public long getN() {
    return totalN;
  }

  /**
   * Gets the normalized rank at the current index.
   * This is equivalent to <i>getNormalizedRank(INCLUSIVE)</i>.
   *
   * <p>Don't call this before calling next() for the first time or after getting false from next().</p>
   *
   * @return the normalized rank at the current index
   */
  public double getNormalizedRank() {
    return (double) getNaturalRank() / totalN;
  }

  /**
   * Gets the normalized rank at the current index (or previous index)
   * based on the chosen search criterion. Where <i>normalized rank = natural rank / N</i> ({@link #getN()})
   * and is a fraction in the range (0,1.0].
   *
   * <p>Don't call this before calling next() for the first time or after getting false from next().</p>
   *
   * @param searchCrit if INCLUSIVE, includes the normalized rank at the current index.
   * Otherwise, returns the normalized rank of the previous index.
   *
   * @return the normalized rank at the current index (or previous index)
   * based on the chosen search criterion.
   */
  public double getNormalizedRank(final QuantileSearchCriteria searchCrit) {
    return (double) getNaturalRank(searchCrit) / totalN;
  }

  /**
   * Gets the weight contribution of the item at the current index.
   *
   * <p>Don't call this before calling next() for the first time or after getting false from next().</p>
   *
   * @return the weight contribution of the item at the current index.
   */
  public long getWeight() {
    if (index == 0) { return cumWeights[0]; }
    return cumWeights[index] - cumWeights[index - 1];
  }

  /**
   * Advances the index and checks if it is valid.
   * The state of this iterator is undefined before the first call of this method.
   * @return true if the next index is valid.
   */
  public boolean next() {
    index++;
    return index < cumWeights.length;
  }

}

