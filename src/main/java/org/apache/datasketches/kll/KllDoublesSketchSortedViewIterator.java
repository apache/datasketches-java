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

package org.apache.datasketches.kll;

import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;

import org.apache.datasketches.QuantileSearchCriteria;

/**
 * Iterator over KllFloatsSketchSortedView
 *
 * <p>The recommended iteration loop:</p>
 * <pre>{@code
 *   KllDoublesSketchSortedViewIterator itr = sketch.getSortedView().iterator();
 *   while (itr.next()) {
 *     double v = itr.getValue();
 *     ...
 *   }
 * }</pre>
 */
public class KllDoublesSketchSortedViewIterator {

  private final double[] values;
  private final long[] cumWeights;
  private final long totalN;
  private int index;

  KllDoublesSketchSortedViewIterator(final double[] values, final long[] cumWeights) {
    this.values = values;
    this.cumWeights = cumWeights;
    this.totalN = (cumWeights.length > 0) ? cumWeights[cumWeights.length - 1] : 0;
    index = -1;
  }

  /**
   * Gets the current value.
   *
   * <p>Don't call this before calling next() for the first time
   * or after getting false from next().</p>
   *
   * @return the current value
   */
  public double getValue() {
    return values[index];
  }

  /**
   * Gets the cumulative weight for the current value.
   *
   * <p>Don't call this before calling next() for the first time
   * or after getting false from next().</p>
   *
   * @param inclusive If true, includes the weight of the current value.
   * Otherwise, returns the cumulative weightof the previous value.
   * @return cumulative weight for the current value.
   */
  public long getCumulativeWeight(final QuantileSearchCriteria inclusive) {
    if (inclusive == INCLUSIVE) { return cumWeights[index]; }
    return (index == 0) ? 0 : cumWeights[index - 1];
  }

  /**
   * Gets the normalized rank for the current value or previous value.
   *
   * <p>Don't call this before calling next() for the first time
   * or after getting false from next().</p>
   *
   * @param inclusive if true, returns the normalized rank of the current value.
   * Otherwise, returns the normalized rank of the previous value.
   * @return normalized rank for the current value or previous value.
   */
  public double getNormalizedRank(final QuantileSearchCriteria inclusive) {
    return (double) getCumulativeWeight(inclusive) / totalN;
  }

  /**
   * Gets the weight of the current value.
   *
   * <p>Don't call this before calling next() for the first time
   * or after getting false from next().</p>
   *
   * @return weight of the current value.
   */
  public long getWeight() {
    if (index == 0) { return cumWeights[0]; }
    return cumWeights[index] - cumWeights[index - 1];
  }

  /**
   * Advancing the iterator and checking existence of the next entry
   * is combined here for efficiency. This results in an undefined
   * state of the iterator before the first call of this method.
   * @return true if the next element exists
   */
  public boolean next() {
    index++;
    return index < values.length;
  }

}
