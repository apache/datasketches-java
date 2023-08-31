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
 * The quantiles SortedView Iterator for generic types.
 * @see SortedViewIterator
 * @param <T> The generic quantile type
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public class GenericSortedViewIterator<T> implements SortedViewIterator {
  private final T[] quantiles;
  private final long[] cumWeights;
  private final long totalN;
  private int index;

  public GenericSortedViewIterator(final T[] quantiles, final long[] cumWeights) {
    this.quantiles = quantiles;   //SpotBugs EI_EXPOSE_REP2 suppressed by FindBugsExcludeFilter
    this.cumWeights = cumWeights; //SpotBugs EI_EXPOSE_REP2 suppressed by FindBugsExcludeFilter
    this.totalN = (cumWeights.length > 0) ? cumWeights[cumWeights.length - 1] : 0;
    index = -1;
  }

  @Override
  public long getCumulativeWeight(final QuantileSearchCriteria searchCrit) {
    if (searchCrit == INCLUSIVE) { return cumWeights[index]; }
    return (index == 0) ? 0 : cumWeights[index - 1];
  }

  public T getQuantile() {
    return quantiles[index];
  }

  @Override
  public long getN() {
    return totalN;
  }

  @Override
  public double getNormalizedRank(final QuantileSearchCriteria searchCrit) {
    return (double) getCumulativeWeight(searchCrit) / totalN;
  }

  @Override
  public long getWeight() {
    if (index == 0) { return cumWeights[0]; }
    return cumWeights[index] - cumWeights[index - 1];
  }

  @Override
  public boolean next() {
    index++;
    return index < quantiles.length;
  }

}
