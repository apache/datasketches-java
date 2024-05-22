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

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.quantilescommon.IncludeMinMax.LongsPair;

import static org.apache.datasketches.quantilescommon.IncludeMinMax.DoublesPair;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesAPI.EMPTY_MSG;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.getNaturalRank;

/**
 * The SortedView of the KllLongsSketch.
 * @author Zac Blanco
 */
public final class LongsSketchSortedView implements LongsSortedView {
  private final long[] quantiles;
  private final long[] cumWeights; //cumulative natural weights
  private final long totalN;

  /**
   * Construct from elements, also used in testing.
   * @param quantiles sorted array of quantiles
   * @param cumWeights sorted, monotonically increasing cumulative weights.
   * @param sk the underlying quantile sketch.
   */
  public LongsSketchSortedView(
      final long[] quantiles,
      final long[] cumWeights,
      final QuantilesLongsAPI sk) {
    final LongsPair dPair =
        IncludeMinMax.includeLongsMinMax(quantiles, cumWeights, sk.getMaxItem(), sk.getMinItem());
    this.quantiles = dPair.quantiles;
    this.cumWeights  = dPair.cumWeights;
    this.totalN = sk.getN();
  }

  //Used for testing
  LongsSketchSortedView(
      final long[] quantiles,
      final long[] cumWeights,
      final long totalN,
      final long maxItem,
      final long minItem) {
    final LongsPair dPair =
        IncludeMinMax.includeLongsMinMax(quantiles, cumWeights, maxItem, minItem);
    this.quantiles = dPair.quantiles;
    this.cumWeights  = dPair.cumWeights;
    this.totalN = totalN;
  }

  @Override
  public long[] getCumulativeWeights() {
    return cumWeights.clone();
  }

  @Override
  public long getMaxItem() {
    final int top = quantiles.length - 1;
    return quantiles[top];
  }

  @Override
  public long getMinItem() {
    return quantiles[0];
  }

  @Override
  public long getN() {
    return totalN;
  }

  @Override
  public int getNumRetained() {
    return quantiles.length;
  }

  @Override
  public long getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    QuantilesUtil.checkNormalizedRankBounds(rank);
    final int len = cumWeights.length;
    final double naturalRank = getNaturalRank(rank, totalN, searchCrit);
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, naturalRank, crit);
    if (index == -1) {
      return quantiles[len - 1]; //EXCLUSIVE (GT) case: normRank == 1.0;
    }
    return quantiles[index];
  }

  @Override
  public long[] getQuantiles() {
    return quantiles.clone();
  }

  @Override
  public double getRank(final long quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    final int len = quantiles.length;
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.LE : InequalitySearch.LT;
    final int index = InequalitySearch.find(quantiles,  0, len - 1, quantile, crit);
    if (index == -1) {
      return 0; //EXCLUSIVE (LT) case: quantile <= minQuantile; INCLUSIVE (LE) case: quantile < minQuantile
    }
    return (double)cumWeights[index] / totalN;
  }

  @Override
  public boolean isEmpty() {
    return totalN == 0;
  }

  @Override
  public LongsSortedViewIterator iterator() {
    return new LongsSortedViewIterator(quantiles, cumWeights);
  }

}
