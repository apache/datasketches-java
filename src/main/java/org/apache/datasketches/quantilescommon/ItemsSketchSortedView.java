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

import static java.lang.Math.min;
import static org.apache.datasketches.quantilescommon.GenericInequalitySearch.find;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesAPI.EMPTY_MSG;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.evenlySpacedDoubles;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.getNaturalRank;

import java.lang.reflect.Array;
import java.util.Comparator;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.quantilescommon.GenericInequalitySearch.Inequality;
import org.apache.datasketches.quantilescommon.IncludeMinMax.ItemsPair;

/**
 * The SortedView for the KllItemsSketch and the classic QuantilesItemsSketch.
 * @param <T> The sketch data type
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public class ItemsSketchSortedView<T> implements GenericSortedView<T> {
  private final T[] quantiles;
  private final long[] cumWeights; //cumulative natural weights
  private final long totalN;
  private final Comparator<? super T> comparator;
  private final Class<T> clazz;
  private final double normRankError;
  private final int numRetItems;

  /**
   * Constructor.
   * @param quantiles the given array of quantiles, which must be ordered.
   * @param cumWeights the given array of cumulative weights, which must be ordered, start with the value one, and
   * the last value must be equal to N, the total number of items updated to the sketch.
   * @param sk the underlying quantile sketch.
   */
  public ItemsSketchSortedView(
      final T[] quantiles,
      final long[] cumWeights, //or Natural Ranks
      final QuantilesGenericAPI<T> sk) {
    this.comparator = sk.getComparator();
    final ItemsPair<T> iPair =
        IncludeMinMax.includeItemsMinMax(quantiles, cumWeights, sk.getMaxItem(), sk.getMinItem(), comparator);
    this.quantiles = iPair.quantiles;
    this.cumWeights = iPair.cumWeights;
    this.totalN = sk.getN();
    this.clazz = sk.getClassOfT();
    this.normRankError = sk.getNormalizedRankError(true);
    this.numRetItems = sk.getNumRetained();
  }

  //Used for testing
  ItemsSketchSortedView(
      final T[] quantiles,
      final long[] cumWeights,
      final long totalN,
      final Comparator<? super T> comparator,
      final T maxItem,
      final T minItem,
      final Class<T> clazz,
      final double normRankError,
      final int numRetItems) {
    this.comparator = comparator;
    final ItemsPair<T> iPair =
        IncludeMinMax.includeItemsMinMax(quantiles, cumWeights, maxItem, minItem, comparator);
    this.quantiles = iPair.quantiles;
    this.cumWeights = iPair.cumWeights;
    this.totalN = totalN;
    this.clazz = clazz;
    this.normRankError = normRankError;
    this.numRetItems = numRetItems;
  }

  //end of constructors

  @Override
  public Comparator<? super T> getComparator() { return comparator; }

  @Override
  public long[] getCumulativeWeights() {
    return cumWeights.clone();
  }

  @Override
  public T getMaxItem() {
    final int top = quantiles.length - 1;
    return quantiles[top];
  }

  @Override
  public T getMinItem() {
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
  public int getMaxPartitions() {
    return (int) min(1.0 / normRankError, numRetItems / 2.0);
  }

  @Override
  public GenericPartitionBoundaries<T> getPartitionBoundariesFromPartSize(
      final long nominalPartitionSize,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(QuantilesAPI.EMPTY_MSG); }
    final long minPartSizeItems = getMinPartitionSizeItems();
    if (nominalPartitionSize < minPartSizeItems) {
      throw new SketchesArgumentException(QuantilesAPI.UNSUPPORTED_MSG
          + " The requested nominal partition size is too small for this sketch.");
    }
    final long totalN = this.totalN;
    final int numEquallySizedParts = (int) min(totalN / minPartSizeItems, getMaxPartitions());
    return getPartitionBoundariesFromNumParts(numEquallySizedParts);
  }

  @Override
  @SuppressWarnings("unchecked")
  public GenericPartitionBoundaries<T> getPartitionBoundariesFromNumParts(
      final int numEquallySizedParts,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(QuantilesAPI.EMPTY_MSG); }
    final int maxParts = getMaxPartitions();
    if (numEquallySizedParts > maxParts) {
      throw new SketchesArgumentException(QuantilesAPI.UNSUPPORTED_MSG
          + " The requested number of partitions is too large for this sketch.");
    }

    final double[] searchNormRanks = evenlySpacedDoubles(0, 1.0, numEquallySizedParts + 1);
    final int partArrLen = searchNormRanks.length;
    final T[] partQuantiles = (T[]) Array.newInstance(clazz, partArrLen);
    final long[] partNatRanks = new long[partArrLen];
    final double[] partNormRanks = new double[partArrLen];

    //compute the quantiles and natural and normalized ranks for the partition boundaries.
    for (int i = 0; i < partArrLen; i++) {
      final int index = getQuantileIndex(searchNormRanks[i], cumWeights, searchCrit);
      partQuantiles[i] = quantiles[index];
      final long cumWt = cumWeights[index];
      partNatRanks[i] = cumWt;
      partNormRanks[i] = (double)cumWt / totalN;
    }
    //Return the GPB of the complete specification of the boundaries.
    final GenericPartitionBoundaries<T> gpb = new GenericPartitionBoundaries<>(
        this.totalN,
        partQuantiles,
        partNatRanks,
        partNormRanks,
        getMaxItem(),
        getMinItem(),
        searchCrit);
    return gpb;
  } //End of getPartitionBoundaries

  @Override
  public T getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    QuantilesUtil.checkNormalizedRankBounds(rank);
    final int index = getQuantileIndex(rank, cumWeights, searchCrit);
    return quantiles[index];
  }

  private int getQuantileIndex(final double normRank, final long[] localCumWeights,
      final QuantileSearchCriteria searchCrit) {
    final int len = localCumWeights.length;
    final double naturalRank = getNaturalRank(normRank, totalN, searchCrit);
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(localCumWeights, 0, len - 1, naturalRank, crit);
    if (index == -1) { return len - 1; }
    return index;
  }

  /**
   * Gets an array of quantiles corresponding to the given array of ranks.
   * @param ranks the given array of normalized ranks
   * @param searchCrit The search criterion: either INCLUSIVE or EXCLUSIVE.
   * @return an array of quantiles corresponding to the given array of ranks.
   */
  @SuppressWarnings("unchecked")
  public T[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    final int len = ranks.length;
    final T[] quants = (T[]) Array.newInstance(clazz, len);
    for (int i = 0; i < len; i++) {
      quants[i] = getQuantile(ranks[i], searchCrit);
    }
    return quants;
  }

  @Override
  public T[] getQuantiles() {
    return quantiles.clone();
  }

  @Override
  public double getRank(final T quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    final int len = quantiles.length;
    final Inequality crit = (searchCrit == INCLUSIVE) ? Inequality.LE : Inequality.LT;
    final int index = find(quantiles,  0, len - 1, quantile, crit, comparator);
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
  public GenericSortedViewIterator<T> iterator() {
    return new GenericSortedViewIterator<>(quantiles, cumWeights);
  }

}
