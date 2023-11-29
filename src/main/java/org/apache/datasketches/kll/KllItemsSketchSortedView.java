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

import static org.apache.datasketches.quantilescommon.GenericInequalitySearch.find;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesAPI.EMPTY_MSG;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.evenlySpacedDoubles;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.getNaturalRank;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.quantilescommon.GenericInequalitySearch.Inequality;
import org.apache.datasketches.quantilescommon.GenericPartitionBoundaries;
import org.apache.datasketches.quantilescommon.GenericSortedView;
import org.apache.datasketches.quantilescommon.GenericSortedViewIterator;
import org.apache.datasketches.quantilescommon.InequalitySearch;
import org.apache.datasketches.quantilescommon.PartitioningFeature;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesAPI;
import org.apache.datasketches.quantilescommon.QuantilesUtil;

/**
 * The SortedView of the KllItemsSketch.
 * @param <T> The sketch data type
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public class KllItemsSketchSortedView<T> implements GenericSortedView<T>, PartitioningFeature<T> {
  private final T[] quantiles;
  private final long[] cumWeights; //comes in as individual weights, converted to cumulative natural weights
  private final long totalN;
  private final Comparator<? super T> comparator;
  private final T maxItem;
  private final T minItem;
  private final Class<T> clazz;
  private final double[] normRanks;

  /**
   * Construct from elements for testing only.
   * @param quantiles sorted array of quantiles
   * @param cumWeights sorted, monotonically increasing cumulative weights.
   * @param totalN the total number of items presented to the sketch.
   * @param minItem used to extract the type of T
   * @param comparator the Comparator for type T
   */
  @SuppressWarnings("unchecked")
  KllItemsSketchSortedView(
      final T[] quantiles,
      final long[] cumWeights,
      final long totalN,
      final Comparator<? super T> comparator,
      final T maxItem,
      final T minItem) {
    this.quantiles = quantiles;
    this.cumWeights  = cumWeights;
    this.totalN = totalN;
    this.comparator = comparator;
    this.maxItem = maxItem;
    this.minItem = minItem;
    this.clazz = (Class<T>)quantiles[0].getClass();
    this.normRanks = convertCumWtsToNormRanks(cumWeights, totalN);
  }

  /**
   * Constructs this Sorted View given the sketch
   * @param sketch the given KllItemsSketch.
   */
  @SuppressWarnings("unchecked")
  KllItemsSketchSortedView(final KllItemsSketch<T> sketch) {
    if (sketch.isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    this.totalN = sketch.getN();
    final T[] srcQuantiles = sketch.getTotalItemsArray();
    final int[] srcLevels = sketch.levelsArr;
    final int srcNumLevels = sketch.getNumLevels();
    this.comparator = sketch.comparator;
    this.maxItem = sketch.getMaxItem();
    this.minItem = sketch.getMinItem();
    this.clazz = (Class<T>)sketch.serDe.getClassOfT();

    if (totalN == 0) { throw new SketchesArgumentException(EMPTY_MSG); }
    if (!sketch.isLevelZeroSorted()) {
      Arrays.sort(srcQuantiles, srcLevels[0], srcLevels[1], comparator);
      if (!sketch.hasMemory()) { sketch.setLevelZeroSorted(true); }
    }

    final int numQuantiles = srcLevels[srcNumLevels] - srcLevels[0]; //remove garbage
    quantiles = (T[]) Array.newInstance(sketch.serDe.getClassOfT(), numQuantiles);
    cumWeights = new long[numQuantiles];
    populateFromSketch(srcQuantiles, srcLevels, srcNumLevels, numQuantiles);
    this.normRanks = convertCumWtsToNormRanks(cumWeights, totalN);
  }

  //end of constructors

  @Override
  public double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    GenericSortedView.validateItems(splitPoints, comparator);
    final int len = splitPoints.length + 1;
    final double[] buckets = new double[len];
    for (int i = 0; i < len - 1; i++) {
      buckets[i] = getRank(splitPoints[i], searchCrit);
    }
    buckets[len - 1] = 1.0;
    return buckets;
  }

  @Override
  public long[] getCumulativeWeights() {
    return cumWeights.clone();
  }

  @Override
  public T getMaxItem() {
    return maxItem;
  }

  @Override
  public T getMinItem() {
    return minItem;
  }

  @Override
  public long getN() {
    return totalN;
  }

  @Override
  public double[] getNormalizedRanks() {
    return normRanks.clone();
  }

  @Override
  @SuppressWarnings("unchecked")
  public GenericPartitionBoundaries<T> getPartitionBoundaries(final int numEquallySized,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    final long totalN = this.totalN;
    final int svLen = cumWeights.length;
    //adjust ends of sortedView arrays
    cumWeights[0] = 1L;
    cumWeights[svLen - 1] = totalN;
    normRanks[0] = 1.0 / totalN;
    normRanks[svLen - 1] = 1.0;
    quantiles[0] = this.getMinItem();
    quantiles[svLen - 1] = this.getMaxItem();

    final double[] evSpNormRanks = evenlySpacedDoubles(0, 1.0, numEquallySized + 1);
    final int len = evSpNormRanks.length;
    final T[] evSpQuantiles = (T[]) Array.newInstance(clazz, len);

    final long[] evSpNatRanks = new long[len];
    for (int i = 0; i < len; i++) {
      final int index = getQuantileIndex(evSpNormRanks[i], searchCrit);
      evSpQuantiles[i] = getQuantileFromIndex(index);
      evSpNatRanks[i] = getCumWeightFromIndex(index);
    }
    final GenericPartitionBoundaries<T> gpb = new GenericPartitionBoundaries<>(
        this.totalN,
        evSpQuantiles,
        evSpNatRanks,
        evSpNormRanks,
        getMaxItem(),
        getMinItem(),
        searchCrit);
    return gpb;
  }

  @Override
  public double[] getPMF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    GenericSortedView.validateItems(splitPoints, comparator);
    final double[] buckets = getCDF(splitPoints, searchCrit);
    final int len = buckets.length;
    for (int i = len; i-- > 1; ) {
      buckets[i] -= buckets[i - 1];
    }
    return buckets;
  }

  @Override
  public T getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    QuantilesUtil.checkNormalizedRankBounds(rank);
    final int index = getQuantileIndex(rank, searchCrit);
    return getQuantileFromIndex(index);
  }

  private T getQuantileFromIndex(final int index) { return quantiles[index]; }

  private long getCumWeightFromIndex(final int index) { return cumWeights[index]; }

  private int getQuantileIndex(final double rank, final QuantileSearchCriteria searchCrit) {
    final int len = cumWeights.length;
    final double naturalRank = getNaturalRank(rank, totalN, searchCrit);
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, naturalRank, crit);
    if (index == -1) { return len - 1; }
    return index;
  }

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
  @SuppressWarnings("unchecked")
  public T[] getQuantiles() {
    final T[] quants = (T[]) Array.newInstance(minItem.getClass(), quantiles.length);
    System.arraycopy(quantiles, 0, quants, 0, quantiles.length);
    return quants;
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

  //restricted methods

  private static double[] convertCumWtsToNormRanks(final long[] cumWeights, final long totalN) {
    final int len = cumWeights.length;
    final double[] normRanks = new double[len];
    for (int i = 0; i < len; i++) { normRanks[i] = (double)cumWeights[i] / totalN; }
    return normRanks;
  }

  private void populateFromSketch(final Object[] srcQuantiles, final int[] srcLevels,
    final int srcNumLevels, final int numItems) {
    final int[] myLevels = new int[srcNumLevels + 1];
    final int offset = srcLevels[0];
    System.arraycopy(srcQuantiles, offset, quantiles, 0, numItems);
    int srcLevel = 0;
    int dstLevel = 0;
    long weight = 1;
    while (srcLevel < srcNumLevels) {
      final int fromIndex = srcLevels[srcLevel] - offset;
      final int toIndex = srcLevels[srcLevel + 1] - offset; // exclusive
      if (fromIndex < toIndex) { // if equal, skip empty level
        Arrays.fill(cumWeights, fromIndex, toIndex, weight);
        myLevels[dstLevel] = fromIndex;
        myLevels[dstLevel + 1] = toIndex;
        dstLevel++;
      }
      srcLevel++;
      weight *= 2;
    }
    final int numLevels = dstLevel;
    blockyTandemMergeSort(quantiles, cumWeights, myLevels, numLevels, comparator); //create unit weights
    KllHelper.convertToCumulative(cumWeights);
  }

  private static <T> void blockyTandemMergeSort(final Object[] quantiles, final long[] weights,
      final int[] levels, final int numLevels, final Comparator<? super T> comp) {
    if (numLevels == 1) { return; }

    // duplicate the input in preparation for the "ping-pong" copy reduction strategy.
    final Object[] quantilesTmp = Arrays.copyOf(quantiles, quantiles.length);
    final long[] weightsTmp = Arrays.copyOf(weights, quantiles.length); // don't need the extra one here

    blockyTandemMergeSortRecursion(quantilesTmp, weightsTmp, quantiles, weights, levels, 0, numLevels, comp);
  }

  private static <T> void blockyTandemMergeSortRecursion(
      final Object[] quantilesSrc, final long[] weightsSrc,
      final Object[] quantilesDst, final long[] weightsDst,
      final int[] levels, final int startingLevel, final int numLevels, final Comparator<? super T> comp) {
    if (numLevels == 1) { return; }
    final int numLevels1 = numLevels / 2;
    final int numLevels2 = numLevels - numLevels1;
    assert numLevels1 >= 1;
    assert numLevels2 >= numLevels1;
    final int startingLevel1 = startingLevel;
    final int startingLevel2 = startingLevel + numLevels1;
    // swap roles of src and dst
    blockyTandemMergeSortRecursion(
        quantilesDst, weightsDst,
        quantilesSrc, weightsSrc,
        levels, startingLevel1, numLevels1, comp);
    blockyTandemMergeSortRecursion(
        quantilesDst, weightsDst,
        quantilesSrc, weightsSrc,
        levels, startingLevel2, numLevels2, comp);
    tandemMerge(
        quantilesSrc, weightsSrc,
        quantilesDst, weightsDst,
        levels,
        startingLevel1, numLevels1,
        startingLevel2, numLevels2, comp);
  }

  @SuppressWarnings("unchecked")
  private static <T> void tandemMerge(
      final Object[] quantilesSrc, final long[] weightsSrc,
      final Object[] quantilesDst, final long[] weightsDst,
      final int[] levelStarts,
      final int startingLevel1, final int numLevels1,
      final int startingLevel2, final int numLevels2, final Comparator<? super T> comp) {
    final int fromIndex1 = levelStarts[startingLevel1];
    final int toIndex1 = levelStarts[startingLevel1 + numLevels1]; // exclusive
    final int fromIndex2 = levelStarts[startingLevel2];
    final int toIndex2 = levelStarts[startingLevel2 + numLevels2]; // exclusive
    int iSrc1 = fromIndex1;
    int iSrc2 = fromIndex2;
    int iDst = fromIndex1;

    while (iSrc1 < toIndex1 && iSrc2 < toIndex2) {
      if (Util.lt((T) quantilesSrc[iSrc1], (T) quantilesSrc[iSrc2], comp)) {
        quantilesDst[iDst] = quantilesSrc[iSrc1];
        weightsDst[iDst] = weightsSrc[iSrc1];
        iSrc1++;
      } else {
        quantilesDst[iDst] = quantilesSrc[iSrc2];
        weightsDst[iDst] = weightsSrc[iSrc2];
        iSrc2++;
      }
      iDst++;
    }
    if (iSrc1 < toIndex1) {
      System.arraycopy(quantilesSrc, iSrc1, quantilesDst, iDst, toIndex1 - iSrc1);
      System.arraycopy(weightsSrc, iSrc1, weightsDst, iDst, toIndex1 - iSrc1);
    } else if (iSrc2 < toIndex2) {
      System.arraycopy(quantilesSrc, iSrc2, quantilesDst, iDst, toIndex2 - iSrc2);
      System.arraycopy(weightsSrc, iSrc2, weightsDst, iDst, toIndex2 - iSrc2);
    }
  }

}
