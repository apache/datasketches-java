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

import static org.apache.datasketches.kll.KllItemsHelper.lt;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.THROWS_EMPTY;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.datasketches.quantilescommon.GenericInequalitySearch;
import org.apache.datasketches.quantilescommon.GenericInequalitySearch.Inequality;
import org.apache.datasketches.quantilescommon.GenericSortedView;
import org.apache.datasketches.quantilescommon.GenericSortedViewIterator;
import org.apache.datasketches.quantilescommon.InequalitySearch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesUtil;

/**
 * The SortedView of the KllItemsSketch.
 * @param <T> The sketch data type
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
@SuppressWarnings("unchecked")
public class KllItemsSketchSortedView<T> implements GenericSortedView<T> {
  private final Object[] quantiles;
  private final long[] cumWeights; //comes in as individual weights, converted to cumulative natural weights
  private final long totalN;
  private final Comparator<? super T> comp;

  /**
   * Construct from elements for testing.
   * @param quantiles sorted array of quantiles
   * @param cumWeights sorted, monotonically increasing cumulative weights.
   * @param totalN the total number of items presented to the sketch.
   * @param T the given type
   */
  KllItemsSketchSortedView(final T[] quantiles, final long[] cumWeights, final long totalN,
      final Comparator<? super T> comparator) {
    this.quantiles = quantiles;
    this.cumWeights  = cumWeights;
    this.totalN = totalN;
    this.comp = comparator;
  }

  /**
   * Constructs this Sorted View given the sketch
   * @param sk the given KllItemsSketch.
   */
  public KllItemsSketchSortedView(final KllItemsSketch<T> sk) {
    this.totalN = sk.getN();
    final Object[] srcQuantiles = sk.getTotalItemsArray();
    final int[] srcLevels = sk.levelsArr;
    final int srcNumLevels = sk.getNumLevels();
    this.comp = sk.comparator;

    if (!sk.isLevelZeroSorted()) {
      Arrays.sort((T[])srcQuantiles, srcLevels[0], srcLevels[1], sk.comparator);
      if (!sk.hasMemory()) { sk.setLevelZeroSorted(true); }
    }

    final int numQuantiles = srcLevels[srcNumLevels] - srcLevels[0]; //remove garbage
    quantiles = new Object[numQuantiles];
    cumWeights = new long[numQuantiles];
    populateFromSketch(srcQuantiles, srcLevels, srcNumLevels, numQuantiles);
  }

  @Override //implemented here because it needs the comparator
  public double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    GenericSortedView.validateItems(splitPoints, comp);
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

  @Override //implemented here because it needs the comparator
  public double[] getPMF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    GenericSortedView.validateItems(splitPoints, comp);
    final double[] buckets = getCDF(splitPoints, searchCrit);
    final int len = buckets.length;
    for (int i = len; i-- > 1; ) {
      buckets[i] -= buckets[i - 1];
    }
    return buckets;
  }

  @Override
  public T getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    QuantilesUtil.checkNormalizedRankBounds(rank);
    final int len = cumWeights.length;
    final long naturalRank = (searchCrit == INCLUSIVE)
        ? (long)Math.ceil(rank * totalN) : (long)Math.floor(rank * totalN);
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, naturalRank, crit);
    if (index == -1) {
      return (T) quantiles[quantiles.length - 1]; //EXCLUSIVE (GT) case: normRank == 1.0;
    }
    return (T) quantiles[index];
  }

  @Override
  public T[] getQuantiles() {
    return (T[])quantiles.clone();
  }

  @Override
  public double getRank(final T quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    final int len = quantiles.length;
    final Inequality crit = (searchCrit == INCLUSIVE) ? Inequality.LE : Inequality.LT;
    final int index = GenericInequalitySearch.find((T[])quantiles,  0, len - 1, quantile, crit, comp);
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
  public KllItemsSketchSortedViewIterator<T> iterator() {
    return new KllItemsSketchSortedViewIterator<T>((T[])quantiles, cumWeights);
  }

  //restricted methods

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
    blockyTandemMergeSort(quantiles, cumWeights, myLevels, numLevels, comp); //create unit weights
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
      if (lt((T) quantilesSrc[iSrc1], (T) quantilesSrc[iSrc2], comp)) {
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

  /**
   * Iterator over KllItemsSketchSortedView.
   * @param <T> type of quantile (item)
   */
  public static class KllItemsSketchSortedViewIterator<T> extends GenericSortedViewIterator<T> {

    KllItemsSketchSortedViewIterator(final T[] quantiles, final long[] cumWeights) {
      super(quantiles, cumWeights);
    }
  }

}
