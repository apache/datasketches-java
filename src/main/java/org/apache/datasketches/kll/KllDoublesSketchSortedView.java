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

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.THROWS_EMPTY;

import java.util.Arrays;

import org.apache.datasketches.quantilescommon.DoublesSortedView;
import org.apache.datasketches.quantilescommon.InequalitySearch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesUtil;

/**
 * The SortedView of the KllDoublesSketch.
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public final class KllDoublesSketchSortedView implements DoublesSortedView {
  private final double[] quantiles;
  private final long[] cumWeights; //comes in as individual weights, converted to cumulative natural weights
  private final long totalN;

  /**
   * Construct from elements for testing.
   * @param quantiles sorted array of quantiles
   * @param cumWeights sorted, monotonically increasing cumulative weights.
   * @param totalN the total number of items presented to the sketch.
   */
  KllDoublesSketchSortedView(final double[] quantiles, final long[] cumWeights, final long totalN) {
    this.quantiles = quantiles;
    this.cumWeights  = cumWeights;
    this.totalN = totalN;
  }

  /**
   * Constructs this Sorted View given the sketch
   * @param sk the given KllDoublesSketch.
   */
  public KllDoublesSketchSortedView(final KllDoublesSketch sk) {
    this.totalN = sk.getN();
    final double[] srcQuantiles = sk.getDoubleItemsArray();
    final int[] srcLevels = sk.levelsArr;
    final int srcNumLevels = sk.getNumLevels();

    if (!sk.isLevelZeroSorted()) {
      Arrays.sort(srcQuantiles, srcLevels[0], srcLevels[1]);
      if (!sk.hasMemory()) { sk.setLevelZeroSorted(true); }
    }

    final int numQuantiles = srcLevels[srcNumLevels] - srcLevels[0]; //remove garbage
    quantiles = new double[numQuantiles];
    cumWeights = new long[numQuantiles];
    populateFromSketch(srcQuantiles, srcLevels, srcNumLevels, numQuantiles);
  }

  @Override
  public double getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    QuantilesUtil.checkNormalizedRankBounds(rank);
    final int len = cumWeights.length;
    final long naturalRank = (searchCrit == INCLUSIVE)
        ? (long)Math.ceil(rank * totalN) : (long)Math.floor(rank * totalN);
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, naturalRank, crit);
    if (index == -1) {
      return quantiles[quantiles.length - 1]; //EXCLUSIVE (GT) case: normRank == 1.0;
    }
    return quantiles[index];
  }

  @Override
  public double getRank(final double quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    final int len = quantiles.length;
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.LE : InequalitySearch.LT;
    final int index = InequalitySearch.find(quantiles,  0, len - 1, quantile, crit);
    if (index == -1) {
      return 0; //EXCLUSIVE (LT) case: quantile <= minQuantile; INCLUSIVE (LE) case: quantile < minQuantile
    }
    return (double)cumWeights[index] / totalN;
  }

  @Override
  public long[] getCumulativeWeights() {
    return cumWeights.clone();
  }

  @Override
  public double[] getQuantiles() {
    return quantiles.clone();
  }

  @Override
  public boolean isEmpty() {
    return totalN == 0;
  }

  @Override
  public KllDoublesSketchSortedViewIterator iterator() {
    return new KllDoublesSketchSortedViewIterator(quantiles, cumWeights);
  }

  //restricted methods

  private void populateFromSketch(final double[] srcQuantiles, final int[] srcLevels,
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
    blockyTandemMergeSort(quantiles, cumWeights, myLevels, numLevels); //create unit weights
    KllHelper.convertToCumulative(cumWeights);
  }

  private static void blockyTandemMergeSort(final double[] quantiles, final long[] weights,
      final int[] levels, final int numLevels) {
    if (numLevels == 1) { return; }

    // duplicate the input in preparation for the "ping-pong" copy reduction strategy.
    final double[] quantilesTmp = Arrays.copyOf(quantiles, quantiles.length);
    final long[] weightsTmp = Arrays.copyOf(weights, quantiles.length); // don't need the extra one

    blockyTandemMergeSortRecursion(quantilesTmp, weightsTmp, quantiles, weights, levels, 0, numLevels);
  }

  private static void blockyTandemMergeSortRecursion(
      final double[] quantilesSrc, final long[] weightsSrc,
      final double[] quantilesDst, final long[] weightsDst,
      final int[] levels, final int startingLevel, final int numLevels) {
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
        levels, startingLevel1, numLevels1);
    blockyTandemMergeSortRecursion(
        quantilesDst, weightsDst,
        quantilesSrc, weightsSrc,
        levels, startingLevel2, numLevels2);
    tandemMerge(
        quantilesSrc, weightsSrc,
        quantilesDst, weightsDst,
        levels,
        startingLevel1, numLevels1,
        startingLevel2, numLevels2);
  }

  private static void tandemMerge(
      final double[] quantilesSrc, final long[] weightsSrc,
      final double[] quantilesDst, final long[] weightsDst,
      final int[] levelStarts,
      final int startingLevel1, final int numLevels1,
      final int startingLevel2, final int numLevels2) {
    final int fromIndex1 = levelStarts[startingLevel1];
    final int toIndex1 = levelStarts[startingLevel1 + numLevels1]; // exclusive
    final int fromIndex2 = levelStarts[startingLevel2];
    final int toIndex2 = levelStarts[startingLevel2 + numLevels2]; // exclusive
    int iSrc1 = fromIndex1;
    int iSrc2 = fromIndex2;
    int iDst = fromIndex1;

    while (iSrc1 < toIndex1 && iSrc2 < toIndex2) {
      if (quantilesSrc[iSrc1] < quantilesSrc[iSrc2]) {
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
