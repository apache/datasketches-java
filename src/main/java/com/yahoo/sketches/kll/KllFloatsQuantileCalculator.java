/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.kll;

import java.util.Arrays;

import com.yahoo.sketches.QuantilesHelper;

/**
 * Data structure for answering quantile queries based on the samples from KllSketch
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
final class KllFloatsQuantileCalculator {

  private long n_;
  private float[] items_;
  private long[] weights_;
  private int[] levels_;
  private int numLevels_;

  // assumes that all levels are sorted including level 0
  KllFloatsQuantileCalculator(final float[] items, final int[] levels, final int numLevels,
      final long n) {
    n_ = n;
    final int numItems = levels[numLevels] - levels[0];
    items_ = new float[numItems];
    weights_ = new long[numItems + 1]; // one more is intentional
    levels_ = new int[numLevels + 1];
    populateFromSketch(items, levels, numLevels, numItems);
    blockyTandemMergeSort(items_, weights_, levels_, numLevels_);
    QuantilesHelper.convertToPrecedingCummulative(weights_);
  }

  float getQuantile(final double phi) {
    final long pos = QuantilesHelper.posOfPhi(phi, n_);
    return approximatelyAnswerPositonalQuery(pos);
  }

  private float approximatelyAnswerPositonalQuery(final long pos) {
    assert pos >= 0;
    assert pos < n_;
    final int index = QuantilesHelper.chunkContainingPos(weights_, pos);
    return items_[index];
  }

  private void populateFromSketch(final float[] srcItems, final int[] srcLevels,
      final int numLevels, final int numItems) {
    final int offset = srcLevels[0];
    System.arraycopy(srcItems, offset, items_, 0, numItems);
    int srcLevel = 0;
    int dstLevel = 0;
    long weight = 1;
    while (srcLevel < numLevels) {
      final int fromIndex = srcLevels[srcLevel] - offset;
      final int toIndex = srcLevels[srcLevel + 1] - offset; // exclusive
      if (fromIndex < toIndex) { // skip empty levels
        Arrays.fill(weights_, fromIndex, toIndex, weight);
        levels_[dstLevel] = fromIndex;
        levels_[dstLevel + 1] = toIndex;
        dstLevel++;
      }
      srcLevel++;
      weight *= 2;
    }
    weights_[numItems] = 0;
    numLevels_ = dstLevel;
  }

  private static void blockyTandemMergeSort(final float[] items, final long[] weights,
      final int[] levels, final int numLevels) {
    if (numLevels == 1) { return; }

    // duplicate the input in preparation for the "ping-pong" copy reduction strategy.
    final float[] itemsTmp = Arrays.copyOf(items, items.length);
    final long[] weightsTmp = Arrays.copyOf(weights, items.length); // don't need the extra one here

    blockyTandemMergeSortRecursion(itemsTmp, weightsTmp, items, weights, levels, 0, numLevels);
  }

  private static void blockyTandemMergeSortRecursion(final float[] itemsSrc, final long[] weightsSrc,
      final float[] itemsDst, final long[] weightsDst, final int[] levels, final int startingLevel,
      final int numLevels) {
    if (numLevels == 1) { return; }
    final int numLevels1 = numLevels / 2;
    final int numLevels2 = numLevels - numLevels1;
    assert numLevels1 >= 1;
    assert numLevels2 >= numLevels1;
    final int startingLevel1 = startingLevel;
    final int startingLevel2 = startingLevel + numLevels1;
    // swap roles of src and dst
    blockyTandemMergeSortRecursion(itemsDst, weightsDst, itemsSrc, weightsSrc, levels,
        startingLevel1, numLevels1);
    blockyTandemMergeSortRecursion(itemsDst, weightsDst, itemsSrc, weightsSrc, levels,
        startingLevel2, numLevels2);
    tandemMerge(itemsSrc, weightsSrc, itemsDst, weightsDst, levels, startingLevel1, numLevels1,
        startingLevel2, numLevels2);
  }

  private static void tandemMerge(final float[] itemsSrc, final long[] weightsSrc,
      final float[] itemsDst, final long[] weightsDst,
      final int[] levelStarts, final int startingLevel1, final int numLevels1,
      final int startingLevel2, final int numLevels2) {
    final int fromIndex1 = levelStarts[startingLevel1];
    final int toIndex1 = levelStarts[startingLevel1 + numLevels1]; // exclusive
    final int fromIndex2 = levelStarts[startingLevel2];
    final int toIndex2 = levelStarts[startingLevel2 + numLevels2]; // exclusive
    int iSrc1 = fromIndex1;
    int iSrc2 = fromIndex2;
    int iDst = fromIndex1;

    while ((iSrc1 < toIndex1) && (iSrc2 < toIndex2)) {
      if (itemsSrc[iSrc1] < itemsSrc[iSrc2]) {
        itemsDst[iDst] = itemsSrc[iSrc1];
        weightsDst[iDst] = weightsSrc[iSrc1];
        iSrc1++;
      } else {
        itemsDst[iDst] = itemsSrc[iSrc2];
        weightsDst[iDst] = weightsSrc[iSrc2];
        iSrc2++;
      }
      iDst++;
    }
    if (iSrc1 < toIndex1) {
      System.arraycopy(itemsSrc, iSrc1, itemsDst, iDst, toIndex1 - iSrc1);
      System.arraycopy(weightsSrc, iSrc1, weightsDst, iDst, toIndex1 - iSrc1);
    } else if (iSrc2 < toIndex2) {
      System.arraycopy(itemsSrc, iSrc2, itemsDst, iDst, toIndex2 - iSrc2);
      System.arraycopy(weightsSrc, iSrc2, weightsDst, iDst, toIndex2 - iSrc2);
    }
  }

}
