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

import java.util.Arrays;

import org.apache.datasketches.QuantilesHelper;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesStateException;

/**
 * Data structure for answering quantile queries based on the samples from KllSketch
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
public final class KllFloatsSketchSortedView {

  private final long n_;
  private final float[] items_;
  private final long[] weights_; //comes in as weights, converted to cumulative weights
  private final int[] levels_;
  private int numLevels_;

  // assumes that all levels are sorted including level 0
  KllFloatsSketchSortedView(final float[] items, final int[] levels, final int numLevels,
      final long n, final boolean cumulative, final boolean inclusive) {
    n_ = n;
    final int numItems = levels[numLevels] - levels[0];
    items_ = new float[numItems];
    weights_ = new long[numItems + 1]; // one more is intentional
    levels_ = new int[numLevels + 1];
    populateFromSketch(items, levels, numLevels, numItems);
    blockyTandemMergeSort(items_, weights_, levels_, numLevels_);
    if (cumulative) {
      QuantilesHelper.convertToPrecedingCumulative(weights_, inclusive);
    }
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
      if (fromIndex < toIndex) { // if equal, skip empty level
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

  static KllFloatsSketchSortedView getFloatsSortedView(final KllSketch sketch,
      final boolean cumulative, final boolean inclusive) {
    final float[] skFloatItemsArr = sketch.getFloatItemsArray();
    final int[] skLevelsArr = sketch.getLevelsArray();

    if (!sketch.isLevelZeroSorted()) {
      Arrays.sort(skFloatItemsArr, skLevelsArr[0], skLevelsArr[1]);
      if (!sketch.hasMemory()) { sketch.setLevelZeroSorted(true); }
    }
    return new KllFloatsSketchSortedView(skFloatItemsArr, skLevelsArr, sketch.getNumLevels(), sketch.getN(),
        cumulative, inclusive);
  }

  //For testing only. Allows testing of getQuantile without a sketch. NOT USED
  KllFloatsSketchSortedView(final float[] items, final long[] weights, final long n) {
    n_ = n;
    items_ = items;
    weights_ = weights; //must be size of items + 1
    levels_ = null;  //not used by test
    numLevels_ = 0;  //not used by test
  }

  public float getQuantile(final double rank) {
    if (weights_[weights_.length - 1] < n_) {
      throw new SketchesStateException("getQuantile must be used with cumulative view only");
    }
    final long pos = QuantilesHelper.posOfRank(rank, n_);
    return approximatelyAnswerPositonalQuery(pos);
  }

  public KllFloatsSketchSortedViewIterator iterator() {
    return new KllFloatsSketchSortedViewIterator(items_, weights_);
  }


  //Called only from KllFloatsSketch
  static double getFloatRank(final KllSketch sketch, final float value, final boolean inclusive) {
    if (sketch.isEmpty()) { return Double.NaN; }
    int level = 0;
    int weight = 1;
    long total = 0;
    final float[] floatItemsArr = sketch.getFloatItemsArray();
    final int[] levelsArr = sketch.getLevelsArray();
    while (level < sketch.getNumLevels()) {
      final int fromIndex = levelsArr[level];
      final int toIndex = levelsArr[level + 1]; // exclusive
      for (int i = fromIndex; i < toIndex; i++) {
        if (inclusive ? floatItemsArr[i] <= value : floatItemsArr[i] < value) {
          total += weight;
        } else if (level > 0 || sketch.isLevelZeroSorted()) {
          break; // levels above 0 are sorted, no point comparing further
        }
      }
      level++;
      weight *= 2;
    }
    return (double) total / sketch.getN();
  }

  //Called only from KllFloatsSketch
  static double[] getFloatsPmfOrCdf(final KllSketch sketch, final float[] splitPoints,
      final boolean isCdf, final boolean inclusive) {
    if (sketch.isEmpty()) { return null; }
    validateFloatValues(splitPoints);
    final double[] buckets = new double[splitPoints.length + 1];
    final int numLevels = sketch.getNumLevels();
    final int[] levelsArr = sketch.getLevelsArray();
    int level = 0;
    int weight = 1;
    while (level < numLevels) {
      final int fromIndex = levelsArr[level];
      final int toIndex = levelsArr[level + 1]; // exclusive
      if (level == 0 && !sketch.isLevelZeroSorted()) {
        incrementFloatBucketsUnsortedLevel(sketch, fromIndex, toIndex, weight, splitPoints, buckets, inclusive);
      } else {
        incrementFloatBucketsSortedLevel(sketch, fromIndex, toIndex, weight, splitPoints, buckets, inclusive);
      }
      level++;
      weight *= 2;
    }
    // normalize and, if CDF, convert to cumulative
    if (isCdf) {
      double subtotal = 0;
      for (int i = 0; i < buckets.length; i++) {
        subtotal += buckets[i];
        buckets[i] = subtotal / sketch.getN();
      }
    } else {
      for (int i = 0; i < buckets.length; i++) {
        buckets[i] /= sketch.getN();
      }
    }
    return buckets;
  }

  /**
   * Checks the sequential validity of the given array of float values.
   * They must be unique, monotonically increasing and not NaN.
   * @param values the given array of values
   */
  private static void validateFloatValues(final float[] values) {
    for (int i = 0; i < values.length; i++) {
      if (!Float.isFinite(values[i])) {
        throw new SketchesArgumentException("Values must be finite");
      }
      if (i < values.length - 1 && values[i] >= values[i + 1]) {
        throw new SketchesArgumentException(
          "Values must be unique and monotonically increasing");
      }
    }
  }

  private static void incrementFloatBucketsSortedLevel(
      final KllSketch sketch, final int fromIndex, final int toIndex, final int weight,
      final float[] splitPoints, final double[] buckets, final boolean inclusive) {
    final float[] floatItemsArr = sketch.getFloatItemsArray();
    int i = fromIndex;
    int j = 0;
    while (i <  toIndex && j < splitPoints.length) {
      if (inclusive ? floatItemsArr[i] <= splitPoints[j]: floatItemsArr[i] < splitPoints[j]) {
        buckets[j] += weight; // this sample goes into this bucket
        i++; // move on to next sample and see whether it also goes into this bucket
      } else {
        j++; // no more samples for this bucket
      }
    }
    // now either i == toIndex (we are out of samples), or
    // j == numSplitPoints (we are out of buckets, but there are more samples remaining)
    // we only need to do something in the latter case
    if (j == splitPoints.length) {
      buckets[j] += weight * (toIndex - i);
    }
  }

  private static void incrementFloatBucketsUnsortedLevel(
      final KllSketch sketch, final int fromIndex, final int toIndex, final int weight,
      final float[] splitPoints, final double[] buckets, final boolean inclusive) {
    final float[] floatItemsArr = sketch.getFloatItemsArray();
    for (int i = fromIndex; i < toIndex; i++) {
      int j;
      for (j = 0; j < splitPoints.length; j++) {
        if (inclusive ? floatItemsArr[i] <= splitPoints[j] : floatItemsArr[i] < splitPoints[j]) {
          break;
        }
      }
      buckets[j] += weight;
    }
  }

  //Called only from KllFloatsSketch
  static float getFloatsQuantile(final KllSketch sketch, final double fraction, final boolean inclusive) {
    if (sketch.isEmpty()) { return Float.NaN; }
    if (fraction < 0.0 || fraction > 1.0) {
      throw new SketchesArgumentException("Fraction cannot be less than zero nor greater than 1.0");
    }
    final KllFloatsSketchSortedView kllFSV = KllFloatsSketchSortedView.getFloatsSortedView(sketch, true, inclusive);
    return kllFSV.getQuantile(fraction);
  }

  //Called only from KllFloatsSketch
  static float[] getFloatsQuantiles(final KllSketch sketch, final double[] fractions, final boolean inclusive) {
    if (sketch.isEmpty()) { return null; }
    KllFloatsSketchSortedView kllFSV = null;
    final float[] quantiles = new float[fractions.length];
    for (int i = 0; i < fractions.length; i++) { //check if fraction are in [0, 1]
      final double fraction = fractions[i];
      if (fraction < 0.0 || fraction > 1.0) {
        throw new SketchesArgumentException("Fraction cannot be less than zero nor greater than 1.0");
      }
      if (kllFSV == null) {
        kllFSV = KllFloatsSketchSortedView.getFloatsSortedView(sketch, true, inclusive);
      }
      quantiles[i] = kllFSV.getQuantile(fraction);
    }
    return quantiles;
  }


  private static void blockyTandemMergeSort(final float[] items, final long[] weights,
      final int[] levels, final int numLevels) {
    if (numLevels == 1) { return; }

    // duplicate the input in preparation for the "ping-pong" copy reduction strategy.
    final float[] itemsTmp = Arrays.copyOf(items, items.length);
    final long[] weightsTmp = Arrays.copyOf(weights, items.length); // don't need the extra one here

    blockyTandemMergeSortRecursion(itemsTmp, weightsTmp, items, weights, levels, 0, numLevels);
  }

  private static void blockyTandemMergeSortRecursion(
      final float[] itemsSrc, final long[] weightsSrc,
      final float[] itemsDst, final long[] weightsDst,
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
        itemsDst, weightsDst,
        itemsSrc, weightsSrc,
        levels, startingLevel1, numLevels1);
    blockyTandemMergeSortRecursion(
        itemsDst, weightsDst,
        itemsSrc, weightsSrc,
        levels, startingLevel2, numLevels2);
    tandemMerge(
        itemsSrc, weightsSrc,
        itemsDst, weightsDst,
        levels,
        startingLevel1, numLevels1,
        startingLevel2, numLevels2);
  }

  private static void tandemMerge(
      final float[] itemsSrc, final long[] weightsSrc,
      final float[] itemsDst, final long[] weightsDst,
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

  private float approximatelyAnswerPositonalQuery(final long pos) {
    assert pos >= 0;
    assert pos < n_;
    final int index = QuantilesHelper.chunkContainingPos(weights_, pos);
    return items_[index];
  }

}
