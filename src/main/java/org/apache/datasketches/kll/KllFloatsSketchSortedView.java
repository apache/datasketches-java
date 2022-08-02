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
import static org.apache.datasketches.QuantileSearchCriteria.NON_INCLUSIVE;
import static org.apache.datasketches.QuantileSearchCriteria.NON_INCLUSIVE_STRICT;

import java.util.Arrays;

import org.apache.datasketches.InequalitySearch;
import org.apache.datasketches.QuantileSearchCriteria;
import org.apache.datasketches.SketchesArgumentException;

/**
 * The Sorted View provides a view of the data retained by the sketch that would be cumbersome to get any other way.
 * One can iterate of the contents of the sketch, but the result is not sorted.
 * Trying to use getQuantiles would be very cumbersome since one doesn't know what ranks to use to supply the
 * getQuantiles method.  Even worse, suppose it is a large sketch that has retained 1000 values from a stream of
 * millions (or billions).  One would have to execute the getQuantiles method many thousands of times, and using
 * trial & error, try to figure out what the sketch actually has retained.
 *
 * <p>The data from a Sorted view is an unbiased sample of the input stream that can be used for other kinds of
 * analysis not directly provided by the sketch.  A good example comparing two sketches using the Kolmogorov-Smirnov
 * test. One needs this sorted view for the test.</p>
 *
 * <p>This sorted view can also be used for multiple getRank and getQuantile queries once it has been created.
 * Because it takes some computational work to create this sorted view, it doesn't make sense to create this sorted view
 * just for single getRank queries.  For the first getQuantile queries, it must be created. But for all queries
 * after the first, assuming the sketch has not been updated, the getQuantile and getRank queries are very fast.</p>
 *
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
public final class KllFloatsSketchSortedView {
  private final long N;
  private final float[] values;
  private final long[] cumWeights; //comes in as individual weights, converted to cumulative natural weights

  public KllFloatsSketchSortedView(final KllFloatsSketch sk) {
    this.N = sk.getN();
    final float[] srcValues = sk.getFloatValuesArray();
    final int[] srcLevels = sk.getLevelsArray();
    final int srcNumLevels = sk.getNumLevels();

    if (!sk.isLevelZeroSorted()) {
      Arrays.sort(srcValues, srcLevels[0], srcLevels[1]);
      if (!sk.hasMemory()) { sk.setLevelZeroSorted(true); }
    }

    final int numItems = srcLevels[srcNumLevels] - srcLevels[0]; //remove garbage
    values = new float[numItems];
    cumWeights = new long[numItems];
    populateFromSketch(srcValues, srcLevels, srcNumLevels, numItems);
    sk.kllFloatsSV = this;
  }

  //populates values, cumWeights, levels, numLevels
  private void populateFromSketch(final float[] srcItems, final int[] srcLevels,
    final int srcNumLevels, final int numItems) {
    final int[] myLevels = new int[srcNumLevels + 1];
    final int offset = srcLevels[0];
    System.arraycopy(srcItems, offset, values, 0, numItems);
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
    blockyTandemMergeSort(values, cumWeights, myLevels, numLevels); //create unit weights
    KllHelper.convertToCumulative(cumWeights);
  }

  /**
   * Gets the quantile based on the given normalized rank,
   * which must be in the range [0.0, 1.0], inclusive.
   * @param normRank the given normalized rank
   * @param inclusive determines the search criterion used.
   * @return the quantile
   */
  public float getQuantile(final double normRank, final QuantileSearchCriteria inclusive) {
    final int len = cumWeights.length;
    final long rank = (int)(normRank * N);
    final InequalitySearch crit = (inclusive == INCLUSIVE) ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, rank, crit);
    if (index == -1) {
      if (inclusive == NON_INCLUSIVE_STRICT) { return Float.NaN; } //GT: normRank == 1.0;
      if (inclusive == NON_INCLUSIVE) { return values[len - 1]; }
    }
    return values[index];
  }

  /**
   * Gets the normalized rank based on the given value.
   * @param value the given value
   * @param inclusive determines the search criterion used.
   * @return the normalized rank
   */
  public double getRank(final float value, final QuantileSearchCriteria inclusive) {
    final int len = values.length;
    final InequalitySearch crit = (inclusive == INCLUSIVE) ? InequalitySearch.LE : InequalitySearch.LT;
    final int index = InequalitySearch.find(values,  0, len - 1, value, crit);
    if (index == -1) {
      return 0; //LT: value <= minValue; LE: value < minValue
    }
    return (double)cumWeights[index] / N;
  }

  /**
   * Returns an iterator for this sorted view
   * @return an iterator for this sorted view
   */
  public KllFloatsSketchSortedViewIterator iterator() {
    return new KllFloatsSketchSortedViewIterator(values, cumWeights);
  }


  //Called only from KllFloatsSketch - original search for rank
  static double getFloatRank(final KllSketch sketch, final float value, final QuantileSearchCriteria inclusive) {
    if (sketch.isEmpty()) { return Double.NaN; }
    int level = 0;
    int weight = 1;
    long total = 0;
    final float[] floatItemsArr = sketch.getFloatValuesArray();
    final int[] levelsArr = sketch.getLevelsArray();
    while (level < sketch.getNumLevels()) {
      final int fromIndex = levelsArr[level];
      final int toIndex = levelsArr[level + 1]; // exclusive
      for (int i = fromIndex; i < toIndex; i++) {
        if (inclusive == INCLUSIVE ? floatItemsArr[i] <= value : floatItemsArr[i] < value) {
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

  //Called only from KllFloatsSketch TODO rewrite using sorted view and new getRanks
  static double[] getFloatsPmfOrCdf(final KllSketch sketch, final float[] splitPoints,
      final boolean isCdf, final QuantileSearchCriteria inclusive) {
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
   * Only used for getPmfOrCdf().
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

  //Only used for getPmfOrCdf
  private static void incrementFloatBucketsSortedLevel(
      final KllSketch sketch, final int fromIndex, final int toIndex, final int weight,
      final float[] splitPoints, final double[] buckets, final QuantileSearchCriteria inclusive) {
    final float[] floatItemsArr = sketch.getFloatValuesArray();
    int i = fromIndex;
    int j = 0;
    while (i <  toIndex && j < splitPoints.length) {
      if (inclusive == INCLUSIVE ? floatItemsArr[i] <= splitPoints[j]: floatItemsArr[i] < splitPoints[j]) {
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

  //Only used for getPmfOrCdf
  private static void incrementFloatBucketsUnsortedLevel(
      final KllSketch sketch, final int fromIndex, final int toIndex, final int weight,
      final float[] splitPoints, final double[] buckets, final QuantileSearchCriteria inclusive) {
    final float[] floatItemsArr = sketch.getFloatValuesArray();
    for (int i = fromIndex; i < toIndex; i++) {
      int j;
      for (j = 0; j < splitPoints.length; j++) {
        if (inclusive == INCLUSIVE ? floatItemsArr[i] <= splitPoints[j] : floatItemsArr[i] < splitPoints[j]) {
          break;
        }
      }
      buckets[j] += weight;
    }
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
    //printAll(itemsSrc, weightsSrc, itemsDst, weightsDst, levels, startingLevel, numLevels);
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

//  @SuppressWarnings("deprecation")
//  private float approximatelyAnswerPositonalQuery(final long pos) {
//    assert pos >= 0;
//    assert pos < N;
//    final int index = KllQuantilesHelper.chunkContainingPos(cumWeights, pos);
//    return values[index];
//  }

  static void printLevels(int[] levels, int numLevels) {
    for (int i = 0; i < levels.length; i++) { print(levels[i] + ", "); }
    println("numLevels: " + numLevels);
  }

  static void printAll(
      float[] values, long[] cumWeights,
      float[] itemsDst, long[] weightsDst,
      int[] levels, int startingLevel, int numLevels) {
    //StringBuilder sb = new StringBuilder();
    println("itemSrc[] len: " + values.length);
    println("weightsSrc[] len: " + cumWeights.length);
    println("itemsDst[] len: " + itemsDst.length);
    println("weightsDst[] len: " + weightsDst.length);
    println("levels[] len: " + levels.length);
    for (int i = 0; i < levels.length; i++) { print(levels[i] + ", "); } println("");
    println("startingLevel: " + startingLevel);
    println("numLevels: " + numLevels);
    println("");
  }

  private final static boolean enablePrinting = true;

  /**
   * @param format the format
   * @param args the args
   */
  static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

  /**
   * @param o the Object to print
   */
  static final void print(final Object o) {
    if (enablePrinting) { System.out.print(o.toString()); }
  }

}
