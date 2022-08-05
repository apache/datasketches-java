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
 * @author Lee Rhodes
 */
public final class KllDoublesSketchSortedView {

  private final double[] values;
  private final long[] cumWeights; //comes in as individual weights, converted to cumulative natural weights
  private final long totalN;

  /**
   * Construct from elements for testing.
   * @param values sorted array of values
   * @param cumWeights sorted, monotonically increasing cumulative weights.
   * @param totalN the total number of values presented to the sketch.
   */
  KllDoublesSketchSortedView(final double[] values, final long[] cumWeights, final long totalN) {
    this.values = values;
    this.cumWeights  = cumWeights;
    this.totalN = totalN;
  }

  /**
   * Constructs the Sorted View given the sketch
   * @param sk the given KllFloatsSketch.
   */
  public KllDoublesSketchSortedView(final KllDoublesSketch sk) {
    this.totalN = sk.getN();
    final double[] srcValues = sk.getDoubleValuesArray();
    final int[] srcLevels = sk.getLevelsArray();
    final int srcNumLevels = sk.getNumLevels();

    if (!sk.isLevelZeroSorted()) {
      Arrays.sort(srcValues, srcLevels[0], srcLevels[1]);
      if (!sk.hasMemory()) { sk.setLevelZeroSorted(true); }
    }

    final int numItems = srcLevels[srcNumLevels] - srcLevels[0]; //remove garbage
    values = new double[numItems];
    cumWeights = new long[numItems];
    populateFromSketch(srcValues, srcLevels, srcNumLevels, numItems);
    sk.kllDoublesSV = this;
  }

  //populates values, cumWeights
  private void populateFromSketch(final double[] srcItems, final int[] srcLevels,
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
  public double getQuantile(final double normRank, final QuantileSearchCriteria inclusive) {
    checkRank(normRank);
    final int len = cumWeights.length;
    final long naturalRank = (int)(normRank * totalN);
    final InequalitySearch crit = (inclusive == INCLUSIVE) ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, naturalRank, crit);
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
  public double getRank(final double value, final QuantileSearchCriteria inclusive) {
    final int len = values.length;
    final InequalitySearch crit = (inclusive == INCLUSIVE) ? InequalitySearch.LE : InequalitySearch.LT;
    final int index = InequalitySearch.find(values,  0, len - 1, value, crit);
    if (index == -1) {
      return 0; //LT: value <= minValue; LE: value < minValue
    }
    return (double)cumWeights[index] / totalN;
  }

  /**
   * Returns an iterator for this sorted view
   * @return an iterator for this sorted view
   */
  public KllDoublesSketchSortedViewIterator iterator() {
    return new KllDoublesSketchSortedViewIterator(values, cumWeights);
  }

  /**
   * Returns an array of fractional intervals of the distribution represented by the sketch as a CDF or PMF.
   * @param splitPoints the given array of splitPoints. These is a sorted, unique, monotonic array of float
   * values in the range of (minValue, maxValue). This array must not include either the minValue or the maxValue.
   * The returned array will have one extra interval representing the very top of the distribution.
   * @param isCdf if true, a CDF will be returned, otherwise, a PMF will be returned.
   * @param inclusive if INCLUSIVE, each interval within the distribution will include its top value and exclude its
   * bottom value. Otherwise, it will be the reverse.  The only exception is that the top portion will always include
   * the top value retained by the sketch.
   * @return an array of fractional portions of the distribution represented by the sketch as a CDF or PMF.
   */
  public double[] getPmfOrCdf(final double[] splitPoints, final boolean isCdf, final QuantileSearchCriteria inclusive) {
    validateDoubleValues(splitPoints);
    final int len = splitPoints.length + 1;
    final double[] buckets = new double[len];
    for (int i = 0; i < len - 1; i++) {
      buckets[i] = getRank(splitPoints[i], inclusive);
    }
    buckets[len - 1] = 1.0;
    if (isCdf) { return buckets; }
    for (int i = len; i-- > 1;) {
      buckets[i] -= buckets[i - 1];
    }
    return buckets;
  }

  private static void blockyTandemMergeSort(final double[] values, final long[] weights,
      final int[] levels, final int numLevels) {
    if (numLevels == 1) { return; }

    // duplicate the input in preparation for the "ping-pong" copy reduction strategy.
    final double[] valuesTmp = Arrays.copyOf(values, values.length);
    final long[] weightsTmp = Arrays.copyOf(weights, values.length); // don't need the extra one here

    blockyTandemMergeSortRecursion(valuesTmp, weightsTmp, values, weights, levels, 0, numLevels);
  }

  private static void blockyTandemMergeSortRecursion(
      final double[] valuesSrc, final long[] weightsSrc,
      final double[] valuesDst, final long[] weightsDst,
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
        valuesDst, weightsDst,
        valuesSrc, weightsSrc,
        levels, startingLevel1, numLevels1);
    blockyTandemMergeSortRecursion(
        valuesDst, weightsDst,
        valuesSrc, weightsSrc,
        levels, startingLevel2, numLevels2);
    tandemMerge(
        valuesSrc, weightsSrc,
        valuesDst, weightsDst,
        levels,
        startingLevel1, numLevels1,
        startingLevel2, numLevels2);
  }

  private static void tandemMerge(
      final double[] valuesSrc, final long[] weightsSrc,
      final double[] valuesDst, final long[] weightsDst,
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
      if (valuesSrc[iSrc1] < valuesSrc[iSrc2]) {
        valuesDst[iDst] = valuesSrc[iSrc1];
        weightsDst[iDst] = weightsSrc[iSrc1];
        iSrc1++;
      } else {
        valuesDst[iDst] = valuesSrc[iSrc2];
        weightsDst[iDst] = weightsSrc[iSrc2];
        iSrc2++;
      }
      iDst++;
    }
    if (iSrc1 < toIndex1) {
      System.arraycopy(valuesSrc, iSrc1, valuesDst, iDst, toIndex1 - iSrc1);
      System.arraycopy(weightsSrc, iSrc1, weightsDst, iDst, toIndex1 - iSrc1);
    } else if (iSrc2 < toIndex2) {
      System.arraycopy(valuesSrc, iSrc2, valuesDst, iDst, toIndex2 - iSrc2);
      System.arraycopy(weightsSrc, iSrc2, weightsDst, iDst, toIndex2 - iSrc2);
    }
  }

  private static final void checkRank(final double rank) {
    if (rank < 0.0 || rank > 1.0) {
      throw new SketchesArgumentException(
          "A normalized rank " + rank + " cannot be less than zero nor greater than 1.0");
    }
  }

  /**
   * Checks the sequential validity of the given array of splitpoints as doubles.
   * They must be unique, monotonically increasing and not NaN.
   * Only used for getPmfOrCdf().
   * @param values the given array of values
   */
  private static void validateDoubleValues(final double[] values) {
    for (int i = 0; i < values.length; i++) {
      if (!Double.isFinite(values[i])) {
        throw new SketchesArgumentException("Values must be finite");
      }
      if (i < values.length - 1 && values[i] >= values[i + 1]) {
        throw new SketchesArgumentException(
          "Values must be unique and monotonically increasing");
      }
    }
  }

}
