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

import java.util.Arrays;

import org.apache.datasketches.DoublesSortedView;
import org.apache.datasketches.InequalitySearch;
import org.apache.datasketches.QuantileSearchCriteria;
import org.apache.datasketches.SketchesArgumentException;

/**
 * The SortedView of the KllDoublesSketch.
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public final class KllDoublesSketchSortedView implements DoublesSortedView {
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
   * Constructs this Sorted View given the sketch
   * @param sk the given KllDoublesSketch.
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

    final int numValues = srcLevels[srcNumLevels] - srcLevels[0]; //remove garbage
    values = new double[numValues];
    cumWeights = new long[numValues];
    populateFromSketch(srcValues, srcLevels, srcNumLevels, numValues);
  }

  @Override
  public double getQuantile(final double normRank, final QuantileSearchCriteria searchCrit) {
    checkRank(normRank);
    final int len = cumWeights.length;
    final long naturalRank = (int)(normRank * totalN);
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, naturalRank, crit);
    if (index == -1) {
      return Double.NaN; //EXCLUSIVE (GT) case: normRank == 1.0;
    }
    return values[index];
  }

  @Override
  public double getRank(final double value, final QuantileSearchCriteria searchCrit) {
    final int len = values.length;
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.LE : InequalitySearch.LT;
    final int index = InequalitySearch.find(values,  0, len - 1, value, crit);
    if (index == -1) {
      return 0; //LT: value <= minValue; LE: value < minValue
    }
    return (double)cumWeights[index] / totalN;
  }

  @Override
  public double[] getCDF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    validateDoubleValues(splitPoints);
    final int len = splitPoints.length + 1;
    final double[] buckets = new double[len];
    for (int i = 0; i < len - 1; i++) {
      buckets[i] = getRank(splitPoints[i], searchCrit);
    }
    buckets[len - 1] = 1.0;
    return buckets;
  }

  @Override
  public double[] getPMF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    final double[] buckets = getCDF(splitPoints, searchCrit);
    final int len = buckets.length;
    for (int i = len; i-- > 1; ) {
      buckets[i] -= buckets[i - 1];
    }
    return buckets;
  }

  @Override
  public long[] getCumulativeWeights() {
    return cumWeights.clone();
  }

  @Override
  public double[] getValues() {
    return values.clone();
  }

  @Override
  public KllDoublesSketchSortedViewIterator iterator() {
    return new KllDoublesSketchSortedViewIterator(values, cumWeights);
  }

  //populates values, cumWeights
  private void populateFromSketch(final double[] srcValues, final int[] srcLevels,
    final int srcNumLevels, final int numValues) {
    final int[] myLevels = new int[srcNumLevels + 1];
    final int offset = srcLevels[0];
    System.arraycopy(srcValues, offset, values, 0, numValues);
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
