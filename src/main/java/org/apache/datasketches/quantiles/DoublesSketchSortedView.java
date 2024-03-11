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

package org.apache.datasketches.quantiles;

import static java.lang.System.arraycopy;
import static org.apache.datasketches.quantiles.DoublesSketchAccessor.BB_LVL_IDX;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesAPI.EMPTY_MSG;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.getNaturalRank;

import java.util.Arrays;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.quantilescommon.DoublesSortedView;
import org.apache.datasketches.quantilescommon.DoublesSortedViewIterator;
import org.apache.datasketches.quantilescommon.InequalitySearch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesUtil;

/**
 * The SortedView of the Classic Quantiles DoublesSketch.
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public final class DoublesSketchSortedView implements DoublesSortedView {
  private final double[] quantiles;
  private final long[] cumWeights; //comes in as individual weights, converted to cumulative natural weights
  private final long totalN;
  private final double maxItem;
  private final double minItem;

  /**
   * Construct from elements, also used in testing.
   * @param quantiles sorted array of quantiles
   * @param cumWeights sorted, monotonically increasing cumulative weights.
   * @param totalN the total number of items presented to the sketch.
   * @param maxItem of type double
   * @param minItem of type double
   */
  DoublesSketchSortedView(final double[] quantiles, final long[] cumWeights, final long totalN,
      final double maxItem, final double minItem) {
    this.quantiles = quantiles;
    this.cumWeights  = cumWeights;
    this.totalN = totalN;
    this.maxItem = maxItem;
    this.minItem = minItem;
  }

  /**
   * Constructs this Sorted View given the sketch
   * @param sketch the given Classic Quantiles DoublesSketch
   */
  public DoublesSketchSortedView(final DoublesSketch sketch) {
    if (sketch.isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    this.totalN = sketch.getN();
    this.maxItem = sketch.getMaxItem();
    this.minItem = sketch.getMinItem();
    final int k = sketch.getK();
    final int numQuantiles = sketch.getNumRetained();
    quantiles = new double[numQuantiles];
    cumWeights = new long[numQuantiles];
    final DoublesSketchAccessor sketchAccessor = DoublesSketchAccessor.wrap(sketch);

    // Populate from DoublesSketch:
    //  copy over the "levels" and then the base buffer, all with appropriate weights
    populateFromDoublesSketch(k, totalN, sketch.getBitPattern(), sketchAccessor, quantiles, cumWeights);

    // Sort the first "numSamples" slots of the two arrays in tandem,
    //  taking advantage of the already sorted blocks of length k
    blockyTandemMergeSort(quantiles, cumWeights, numQuantiles, k);
   if (convertToCumulative(cumWeights) != totalN) {
     throw new SketchesStateException("Sorted View is misconfigured. TotalN does not match cumWeights.");
   }
  }

  @Override
  public long[] getCumulativeWeights() {
    return cumWeights.clone();
  }

  @Override
  public double getMaxItem() {
    return maxItem;
  }

  @Override
  public double getMinItem() {
    return minItem;
  }

  @Override
  public long getN() {
    return totalN;
  }

  @Override
  public double getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(EMPTY_MSG); }
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
  public double getRank(final double quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(EMPTY_MSG); }
    final int len = quantiles.length;
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.LE : InequalitySearch.LT;
    final int index = InequalitySearch.find(quantiles,  0, len - 1, quantile, crit);
    if (index == -1) {
      return 0; //EXCLUSIVE (LT) case: quantile <= minQuantile; INCLUSIVE (LE) case: quantile < minQuantile
    }
    return (double)cumWeights[index] / totalN;
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
  public DoublesSortedViewIterator iterator() {
    return new DoublesSortedViewIterator(quantiles, cumWeights);
  }

  //restricted methods

  /**
   * Populate the arrays and registers from a DoublesSketch
   * @param k K parameter of the sketch
   * @param n The current size of the stream
   * @param bitPattern the bit pattern for valid log levels
   * @param sketchAccessor A DoublesSketchAccessor around the sketch
   * @param quantilesArr the consolidated array of all items from the sketch
   * @param cumWtsArr populates this array with the raw individual weights from the sketch,
   * it will be cumulative later.
   */
  private final static void populateFromDoublesSketch(
          final int k, final long n, final long bitPattern,
          final DoublesSketchAccessor sketchAccessor,
          final double[] quantilesArr, final long[] cumWtsArr) {
    long weight = 1;
    int nxt = 0;
    long bits = bitPattern;
    assert bits == (n / (2L * k)); // internal consistency check
    for (int lvl = 0; bits != 0L; lvl++, bits >>>= 1) {
      weight *= 2;
      if ((bits & 1L) > 0L) {
        sketchAccessor.setLevel(lvl);
        for (int i = 0; i < sketchAccessor.numItems(); i++) {
          quantilesArr[nxt] = sketchAccessor.get(i);
          cumWtsArr[nxt] = weight;
          nxt++;
        }
      }
    }

    weight = 1; //NOT a mistake! We just copied the highest level; now we need to copy the base buffer
    final int startOfBaseBufferBlock = nxt;

    // Copy BaseBuffer over, along with weight = 1
    sketchAccessor.setLevel(BB_LVL_IDX);
    for (int i = 0; i < sketchAccessor.numItems(); i++) {
      quantilesArr[nxt] = sketchAccessor.get(i);
      cumWtsArr[nxt] = weight;
      nxt++;
    }
    assert nxt == quantilesArr.length;

    // Must sort the items that came from the base buffer.
    // Don't need to sort the corresponding weights because they are all the same.
    final int numSamples = nxt;
    Arrays.sort(quantilesArr, startOfBaseBufferBlock, numSamples);
  }

  /**
   * blockyTandemMergeSort() is an implementation of top-down merge sort specialized
   * for the case where the input contains successive equal-length blocks
   * that have already been sorted, so that only the top part of the
   * merge tree remains to be executed. Also, two arrays are sorted in tandem,
   * as discussed below.
   * @param quantiles array of quantiles
   * @param cumWts array of cum weights
   * @param arrLen length of quantiles array and cumWts array
   * @param blkSize size of internal sorted blocks
   */
  //used by this and UtilTest
  static void blockyTandemMergeSort(final double[] quantiles, final long[] cumWts, final int arrLen,
      final int blkSize) {
    assert blkSize >= 1;
    if (arrLen <= blkSize) { return; }
    int numblks = arrLen / blkSize;
    if ((numblks * blkSize) < arrLen) { numblks += 1; }
    assert ((numblks * blkSize) >= arrLen);

    // duplication of the input arrays is preparation for the "ping-pong" copy reduction strategy.
    final double[] qSrc = Arrays.copyOf(quantiles, arrLen);
    final long[] cwSrc   = Arrays.copyOf(cumWts, arrLen);

    blockyTandemMergeSortRecursion(qSrc, cwSrc,
                                   quantiles, cumWts,
                                   0, numblks,
                                   blkSize, arrLen);
  }

  /**
   *  blockyTandemMergeSortRecursion() is called by blockyTandemMergeSort().
   *  In addition to performing the algorithm's top down recursion,
   *  it manages the buffer swapping that eliminates most copying.
   *  It also maps the input's pre-sorted blocks into the subarrays
   *  that are processed by tandemMerge().
   * @param qSrc source array of quantiles
   * @param cwSrc source weights array
   * @param qDst destination quantiles array
   * @param cwDst destination weights array
   * @param grpStart group start, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param grpLen group length, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param blkSize block size
   * @param arrLim array limit
   */
  private static void blockyTandemMergeSortRecursion(final double[] qSrc, final long[] cwSrc,
      final double[] qDst, final long[] cwDst, final int grpStart, final int grpLen,
      /* indices of blocks */ final int blkSize, final int arrLim) {
    // Important note: grpStart and grpLen do NOT refer to positions in the underlying array.
    // Instead, they refer to the pre-sorted blocks, such as block 0, block 1, etc.

    assert (grpLen > 0);
    if (grpLen == 1) { return; }
    final int grpLen1 = grpLen / 2;
    final int grpLen2 = grpLen - grpLen1;
    assert (grpLen1 >= 1);
    assert (grpLen2 >= grpLen1);

    final int grpStart1 = grpStart;
    final int grpStart2 = grpStart + grpLen1;

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(qDst, cwDst,
                           qSrc, cwSrc,
                           grpStart1, grpLen1, blkSize, arrLim);

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(qDst, cwDst,
                           qSrc, cwSrc,
                           grpStart2, grpLen2, blkSize, arrLim);

    // here we convert indices of blocks into positions in the underlying array.
    final int arrStart1 = grpStart1 * blkSize;
    final int arrStart2 = grpStart2 * blkSize;
    final int arrLen1   = grpLen1   * blkSize;
    int arrLen2         = grpLen2   * blkSize;

    // special case for the final block which might be shorter than blkSize.
    if ((arrStart2 + arrLen2) > arrLim) { arrLen2 = arrLim - arrStart2; }

    tandemMerge(qSrc, cwSrc,
                arrStart1, arrLen1,
                arrStart2, arrLen2,
                qDst, cwDst,
                arrStart1); // which will be arrStart3
  }

  /**
   *  Performs two merges in tandem. One of them provides the sort keys
   *  while the other one passively undergoes the same data motion.
   * @param qSrc quantiles source
   * @param cwSrc cumulative weights source
   * @param arrStart1 Array 1 start offset
   * @param arrLen1 Array 1 length
   * @param arrStart2 Array 2 start offset
   * @param arrLen2 Array 2 length
   * @param qDst quantiles destination
   * @param cwDst cumulative weights destination
   * @param arrStart3 Array 3 start offset
   */
  private static void tandemMerge(final double[] qSrc, final long[] cwSrc,
                                  final int arrStart1, final int arrLen1,
                                  final int arrStart2, final int arrLen2,
                                  final double[] qDst, final long[] cwDst,
                                  final int arrStart3) {
    final int arrStop1 = arrStart1 + arrLen1;
    final int arrStop2 = arrStart2 + arrLen2;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while ((i1 < arrStop1) && (i2 < arrStop2)) {
      if (qSrc[i2] < qSrc[i1]) {
        qDst[i3] = qSrc[i2];
        cwDst[i3] = cwSrc[i2];
        i2++;
      } else {
        qDst[i3] = qSrc[i1];
        cwDst[i3] = cwSrc[i1];
        i1++;
      }
      i3++;
    }

    if (i1 < arrStop1) {
      arraycopy(qSrc, i1, qDst, i3, arrStop1 - i1);
      arraycopy(cwSrc, i1, cwDst, i3, arrStop1 - i1);
    } else {
      assert i2 < arrStop2;
      arraycopy(qSrc, i2, qDst, i3, arrStop2 - i2);
      arraycopy(cwSrc, i2, cwDst, i3, arrStop2 - i2);
    }
  }

  /**
   * Convert the individual weights into cumulative weights.
   * An array of {1,1,1,1} becomes {1,2,3,4}
   * @param array of actual weights from the sketch, none of the weights may be zero
   * @return total weight
   */
  private static long convertToCumulative(final long[] array) {
    long subtotal = 0;
    for (int i = 0; i < array.length; i++) {
      final long newSubtotal = subtotal + array[i];
      subtotal = array[i] = newSubtotal;
    }
    return subtotal;
  }

}
