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
import static org.apache.datasketches.quantiles.Util.checkFractionalRankBounds;

import java.util.Arrays;

/**
 * The Sorted View provides a view of the data retained by the sketch that would be cumbersome to get any other way.
 * One can iterate of the contents of the sketch, but the result is not sorted.
 * Trying to use getQuantiles would be very cumbersome since one doesn't know what ranks to use to supply the
 * getQuantiles method.  Even worse, suppose it is a large sketch that has retained 1000 values from a stream of
 * millions (or billions).  One would have to execute the getQuantiles method many thousands of times, and using
 * trial &amp; error, try to figure out what the sketch actually has retained.
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
 * @author Lee Rhodes
 */
public final class DoublesSketchSortedView {
  long auxN_;
  double[] auxSamplesArr_; //array of size samples
  long[] auxCumWtsArr_;

  /**
   * Constructs the Auxiliary structure from the DoublesSketch
   * @param qs a DoublesSketch
   * @param inclusive if true, fractional ranks are considered inclusive
   */
  @SuppressWarnings("deprecation")
  DoublesSketchSortedView(final DoublesSketch qs, final boolean cumulative, final boolean inclusive) {
    final int k = qs.getK();
    final long n = qs.getN();
    final long bitPattern = qs.getBitPattern();
    final int numSamples = qs.getRetainedItems();
    final DoublesSketchAccessor sketchAccessor = DoublesSketchAccessor.wrap(qs);

    final double[] itemsArr = new double[numSamples];
    final long[] cumWtsArr = new long[numSamples + 1]; // the extra slot is very important

    // Populate from DoublesSketch:
    //  copy over the "levels" and then the base buffer, all with appropriate weights
    populateFromDoublesSketch(k, n, bitPattern, sketchAccessor, itemsArr, cumWtsArr);

    // Sort the first "numSamples" slots of the two arrays in tandem,
    //  taking advantage of the already sorted blocks of length k
    blockyTandemMergeSort(itemsArr, cumWtsArr, numSamples, k);

    if (cumulative) {
      final long total = ClassicQuantilesHelper.convertToPrecedingCumulative(cumWtsArr, inclusive);
      assert total == n;
    }

    auxN_ = n;
    auxSamplesArr_ = itemsArr;
    auxCumWtsArr_ = cumWtsArr;
  }

  /**
   * Get the estimated quantile given a fractional rank.
   * @param rank the normalized rank where: 0 &le; rank &le; 1.0.
   * @return the estimated quantile
   */
  @SuppressWarnings("deprecation")
  public double getQuantile(final double rank) {
    checkFractionalRankBounds(rank);
    final long pos = ClassicQuantilesHelper.posOfRank(rank, auxN_);
    return approximatelyAnswerPositionalQuery(pos);
  }

  public DoublesSketchSortedViewIterator iterator() {
    return new DoublesSketchSortedViewIterator(auxSamplesArr_, auxCumWtsArr_);
  }

  /**
   * Assuming that there are n items in the true stream, this asks what
   * item would appear in position 0 &le; pos &lt; n of a hypothetical sorted
   * version of that stream.
   *
   * <p>Note that since the true stream is unavailable,
   * we don't actually answer the question for that stream, but rather for
   * a <i>different</i> stream of the same length, that could hypothetically
   * be reconstructed from the weighted samples in our sketch.
   * @param pos position
   * @return approximate answer
   */
  @SuppressWarnings("deprecation")
  private double approximatelyAnswerPositionalQuery(final long pos) {
    assert 0 <= pos;
    assert pos < auxN_;
    final int index = ClassicQuantilesHelper.chunkContainingPos(auxCumWtsArr_, pos);
    return auxSamplesArr_[index];
  }

  /**
   * Populate the arrays and registers from a DoublesSketch
   * @param k K value of sketch
   * @param n The current size of the stream
   * @param bitPattern the bit pattern for valid log levels
   * @param sketchAccessor A DoublesSketchAccessor around the sketch
   * @param itemsArr the consolidated array of all items from the sketch populated here
   * @param cumWtsArr the cumulative weights for each item from the sketch populated here
   */
  private final static void populateFromDoublesSketch(
          final int k, final long n, final long bitPattern,
          final DoublesSketchAccessor sketchAccessor,
          final double[] itemsArr, final long[] cumWtsArr) {
    long weight = 1;
    int nxt = 0;
    long bits = bitPattern;
    assert bits == (n / (2L * k)); // internal consistency check
    for (int lvl = 0; bits != 0L; lvl++, bits >>>= 1) {
      weight *= 2;
      if ((bits & 1L) > 0L) {
        sketchAccessor.setLevel(lvl);
        for (int i = 0; i < sketchAccessor.numItems(); i++) {
          itemsArr[nxt] = sketchAccessor.get(i);
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
      itemsArr[nxt] = sketchAccessor.get(i);
      cumWtsArr[nxt] = weight;
      nxt++;
    }
    assert nxt == itemsArr.length;

    // Must sort the items that came from the base buffer.
    // Don't need to sort the corresponding weights because they are all the same.
    final int numSamples = nxt;
    Arrays.sort(itemsArr, startOfBaseBufferBlock, numSamples);
    cumWtsArr[numSamples] = 0;
  }

  /**
   * blockyTandemMergeSort() is an implementation of top-down merge sort specialized
   * for the case where the input contains successive equal-length blocks
   * that have already been sorted, so that only the top part of the
   * merge tree remains to be executed. Also, two arrays are sorted in tandem,
   * as discussed below.
   * @param keyArr array of keys
   * @param valArr array of values
   * @param arrLen length of keyArr and valArr
   * @param blkSize size of internal sorted blocks
   */
  //used by DoublesAuxiliary and UtilTest
  static void blockyTandemMergeSort(final double[] keyArr, final long[] valArr, final int arrLen,
      final int blkSize) {
    assert blkSize >= 1;
    if (arrLen <= blkSize) { return; }
    int numblks = arrLen / blkSize;
    if ((numblks * blkSize) < arrLen) { numblks += 1; }
    assert ((numblks * blkSize) >= arrLen);

    // duplicate the input is preparation for the "ping-pong" copy reduction strategy.
    final double[] keyTmp = Arrays.copyOf(keyArr, arrLen);
    final long[] valTmp   = Arrays.copyOf(valArr, arrLen);

    blockyTandemMergeSortRecursion(keyTmp, valTmp,
                                   keyArr, valArr,
                                   0, numblks,
                                   blkSize, arrLen);
  }

  /**
   *  blockyTandemMergeSortRecursion() is called by blockyTandemMergeSort().
   *  In addition to performing the algorithm's top down recursion,
   *  it manages the buffer swapping that eliminates most copying.
   *  It also maps the input's pre-sorted blocks into the subarrays
   *  that are processed by tandemMerge().
   * @param keySrc key source
   * @param valSrc value source
   * @param keyDst key destination
   * @param valDst value destination
   * @param grpStart group start, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param grpLen group length, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param blkSize block size
   * @param arrLim array limit
   */
  private static void blockyTandemMergeSortRecursion(final double[] keySrc, final long[] valSrc,
      final double[] keyDst, final long[] valDst, final int grpStart, final int grpLen,
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
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart1, grpLen1, blkSize, arrLim);

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart2, grpLen2, blkSize, arrLim);

    // here we convert indices of blocks into positions in the underlying array.
    final int arrStart1 = grpStart1 * blkSize;
    final int arrStart2 = grpStart2 * blkSize;
    final int arrLen1   = grpLen1   * blkSize;
    int arrLen2         = grpLen2   * blkSize;

    // special case for the final block which might be shorter than blkSize.
    if ((arrStart2 + arrLen2) > arrLim) { arrLen2 = arrLim - arrStart2; }

    tandemMerge(keySrc, valSrc,
                arrStart1, arrLen1,
                arrStart2, arrLen2,
                keyDst, valDst,
                arrStart1); // which will be arrStart3
  }

  /**
   *  Performs two merges in tandem. One of them provides the sort keys
   *  while the other one passively undergoes the same data motion.
   * @param keySrc key source
   * @param valSrc value source
   * @param arrStart1 Array 1 start offset
   * @param arrLen1 Array 1 length
   * @param arrStart2 Array 2 start offset
   * @param arrLen2 Array 2 length
   * @param keyDst key destination
   * @param valDst value destination
   * @param arrStart3 Array 3 start offset
   */
  private static void tandemMerge(final double[] keySrc, final long[] valSrc,
                                  final int arrStart1, final int arrLen1,
                                  final int arrStart2, final int arrLen2,
                                  final double[] keyDst, final long[] valDst,
                                  final int arrStart3) {
    final int arrStop1 = arrStart1 + arrLen1;
    final int arrStop2 = arrStart2 + arrLen2;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while ((i1 < arrStop1) && (i2 < arrStop2)) {
      if (keySrc[i2] < keySrc[i1]) {
        keyDst[i3] = keySrc[i2];
        valDst[i3] = valSrc[i2];
        i2++;
      } else {
        keyDst[i3] = keySrc[i1];
        valDst[i3] = valSrc[i1];
        i1++;
      }
      i3++;
    }

    if (i1 < arrStop1) {
      arraycopy(keySrc, i1, keyDst, i3, arrStop1 - i1);
      arraycopy(valSrc, i1, valDst, i3, arrStop1 - i1);
    } else {
      assert i2 < arrStop2;
      arraycopy(keySrc, i2, keyDst, i3, arrStop2 - i2);
      arraycopy(valSrc, i2, valDst, i3, arrStop2 - i2);
    }
  }

} // end of class Auxiliary
