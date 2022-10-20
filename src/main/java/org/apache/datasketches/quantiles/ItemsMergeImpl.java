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
import static org.apache.datasketches.common.Util.checkIfIntPowerOf2;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * Down-sampling and merge algorithms
 *
 * @author Lee Rhodes
 * @author Alexander Saydakov
 * @author Kevin Lang
 */
final class ItemsMergeImpl {

  private ItemsMergeImpl() {}

  /**
     * Merges the source sketch into the target sketch that can have a smaller K parameter.
     * However, it is required that the ratio of the two K parameters be a power of 2.
     * I.e., source.getK() = target.getK() * 2^(nonnegative integer).
     * The source is not modified.
     *
     * <p>Note: It is easy to prove that the following simplified code which launches multiple waves of
     * carry propagation does exactly the same amount of merging work (including the work of
     * allocating fresh buffers) as the more complicated and seemingly more efficient approach that
     * tracks a single carry propagation wave through both sketches.
     *
     * <p>This simplified code probably does do slightly more "outer loop" work, but I am pretty
     * sure that even that is within a constant factor of the more complicated code, plus the
     * total amount of "outer loop" work is at least a factor of K smaller than the total amount of
     * merging work, which is identical in the two approaches.
     *
     * <p>Note: a two-way merge that doesn't modify either of its two inputs could be implemented
     * by making a deep copy of the larger sketch and then merging the smaller one into it.
     * However, it was decided not to do this.
     *
     * @param <T> the data type
     * @param src The source sketch
     * @param tgt The target sketch
     */
  @SuppressWarnings("unchecked")
  static <T> void mergeInto(final ItemsSketch<T> src, final ItemsSketch<T> tgt) {
    final int srcK = src.getK();
    final int tgtK = tgt.getK();
    final long srcN = src.getN();
    final long tgtN = tgt.getN();

    if (srcK != tgtK) {
      downSamplingMergeInto(src, tgt);
      return;
    }
    //The remainder of this code is for the case where the k's are equal

    final Object[] srcCombBuf     = src.getCombinedBuffer();
    final long nFinal = tgtN + srcN;

    for (int i = 0; i < src.getBaseBufferCount(); i++) { //update only the base buffer
      tgt.update((T) srcCombBuf[i]);
    }

    ItemsUpdateImpl.maybeGrowLevels(tgt, nFinal);

    final Object[] scratchBuf = new Object[2 * tgtK];

    long srcBitPattern = src.getBitPattern();
    assert srcBitPattern == (srcN / (2L * srcK));

    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) { //only one level above base buffer
        ItemsUpdateImpl.inPlacePropagateCarry(
            srcLvl,
            (T[]) srcCombBuf, (2 + srcLvl) * tgtK,
            (T[]) scratchBuf, 0,
            false,
            tgt);
      // won't update tgt.n_ until the very end
      }
    }
    tgt.n_ = nFinal;

    assert (tgt.getN() / (2L * tgtK)) == tgt.getBitPattern(); // internal consistency check

    final T srcMax = src.isEmpty() ? null : src.getMaxItem();
    final T srcMin = src.isEmpty() ? null : src.getMinItem();
    final T tgtMax = tgt.isEmpty() ? null : tgt.getMaxItem();
    final T tgtMin = tgt.isEmpty() ? null : tgt.getMinItem();

    if ((srcMax != null) && (tgtMax != null)) {
      tgt.maxItem_ = (src.getComparator().compare(srcMax, tgtMax) > 0) ? srcMax : tgtMax;
    } //only one could be null
    else if (tgtMax == null) { //if srcMax were null we would leave tgt alone
      tgt.maxItem_ = srcMax;
    }

    if ((srcMin != null) && (tgtMin != null)) {
      tgt.minItem_ = (src.getComparator().compare(srcMin, tgtMin) > 0) ? tgtMin : srcMin;
    } //only one could be null
    else if (tgtMin == null) { //if srcMin were null we would leave tgt alone
      tgt.minItem_ = srcMin;
    }
  }

  /**
   * Merges the source sketch into the target sketch that can have a smaller parameter K.
   * However, it is required that the ratio of the two K parameters be a power of 2.
   * I.e., source.getK() = target.getK() * 2^(nonnegative integer).
   * The source is not modified.
   * @param <T> the data type
   * @param src The source sketch
   * @param tgt The target sketch
   */
  @SuppressWarnings("unchecked") //also used by ItemsSketch and ItemsUnion
  static <T> void downSamplingMergeInto(final ItemsSketch<T> src, final ItemsSketch<T> tgt) {
    final int sourceK = src.getK();
    final int targetK = tgt.getK();

    if ((sourceK % targetK) != 0) {
      throw new SketchesArgumentException(
          "source.getK() must equal target.getK() * 2^(nonnegative integer).");
    }

    final int downFactor = sourceK / targetK;
    checkIfIntPowerOf2(downFactor, "source.getK()/target.getK() ratio");
    final int lgDownFactor = Integer.numberOfTrailingZeros(downFactor);

    final Object[] sourceLevels     = src.getCombinedBuffer(); // aliasing is a bit dangerous
    final Object[] sourceBaseBuffer = src.getCombinedBuffer(); // aliasing is a bit dangerous

    final long nFinal = tgt.getN() + src.getN();

    for (int i = 0; i < src.getBaseBufferCount(); i++) {
      tgt.update((T) sourceBaseBuffer[i]);
    }

    ItemsUpdateImpl.maybeGrowLevels(tgt, nFinal);

    final Object[] scratchBuf = new Object[2 * targetK];
    final Object[] downBuf    = new Object[targetK];

    long srcBitPattern = src.getBitPattern();
    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        ItemsMergeImpl.justZipWithStride(
            sourceLevels, (2 + srcLvl) * sourceK,
            downBuf, 0,
            targetK,
            downFactor
        );
        ItemsUpdateImpl.inPlacePropagateCarry(
            srcLvl + lgDownFactor,
            (T[]) downBuf, 0,
            (T[]) scratchBuf, 0,
            false, tgt
        );
        // won't update target.n_ until the very end
      }
    }
    tgt.n_ = nFinal;

    assert (tgt.getN() / (2L * targetK)) == tgt.getBitPattern(); // internal consistency check

    final T srcMax = src.isEmpty() ? null : src.getMaxItem();
    final T srcMin = src.isEmpty() ? null : src.getMinItem();
    final T tgtMax = tgt.isEmpty() ? null : tgt.getMaxItem();
    final T tgtMin = tgt.isEmpty() ? null : tgt.getMinItem();

    if ((srcMax != null) && (tgtMax != null)) {
      tgt.maxItem_ = (src.getComparator().compare(srcMax, tgtMax) > 0) ? srcMax : tgtMax;
    } //only one could be null
    else if (tgtMax == null) { //if srcMax were null we would leave tgt alone
      tgt.maxItem_ = srcMax;
    }

    if ((srcMin != null) && (tgtMin != null)) {
      tgt.minItem_ = (src.getComparator().compare(srcMin, tgtMin) > 0) ? tgtMin : srcMin;
    } //only one could be null
    else if (tgtMin == null) { //if srcMin were null we would leave tgt alone
      tgt.minItem_ = srcMin;
    }
  }

  private static <T> void justZipWithStride(
      final T[] bufSrc, final int startSrc, // input
      final T[] bufC, final int startC, // output
      final int kC, // number of items that should be in the output
      final int stride) {
    final int randomOffset = ItemsSketch.rand.nextInt(stride);
    final int limC = startC + kC;
    for (int a = startSrc + randomOffset, c = startC; c < limC; a += stride, c++ ) {
      bufC[c] = bufSrc[a];
    }
  }

  /**
   * blockyTandemMergeSort() is an implementation of top-down merge sort specialized
   * for the case where the input contains successive equal-length blocks
   * that have already been sorted, so that only the top part of the
   * merge tree remains to be executed. Also, two arrays are sorted in tandem,
   * as discussed above.
   * @param <T> the data type
   * @param quantiles array of quantiles
   * @param cumWts array of cum weights
   * @param arrLen length of quantiles array and cumWts array
   * @param blkSize size of internal sorted blocks
   * @param comparator the comparator for data type T
   */
  //also used by ItemsSketchSortedView
  static <T> void blockyTandemMergeSort(final T[] quantiles, final long[] cumWts, final int arrLen,
      final int blkSize, final Comparator<? super T> comparator) {
    assert blkSize >= 1;
    if (arrLen <= blkSize) { return; }
    int numblks = arrLen / blkSize;
    if ((numblks * blkSize) < arrLen) { numblks += 1; }
    assert ((numblks * blkSize) >= arrLen);

    // duplicate the input is preparation for the "ping-pong" copy reduction strategy.
    final T[] keyTmp = Arrays.copyOf(quantiles, arrLen);
    final long[] valTmp = Arrays.copyOf(cumWts, arrLen);

    blockyTandemMergeSortRecursion(keyTmp, valTmp,
                                   quantiles, cumWts,
                                   0, numblks,
                                   blkSize, arrLen, comparator);
  }

  /**
   *  blockyTandemMergeSortRecursion() is called by blockyTandemMergeSort().
   *  In addition to performing the algorithm's top down recursion,
   *  it manages the buffer swapping that eliminates most copying.
   *  It also maps the input's pre-sorted blocks into the subarrays
   *  that are processed by tandemMerge().
   * @param <T> the data type
   * @param qSrc source array of quantiles
   * @param cwSrc source weights array
   * @param qDst destination quantiles array
   * @param cwDest destination weights array
   * @param grpStart group start, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param grpLen group length, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param blkSize block size
   * @param arrLim array limit
   * @param comparator to compare keys
   */
  private static <T> void blockyTandemMergeSortRecursion(final T[] qSrc, final long[] cwSrc,
      final T[] qDst, final long[] cwDest, final int grpStart, final int grpLen, // block indices
      final int blkSize, final int arrLim, final Comparator<? super T> comparator) {
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
    blockyTandemMergeSortRecursion(qDst, cwDest,
                           qSrc, cwSrc,
                           grpStart1, grpLen1, blkSize, arrLim, comparator);

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(qDst, cwDest,
                           qSrc, cwSrc,
                           grpStart2, grpLen2, blkSize, arrLim, comparator);

    // here we convert indices of blocks into positions in the underlying array.
    final int arrStart1 = grpStart1 * blkSize;
    final int arrStart2 = grpStart2 * blkSize;
    final int arrLen1   = grpLen1   * blkSize;
    int arrLen2   = grpLen2   * blkSize;

    // special case for the final block which might be shorter than blkSize.
    if ((arrStart2 + arrLen2) > arrLim) {
      arrLen2 = arrLim - arrStart2;
    }

    tandemMerge(qSrc, cwSrc,
                arrStart1, arrLen1,
                arrStart2, arrLen2,
                qDst, cwDest,
                arrStart1, comparator); // which will be arrStart3
  }

  /**
   *  Performs two merges in tandem. One of them provides the sort keys
   *  while the other one passively undergoes the same data motion.
   * @param <T> the data type
   * @param qSrc quantiles source
   * @param cwSrc cum wts source
   * @param arrStart1 Array 1 start offset
   * @param arrLen1 Array 1 length
   * @param arrStart2 Array 2 start offset
   * @param arrLen2 Array 2 length
   * @param qDst quantiles destination
   * @param cwDst cum wts destination
   * @param arrStart3 Array 3 start offset
   * @param comparator to compare keys
   */
  private static <T> void tandemMerge(final T[] qSrc, final long[] cwSrc,
                                  final int arrStart1, final int arrLen1,
                                  final int arrStart2, final int arrLen2,
                                  final T[] qDst, final long[] cwDst,
                                  final int arrStart3, final Comparator<? super T> comparator) {
    final int arrStop1 = arrStart1 + arrLen1;
    final int arrStop2 = arrStart2 + arrLen2;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while ((i1 < arrStop1) && (i2 < arrStop2)) {
      if (comparator.compare(qSrc[i2], qSrc[i1]) < 0) {
        qDst[i3] = qSrc[i2];
        cwDst[i3] = cwSrc[i2];
        i3++; i2++;
      } else {
        qDst[i3] = qSrc[i1];
        cwDst[i3] = cwSrc[i1];
        i3++; i1++;
      }
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

}
