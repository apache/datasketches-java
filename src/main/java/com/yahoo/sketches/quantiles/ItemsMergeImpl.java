/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static java.lang.System.arraycopy;

import java.util.Arrays;
import java.util.Comparator;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * Down-sampling and merge algorithms for items quantiles.
 *
 * @author Lee Rhodes
 * @author Alexander Saydakov
 * @author Kevin Lang
 */
final class ItemsMergeImpl {

  private ItemsMergeImpl() {}

  /**
     * Merges the source sketch into the target sketch that can have a smaller value of K.
     * However, it is required that the ratio of the two K values be a power of 2.
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

    final T srcMax = src.getMaxValue();
    final T srcMin = src.getMinValue();
    final T tgtMax = tgt.getMaxValue();
    final T tgtMin = tgt.getMinValue();

    if ((srcMax != null) && (tgtMax != null)) {
      tgt.maxValue_ = (src.getComparator().compare(srcMax, tgtMax) > 0) ? srcMax : tgtMax;
    } //only one could be null
    else if (tgtMax == null) { //if srcMax were null we would leave tgt alone
      tgt.maxValue_ = srcMax;
    }

    if ((srcMin != null) && (tgtMin != null)) {
      tgt.minValue_ = (src.getComparator().compare(srcMin, tgtMin) > 0) ? tgtMin : srcMin;
    } //only one could be null
    else if (tgtMin == null) { //if srcMin were null we would leave tgt alone
      tgt.minValue_ = srcMin;
    }
  }

  /**
   * Merges the source sketch into the target sketch that can have a smaller value of K.
   * However, it is required that the ratio of the two K values be a power of 2.
   * I.e., source.getK() = target.getK() * 2^(nonnegative integer).
   * The source is not modified.
   * @param <T> the data type
   * @param src The source sketch
   * @param tgt The target sketch
   */
  @SuppressWarnings("unchecked") //also used by ItemsSketch and ItemsUnion
  static <T> void downSamplingMergeInto(final ItemsSketch<T> src, final ItemsSketch<T> tgt) {
    final int targetK = tgt.getK();
    final int sourceK = src.getK();

    if ((sourceK % targetK) != 0) {
      throw new SketchesArgumentException(
          "source.getK() must equal target.getK() * 2^(nonnegative integer).");
    }

    final int downFactor = sourceK / targetK;
    checkIfPowerOf2(downFactor, "source.getK()/target.getK() ratio");
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
            downFactor);
        ItemsUpdateImpl.inPlacePropagateCarry(
            srcLvl + lgDownFactor,
            (T[]) downBuf, 0,
            (T[]) scratchBuf, 0,
            false, tgt);
        // won't update target.n_ until the very end
      }
    }
    tgt.n_ = nFinal;

    assert (tgt.getN() / (2L * targetK)) == tgt.getBitPattern(); // internal consistency check

    final T srcMax = src.getMaxValue();
    final T srcMin = src.getMinValue();
    final T tgtMax = tgt.getMaxValue();
    final T tgtMin = tgt.getMinValue();

    if ((srcMax != null) && (tgtMax != null)) {
      tgt.maxValue_ = (src.getComparator().compare(srcMax, tgtMax) > 0) ? srcMax : tgtMax;
    } //only one could be null
    else if (tgtMax == null) { //if srcMax were null we would leave tgt alone
      tgt.maxValue_ = srcMax;
    }

    if ((srcMin != null) && (tgtMin != null)) {
      tgt.minValue_ = (src.getComparator().compare(srcMin, tgtMin) > 0) ? tgtMin : srcMin;
    } //only one could be null
    else if (tgtMin == null) { //if srcMin were null we would leave tgt alone
      tgt.minValue_ = srcMin;
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
   * @param keyArr array of keys
   * @param valArr array of values
   * @param arrLen length of keyArr and valArr
   * @param blkSize size of internal sorted blocks
   * @param comparator the comparator for data type T
   */
  //also used by ItemsAuxiliary
  static <T> void blockyTandemMergeSort(final T[] keyArr, final long[] valArr, final int arrLen,
      final int blkSize, final Comparator<? super T> comparator) {
    assert blkSize >= 1;
    if (arrLen <= blkSize) { return; }
    int numblks = arrLen / blkSize;
    if ((numblks * blkSize) < arrLen) { numblks += 1; }
    assert ((numblks * blkSize) >= arrLen);

    // duplicate the input is preparation for the "ping-pong" copy reduction strategy.
    final T[] keyTmp = Arrays.copyOf(keyArr, arrLen);
    final long[] valTmp = Arrays.copyOf(valArr, arrLen);

    blockyTandemMergeSortRecursion(keyTmp, valTmp,
                                   keyArr, valArr,
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
   * @param keySrc key source
   * @param valSrc value source
   * @param keyDst key destination
   * @param valDst value destination
   * @param grpStart group start, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param grpLen group length, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param blkSize block size
   * @param arrLim array limit
   * @param comparator to compare keys
   */
  private static <T> void blockyTandemMergeSortRecursion(final T[] keySrc, final long[] valSrc,
      final T[] keyDst, final long[] valDst, final int grpStart, final int grpLen, // block indices
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
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart1, grpLen1, blkSize, arrLim, comparator);

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
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

    tandemMerge(keySrc, valSrc,
                arrStart1, arrLen1,
                arrStart2, arrLen2,
                keyDst, valDst,
                arrStart1, comparator); // which will be arrStart3
  }


  /**
   *  Performs two merges in tandem. One of them provides the sort keys
   *  while the other one passively undergoes the same data motion.
   * @param <T> the data type
   * @param keySrc key source
   * @param valSrc value source
   * @param arrStart1 Array 1 start offset
   * @param arrLen1 Array 1 length
   * @param arrStart2 Array 2 start offset
   * @param arrLen2 Array 2 length
   * @param keyDst key destination
   * @param valDst value destination
   * @param arrStart3 Array 3 start offset
   * @param comparator to compare keys
   */
  private static <T> void tandemMerge(final T[] keySrc, final long[] valSrc,
                                  final int arrStart1, final int arrLen1,
                                  final int arrStart2, final int arrLen2,
                                  final T[] keyDst, final long[] valDst,
                                  final int arrStart3, final Comparator<? super T> comparator) {
    final int arrStop1 = arrStart1 + arrLen1;
    final int arrStop2 = arrStart2 + arrLen2;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while ((i1 < arrStop1) && (i2 < arrStop2)) {
      if (comparator.compare(keySrc[i2], keySrc[i1]) < 0) {
        keyDst[i3] = keySrc[i2];
        valDst[i3] = valSrc[i2];
        i3++; i2++;
      } else {
        keyDst[i3] = keySrc[i1];
        valDst[i3] = valSrc[i1];
        i3++; i1++;
      }
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

}
