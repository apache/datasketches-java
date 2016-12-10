/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static java.lang.System.arraycopy;

import java.util.Arrays;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * Down-sampling and merge algorithms for quantiles.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DoublesMergeImpl {

  /**
   * Merges the source sketch into the target sketch that can have a smaller value of K.
   * However, it is required that the ratio of the two K values be a power of 2.
   * I.e., source.getK() = target.getK() * 2^(nonnegative integer).
   * The source is not modified.
   *
   * @param src The source sketch
   * @param tgt The target sketch
   */
  static void downSamplingMergeInto(final DoublesSketch src, final DoublesSketch tgt) {
    final int srcK = src.getK();
    final int tgtK = tgt.getK();

    if ((srcK % tgtK) != 0) {
      throw new SketchesArgumentException(
          "source.getK() must equal target.getK() * 2^(nonnegative integer).");
    }

    final int downFactor = srcK / tgtK;
    checkIfPowerOf2(downFactor, "source.getK()/target.getK() ratio");
    final int lgDownFactor = Integer.numberOfTrailingZeros(downFactor);

    final double[] srcCombBuf = src.getCombinedBuffer();

    final long nFinal = tgt.getN() + src.getN();

    for (int i = 0; i < src.getBaseBufferCount(); i++) {
      tgt.update(srcCombBuf[i]);
    }

    final int spaceNeeded = DoublesUpdateImpl.maybeGrowLevels(nFinal, tgtK);
    final double[] tgtCombBuf;

    if (spaceNeeded > tgt.getCombinedBufferItemCapacity()) {
      // heap: copies base buffer plus old levels
      // off-heap: just checks for enough room, for now, and extracts to heap
      tgtCombBuf = tgt.growCombinedBuffer(spaceNeeded);
    } else {
      tgtCombBuf = tgt.getCombinedBuffer();
    }

    //working scratch buffers
    final double[] scratch2KBuf = new double [2 * tgtK];
    final double[] downScratchKBuf = new double [tgtK];

    long srcBitPattern = src.getBitPattern();
    long newTgtBitPattern = tgt.getBitPattern();
    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        justZipWithStride(
            srcCombBuf, ((2 + srcLvl) * srcK),
            downScratchKBuf, 0,
            tgtK,
            downFactor
        );
        newTgtBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(
            srcLvl + lgDownFactor,    //starting level
            downScratchKBuf, 0,       //sizeKBuf, start
            scratch2KBuf, 0,          //size2KBuf, start
            false,                    //do mergeInto version
            tgtK,
            tgtCombBuf,
            newTgtBitPattern
        );
      }
    }
    tgt.putCombinedBuffer(tgtCombBuf);
    tgt.putBitPattern(newTgtBitPattern); //off-heap is a no-op
    tgt.putN(nFinal);

    assert tgt.getN() / (2 * tgtK) == newTgtBitPattern; // internal consistency check

    final double srcMax = src.getMaxValue();
    final double srcMin = src.getMinValue();
    final double tgtMax = tgt.getMaxValue();
    final double tgtMin = tgt.getMinValue();

    if (srcMax > tgtMax) { tgt.putMaxValue(srcMax); }
    if (srcMin < tgtMin) { tgt.putMinValue(srcMin); }
  }

  /**
   * blockyTandemMergeSort() is an implementation of top-down merge sort specialized
   * for the case where the input contains successive equal-length blocks
   * that have already been sorted, so that only the top part of the
   * merge tree remains to be executed. Also, two arrays are sorted in tandem,
   * as discussed above.
   * @param keyArr array of keys
   * @param valArr array of values
   * @param arrLen length of keyArr and valArr
   * @param blkSize size of internal sorted blocks
   */
  //also used by DoublesAuxiliary and UtilTest
  static void blockyTandemMergeSort(final double[] keyArr, final long[] valArr, final int arrLen,
      final int blkSize) {
    assert blkSize >= 1;
    if (arrLen <= blkSize) { return; }
    int numblks = arrLen / blkSize;
    if (numblks * blkSize < arrLen) { numblks += 1; }
    assert (numblks * blkSize >= arrLen);

    // duplicate the input is preparation for the "ping-pong" copy reduction strategy.
    final double[] keyTmp = Arrays.copyOf(keyArr, arrLen);
    final long[] valTmp   = Arrays.copyOf(valArr, arrLen);

    blockyTandemMergeSortRecursion(keyTmp, valTmp,
                                   keyArr, valArr,
                                   0, numblks,
                                   blkSize, arrLen);
  }

  private static void justZipWithStride(
      final double[] bufA, final int startA, // input
      final double[] bufC, final int startC, // output
      final int kC, // number of items that should be in the output
      final int stride) {
    final int randomOffset = DoublesSketch.rand.nextInt(stride);
    final int limC = startC + kC;
    for (int a = startA + randomOffset, c = startC; c < limC; a += stride, c++ ) {
      bufC[c] = bufA[a];
    }
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
    if (arrStart2 + arrLen2 > arrLim) { arrLen2 = arrLim - arrStart2; }

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
    while (i1 < arrStop1 && i2 < arrStop2) {
      if (keySrc[i2] < keySrc[i1]) {
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
