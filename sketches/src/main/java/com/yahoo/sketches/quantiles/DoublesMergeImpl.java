/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.FLAGS_BYTE;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Down-sampling and merge algorithms for doubles quantiles.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class DoublesMergeImpl {

  private DoublesMergeImpl() {}

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
   * @param src The source sketch
   * @param tgt The target sketch
   */
  static void mergeInto(final DoublesSketch src, final DoublesSketch tgt) {
    final int srcK = src.getK();
    final int tgtK = tgt.getK();
    final long srcN = src.getN();
    final long tgtN = tgt.getN();

    if (srcK != tgtK) {
      downSamplingMergeInto(src, tgt);
      return;
    }
    //The remainder of this code is for the case where the k's are equal

    final double[] srcCombBuf = src.getCombinedBuffer();
    final long nFinal = tgtN + srcN;

    for (int i = 0; i < src.getBaseBufferCount(); i++) { //update only the base buffer
      tgt.update(srcCombBuf[i]);
    }

    final int spaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(tgtK, nFinal);

    final int tgtCombBufItemCap = tgt.getCombinedBufferItemCapacity();
    final double[] newTgtCombBuf;

    if (spaceNeeded > tgtCombBufItemCap) { //copies base buffer plus current levels
      newTgtCombBuf = tgt.growCombinedBuffer(tgtCombBufItemCap, spaceNeeded);
    } else {
      newTgtCombBuf = tgt.getCombinedBuffer();
    }
    final double[] scratch2KBuf = new double[2 * tgtK];

    long srcBitPattern = src.getBitPattern();
    assert srcBitPattern == (srcN / (2L * srcK));



    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        final long newTgtBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(
            srcLvl,
            srcCombBuf, ((2 + srcLvl) * tgtK),
            scratch2KBuf, 0,
            false,
            tgtK,
            newTgtCombBuf,
            tgt.getBitPattern()
        );
        tgt.putBitPattern(newTgtBitPattern);
        // won't update tgt.n_ until the very end
      }
    }

    if (tgt.isDirect() && (nFinal > 0)) {
      tgt.putCombinedBuffer(newTgtCombBuf);
      final Memory mem = tgt.getMemory();
      mem.clearBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    }

    tgt.putN(nFinal);

    assert tgt.getN() / (2 * tgtK) == tgt.getBitPattern(); // internal consistency check

    final double srcMax = src.getMaxValue();
    final double srcMin = src.getMinValue();
    final double tgtMax = tgt.getMaxValue();
    final double tgtMin = tgt.getMinValue();
    tgt.putMaxValue(Math.max(srcMax, tgtMax));
    tgt.putMinValue(Math.min(srcMin, tgtMin));
  }

  /**
   * Merges the source sketch into the target sketch that can have a smaller value of K.
   * However, it is required that the ratio of the two K values be a power of 2.
   * I.e., source.getK() = target.getK() * 2^(nonnegative integer).
   * The source is not modified.
   *
   * @param src The source sketch
   * @param tgt The target sketch
   */
  //also used by DoublesSketch, DoublesUnionImpl and HeapDoublesSketchTest
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

    final int spaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(tgtK, nFinal);
    final double[] tgtCombBuf;
    final int curCombBufCap = tgt.getCombinedBufferItemCapacity();
    if (spaceNeeded > curCombBufCap) {
      // heap: copies base buffer plus old levels
      // off-heap: just checks for enough room, for now, and extracts to heap
      tgtCombBuf = tgt.growCombinedBuffer(curCombBufCap, spaceNeeded);
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
            downScratchKBuf, 0,       //optSrcKBuf, optSrcKBufStrt
            scratch2KBuf, 0,          //size2KBuf, size2Kstart
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



}
