/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import com.yahoo.memory.Memory;

/**
 * The doubles update algorithms for quantiles.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class DoublesUpdateImpl {

  private DoublesUpdateImpl() {}

  /**
   * Returns item capacity needed based on new value of n and k, which may or may not be larger than
   * current space allocated.
   * @param k current value of k
   * @param newN the new value of n
   * @return item capacity based on new value of n and k. It may not be different.
   */
  //important: newN might not equal n_
  // This only increases the size and does not touch or move any data.
  static int getRequiredItemCapacity(final int k, final long newN) {
    final int numLevelsNeeded = Util.computeNumLevelsNeeded(k, newN);
    if (numLevelsNeeded == 0) {
      // don't need any levels yet, and might have small base buffer; this can happen during a merge
      return 2 * k;
    }
    // from here on we need a full-size base buffer and at least one level
    assert newN >= 2L * k;
    assert numLevelsNeeded > 0;
    final int spaceNeeded = (2 + numLevelsNeeded) * k;
    return spaceNeeded;
  }

  /**
   * This is used to propagate-carry (ripple-carry) an update that will cause the full, sorted
   * base buffer to empty into the levels hierarchy, thus creating a ripple effect up
   * through the higher levels. It is also used during merge operations with the only difference
   * is the base buffer(s) could have valid data and is less than full.
   * This distinction is determined by the <i>doUpdateVersion</i> flag.
   *
   * <p>Prior to this method being called, any extra space for the combined buffer required
   * by either the update or merge operations must already be allocated.</p>
   *
   * <p><b>Update Version:</b> The base buffer is initially full, and after it has been sorted and
   * zipped, will be used as a size2KBuf scratch buffer for the remaining recursive carries.
   * The lowest non-valid level, determined by the bit-pattern, will used internally as a
   * size K scratch buffer and the ultimate target.
   * Thus no additional buffer storage is required outside the combined buffer.</p>
   *
   * <p><b>Merge Version:</b> During merging, each level from the source sketch that must be
   * merged is entered into this method and is assigned to the optional source size K buffer
   * (<i>optSrcKBuf</i>). Because the base buffer may have data, a separate size2K
   * scratch buffer must be provided. The next-lowest. non-valid level, determined by the
   * bit-pattern, will used as a sizeKBuf scratch buffer.</p>
   *
   * <p><b>Downsample Merge Version:</b> This is a variant of the above Merge Version, except at
   * each level the downsampling is performed and the target level is computed for the target merge.
   * In this case the optSrcKBuf is the result of the downsample process and needs to be allocated
   * for that purpose.
   *
   * <p><b>Recursive carry:</b> This starts with a given sorted, size 2K buffer, which is zipped
   * into a size K buffer. If the next level is not valid, the size K buffer is already in position,
   * the bit pattern is updated and returned.</p>
   *
   * <p>If the next level is valid, it is merged with the size K buffer into the size 2K buffer.
   * Continue the recursion until a non-valid level becomes filled by the size K buffer,
   * the bit pattern is updated and returned.</p>
   *
   * @param startingLevel 0-based starting level
   * @param optSrcKBuf optional, size k source, read only buffer
   * @param optSrcKBufStrt starting offset for sizeKBuf
   * @param size2KBuf size 2k scratch buffer
   * @param size2KStart starting offset for size2KBuf
   * @param doUpdateVersion true if update version
   * @param k the target value of k
   * @param tgtCombinedBuffer the full combined buffer
   * @param bitPattern the current bitPattern, prior to this call
   * @return The updated bit pattern.  The updated combined buffer is output as a side effect.
   */
  static long inPlacePropagateCarry( //only operates on parameters
      final int startingLevel,
      final double[] optSrcKBuf, final int optSrcKBufStrt,
      final double[] size2KBuf, final int size2KStart,
      final boolean doUpdateVersion, //false = mergeInto version
      final int k,
      final double[] tgtCombinedBuffer, //ref to combined buffer, which includes base buffer
      final long bitPattern //the current bitPattern
    ) {

    final int endingLevel = Util.lowestZeroBitStartingAt(bitPattern, startingLevel);
    final int tgtStart = (2 + endingLevel) * k;
    assert tgtStart + k <= tgtCombinedBuffer.length;
    if (doUpdateVersion) { // update version of computation
      // its is okay for optSrcKBuf to be null in this case
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          tgtCombinedBuffer, tgtStart,
          k);
    } else { // mergeInto version of computation
      try {
      System.arraycopy(
        optSrcKBuf, optSrcKBufStrt, //2, 0
        tgtCombinedBuffer, tgtStart, //4, 8, k = 2
        k);
      } catch (final Exception e) {
        final String s = String.format("%d %d %d %d %d",
            optSrcKBuf.length, optSrcKBufStrt, tgtCombinedBuffer.length, tgtStart, k);
        System.out.println(s); //2 0 4 8 2
        throw new RuntimeException(e);
      }
    }

    for (int lvl = startingLevel; lvl < endingLevel; lvl++) {
      assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
      mergeTwoSizeKBuffers(
          tgtCombinedBuffer, (2 + lvl) * k,
          tgtCombinedBuffer, (2 + endingLevel) * k,
          size2KBuf, size2KStart,
          k);
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          tgtCombinedBuffer, (2 + endingLevel) * k,
          k);
    } // end of loop over lower levels

    // update bit pattern with binary-arithmetic ripple carry
    return bitPattern + (1L << startingLevel);
  }

  private static void zipSize2KBuffer(
      final double[] bufIn, final int startIn,
      final double[] bufOut, final int startOut,
      final int k) {
    final int randomOffset = DoublesSketch.rand.nextBoolean() ? 1 : 0;
    final int limOut = startOut + k;
    for (int idxIn = startIn + randomOffset, idxOut = startOut; idxOut < limOut;
        idxIn += 2, idxOut++) {
      bufOut[idxOut] = bufIn[idxIn];
    }
  }

  private static void mergeTwoSizeKBuffers(
      final double[] src1, final int start1,
      final double[] src2, final int start2,
      final double[] dst, final int startDst,
      final int k) {
    final int stop1 = start1 + k;
    final int stop2 = start2 + k;

    int i1 = start1;
    int i2 = start2;
    int iDst = startDst;
    while (i1 < stop1 && i2 < stop2) {
      if (src2[i2] < src1[i1]) {
        dst[iDst++] = src2[i2++];
      } else {
        dst[iDst++] = src1[i1++];
      }
    }

    if (i1 < stop1) {
      System.arraycopy(src1, i1, dst, iDst, stop1 - i1);
    } else {
      assert start2 < stop2;
      System.arraycopy(src2, i2, dst, iDst, stop2 - i2);
    }
  }

  //Memory based:

  //see javadocs for inPlacePropagateCarry
  static long inPlacePropagateMemCarry( //only operates on parameters
      final int startingLevel,
      final Memory optSrcKBuf, final int optSrcKBufStrt,
      final Memory size2KBuf, final int size2KStart,
      final boolean doUpdateVersion, //false = mergeInto version
      final int k,
      final Memory tgtCombinedBuffer, //ref to combined buffer, which includes base buffer
      final long bitPattern //the current bitPattern
    ) {

    final int endingLevel = Util.lowestZeroBitStartingAt(bitPattern, startingLevel);

    if (doUpdateVersion) { // update version of computation
      // its is okay for optSrcKBuf to be null in this case
      zipSize2KMemBuffer(
          size2KBuf, size2KStart,
          tgtCombinedBuffer, (2 + endingLevel) * k,
          k);
    } else { // mergeInto version of computation
      optSrcKBuf.copy(
          optSrcKBufStrt << 3,
          tgtCombinedBuffer, ((2 + endingLevel) * k) << 3,
          k << 3);
    }

    for (int lvl = startingLevel; lvl < endingLevel; lvl++) {
      assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
      mergeTwoSizeKMemBuffers(
          tgtCombinedBuffer, (2 + lvl) * k,
          tgtCombinedBuffer, (2 + endingLevel) * k,
          size2KBuf, size2KStart,
          k);
      zipSize2KMemBuffer(
          size2KBuf, size2KStart,
          tgtCombinedBuffer, (2 + endingLevel) * k,
          k);
    } // end of loop over lower levels

    // update bit pattern with binary-arithmetic ripple carry
    return bitPattern + (1L << startingLevel);
  }


  private static void zipSize2KMemBuffer(
      final Memory bufIn, final int startIn,
      final Memory bufOut, final int startOut,
      final int k) {
    final int randomOffset = DoublesSketch.rand.nextBoolean() ? 1 : 0;
    final int limOut = startOut + k;
    for (int idxIn = startIn + randomOffset, idxOut = startOut; idxOut < limOut;
        idxIn += 2, idxOut++) {
      bufOut.putDouble(idxOut << 3, bufIn.getDouble(idxIn << 3));
    }
  }

  private static void mergeTwoSizeKMemBuffers(
      final Memory src1, final int start1,
      final Memory src2, final int start2,
      final Memory dst, final int startDst,
      final int k) {
    final int stop1 = start1 + k;
    final int stop2 = start2 + k;

    int i1 = start1;
    int i2 = start2;
    int iDst = startDst;
    while (i1 < stop1 && i2 < stop2) {
      if (src2.getDouble(i2 << 3) < src1.getDouble(i1 << 3)) {
        dst.putDouble(iDst << 3, src2.getDouble(i2 << 3));
        iDst++; i2++;
      } else {
        dst.putDouble(iDst << 3, src1.getDouble(i1 << 3));
        iDst++; i1++;
      }
    }

    if (i1 < stop1) { //copy the remainder
      src1.copy(i1 << 3, dst, iDst << 3, (stop1 << 3) - (i1 << 3));
    } else {
      assert start2 < stop2;
      src2.copy(i2 << 3, dst, iDst << 3, (stop2 << 3) - (i2 << 3));
    }
  }

}
