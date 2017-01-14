/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

/**
 * The doubles update algorithms for quantiles.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class DoublesUpdateImpl {

  private DoublesUpdateImpl() {}

  /**
   * Returns space needed based on new value of n and k, which may or may not be larger that
   * current space allocated.
   * @param k current value of k
   * @param newN the new value of n
   * @return space needed based on new value of n and k. It may not be different
   */
  //important: newN might not equal n_
  // This only increases the size and does not touch or move any data.
  static int maybeGrowLevels(final int k, final long newN) {
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
   *
   * @param startingLevel 0-based starting level
   * @param sizeKBuf size k scratch buffer
   * @param sizeKStart starting offset for sizeKBuf
   * @param size2KBuf size 2k scratch buffer
   * @param size2KStart starting offset for size2KBuf
   * @param doUpdateVersion true if update version
   * @param k the target value of k
   * @param combinedBuffer the full combined buffer
   * @param bitPattern the current bitPattern, prior to this call
   * @return The updated bit pattern.  The updated combined buffer is output as a side effect.
   */
  static long inPlacePropagateCarry( //only operates on parameters
      final int startingLevel,
      final double[] sizeKBuf, final int sizeKStart,
      final double[] size2KBuf, final int size2KStart,
      final boolean doUpdateVersion, //false = mergeInto version
      final int k,
      final double[] combinedBuffer, //ref to combined buffer, which includes base buffer
      final long bitPattern //the current bitPattern
    ) {
    // else doMergeIntoVersion

    final int endingLevel = Util.positionOfLowestZeroBitStartingAt(bitPattern, startingLevel);

    if (doUpdateVersion) { // update version of computation
      // its is okay for sizeKBuf to be null in this case
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          combinedBuffer, (2 + endingLevel) * k,
          k);
    } else { // mergeInto version of computation //TODO ERROR on next
      System.arraycopy(
          sizeKBuf, sizeKStart,
          combinedBuffer, (2 + endingLevel) * k,
          k);
    }

    for (int lvl = startingLevel; lvl < endingLevel; lvl++) {
      assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
      mergeTwoSizeKBuffers(
          combinedBuffer, (2 + lvl) * k,
          combinedBuffer, (2 + endingLevel) * k,
          size2KBuf, size2KStart,
          k);
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          combinedBuffer, (2 + endingLevel) * k,
          k);
    } // end of loop over lower levels

    // update bit pattern with binary-arithmetic ripple carry
    return bitPattern + (1L << startingLevel);
  }

  private static void zipSize2KBuffer( //only operates on parameters
      final double[] bufA, final int startA, // input
      final double[] bufC, final int startC, // output
      final int k) {
    final int randomOffset = DoublesSketch.rand.nextBoolean() ? 1 : 0;
    final int limC = startC + k;
    for (int a = startA + randomOffset, c = startC; c < limC; a += 2, c++) {
      bufC[c] = bufA[a];
    }
  }

  private static void mergeTwoSizeKBuffers( //only operates on parameters
      final double[] keySrc1, final int arrStart1,
      final double[] keySrc2, final int arrStart2,
      final double[] keyDst,  final int arrStart3,
      final int k) {
    final int arrStop1 = arrStart1 + k;
    final int arrStop2 = arrStart2 + k;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while (i1 < arrStop1 && i2 < arrStop2) {
      if (keySrc2[i2] < keySrc1[i1]) {
        keyDst[i3++] = keySrc2[i2++];
      } else {
        keyDst[i3++] = keySrc1[i1++];
      }
    }

    if (i1 < arrStop1) {
      System.arraycopy(keySrc1, i1, keyDst, i3, arrStop1 - i1);
    } else {
      assert i2 < arrStop2;
      System.arraycopy(keySrc1, i2, keyDst, i3, arrStop2 - i2);
    }
  }

}
