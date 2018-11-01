/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import java.util.Arrays;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class CpcUtil {
  static final int minLgK = 4;
  static final int maxLgK = 26;

  static void checkLgK(final int lgK) {
    if ((lgK < minLgK) || (lgK > maxLgK)) {
      throw new SketchesArgumentException("LgK must be >= 4 and <= 26: " + lgK);
    }
  }

  static Flavor determineFlavor(final int lgK, final long numCoupons) {
    final long c = numCoupons;
    final long k = 1L << lgK;
    final long c2 = c << 1;
    final long c8 = c << 3;
    final long c32 = c << 5;
    if (c == 0) {
      return Flavor.EMPTY;    //    0  == C <    1
    }
    if (c32 < (3 * k)) {
      return Flavor.SPARSE;   //    1  <= C <   3K/32
    }
    if (c2 < k) {
      return Flavor.HYBRID;   // 3K/32 <= C <   K/2
    }
    if (c8 < (27 * k)) {
      return Flavor.PINNED;   //   K/2 <= C < 27K/8
    }
    else {
      return Flavor.SLIDING;  // 27K/8 <= C
    }
  }

  /**
   * Warning: this is called in several places, including during the
   * transitional moments during which sketch invariants involving
   * flavor and offset are out of whack and in fact we are re-imposing
   * them. Therefore it cannot rely on determineFlavor() or
   * determineCorrectOffset(). Instead it interprets the low level data
   * structures "as is".
   *
   * <p>This produces a full-size k-by-64 bit matrix from any Live sketch.
   *
   * @param sketch the given sketch
   * @return the bit matrix as an array of longs.
   */
  static long[] bitMatrixOfSketch(final CpcSketch sketch) {
    final int k = (1 << sketch.lgK);
    final int offset = sketch.windowOffset;
    assert (offset >= 0) && (offset <= 56);

    final long[] matrix = new long[k];

    if (sketch.numCoupons == 0) {
      return matrix; // Returning a matrix of zeros rather than NULL.
    }

    //Fill the matrix with default rows in which the "early zone" is filled with ones.
    //This is essential for the routine's O(k) time cost (as opposed to O(C)).
    final long defaultRow = (1L << offset) - 1L;
    Arrays.fill(matrix, defaultRow);

    final byte[] window = sketch.slidingWindow;
    if (window != null) { // In other words, we are in window mode, not sparse mode.
      for (int i = 0; i < k; i++) { // set the window bits, trusting the sketch's current offset.
        matrix[i] |= ((window[i] & 0XFFL) << offset);
      }
    }
    final PairTable table = sketch.pairTable;
    assert (table != null);
    final int[] slots = table.getSlotsArr();
    final int numSlots = 1 << table.getLgSizeInts();

    for (int i = 0; i < numSlots; i++) {
      final int rowCol = slots[i];
      if (rowCol != -1) {
        final int col = rowCol & 63;
        final int row = rowCol >>> 6;
        // Flip the specified matrix bit from its default value.
        // In the "early" zone the bit changes from 1 to 0.
        // In the "late" zone the bit changes from 0 to 1.
        matrix[row] ^= (1L << col);
      }
    }
    return matrix;
  }

  static long countBitsSetInMatrix(final long[] matrix) {
    long count = 0;
    final int len = matrix.length;
    for (int i = 0; i < len; i++) { count += Long.bitCount(matrix[i]); }
    return count;
  }

  static int determineCorrectOffset(final int lgK, final long numCoupons) {
    final long c = numCoupons;
    final long k = (1L << lgK);
    final long tmp = (c << 3) - (19L * k); // 8C - 19K
    if (tmp < 0) { return 0; }
    return (int) (tmp >>> (lgK + 3));      // tmp / 8K
  }

}
