/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.invPow2;
import static java.lang.Math.pow;
import static java.lang.Math.round;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class CpcUtil {
  static final int minLgK = 4;
  static final int maxLgK = 26;

  static final double[] kxpByteLookup = new double[256];

  private static void fillKxpByteLookup() { //called from static initializer
    for (int b = 0; b < 256; b++) {
      double sum = 0;
      for (int col = 0; col < 8; col++) {
        final int bit = (b >>> col) & 1;
        if (bit == 0) { // note the inverted logic
          sum += invPow2(col + 1); //note the "+1"
        }
      }
      kxpByteLookup[b] = sum;
    }
  }

  static long divideLongsRoundingUp(final long x, final long y) {
    assert (x >= 0) && (y > 0);
    final long quotient = x / y;
    return ((quotient * y) == x) ?  quotient : quotient + 1;
  }

  /**
   * Returns the floor of Log2(x)
   * @param x the given x
   * @return  the floor of Log2(x)
   */
  static long floorLog2ofX(final long x) {
    if (x < 1L) {
      throw new SketchesArgumentException("x must be > 0: " + x);
    }
    long p = 0;
    long y = 1;
    while (true) {
      if (y == x) { return (p); }
      if (y  > x) { return (p - 1); }
      p  += 1;
      y <<= 1;
    }
  }

  static int golombChooseNumberOfBaseBits(final int k, final long count) {
    assert k >= 1L;
    assert count >= 1L;
    final long quotient = (k - count) / count; // integer division
    return (quotient == 0) ? 0 : (int) floorLog2ofX(quotient);
  }

  static long countBitsSetInMatrix(final long[] array) {
    long count = 0;
    final int len = array.length;
    for (int i = 0; i < len; i++) { count += Long.bitCount(array[i]); }
    return count;
  }

  static int rowColFromTwoHashes(final long hash0, final long hash1, final int lgK) {
    final int kMask = (1 << lgK) - 1;
    int col = Long.numberOfLeadingZeros(hash1);
    if (col > 63) { col = 63; } // clip so that 0 <= col <= 63
    final int row = (int) (hash0 & kMask);
    int rowCol = (row << 6) | col;
    // Avoid the hash table's "empty" value which is (2^26 -1, 63) (all ones) by changing it
    // to the pair (2^26 - 2, 63), which effectively merges the two cells.
    // This case is *extremely* unlikely, but we might as well handle it.
    // It can't happen at all if lgK (or maxLgK) < 26.
    if (rowCol == -1) { rowCol ^= (1 << 6); }
    return rowCol;
  }

  static void checkLgK(final int lgK) {
    if ((lgK < minLgK) || (lgK > maxLgK)) {
      throw new SketchesArgumentException("LgK must be >= 4 and <= 26: " + lgK);
    }
  }

  static final double pwrLaw10NextDouble(final int ppb, final double curPoint) {
    final double cur = (curPoint < 1.0) ? 1.0 : curPoint;
    double gi = round(Math.log10(cur) * ppb); //current generating index
    double next;
    do {
      next = round(pow(10.0, ++gi / ppb));
    } while (next <= curPoint);
    return next;
  }


  static {
    fillKxpByteLookup();
  }

}
