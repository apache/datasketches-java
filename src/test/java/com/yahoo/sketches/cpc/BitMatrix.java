/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.hash.MurmurHash3.hash;

import java.util.Arrays;

/**
 * Used only in test.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class BitMatrix {
  private final int lgK;
  private final long seed;
  private long numCoupons;
  private long[] bitMatrix;
  private boolean numCouponsInvalid; //only used if we allowed merges

  BitMatrix(int lgK) {
    this(lgK, DEFAULT_UPDATE_SEED);
  }

  BitMatrix(int lgK, long seed) {
    this.lgK = lgK;
    this.seed = seed;
    bitMatrix = new long[1 << lgK];
    numCoupons = 0;
    numCouponsInvalid = false;
  }

  //leaves lgK and seed untouched
  void reset() {
    Arrays.fill(bitMatrix, 0);
    numCoupons = 0;
    numCouponsInvalid = false;
  }

  long getNumCoupons() {
    if (numCouponsInvalid) {
      numCoupons = countCoupons();
    }
    return numCoupons;
  }

  long[] getMatrix() {
    return bitMatrix;
  }

  /**
   * Present the given long as a potential unique item.
   *
   * @param datum The given long datum.
   */
  public void update(final long datum) {
    final long[] data = { datum };
    final long[] harr = hash(data, seed);
    hashUpdate(harr[0], harr[1]);
  }

  private void hashUpdate(final long hash0, final long hash1) {
    int col = Long.numberOfLeadingZeros(hash1);
    if (col > 63) { col = 63; } // clip so that 0 <= col <= 63
    final long kMask = (1L << lgK) - 1L;
    int row = (int) (hash0 & kMask);
    int rowCol = (row << 6) | col;

    // Avoid the hash table's "empty" value which is (2^26 -1, 63) (all ones) by changing it
    // to the pair (2^26 - 2, 63), which effectively merges the two cells.
    // This case is *extremely* unlikely, but we might as well handle it.
    // It can't happen at all if lgK (or maxLgK) < 26.
    if (rowCol == -1) { row ^= 1; } //set the LSB of row to 0

    long oldPattern = bitMatrix[row];
    long newPattern = oldPattern | (1L << col);
    if (newPattern != oldPattern) {
      numCoupons++;
      bitMatrix[row] = newPattern;
    }
  }

  private long countCoupons() {
    long count = 0;
    final int len = bitMatrix.length;
    for (int i = 0; i < len; i++) { count += Long.bitCount(bitMatrix[i]); }
    numCouponsInvalid = false;
    return count;
  }

}
