/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.hash.MurmurHash3.hash;

import java.util.Arrays;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class Simple85 {
  long[] bitMatrix;
  long numCoupons;
  final int lgK;
  final long seed;

  public Simple85(int lgK) {
    this(lgK, DEFAULT_UPDATE_SEED);
  }

  public Simple85(int lgK, long seed) {
    this.lgK = lgK;
    this.seed = seed;
    bitMatrix = new long[1 << lgK];
    numCoupons = 0;
  }

  public void reset() {
    Arrays.fill(bitMatrix, 0);
    numCoupons = 0;
  }

  public long getNumCoupons() {
    return numCoupons;
  }

  public double getIconEstimate() {
    return IconEstimator.getIconEstimate(lgK, numCoupons);
  }

  public boolean equals(Fm85 sketch) {
    if (lgK != sketch.lgK) { return false; }
    final long[] skMatrix = Fm85.bitMatrixOfSketch(sketch);
    return Arrays.equals(skMatrix, bitMatrix);
  }

  public void update(final long datum) {
    final long[] data = { datum };
    final long[] harr = hash(data, seed);
    hashUpdate(harr[0], harr[1]);
  }

  void hashUpdate(final long hash0, final long hash1) {
    final int kMask = (1 << lgK) - 1;
    int col = Long.numberOfLeadingZeros(hash1);
    if (col > 63) { col = 63; } // clip so that 0 <= col <= 63
    int row = (int) (hash0 & kMask);
    // Avoid the hash table's "empty" value which is (2^26 -1, 63) (all ones) by changing it
    // to the pair (2^26 - 2, 63), which effectively merges the two cells.
    // This case is *extremely* unlikely, but we might as well handle it.
    // It can't happen at all if lgK (or maxLgK) < 26.
    if (((row << 6) | col) == -1) { row &= -2; } //set the LSB of row to 0
    long oldPattern = bitMatrix[row];
    long newPattern = oldPattern | (1L << col);
    if (newPattern != oldPattern) {
      numCoupons++;
      bitMatrix[row] = newPattern;
    }
  }

}
