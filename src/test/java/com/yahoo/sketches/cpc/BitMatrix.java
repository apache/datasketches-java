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
class BitMatrix {
  private final int lgK;
  private final long seed;
  private long numCoupons;
  private long[] bitMatrix;
  private boolean mergeFlag;
  private boolean numCouponsInvalid;

  BitMatrix(int lgK) {
    this(lgK, DEFAULT_UPDATE_SEED);
  }

  BitMatrix(int lgK, long seed) {
    this.lgK = lgK;
    this.seed = seed;
    bitMatrix = new long[1 << lgK];
    numCoupons = 0;
    mergeFlag = false;
    numCouponsInvalid = false;
  }

  //leaves lgK and seed untouched
  void reset() {
    Arrays.fill(bitMatrix, 0);
    numCoupons = 0;
    mergeFlag = false;
    numCouponsInvalid = false;
  }

  int getLgK() {
    return lgK;
  }

  long getSeed() {
    return seed;
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

  boolean isMerged() {
    return mergeFlag;
  }

  double getIconEstimate() {
    return IconEstimator.getIconEstimate(lgK, getNumCoupons());
  }

  double getIconUpperBound(final int kappa) {
    return CpcConfidence.getIconConfidenceUB(lgK, getNumCoupons(), kappa);
  }

  double getIconLowerBound(final int kappa) {
    return CpcConfidence.getIconConfidenceLB(lgK, getNumCoupons(), kappa);
  }

  boolean equalTo(BitMatrix sketch) {
    if (lgK != sketch.lgK) { return false; }
    if (seed != sketch.seed) { return false; }
    if (getNumCoupons() != sketch.getNumCoupons()) { return false; }
    return Arrays.equals(bitMatrix, sketch.bitMatrix);
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

  private long countCoupons() {
    long count = 0;
    final int len = bitMatrix.length;
    for (int i = 0; i < len; i++) { count += Long.bitCount(bitMatrix[i]); }
    numCouponsInvalid = false;
    return count;
  }

}
