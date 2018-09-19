/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.computeSeedHash;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class CompressedState {
  final int lgK;
  final short seedHash;
  int firstInterestingColumn;
  boolean mergeFlag; //compliment of HIP Flag
  boolean svIsValid;
  boolean windowIsValid;

  long numCoupons;

  double kxp;
  double hipEstAccum;

  int numCompressedSurprisingValues;
  int[] compressedSurprisingValues; //may be longer than required
  int csvLength;
  int[] compressedWindow; //may be longer than required
  int cwLength;

  CompressedState(final int lgK, final short seedHash) {
    this.lgK = lgK;
    this.seedHash = seedHash;
  }

  static CompressedState compress(final CpcSketch source) {
    final short seedHash = computeSeedHash(source.seed);
    final CompressedState target = new CompressedState(source.lgK, seedHash);
    target.firstInterestingColumn = source.firstInterestingColumn;
    target.mergeFlag = source.mergeFlag;
    target.numCoupons = source.numCoupons;
    target.kxp = source.kxp;
    target.hipEstAccum = source.hipEstAccum;

    target.svIsValid = source.surprisingValueTable != null;
    target.windowIsValid = (source.slidingWindow != null);

    target.numCompressedSurprisingValues = 0;
    target.compressedSurprisingValues = null;
    target.csvLength = 0;
    target.compressedWindow = null;
    target.cwLength = 0;
    CpcCompression.compress(source, target);
    return target;
  }

  Flavor getFlavor() {
    return CpcUtil.determineFlavor(lgK, numCoupons);
  }

  int getWindowOffset() {
    return CpcSketch.determineCorrectOffset(lgK, numCoupons);
  }

}

