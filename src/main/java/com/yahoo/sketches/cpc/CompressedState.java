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
  short seedHash;
  int lgK;
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

  CompressedState(final CpcSketch source) {
    seedHash = computeSeedHash(source.getSeed());
    lgK = source.getLgK();
    firstInterestingColumn = source.getFirstInterestingColumn();
    mergeFlag = source.isMerged();
    numCoupons = source.getNumCoupons();

    if (mergeFlag) { //compliment of HIP Flag
      kxp = 0.0;
      hipEstAccum = 0.0;
    } else {
      kxp = source.getKxp();
      hipEstAccum = source.getHipAccum();
    }
    svIsValid = source.getSurprisingValueTable() != null;
    windowIsValid = (source.getSlidingWindow() != null);

    //To be filled in
    numCompressedSurprisingValues = 0;
    compressedSurprisingValues = null;
    csvLength = 0;
    compressedWindow = null;
    cwLength = 0;
  }

  Flavor getFlavor() {
    return CpcUtil.determineFlavor(lgK, numCoupons);
  }

  int getWindowOffset() {
    return CpcSketch.determineCorrectOffset(lgK, numCoupons);
  }

}

