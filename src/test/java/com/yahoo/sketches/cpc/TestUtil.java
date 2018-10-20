/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssert;
import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssertEquals;
import static java.lang.Math.pow;
import static java.lang.Math.round;

/**
 * @author Lee Rhodes
 */
public class TestUtil {

  static final double pwrLaw10NextDouble(final int ppb, final double curPoint) {
    final double cur = (curPoint < 1.0) ? 1.0 : curPoint;
    double gi = round(Math.log10(cur) * ppb); //current generating index
    double next;
    do {
      next = round(pow(10.0, ++gi / ppb));
    } while (next <= curPoint);
    return next;
  }

  static boolean specialEquals(final CpcSketch sk1, final CpcSketch sk2,
      final boolean sk1wasMerged, final boolean sk2wasMerged) {
    rtAssertEquals(sk1.seed, sk2.seed);
    rtAssertEquals(sk1.lgK, sk2.lgK);
    rtAssertEquals(sk1.numCoupons, sk2.numCoupons);

    rtAssertEquals(sk1.windowOffset, sk2.windowOffset);
    rtAssertEquals(sk1.slidingWindow, sk2.slidingWindow);
    PairTable.equals(sk1.pairTable, sk2.pairTable);

    // fiCol is only updated occasionally while stream processing,
    // therefore, the stream sketch could be behind the merged sketch.
    // So we have to recalculate the FiCol on the stream sketch.
    final int ficolA = sk1.fiCol;
    final int ficolB = sk2.fiCol;

    if (!sk1wasMerged && sk2wasMerged) {
      rtAssert(!sk1.mergeFlag && sk2.mergeFlag);
      final int fiCol1 = calculateFirstInterestingColumn(sk1);
      rtAssertEquals(fiCol1, sk2.fiCol);
    } else if (sk1wasMerged && !sk2wasMerged) {
      rtAssert(sk1.mergeFlag && !sk2.mergeFlag);
      final int fiCol2 = calculateFirstInterestingColumn(sk2);
      rtAssertEquals(fiCol2,sk1.fiCol);
    } else {
      rtAssertEquals(sk1.mergeFlag, sk2.mergeFlag);
      rtAssertEquals(ficolA, ficolB);
      rtAssertEquals(sk1.kxp, sk2.kxp, .01 * sk1.kxp); //1% tolerance
      rtAssertEquals(sk1.hipEstAccum, sk2.hipEstAccum, 01 * sk1.hipEstAccum); //1% tolerance
    }
    return true;
  }

  static int calculateFirstInterestingColumn(CpcSketch sketch) {
    int offset = sketch.windowOffset;
    if (offset == 0) {
      return 0;
    }
    PairTable table = sketch.pairTable;
    assert (table != null);
    int[] slots = table.getSlotsArr();
    int numSlots = 1 << table.getLgSizeInts();
    int i;
    int result = offset;
    for (i = 0; i < numSlots; i++) {
      int rowCol = slots[i];
      if (rowCol != -1) {
        int col = rowCol & 63;
        if (col < result) { result = col; }
      }
    }
    return result;
  }

}
