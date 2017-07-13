/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class HllUtil {
  static final int KEY_BITS_26 = 26;
  static final int VAL_BITS_6 = 6;
  static final int KEY_MASK_26 = (1 << KEY_BITS_26) - 1;
  static final int VAL_MASK_6 = (1 << VAL_BITS_6) - 1;
  static final int EMPTY = 0;
  static final int MIN_LOG_K = 4;
  static final int MAX_LOG_K = 21;

  static final int RESIZE_NUMER = 3;
  static final int RESIZE_DENOM = 4;

  static final int checkLgK(final int lgK) {
    if ((lgK >= MIN_LOG_K) && (lgK <= MAX_LOG_K)) { return lgK; }
    throw new SketchesArgumentException(
      "Log K must be between 4 and 21, inclusive: " + lgK);
  }

  //when called from CouponList, tgtLgK == lgConfigK
  static final CouponHashSet makeSetFromList(final CouponList list, final int tgtLgK,
      final TgtHllType tgtHllType) {
    assert tgtLgK <= list.getLgConfigK();
    final int couponCount = list.getCouponCount();
    final int[] arr = list.getCouponIntArr();
    final CouponHashSet chSet = new CouponHashSet(tgtLgK, tgtHllType);
    for (int i = 0; i < couponCount; i++) {
      chSet.couponUpdate(arr[i]);
    }
    chSet.putOutOfOrderFlag(true);
    return chSet;
  }

  //This is ONLY called when src is not in HLL mode and creating a new tgt HLL
  //Src can be either list or set.
  //Used by CouponList, CouponHashSet and the Union operator
  static final HllSketchImpl makeHllFromCoupons(final CouponList src, final int tgtLgK,
      final TgtHllType tgtHllType) {
    final HllArray tgtHllArr = newHll(tgtLgK, tgtHllType);
    final PairIterator srcItr = src.getIterator();
    while (srcItr.nextValid()) {
      tgtHllArr.couponUpdate(srcItr.getPair());
    }
    tgtHllArr.putHipAccum(src.getEstimate());
    tgtHllArr.putOutOfOrderFlag(false);
    return tgtHllArr;
  }

  //Used by union operator and HllSketch pairUpdate.  Always copies or downsamples to HLL_8.
  //Caller must ultimately manage oooFlag, as caller has more info
  static final HllSketchImpl copyOrDownsampleHll(
      final HllSketchImpl srcSketch, final int tgtLgK) {
    final HllArray src = (HllArray) srcSketch;
    final int srcLgK = src.getLgConfigK();
    if ((srcLgK <= tgtLgK) && (src.getTgtHllType() == TgtHllType.HLL_8)) {
      return src.copy();
    }
    final int minLgK = Math.min(srcLgK, tgtLgK);
    final HllArray tgtHllArr = newHll(minLgK, TgtHllType.HLL_8);
    final PairIterator srcItr = src.getIterator();
    while (srcItr.nextValid()) {
      tgtHllArr.couponUpdate(srcItr.getPair());
    }
    //both of these are required for isomorphism
    tgtHllArr.putHipAccum(src.getHipAccum());
    tgtHllArr.putOutOfOrderFlag(src.isOutOfOrderFlag());
    return tgtHllArr;
  }

  static final void checkNumStdDev(final int numStdDev) {
    if ((numStdDev < 1) || (numStdDev > 3)) {
      throw new SketchesArgumentException(
          "NumStdDev may not be less than 1 or greater than 3.");
    }
  }

  private static final HllArray newHll(final int tgtLgK, final TgtHllType tgtHllType) {
    if (tgtHllType == HLL_4) { return new Hll4Array(tgtLgK); }
    if (tgtHllType == HLL_6) { return new Hll6Array(tgtLgK); }
    return new Hll8Array(tgtLgK);
  }

}
