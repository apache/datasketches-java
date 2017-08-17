/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.COUPON_RSE;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_LIST_SIZE;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_INT_ARR_START;
import static java.lang.Math.max;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
abstract class AbstractCoupons extends HllSketchImpl {

  AbstractCoupons(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode) {
    super(lgConfigK, tgtHllType, curMode);
  }

  @Override
  int getCompactSerializationBytes() {
    final int dataStart = (getCurMode() == CurMode.LIST)
        ? LIST_INT_ARR_START
        : HASH_SET_INT_ARR_START;
    return dataStart +  (getCouponCount() << 2);
  }

  @Override
  double getCompositeEstimate() {
    return getEstimate();
  }

  abstract int getCouponCount();

  abstract void getCouponsToMemoryInts(WritableMemory dstWmem, int lenInts);

  /**
   * This is the estimator for the Coupon List mode and Coupon Hash Set mode.
   *
   * <p>Note: This is an approximation to the true mapping from numCoupons to N,
   * which has a range of validity roughly from 0 to 6 million coupons.</p>
   *
   * <p>The k of the implied coupon sketch, which must not be confused with the k of the HLL
   * sketch.  In this application k is always 2^26, which is the number of address bits of the
   * 32-bit coupon.</p>
   * @return the unique count estimate.
   */
  @Override
  double getEstimate() {
    return getEstimate(getCouponCount());
  }

  static final double getEstimate(final int couponCount) {
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    return max(est, couponCount);
  }

  abstract int getLgCouponArrInts();

  @Override
  double getLowerBound(final int numStdDev) {
    final int couponCount = getCouponCount();
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    final double tmp = est / (1.0 + couponEstimatorEps(numStdDev));
    return max(tmp, couponCount);
  }

  @Override
  double getUpperBound(final int numStdDev) {
    final int couponCount = getCouponCount();
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    final double tmp = est / (1.0 - couponEstimatorEps(numStdDev));
    return max(tmp, couponCount);
  }

  @Override
  int getUpdatableSerializationBytes() {
    if (getCurMode() == CurMode.LIST) {
      return LIST_INT_ARR_START + (4 << LG_INIT_LIST_SIZE);
    } else {
      return HASH_SET_INT_ARR_START + (4 << getLgCouponArrInts());
    }
  }

  @Override
  boolean isEmpty() {
    return getCouponCount() == 0;
  }

  abstract void putCouponCount(int couponCount);

  abstract void putCouponsFromMemoryInts(Memory srcMem, int lenInts);

  abstract void putCouponIntArr(int[] couponIntArr);

  abstract void putLgCouponArrInts(int lgCouponArrInts);

  static final double couponEstimatorEps(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    return (numStdDev * COUPON_RSE);
  }

}
