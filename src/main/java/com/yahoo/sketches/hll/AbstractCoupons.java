/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.COUPON_RSE;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static com.yahoo.sketches.hll.ToByteArrayImpl.toCouponByteArray;
import static java.lang.Math.max;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
abstract class AbstractCoupons extends HllSketchImpl {

  AbstractCoupons(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode) {
    super(lgConfigK, tgtHllType, curMode);
  }

  @Override
  double getCompositeEstimate() {
    return getEstimate();
  }

  abstract int getCouponCount();

  abstract int[] getCouponIntArr();

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
    final int couponCount = getCouponCount();
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    return max(est, couponCount);
  }

  abstract int getLgCouponArrInts();

  @Override
  double getLowerBound(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    final int couponCount = getCouponCount();
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    final double tmp = est / (1.0 + (numStdDev * COUPON_RSE));
    return max(tmp, couponCount);
  }

  abstract Memory getMemory();

  @Override
  double getUpperBound(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    final int couponCount = getCouponCount();
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    final double tmp = est / (1.0 - (numStdDev * COUPON_RSE));
    return max(tmp, couponCount);
  }

  @Override
  int getUpdatableSerializationBytes() {
    return getMemDataStart() + (4 << getLgCouponArrInts());
  }

  @Override
  boolean isEmpty() {
    return getCouponCount() == 0;
  }

  @Override
  byte[] toCompactByteArray() {
    return toCouponByteArray(this, true);
  }

  @Override
  byte[] toUpdatableByteArray() {
    return toCouponByteArray(this, false);
  }

  //FIND for Heap and Direct
  //Searches the Coupon hash table for an empty slot or a duplicate depending on the context.
  //If entire entry is empty, returns one's complement of index = found empty.
  //If entry equals given coupon, returns its index = found duplicate coupon
  //Continues searching
  //If the probe comes back to original index, throws an exception.
  //Called by CouponHashSet.couponUpdate()
  //Called by CouponHashSet.growHashSet()
  //Called by DirectCouponHashSet.growHashSet()
  static final int find(final int[] array, final int lgArrInts, final int coupon) {
    final int arrMask = array.length - 1;
    int probe = coupon & arrMask;
    final int loopIndex = probe;
    do {
      final int couponAtIdx = array[probe];
      if (couponAtIdx == EMPTY) {
        return ~probe; //empty
      }
      else if (coupon == couponAtIdx) {
        return probe; //duplicate
      }
      final int stride = ((coupon & KEY_MASK_26) >>> lgArrInts) | 1;
      probe = (probe + stride) & arrMask;
    } while (probe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

}
