/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_DENOM;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_NUMER;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class CouponHashSet extends CouponList {

  /**
   * Standard constructor
   * @param lgConfigK the configured Lg K
   * @param curMode SET
   */
  CouponHashSet(final int lgConfigK, final TgtHllType tgtHllType) {
    super(lgConfigK, tgtHllType, CurMode.SET);
  }

  /**
   * Copy constructor
   * @param that another CouponHashSet
   */
  CouponHashSet(final CouponHashSet that) {
    super(that);
  }

  /**
   * Copy As constructor. Performs an isomorphic transformation.
   * @param that another CouponHasSet
   * @param tgtHllType the new target Hll type
   */
  private CouponHashSet(final CouponHashSet that, final TgtHllType tgtHllType) {
    super(that, tgtHllType);
  }

  @Override
  CouponHashSet copy() {
    return new CouponHashSet(this);
  }

  @Override
  CouponHashSet copyAs(final TgtHllType tgtHllType) {
    return new CouponHashSet(this, tgtHllType);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    final int index = find(couponIntArr, lgCouponArrInts, coupon);
    if (index >= 0) {
      return this;
    } //found duplicate, ignore
    couponIntArr[~index] = coupon;
    couponCount++;
    super.oooFlag = true; //could be moved out
    final boolean promote = checkGrowOrPromote();
    if (!promote) { return this; }
    return HllUtil.makeHllFromCoupons(this, lgConfigK, tgtHllType);
  }

  private boolean checkGrowOrPromote() {
    final int len = 1 << lgCouponArrInts;
    if ((RESIZE_DENOM * couponCount) > (RESIZE_NUMER * len)) {
      if (len == (1 << lgMaxArrInts)) {
        return true; // promote
      }
      couponIntArr = growHashSet(couponIntArr, lgCouponArrInts); //TODO ask for more memory
      lgCouponArrInts++;
    }
    return false;
  }

  private static final int[] growHashSet(final int[] coupIntArr, final int lgCoupArrSize) {
    final int newLgCoupArrSize = lgCoupArrSize + 1;
    final int newArrSize = 1 << newLgCoupArrSize;
    final int newArrMask = newArrSize - 1;
    final int[] newCoupIntArr = new int[newArrSize];
    final int len = coupIntArr.length;
    for (int i = 0; i < len; i++) {
      final int fetched = coupIntArr[i];
      if (fetched != EMPTY) {
        final int idx = find(newCoupIntArr, newLgCoupArrSize, fetched & newArrMask);
        if (idx < 0) { //found EMPTY
          newCoupIntArr[~idx] = fetched; //insert
          continue;
        }
        throw new SketchesStateException("Error: found duplicate.");
      }
    }
    return newCoupIntArr;
  }

  private static final int find(final int[] array, final int lgArr, final int coupon) {
    final int arrMask = (1 << lgArr) - 1;
    int probe = coupon & arrMask;
    final int loopIndex = probe;
    do {
      if (array[probe] == EMPTY) {
        return ~probe; //empty
      } else if (coupon == array[probe]) { return probe; } //duplicate
      final int stride = ((coupon & KEY_MASK_26) >>> lgArr) | 1;
      probe = (probe + stride) & arrMask;
    } while (probe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

}
