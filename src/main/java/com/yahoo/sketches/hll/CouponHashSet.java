/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_SET_SIZE;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_DENOM;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_NUMER;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.computeLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.extractCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractCurMode;
import static com.yahoo.sketches.hll.PreambleUtil.extractHashSetCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractInt;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;
import static com.yahoo.sketches.hll.PreambleUtil.extractTgtHllType;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class CouponHashSet extends CouponList {

  /**
   * Constructs this sketch with the intent of loading it with data
   * @param lgConfigK the configured Lg K
   * @param tgtHllType the new target Hll type
   */
  CouponHashSet(final int lgConfigK, final TgtHllType tgtHllType) {
    super(lgConfigK, tgtHllType, CurMode.SET);
    assert lgConfigK > 7;
  }

  /**
   * Copy constructor
   * @param that another CouponHashSet
   */
  CouponHashSet(final CouponHashSet that) {
    super(that);
  }

  /**
   * Copy As constructor.
   * @param that another CouponHashSet
   * @param tgtHllType the new target Hll type
   */
  CouponHashSet(final CouponHashSet that, final TgtHllType tgtHllType) {
    super(that, tgtHllType);
  }

  //will also accept List, but results in a Set
  static final CouponHashSet heapifySet(final Memory mem) {
    final int lgConfigK = extractLgK(mem);
    final TgtHllType tgtHllType = extractTgtHllType(mem);

    final CurMode curMode = extractCurMode(mem);
    final int memArrStart = (curMode == CurMode.LIST) ? LIST_INT_ARR_START : HASH_SET_INT_ARR_START;
    final CouponHashSet set = new CouponHashSet(lgConfigK, tgtHllType);
    set.putOutOfOrderFlag(true);
    final boolean memIsCompact = extractCompactFlag(mem);
    final int couponCount = extractHashSetCount(mem);
    int lgCouponArrInts = extractLgArr(mem);
    if (lgCouponArrInts < LG_INIT_SET_SIZE) {
      lgCouponArrInts = computeLgArr(mem, couponCount, lgConfigK);
    }
    if (memIsCompact) {
      for (int i = 0; i < couponCount; i++) {
        set.couponUpdate(extractInt(mem, memArrStart + (i << 2)));
      }
    } else { //updatable
      set.couponCount = couponCount;
      set.lgCouponArrInts = lgCouponArrInts;
      final int couponArrInts = 1 << lgCouponArrInts;
      set.couponIntArr = new int[couponArrInts];
      mem.getIntArray(HASH_SET_INT_ARR_START, set.couponIntArr, 0, couponArrInts);
    }
    return set;
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
      return this; //found duplicate, ignore
    }
    couponIntArr[~index] = coupon; //found empty
    couponCount++;
    if (checkGrowOrPromote()) {
      return promoteHeapListOrSetToHll(this);
    }
    return this;
  }

  @Override
  int getMemDataStart() {
    return HASH_SET_INT_ARR_START;
  }

  @Override
  int getPreInts() {
    return HASH_SET_PREINTS;
  }

  private boolean checkGrowOrPromote() {
    if ((RESIZE_DENOM * couponCount) > (RESIZE_NUMER * (1 << lgCouponArrInts))) {
      if (lgCouponArrInts == (lgConfigK - 3)) { //at max size
        return true; // promote to HLL
      }
      couponIntArr = growHashSet(couponIntArr, ++lgCouponArrInts);
    }
    return false;
  }

  private static final int[] growHashSet(final int[] coupIntArr, final int tgtLgCoupArrSize) {
    final int[] tgtCouponIntArr = new int[1 << tgtLgCoupArrSize]; //create tgt
    final int len = coupIntArr.length;
    for (int i = 0; i < len; i++) { //scan input arr for non-zero values
      final int fetched = coupIntArr[i];
      if (fetched != EMPTY) {
        final int idx = find(tgtCouponIntArr, tgtLgCoupArrSize, fetched); //find empty in tgt
        if (idx < 0) { //found EMPTY
          tgtCouponIntArr[~idx] = fetched; //insert
          continue;
        }
        throw new SketchesStateException("Error: found duplicate.");
      }
    }
    return tgtCouponIntArr;
  }

}
