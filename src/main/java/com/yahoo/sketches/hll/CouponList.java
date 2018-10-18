/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_LIST_SIZE;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_SET_SIZE;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;
import static com.yahoo.sketches.hll.PreambleUtil.extractListCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractTgtHllType;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class CouponList extends AbstractCoupons {
  boolean oooFlag = false; //Out-Of-Order Flag
  int lgCouponArrInts;
  int couponCount;
  int[] couponIntArr;

  /**
   * New instance constructor for LIST or SET.
   * @param lgConfigK the configured Lg K
   * @param tgtHllType the configured HLL target
   * @param curMode LIST or SET
   */
  CouponList(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode) {
    super(lgConfigK, tgtHllType, curMode);
    if (curMode == CurMode.LIST) {
      lgCouponArrInts = LG_INIT_LIST_SIZE;
      oooFlag = false;
    } else { //SET
      lgCouponArrInts = LG_INIT_SET_SIZE;
      assert lgConfigK > 7;
      oooFlag = true;
    }
    couponIntArr = new int[1 << lgCouponArrInts];
    couponCount = 0;
  }

  /**
   * Copy Constructor
   * @param that another CouponArray
   */
  CouponList(final CouponList that) {
    super(that.lgConfigK, that.tgtHllType, that.curMode);
    oooFlag = that.oooFlag;
    lgCouponArrInts = that.lgCouponArrInts;
    couponCount = that.couponCount;
    couponIntArr = that.couponIntArr.clone();
  }

  /**
   * Copy As constructor.
   * @param that another CouponList
   * @param tgtHllType the new target Hll type
   */ //also used by CouponHashSet
  CouponList(final CouponList that, final TgtHllType tgtHllType) {
    super(that.lgConfigK, tgtHllType, that.curMode);
    oooFlag = that.oooFlag;
    lgCouponArrInts = that.lgCouponArrInts;
    couponCount = that.couponCount;
    couponIntArr = that.couponIntArr.clone();
  }

  static final CouponList heapifyList(final Memory mem) {
    final int lgConfigK = extractLgK(mem);
    final TgtHllType tgtHllType = extractTgtHllType(mem);

    final CouponList list = new CouponList(lgConfigK, tgtHllType, CurMode.LIST);
    final int couponCount = extractListCount(mem);
    mem.getIntArray(LIST_INT_ARR_START, list.couponIntArr, 0, couponCount);
    list.couponCount = couponCount;
    list.putOutOfOrderFlag(extractOooFlag(mem));
    return list;
  }

  @Override
  CouponList copy() {
    return new CouponList(this);
  }

  @Override
  CouponList copyAs(final TgtHllType tgtHllType) {
    return new CouponList(this, tgtHllType);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    final int len = 1 << lgCouponArrInts;
    for (int i = 0; i < len; i++) { //search for empty slot
      final int couponAtIdx = couponIntArr[i];
      if (couponAtIdx == EMPTY) {
        couponIntArr[i] = coupon; //update
        couponCount++;
        if (couponCount >= len) { //array full
          if (lgConfigK < 8) {
            return promoteHeapListOrSetToHll(this); //oooFlag = false
          }
          return promoteHeapListToSet(this); //oooFlag = true
        }
        return this;
      }
      //cell not empty
      if (couponAtIdx == coupon) {
        return this; //duplicate
      }
      //cell not empty & not a duplicate, continue
    } //end for
    throw new SketchesStateException("Array invalid: no empties & no duplicates");
  }

  @Override
  int getCompactSerializationBytes() {
    return getMemDataStart() + (couponCount << 2);
  }

  @Override
  int getCouponCount() {
    return couponCount;
  }

  @Override
  int[] getCouponIntArr() {
    return couponIntArr;
  }

  @Override
  PairIterator iterator() {
    return new IntArrayPairIterator(couponIntArr, lgConfigK);
  }

  @Override
  int getLgCouponArrInts() {
    return lgCouponArrInts;
  }

  @Override
  int getMemDataStart() {
    return LIST_INT_ARR_START;
  }

  @Override
  Memory getMemory() {
    return null;
  }

  @Override
  int getPreInts() {
    return LIST_PREINTS;
  }

  @Override
  WritableMemory getWritableMemory() {
    return null;
  }

  @Override
  boolean isCompact() {
    return false;
  }

  @Override
  boolean isMemory() {
    return false;
  }

  @Override
  boolean isOffHeap() {
    return false;
  }

  @Override
  boolean isOutOfOrderFlag() {
    return oooFlag;
  }

  @Override
  boolean isSameResource(final Memory mem) {
    return false;
  }

  @Override
  void putOutOfOrderFlag(final boolean oooFlag) {
    this.oooFlag = oooFlag;
  }

  @Override
  CouponList reset() {
    return new CouponList(lgConfigK, tgtHllType, CurMode.LIST);
  }

  static final HllSketchImpl promoteHeapListToSet(final CouponList list) {
    final int couponCount = list.couponCount;
    final int[] arr = list.couponIntArr;
    final CouponHashSet chSet = new CouponHashSet(list.lgConfigK, list.tgtHllType);
    for (int i = 0; i < couponCount; i++) {
      chSet.couponUpdate(arr[i]);
    }
    chSet.putOutOfOrderFlag(true);
    return chSet;
  }

  //Promotional move of coupons to an HllSketch from either List or Set.
  //called by CouponHashSet.couponUpdate()
  //called by CouponList.couponUpdate()
  static final HllSketchImpl promoteHeapListOrSetToHll(final CouponList src) {
    final HllArray tgtHllArr = HllArray.newHeapHll(src.lgConfigK, src.tgtHllType);
    final PairIterator srcItr = src.iterator();
    tgtHllArr.putKxQ0(1 << src.lgConfigK);
    while (srcItr.nextValid()) {
      tgtHllArr.couponUpdate(srcItr.getPair());
    }
    tgtHllArr.putHipAccum(src.getEstimate());

    tgtHllArr.putOutOfOrderFlag(false);
    return tgtHllArr;
  }

}
