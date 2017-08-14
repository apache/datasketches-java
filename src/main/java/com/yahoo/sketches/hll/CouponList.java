/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_LIST_SIZE;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_SET_SIZE;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;
import static com.yahoo.sketches.hll.PreambleUtil.extractListCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractTgtHllType;
import static com.yahoo.sketches.hll.PreambleUtil.insertCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMode;
import static com.yahoo.sketches.hll.PreambleUtil.insertEmptyFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.insertHashSetCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgK;
import static com.yahoo.sketches.hll.PreambleUtil.insertListCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.hll.PreambleUtil.insertTgtHllType;

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
   * Standard constructor for LIST or SET.
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
    final Object memArr = ((WritableMemory) mem).getArray();
    final long memAdd = mem.getCumulativeOffset(0);
    final int lgConfigK = extractLgK(memArr, memAdd);
    final TgtHllType tgtHllType = extractTgtHllType(memArr, memAdd);

    final CouponList list = new CouponList(lgConfigK, tgtHllType, CurMode.LIST);
    final int couponCount = extractListCount(memArr, memAdd);
    list.putCouponsFromMemoryInts(mem, couponCount);
    list.putCouponCount(couponCount);
    list.putOutOfOrderFlag(extractOooFlag(memArr, memAdd));
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

  @Override //get coupons from internal int[] to dstMem
  //Called by CouponList.insertList
  //Called by CouponList.insertSet
  void getCouponsToMemoryInts(final WritableMemory dstWmem, final int lenInts) {
    dstWmem.putIntArray(LIST_INT_ARR_START, couponIntArr, 0, lenInts);
  }

  @Override //put coupons from srcMem to internal int[]
  //Called by CouponList.heapifyList(Memory)
  void putCouponsFromMemoryInts(final Memory srcMem, final int lenInts) {
    srcMem.getIntArray(LIST_INT_ARR_START, couponIntArr, 0, lenInts);
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
            return promoteHeapListOrSetToHll(this);//oooFlag = false
          }
          return promoteHeapListToSet(this); //oooFlag = true
        }
        return this;
      }
      //cell not empty
      if (couponAtIdx == coupon) { return this; } //duplicate
      //cell not empty & not a duplicate, continue
    } //end for
    throw new SketchesStateException("Array invalid: no empties & no duplicates");
  }

  @Override
  int getCouponCount() {
    return couponCount;
  }

  @Override
  PairIterator getIterator() {
    return new IntArrayPairIterator(couponIntArr, lgConfigK);
  }

  @Override
  int getLgCouponArrInts() {
    return lgCouponArrInts;
  }

  @Override
  int getMemArrStart() {
    return LIST_INT_ARR_START;
  }

  @Override
  int getPreInts() {
    return LIST_PREINTS;
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
  void putCouponCount(final int couponCount) {
    this.couponCount = couponCount;
  }

  @Override
  void putCouponIntArr(final int[] couponIntArr) {
    this.couponIntArr = couponIntArr;
  }

  @Override
  void putLgCouponArrInts(final int lgCouponArrInts) {
    this.lgCouponArrInts = lgCouponArrInts;
  }

  @Override
  void putOutOfOrderFlag(final boolean oooFlag) {
    this.oooFlag = oooFlag;
  }

  @Override
  byte[] toCompactByteArray() {
    return toByteArray(this, true);
  }

  @Override
  byte[] toUpdatableByteArray() {
    return toByteArray(this, false);
  }

  private static final byte[] toByteArray(final AbstractCoupons impl, final boolean compact) {
    final byte[] byteArr;
    final int arrLenBytes;
    arrLenBytes = (compact)
        ? impl.getCouponCount() << 2
        : 4 << impl.getLgCouponArrInts();
    byteArr = new byte[impl.getMemArrStart() + arrLenBytes];
    final WritableMemory wmem = WritableMemory.wrap(byteArr);

    if (impl.getCurMode() == CurMode.LIST) {
      insertList(impl, wmem, compact);
    } else { //SET
      insertSet(impl, wmem, compact);
    }
    return byteArr;
  }

  //wmem must be clear.
  //Called by CouponList.toByteArray()
  static final void insertList(final AbstractCoupons impl, final WritableMemory wmem,
      final boolean compact) {
    final Object memObj = wmem.getArray();
    final long memAdd = wmem.getCumulativeOffset(0L);

    final int couponCount = impl.getCouponCount();
    insertListCount(memObj, memAdd, couponCount);
    insertCompactFlag(memObj, memAdd, compact);
    insertCommonList(impl, memObj, memAdd);
    final int lenInts = (compact) ? couponCount : 1 << impl.getLgCouponArrInts();
    impl.getCouponsToMemoryInts(wmem, lenInts);
  }

  //wmem must be clear.  impl must be a set
  //Called by CouponList.toByteArray()
  static final void insertSet(final AbstractCoupons impl, final WritableMemory wmem,
      final boolean compact) {
    final Object memObj = wmem.getArray();
    final long memAdd = wmem.getCumulativeOffset(0L);

    insertHashSetCount(memObj, memAdd, impl.getCouponCount());
    insertCompactFlag(memObj, memAdd, compact);
    insertCommonList(impl, memObj, memAdd);

    if (compact) {
      final PairIterator itr = impl.getIterator();
      int cnt = 0;
      while (itr.nextValid()) {
        wmem.putInt(HASH_SET_INT_ARR_START + (cnt++ << 2), itr.getPair());
      }
    } else { //updatable
      impl.getCouponsToMemoryInts(wmem, 1 << impl.getLgCouponArrInts());
    }
  }

  static final void insertCommonList(final AbstractCoupons impl, final Object memObj,
      final long memAdd) {
    insertPreInts(memObj, memAdd, impl.getPreInts());
    insertSerVer(memObj, memAdd);
    insertFamilyId(memObj, memAdd);
    insertLgK(memObj, memAdd, impl.getLgConfigK());
    insertLgArr(memObj, memAdd, impl.getLgCouponArrInts());
    insertEmptyFlag(memObj, memAdd, impl.isEmpty());
    insertOooFlag(memObj, memAdd, impl.isOutOfOrderFlag());
    insertCurMode(memObj, memAdd, impl.getCurMode());
    insertTgtHllType(memObj, memAdd, impl.getTgtHllType());
  }

  //Promotional move of coupons to an HllSketch from either List or Set.
  //called by CouponHashSet.couponUpdate()
  //called by CouponList.couponUpdate()
  static final HllSketchImpl promoteHeapListOrSetToHll(final CouponList src) {
    final HllArray tgtHllArr = HllArray.newHll(src.lgConfigK, src.tgtHllType);
    final PairIterator srcItr = src.getIterator();
    tgtHllArr.putKxQ0(1 << src.lgConfigK);
    while (srcItr.nextValid()) {
      tgtHllArr.couponUpdate(srcItr.getPair());
    }
    tgtHllArr.putHipAccum(src.getEstimate());

    tgtHllArr.putOutOfOrderFlag(false);
    return tgtHllArr;
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
}
