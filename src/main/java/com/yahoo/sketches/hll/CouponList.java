/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.COUPON_RSE;
import static com.yahoo.sketches.hll.HllUtil.COUPON_RSE_FACTOR;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_LIST_SIZE;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_SET_SIZE;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_PREINTS;
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
import static java.lang.Math.max;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class CouponList extends HllSketchImpl {
  final int lgConfigK;
  final TgtHllType tgtHllType;
  final CurMode curMode;
  final int lgMaxCouponArrInts;
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
  CouponList(final int lgConfigK, final TgtHllType tgtHllType,
      final CurMode curMode) {
    this.lgConfigK = lgConfigK;
    this.tgtHllType = tgtHllType;
    this.curMode = curMode;
    if (curMode == CurMode.LIST) {
      lgCouponArrInts = lgMaxCouponArrInts = LG_INIT_LIST_SIZE;
      oooFlag = false;
    } else { //SET
      lgCouponArrInts = LG_INIT_SET_SIZE;
      lgMaxCouponArrInts = lgConfigK - 3;
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
    lgConfigK = that.getLgConfigK();
    tgtHllType = that.getTgtHllType();
    curMode = that.getCurMode();
    oooFlag = that.isOutOfOrderFlag();
    lgCouponArrInts = that.getLgCouponArrInts();
    couponCount = that.getCouponCount();
    couponIntArr = that.getCouponIntArr().clone();
    lgMaxCouponArrInts = that.getLgMaxCouponArrInts();
  }

  /**
   * Copy As constructor.
   * @param that another CouponList
   * @param tgtHllType the new target Hll type
   */ //also used by CouponHashSet
  CouponList(final CouponList that, final TgtHllType tgtHllType) {
    super();
    lgConfigK = that.getLgConfigK();
    this.tgtHllType = tgtHllType;
    curMode = that.getCurMode();
    oooFlag = that.isOutOfOrderFlag();
    lgCouponArrInts = that.getLgCouponArrInts();
    couponCount = that.getCouponCount();
    couponIntArr = that.getCouponIntArr().clone();
    lgMaxCouponArrInts = that.getLgMaxCouponArrInts();
  }

  //only does list
  static final CouponList heapifyList(final Memory mem) {
    final Object memArr = ((WritableMemory) mem).getArray();
    final long memAdd = mem.getCumulativeOffset(0);
    final int lgConfigK = extractLgK(memArr, memAdd);
    final TgtHllType tgtHllType = extractTgtHllType(memArr, memAdd);

    final CouponList list = new CouponList(lgConfigK, tgtHllType, CurMode.LIST);
    final int couponCount = extractListCount(memArr, memAdd);
    final int[] data = new int[couponCount];
    mem.getIntArray(LIST_INT_ARR_START, data, 0, couponCount);

    for (int i = 0; i < couponCount; i++) { //LIST is always stored compact
      list.couponUpdate(data[i]);
    }

    list.putOutOfOrderFlag(extractOooFlag(memArr, memAdd));
    return list;
  }

  //when called from CouponList, tgtLgK == lgConfigK
  static final HllSketchImpl morphFromListToSet(final CouponList list) {
    final int couponCount = list.couponCount;
    final int[] arr = list.couponIntArr;
    final CouponHashSet chSet = new CouponHashSet(list.lgConfigK, list.tgtHllType);
    for (int i = 0; i < couponCount; i++) {
      chSet.couponUpdate(arr[i]);
    }
    chSet.putOutOfOrderFlag(true);
    return chSet;
  }

  //This is ONLY called when src is not in HLL mode and creating a new tgt HLL
  //Src can be either list or set.
  //Used by CouponList, CouponHashSet
  static final HllSketchImpl morphFromCouponsToHll(final CouponList src) {
    final HllArray tgtHllArr = HllArray.newHll(src.lgConfigK, src.tgtHllType);
    final PairIterator srcItr = src.getIterator();
    while (srcItr.nextValid()) {
      tgtHllArr.couponUpdate(srcItr.getPair());
    }
    tgtHllArr.putHipAccum(src.getEstimate());
    tgtHllArr.putOutOfOrderFlag(false);
    return tgtHllArr;
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
    final int len = 1 << getLgCouponArrInts();
    final int lgConfigK = getLgConfigK();
    for (int i = 0; i < len; i++) { //search for empty slot
      final int couponAtIdx = couponIntArr[i];
      if (couponAtIdx == EMPTY) {
        couponIntArr[i] = coupon; //update
        couponCount++;
        if (couponCount >= len) { //array full
          if (lgConfigK < 8) {
            return morphFromCouponsToHll(this);//oooFlag = false
          }
          return morphFromListToSet(this); //oooFlag = true
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
  AuxHashMap getAuxHashMap() {
    return null;
  }

  @Override
  PairIterator getAuxIterator() {
    return null; //always null from LIST or SET
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
  int getCurMin() {
    return -1;
  }

  @Override
  CurMode getCurMode() {
    return curMode;
  }

  @Override
  int getCompactSerializationBytes() {
    final int dataStart = (getCurMode() == CurMode.LIST) ? LIST_INT_ARR_START
        : HASH_SET_INT_ARR_START;
    return dataStart +  (getCouponCount() << 2);
  }

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

  @Override
  double getCompositeEstimate() {
    return getEstimate();
  }

  @Override
  double getHipAccum() {
    return getCouponCount();
  }

  @Override
  byte[] getHllByteArr() {
    return null;
  }

  @Override
  PairIterator getIterator() {
    return new CouponIterator();
  }

  @Override
  double getKxQ0() {
    return -1.0;
  }

  @Override
  double getKxQ1() {
    return -1.0;
  }

  @Override
  int getLgConfigK() {
    return lgConfigK;
  }

  @Override
  int getLgCouponArrInts() {
    return lgCouponArrInts;
  }

  @Override
  int getLgMaxCouponArrInts() {
    return lgMaxCouponArrInts;
  }

  @Override
  double getLowerBound(final int numStdDev) {
    final int couponCount = getCouponCount();
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    final double tmp = est / (1.0 + couponEstimatorEps(numStdDev));
    return max(tmp, couponCount);
  }

  @Override
  int getNumAtCurMin() {
    return -1;
  }

  @Override
  double getRelErr(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    return numStdDev * COUPON_RSE;
  }

  @Override
  double getRelErrFactor(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    return numStdDev * COUPON_RSE_FACTOR;
  }

  @Override
  TgtHllType getTgtHllType() {
    return tgtHllType;
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
  double getUpperBound(final int numStdDev) {
    final int couponCount = getCouponCount();
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    final double tmp = est / (1.0 - couponEstimatorEps(numStdDev));
    return max(tmp, couponCount);
  }

  void incCouponCount() {
    couponCount++;
  }

  @Override
  boolean isEmpty() {
    return getCouponCount() == 0;
  }

  @Override
  boolean isOutOfOrderFlag() {
    return oooFlag;
  }

  @Override
  void putCouponCount(final int couponCount) {
    this.couponCount = couponCount;
  }

  void putCouponIntArr(final int[] couponIntArr, final int lgCouponArrInts) {
    this.couponIntArr = couponIntArr;
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

  private static final byte[] toByteArray(final HllSketchImpl impl, final boolean compact) {
    final byte[] byteArr;

    if (impl.getCurMode() == CurMode.LIST) {
      byteArr = new byte[LIST_INT_ARR_START + (impl.getCouponCount() << 2)];
      final WritableMemory wmem = WritableMemory.wrap(byteArr);
      insertList(impl, wmem, compact);

    } else { //SET
      final int bytes = (compact) ? impl.getCouponCount() << 2 : 4 << impl.getLgCouponArrInts();
      byteArr = new byte[HASH_SET_INT_ARR_START + bytes];
      final WritableMemory wmem = WritableMemory.wrap(byteArr);
      insertSet(impl, wmem, compact);
    }
    return byteArr;
  }

  //also used by DirectCouponList. wmem must be clear
  static final void insertList(final HllSketchImpl impl, final WritableMemory wmem,
      final boolean compact) {
    final Object memObj = wmem.getArray();
    final long memAdd = wmem.getCumulativeOffset(0L);
    final int couponCount = impl.getCouponCount();
    final int[] couponIntArr = impl.getCouponIntArr();
    insertPreInts(memObj, memAdd, LIST_PREINTS);
    insertListCount(memObj, memAdd, couponCount);
    insertCompactFlag(memObj, memAdd, compact);
    insertCommonList(impl, memObj, memAdd);

    wmem.putIntArray(LIST_INT_ARR_START, couponIntArr, 0, couponCount);
  }

  //also used by DirectCouponList. wmem must be clear
  static final void insertSet(final HllSketchImpl impl, final WritableMemory wmem,
      final boolean compact) {
    final Object memObj = wmem.getArray();
    final long memAdd = wmem.getCumulativeOffset(0L);
    final int couponCount = impl.getCouponCount();
    final int[] couponIntArr = impl.getCouponIntArr();
    insertPreInts(memObj, memAdd, HASH_SET_PREINTS);
    insertHashSetCount(memObj, memAdd, couponCount);
    insertCompactFlag(memObj, memAdd, compact);
    insertCommonList(impl, memObj, memAdd);

    if (compact) {
      final PairIterator itr = impl.getIterator();
      int cnt = 0;
      while (itr.nextValid()) {
        wmem.putInt(HASH_SET_INT_ARR_START + (cnt++ << 2), itr.getPair());
      }
    } else { //updatable
      wmem.putIntArray(HASH_SET_INT_ARR_START, couponIntArr, 0, 1 << impl.getLgCouponArrInts());
    }
  }

  static final void insertCommonList(final HllSketchImpl impl, final Object memObj,
      final long memAdd) {
    insertSerVer(memObj, memAdd);
    insertFamilyId(memObj, memAdd);
    insertLgK(memObj, memAdd, impl.getLgConfigK());
    insertLgArr(memObj, memAdd, impl.getLgCouponArrInts());
    insertEmptyFlag(memObj, memAdd, impl.isEmpty());
    insertOooFlag(memObj, memAdd, impl.isOutOfOrderFlag());
    insertCurMode(memObj, memAdd, impl.getCurMode());
    insertTgtHllType(memObj, memAdd, impl.getTgtHllType());
  }

  //Iterator for SET and LIST

  final class CouponIterator implements PairIterator {
    final int len;
    int index;
    int coupon;
    final int[] array;

    CouponIterator() {
      array = getCouponIntArr();
      len = array.length;
      index = - 1;
    }

    @Override
    public boolean nextValid() {
      while (++index < len) {
        final int coupon = array[index];
        if (array[index] != EMPTY) {
          this.coupon = coupon;
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean nextAll() {
      if (++index < len) {
        coupon = array[index];
        return true;
      }
      return false;
    }

    @Override
    public int getPair() {
      return array[index];
    }

    @Override
    public int getKey() {
      return BaseHllSketch.getLow26(coupon);
    }

    @Override
    public int getValue() {
      return BaseHllSketch.getValue(coupon);
    }

    @Override
    public int getIndex() {
      return index;
    }
  }
  //END Iterators

  static final double couponEstimatorEps(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    return (numStdDev * COUPON_RSE);
  }

}
