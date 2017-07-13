/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.PreambleUtil.FAMILY_ID;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.SER_VER;
import static com.yahoo.sketches.hll.PreambleUtil.extractCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.extractHashSetCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractInt;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;
import static com.yahoo.sketches.hll.PreambleUtil.extractListCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.extractSerVer;
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
  // This RSE is computed at the transition point from coupons to HLL and not for the asymptote.
  private static final double COUPON_RSE_FACTOR = .409;
  private static final int LG_INIT_LIST_SIZE = 3;
  private static final int LG_INIT_SET_SIZE = 5;
  private static final double COUPON_RSE = COUPON_RSE_FACTOR / (1 << 13);

  final int lgMaxCouponArrInts;
  int lgCouponArrInts;
  int couponCount;
  int[] couponIntArr;

  /**
   * Standard constructor for LIST or SET.
   * @param lgConfigK the configured Lg K
   * @param tgtHllType the configured HLL target
   * @param curMode LIST or SET
   */
  CouponList(final int lgConfigK,
      final TgtHllType tgtHllType,
      final CurMode curMode) {
    super(lgConfigK, tgtHllType, curMode);
    if (curMode == CurMode.LIST) {
      lgCouponArrInts = lgMaxCouponArrInts = LG_INIT_LIST_SIZE;
    } else { //SET
      lgCouponArrInts = LG_INIT_SET_SIZE;
      lgMaxCouponArrInts = lgConfigK - 3;
    }
    couponIntArr = new int[1 << lgCouponArrInts];
    couponCount = 0;
  }

  /**
   * Copy Constructor
   * @param that another CouponArray
   */
  CouponList(final CouponList that) {
    super(that);
    lgCouponArrInts = that.lgCouponArrInts;
    couponCount = that.couponCount;
    couponIntArr = that.couponIntArr.clone();
    lgMaxCouponArrInts = that.lgMaxCouponArrInts;
  }

  /**
   * Copy As constructor. Performs an isomorphic transformation.
   * @param that another CouponList
   * @param tgtHllType the new target Hll type
   */ //also used by CouponHashSet
  CouponList(final CouponList that, final TgtHllType tgtHllType) {
    super(that, tgtHllType);
    lgCouponArrInts = that.lgCouponArrInts;
    couponCount = that.couponCount;
    couponIntArr = that.couponIntArr.clone();
    lgMaxCouponArrInts = that.lgMaxCouponArrInts;
  }

  //Factory
  static final CouponList heapify(final Memory mem, final CurMode curMode) {
    final Object memArr = ((WritableMemory) mem).getArray();
    final long memAdd = mem.getCumulativeOffset(0);
    checkPreamble(mem, memArr, memAdd, curMode);
    final int lgConfigK = extractLgK(memArr, memAdd);
    final TgtHllType tgtHllType = extractTgtHllType(memArr, memAdd);
    final int lgCouponArrInts = extractLgArr(memArr, memAdd);
    final boolean compact = extractCompactFlag(memArr, memAdd);

    if (curMode == CurMode.LIST) {
      final CouponList list = new CouponList(lgConfigK, tgtHllType, curMode);
      final int couponCount = extractListCount(memArr, memAdd);
      final int[] data = new int[couponCount];
      mem.getIntArray(LIST_INT_ARR_START, data, 0, couponCount);

      for (int i = 0; i < couponCount; i++) { //LIST is always stored compact
        list.couponUpdate(data[i]);
      }

      list.putOutOfOrderFlag(extractOooFlag(memArr, memAdd));
      return list;
    }

    // else SET
    final CouponHashSet set = new CouponHashSet(lgConfigK, tgtHllType);
    final int couponCount = extractHashSetCount(memArr, memAdd);
    if (compact) {
      for (int i = 0; i < couponCount; i++) {
        final int coupon = extractInt(memArr, memAdd, HASH_SET_INT_ARR_START + (i << 2));
        if (coupon == EMPTY) { continue; }
        set.couponUpdate(coupon);
      }
    } else { //updatable
      set.putCouponCount(couponCount);
      final int couponArrInts = 1 << lgCouponArrInts;
      final int[] newCouponIntArr = new int[couponArrInts];
      mem.getIntArray(HASH_SET_INT_ARR_START, newCouponIntArr, 0, couponArrInts);
      set.putCouponIntArr(newCouponIntArr, lgCouponArrInts);
    }

    set.putOutOfOrderFlag(true);
    return set;
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
    for (int i = 0; i < len; i++) { //search for empty slot
      final int[] couponIntArr = getCouponIntArr();
      final int indexValue = couponIntArr[i];
      if (indexValue == EMPTY) {
        putIntoCouponIntArr(i, coupon); //update
        incCouponCount();
        if (getCouponCount() >= len) { //array full
          final int lgConfigK = getLgConfigK();
          if (lgConfigK < 8) {
            return HllUtil.makeHllFromCoupons(this, lgConfigK, getTgtHllType()); //sets oooFlag false
          }
          return HllUtil.makeSetFromList(this, lgConfigK, getTgtHllType()); //sets oooFlag true
        }
        return this;
      }
      //cell not empty
      if (indexValue == coupon) { return this; } //duplicate
      //cell not empty & not a duplicate, continue
    } //end for
    throw new SketchesStateException("Array invalid: no empties & no duplicates");
  }

  @Override
  PairIterator getAuxIterator() {
    return null; //always null from LIST or SET
  }

  @Override
  int getCouponCount() {
    return couponCount;
  }

  int[] getCouponIntArr() {
    return couponIntArr;
  }

  @Override
  int getCurMin() {
    return -1;
  }

  @Override
  int getCurrentSerializationBytes() {
    final int dataStart = (getCurMode() == CurMode.LIST) ? LIST_INT_ARR_START : HASH_SET_INT_ARR_START;
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
    final int couponCount = getCouponCount();
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
  PairIterator getIterator() {
    return new CouponIterator();
  }

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

  void putCouponCount(final int couponCount) {
    this.couponCount = couponCount;
  }

  void putCouponIntArr(final int[] couponIntArr, final int lgCouponArrInts) {
    this.couponIntArr = couponIntArr;
    this.lgCouponArrInts = lgCouponArrInts;
  }

  void putIntoCouponIntArr(final int index, final int value) {
    couponIntArr[index] = value;
  }

  @Override
  byte[] toCompactByteArray() {
    return toByteArray(true);
  }

  @Override
  byte[] toUpdatableByteArray() {
    return toByteArray(false);
  }

  private byte[] toByteArray(final boolean compact) {
    final byte[] memArr;
    final WritableMemory wmem;
    final long memAdd;
    final int couponCount = getCouponCount();
    final int[] couponIntArr = getCouponIntArr();

    if (getCurMode() == CurMode.LIST) {
      memArr = new byte[LIST_INT_ARR_START + (couponCount << 2)];
      wmem = WritableMemory.wrap(memArr);
      memAdd = wmem.getCumulativeOffset(0);
      insertPreInts(memArr, memAdd, LIST_PREINTS);
      insertListCount(memArr, memAdd, couponCount);
      insertCompactFlag(memArr, memAdd, compact);
      insertCommon(memArr, memAdd);
      wmem.putIntArray(LIST_INT_ARR_START, couponIntArr, 0, couponCount);

    } else { //SET
      final int lgCouponArrInts = getLgCouponArrInts();
      final int len = (compact) ? couponCount << 2 : 4 << lgCouponArrInts;
      memArr = new byte[HASH_SET_INT_ARR_START + len];
      wmem = WritableMemory.wrap(memArr);
      memAdd = wmem.getCumulativeOffset(0);
      insertPreInts(memArr, memAdd, HASH_SET_PREINTS);
      insertHashSetCount(memArr, memAdd, couponCount);
      insertCompactFlag(memArr, memAdd, compact);
      insertCommon(memArr, memAdd);

      if (compact) {
        final PairIterator itr = getIterator();
        int cnt = 0;
        while (itr.nextValid()) {
          wmem.putInt(HASH_SET_INT_ARR_START + (cnt++ << 2), itr.getPair());
        }
      } else { //updatable
        wmem.putIntArray(HASH_SET_INT_ARR_START, couponIntArr, 0, 1 << lgCouponArrInts);
      }
    }
    return memArr;
  }

  private void insertCommon(final byte[] memArr, final long memAdd) {
    insertSerVer(memArr, memAdd);
    insertFamilyId(memArr, memAdd);
    insertLgK(memArr, memAdd, getLgConfigK());
    insertLgArr(memArr, memAdd, getLgCouponArrInts());
    insertEmptyFlag(memArr, memAdd, isEmpty());
    insertOooFlag(memArr, memAdd, isOutOfOrderFlag());
    insertCurMode(memArr, memAdd, getCurMode());
    insertTgtHllType(memArr, memAdd, getTgtHllType());
  }

  //Iterator

  final class CouponIterator implements PairIterator {
    final int len;
    int index;
    final int[] array;

    CouponIterator() {
      array = getCouponIntArr();
      len = array.length;
      index = - 1;
    }

    @Override
    public boolean nextValid() {
      while (++index < len) {
        if (array[index] != EMPTY) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean nextAll() {
      return ++index < len;
    }

    @Override
    public int getPair() {
      return array[index];
    }

    @Override
    public int getKey() {
      return BaseHllSketch.getLow26(array[index]);
    }

    @Override
    public int getValue() {
      return BaseHllSketch.getValue(array[index]);
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

  static final void checkPreamble(final Memory mem, final Object memArr, final long memAdd,
      final CurMode curMode) {
    final int memPreInts = extractPreInts(memArr, memAdd);
    final int expPreInts = (curMode == CurMode.LIST) ? LIST_PREINTS : HASH_SET_PREINTS;
    final int serVer = extractSerVer(memArr, memAdd);
    final int famId = extractFamilyId(memArr, memAdd);
    if ( (memPreInts != expPreInts) || (serVer != SER_VER) || (famId != FAMILY_ID) ) {
      badPreambleState(mem);
    }
  }

}
