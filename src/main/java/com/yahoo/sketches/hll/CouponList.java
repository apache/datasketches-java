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
import static com.yahoo.sketches.hll.PreambleUtil.extractFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.extractHashSetCount;
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
  private static final double COUPON_ESTIMATOR_RSE = .409 / (1 << 13); //=.409 / sqrt(2^26)
  private static final int LG_INIT_LIST_SIZE = 3;
  private static final int LG_INIT_SET_SIZE = 5;

  final int lgMaxArrInts;
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
    lgCouponArrInts = (curMode == CurMode.LIST) ? LG_INIT_LIST_SIZE : LG_INIT_SET_SIZE;
    couponIntArr = new int[1 << lgCouponArrInts];
    couponCount = 0;
    lgMaxArrInts = (curMode == CurMode.LIST) ? LG_INIT_LIST_SIZE : (lgConfigK - 3);
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
    lgMaxArrInts = that.lgMaxArrInts;
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
    lgMaxArrInts = that.lgMaxArrInts;
  }

  //Factory
  static final CouponList heapify(final Memory mem, final CurMode curMode) {
    final Object memArr = ((WritableMemory) mem).getArray();
    final long memAdd = mem.getCumulativeOffset(0);
    checkPreamble(mem, memArr, memAdd, curMode);
    final int lgConfigK = extractLgK(memArr, memAdd);
    final TgtHllType tgtHllType = extractTgtHllType(memArr, memAdd);

    if (curMode == CurMode.LIST) {
      final CouponList list = new CouponList(lgConfigK, tgtHllType, curMode);
      final int couponCount = extractListCount(memArr, memAdd);
      final int[] data = new int[couponCount];
      mem.getIntArray(LIST_INT_ARR_START, data, 0, couponCount);
      for (int i = 0; i < couponCount; i++) {
        list.couponUpdate(data[i]);
      }
      list.putOooFlag(extractOooFlag(memArr, memAdd));
      return list;
    }
    // else SET
    final CouponHashSet set = new CouponHashSet(lgConfigK, tgtHllType);
    final int couponCount = extractHashSetCount(memArr, memAdd);
    final int[] data = new int[couponCount];
    mem.getIntArray(HASH_SET_INT_ARR_START, data, 0, couponCount);
    for (int i = 0; i < couponCount; i++) {
      set.couponUpdate(data[i]);
    }
    set.putOooFlag(true);
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
    final int len = couponIntArr.length;
    for (int i = 0; i < len; i++) { //search for empty slot
      if (couponIntArr[i] == EMPTY) {
        couponIntArr[i] = coupon; //update
        couponCount++;
        if (couponCount >= len) { //array full
          if (lgConfigK < 8) {
            return HllUtil.makeHllFromCoupons(this, lgConfigK, tgtHllType); //sets oooFlag false
          }
          return HllUtil.makeSetFromList(this, lgConfigK, tgtHllType); //sets oooFlag true
        }
        return this;
      }
      //cell not empty
      if (couponIntArr[i] == coupon) { return this; } //duplicate
      //cell not empty & not a duplicate, continue
    } //end for
    throw new SketchesStateException("Array invalid: no empties & no duplicates");
  }

  @Override
  PairIterator getAuxIterator() {
    return null; //always null from LIST or SET
  }

  @Override
  int getCount() {
    return couponCount;
  }

  @Override
  int getCurMin() {
    return -1;
  }

  @Override
  int getCurrentSerializationBytes() {
    final int dataStart = (curMode == CurMode.LIST) ? LIST_INT_ARR_START : HASH_SET_INT_ARR_START;
    return dataStart +  (couponCount << 2);
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
    final double est = Interpolation.cubicInterpolateUsingTable(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    return max(est, couponCount);
  }

  @Override
  double getHipAccum() {
    return couponCount;
  }

  @Override
  PairIterator getIterator() {
    return new CouponIterator();
  }

  @Override
  double getLowerBound(final double numStdDev) {
    final double est = Interpolation.cubicInterpolateUsingTable(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    final double tmp = est / (1.0 + couponEstimatorEps(numStdDev));
    return max(tmp, couponCount);
  }

  @Override
  int getMaxCouponArrInts() {
    return 1 << lgMaxArrInts;
  }

  @Override
  int getNumAtCurMin() {
    return -1;
  }

  @Override
  double getUpperBound(final double numStdDev) {
    final double est = Interpolation.cubicInterpolateUsingTable(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    final double tmp = est / (1.0 - couponEstimatorEps(numStdDev));
    return max(tmp, couponCount);
  }

  @Override
  boolean isEmpty() {
    return couponCount == 0;
  }

  @Override
  void putHipAccum(final double value) {
    throw new SketchesStateException("Cannot put Hip accum here.");
  }

  @Override
  byte[] toCompactByteArray() {
    final byte[] memArr;
    final WritableMemory wmem;
    final long memAdd;

    if (curMode == CurMode.LIST) {
      memArr = new byte[8 + (4 * couponCount)]; //unique to LIST
      wmem = WritableMemory.wrap(memArr);
      memAdd = wmem.getCumulativeOffset(0);
      insertPreInts(memArr, memAdd, LIST_PREINTS); //unique to LIST
      insertSerVer(memArr, memAdd);
      insertFamilyId(memArr, memAdd);
      insertLgK(memArr, memAdd, lgConfigK);
      insertLgArr(memArr, memAdd, lgCouponArrInts);
      insertEmptyFlag(memArr, memAdd, isEmpty());
      insertCompactFlag(memArr, memAdd, true);
      insertOooFlag(memArr, memAdd, oooFlag);
      insertListCount(memArr, memAdd, couponCount); //unique to LIST
      insertCurMode(memArr, memAdd, curMode);
      insertTgtHllType(memArr, memAdd, tgtHllType);
      wmem.putIntArray(LIST_INT_ARR_START, couponIntArr, 0, couponCount); //unique to LIST

    } else { //SET
      memArr = new byte[12 + (4 * couponCount)]; //unique to SET
      wmem = WritableMemory.wrap(memArr);
      memAdd = wmem.getCumulativeOffset(0);
      insertPreInts(memArr, memAdd, HASH_SET_PREINTS); //unique to SET
      insertSerVer(memArr, memAdd);
      insertFamilyId(memArr, memAdd);
      insertLgK(memArr, memAdd, lgConfigK);
      insertLgArr(memArr, memAdd, lgCouponArrInts);
      insertEmptyFlag(memArr, memAdd, isEmpty());
      insertCompactFlag(memArr, memAdd, true);
      insertOooFlag(memArr, memAdd, oooFlag);
      insertCurMode(memArr, memAdd, curMode);
      insertTgtHllType(memArr, memAdd, tgtHllType);
      insertHashSetCount(memArr, memAdd, couponCount); //unique to SET

      final PairIterator itr = getIterator(); //unique to SET
      int cnt = 0;
      while (itr.nextValid()) {
        wmem.putInt(HASH_SET_INT_ARR_START + (4 * cnt++), itr.getPair());
      }
    }
    return memArr;
  }

  //Iterator

  final class CouponIterator implements PairIterator {
    final int len;
    int index;
    final int[] array;

    CouponIterator() {
      array = couponIntArr;
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

  static final double couponEstimatorEps(final double numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    return (numStdDev * COUPON_ESTIMATOR_RSE);
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
