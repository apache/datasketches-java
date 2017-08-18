/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.COUPON_RSE;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_LIST_SIZE;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_INT_ARR_START;
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

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

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

  @Override
  byte[] toCompactByteArray() {
    return toByteArray(this, true);
  }

  @Override
  byte[] toUpdatableByteArray() {
    return toByteArray(this, false);
  }

  //TO BYTE ARRAY

  static final byte[] toByteArray(final AbstractCoupons impl, final boolean compact) {
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

  private static final void insertList(final AbstractCoupons impl, final WritableMemory wmem,
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

  private static final void insertSet(final AbstractCoupons impl, final WritableMemory wmem,
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

  private static final void insertCommonList(final AbstractCoupons impl, final Object memObj,
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

  //ESTIMATE RELATED

  private static final double getEstimate(final int couponCount) {
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    return max(est, couponCount);
  }

  private static final double couponEstimatorEps(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    return (numStdDev * COUPON_RSE);
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
      final int arrVal = array[probe];
      if (arrVal == EMPTY) { //Compares on entire coupon
        return ~probe; //empty
      }
      else if (coupon == arrVal) { //Compares on entire coupon
        return probe; //duplicate
      }
      final int stride = ((coupon & KEY_MASK_26) >>> lgArrInts) | 1;
      probe = (probe + stride) & arrMask;
    } while (probe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

}
