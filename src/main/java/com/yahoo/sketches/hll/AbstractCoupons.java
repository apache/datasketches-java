/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.COUPON_RSE;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
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

  //To byte array for Heap and when direct memory is different from compact request
  static final byte[] toByteArray(final AbstractCoupons impl, final boolean compact) {
    final byte[] byteArr;
    final int arrLenBytes;
    final int couponCount = impl.getCouponCount();
    final int couponArrInts = 1 << impl.getLgCouponArrInts();
    arrLenBytes = (compact)
        ? couponCount << 2
        : couponArrInts << 2;
    byteArr = new byte[impl.getMemDataStart() + arrLenBytes];
    final WritableMemory tgtWmem = WritableMemory.wrap(byteArr);
    final Object memObj = tgtWmem.getArray();
    final long memAdd = tgtWmem.getCumulativeOffset(0L);

    insertCompactFlag(memObj, memAdd, compact);
    insertCommonListAndSet(impl, memObj, memAdd);

    if (impl.getCurMode() == CurMode.LIST) {
      final int lenInts = (compact) ? couponCount : couponArrInts;
      insertListCount(memObj, memAdd, couponCount);
      impl.getCouponsToMemoryInts(tgtWmem, lenInts);
    }
    else { //SET
      insertHashSetCount(memObj, memAdd, couponCount);
      if (compact) {
        final PairIterator itr = impl.getIterator();
        int cnt = 0;
        while (itr.nextValid()) {
          tgtWmem.putInt(HASH_SET_INT_ARR_START + (cnt++ << 2), itr.getPair());
        }
      } else { //updatable
        impl.getCouponsToMemoryInts(tgtWmem, couponArrInts);
      }
    }
    return byteArr;
  }

  private static final void insertCommonListAndSet(final AbstractCoupons impl,
      final Object memObj, final long memAdd) {
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
