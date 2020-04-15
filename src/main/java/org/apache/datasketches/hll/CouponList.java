/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hll;

import static org.apache.datasketches.hll.HllUtil.EMPTY;
import static org.apache.datasketches.hll.HllUtil.LG_INIT_LIST_SIZE;
import static org.apache.datasketches.hll.HllUtil.LG_INIT_SET_SIZE;
import static org.apache.datasketches.hll.PreambleUtil.LIST_INT_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.LIST_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.extractLgK;
import static org.apache.datasketches.hll.PreambleUtil.extractListCount;
import static org.apache.datasketches.hll.PreambleUtil.extractTgtHllType;

import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class CouponList extends AbstractCoupons {
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
    } else { //SET
      lgCouponArrInts = LG_INIT_SET_SIZE;
      assert lgConfigK > 7;
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
  boolean isSameResource(final Memory mem) {
    return false;
  }

  @Override
  PairIterator iterator() {
    return new IntArrayPairIterator(couponIntArr, lgConfigK);
  }

  @Override
  void mergeTo(final HllSketch that) {
    final int arrLen = couponIntArr.length;
    for (int i = 0; i < arrLen; i++) {
      final int pair = couponIntArr[i];
      if (pair == 0) { continue; }
      that.couponUpdate(pair);
    }
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

    tgtHllArr.putOutOfOrder(false);
    return tgtHllArr;
  }

}
