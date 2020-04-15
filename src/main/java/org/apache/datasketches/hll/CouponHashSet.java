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
import static org.apache.datasketches.hll.HllUtil.LG_INIT_SET_SIZE;
import static org.apache.datasketches.hll.HllUtil.RESIZE_DENOM;
import static org.apache.datasketches.hll.HllUtil.RESIZE_NUMER;
import static org.apache.datasketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.HASH_SET_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.LIST_INT_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.computeLgArr;
import static org.apache.datasketches.hll.PreambleUtil.extractCompactFlag;
import static org.apache.datasketches.hll.PreambleUtil.extractCurMode;
import static org.apache.datasketches.hll.PreambleUtil.extractHashSetCount;
import static org.apache.datasketches.hll.PreambleUtil.extractInt;
import static org.apache.datasketches.hll.PreambleUtil.extractLgArr;
import static org.apache.datasketches.hll.PreambleUtil.extractLgK;
import static org.apache.datasketches.hll.PreambleUtil.extractTgtHllType;

import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.Memory;

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
