/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_DENOM;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_NUMER;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractHashSetCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractInt;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.insertHashSetCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertInt;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgArr;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
class DirectCouponHashSet extends DirectCouponList {

  //Constructs this sketch with data.
  DirectCouponHashSet(final WritableMemory wmem, final int lgConfigK) {
    super(wmem, lgConfigK - 3);
  }

  //Constructs this sketch with read-only data.
  DirectCouponHashSet(final Memory mem, final int lgConfigK) {
    super(mem, lgConfigK - 3);
  }

  static final HllSketchImpl morphFromListToSet(final DirectCouponList src) {
    final CouponHashSet chSet = CouponHashSet.heapifySet(src.mem); //sets oooFlag

    final int minBytes = chSet.getUpdatableSerializationBytes();
    HllUtil.checkMemSize(minBytes, src.wmem.getCapacity());

    src.wmem.clear();
    CouponList.insertSet(chSet, src.wmem, false); //not compact
    final DirectCouponHashSet dchSet = new DirectCouponHashSet(src.wmem, src.getLgConfigK());
    return dchSet;
  }

  static final HllSketchImpl morphFromCouponsToHll(final DirectCouponList src,
      final int lgConfigK, final TgtHllType tgtHllType) {
    final HllArray hllArray = HllArray.newHll(lgConfigK, tgtHllType);
    HllArray.heapifyFromListOrSet(src.mem, hllArray); //temp storage, sets oooFlag & hipAccum

    final int minBytes = hllArray.getUpdatableSerializationBytes();
    HllUtil.checkMemSize(minBytes, src.mem.getCapacity());

    src.wmem.clear();
    HllArray.insertHll(hllArray, src.wmem, false);
    return hllArray;
  }

  @Override //returns on-heap Set
  CouponHashSet copy() {
    return CouponHashSet.heapifySet(mem);
  }

  @Override //returns on-heap Set
  CouponHashSet copyAs(final TgtHllType tgtHllType) {
    final CouponHashSet clist = CouponHashSet.heapifySet(mem);
    return new CouponHashSet(clist, tgtHllType);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    if (coupon == EMPTY) {
      return this; //empty coupon, ignore
    }
    //final int[] couponIntArr = getCouponIntArr();
    final int index = find(memObj, memAdd, getLgCouponArrInts(), coupon);
    if (index >= 0) {
      return this; //found duplicate, ignore
    }
    insertInt(memObj, memAdd, HASH_SET_INT_ARR_START + (~index << 2), coupon);
    incCouponCount();
    putOutOfOrderFlag(true); //could be moved out
    final boolean promote = checkGrowOrPromote();
    if (!promote) { return this; }
    return morphFromCouponsToHll(this, getLgConfigK(), getTgtHllType());
  }


  @Override
  int getCouponCount() {
    return extractHashSetCount(memObj, memAdd);
  }

  @Override
  int[] getCouponIntArr() {
    final int len = 1 << getLgCouponArrInts();
    final int[] intArr = new int[len];
    mem.getIntArray(HASH_SET_INT_ARR_START, intArr, 0, len);
    return intArr;
  }

  @Override
  int getCompactSerializationBytes() {
    return HASH_SET_INT_ARR_START +  (getCouponCount() << 2);
  }

  void incCouponCount() {
    assert wmem != null;
    int count = extractHashSetCount(memObj, memAdd);
    insertHashSetCount(memObj, memAdd, ++count);
  }

  @Override
  void putCouponCount(final int couponCount) {
    assert wmem != null;
    insertHashSetCount(memObj, memAdd, couponCount);
  }

  @Override
  void putCouponIntArr(final int[] couponIntArr, final int lgCouponArrInts) {
    assert wmem != null;
    final int len = 1 << lgCouponArrInts;
    wmem.putIntArray(HASH_SET_INT_ARR_START, couponIntArr, 0, len);
    insertLgArr(memObj, memAdd, lgCouponArrInts);
  }

  private boolean checkGrowOrPromote() {
    int lgCouponArrInts = getLgCouponArrInts();
    if ((RESIZE_DENOM * getCouponCount()) > (RESIZE_NUMER * (1 << lgCouponArrInts))) {
      if (lgCouponArrInts == getLgMaxCouponArrInts()) {
        return true; // promote
      }
      //TODO if direct, ask for more memory
      insertLgArr(memObj, memAdd, ++lgCouponArrInts);
      growHashSet(wmem, memObj, memAdd, lgCouponArrInts);
    }
    return false;
  }

  private static final void growHashSet(final WritableMemory wmem, final Object memObj,
      final long memAdd, final int tgtLgCouponArrSize) {
    final int tgtArrSize = 1 << tgtLgCouponArrSize;
    final int[] tgtCouponIntArr = new int[tgtArrSize];
    final int oldLen = 1 << extractLgArr(memObj, memAdd);
    for (int i = 0; i < oldLen; i++) {
      final int fetched = extractInt(memObj, memAdd, HASH_SET_INT_ARR_START + (i << 2));
      if (fetched != EMPTY) {
        final int idx = CouponHashSet.find(tgtCouponIntArr, tgtLgCouponArrSize, fetched);
        if (idx < 0) { //found EMPTY
          tgtCouponIntArr[~idx] = fetched; //insert
          continue;
        }
        throw new SketchesStateException("Error: found duplicate.");
      }
    }
    wmem.clear(HASH_SET_INT_ARR_START, tgtArrSize << 2);
    wmem.putIntArray(HASH_SET_INT_ARR_START, tgtCouponIntArr, 0, tgtArrSize);
  }

  private static final int find(final Object memObj, final long memAdd, final int lgArr,
      final int coupon) {
    final int arrMask = (1 << lgArr) - 1;
    int probe = coupon & arrMask;
    final int loopIndex = probe;
    do {
      final int couponAtIndex = extractInt(memObj, memAdd, HASH_SET_INT_ARR_START + (probe << 2));
      if (couponAtIndex == EMPTY) { return ~probe; } //empty
      else if (coupon == couponAtIndex) { return probe; } //duplicate
      final int stride = ((coupon & KEY_MASK_26) >>> lgArr) | 1;
      probe = (probe + stride) & arrMask;
    } while (probe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

}
