/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_DENOM;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_NUMER;
import static com.yahoo.sketches.hll.HllUtil.noWriteAccess;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.LG_K_BYTE;
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
  DirectCouponHashSet(final int lgConfigK, final TgtHllType tgtHllType,
      final WritableMemory wmem) {
    super(lgConfigK, tgtHllType, CurMode.SET, wmem);
    assert wmem.getByte(LG_K_BYTE) > 7;
  }

  //Constructs this sketch with read-only data.
  DirectCouponHashSet(final int lgConfigK, final TgtHllType tgtHllType,
      final Memory mem) {
    super(lgConfigK, tgtHllType, CurMode.SET, mem);
    assert mem.getByte(LG_K_BYTE) > 7;
  }

  //  /**
  //   * Standard factory for new DirectCouponHashSet. Primarily used in test.
  //   * This initializes the given WritableMemory.
  //   * @param lgConfigK the configured Lg K, must be greater than 7
  //   * @param tgtHllType the configured HLL target
  //   * @param dstMem the destination memory for the sketch.
  //   */
  //  static DirectCouponHashSet newInstance(final int lgConfigK, final TgtHllType tgtHllType,
  //      final WritableMemory dstMem) {
  //    final long capBytes = dstMem.getCapacity();
  //    final int minBytes = HASH_SET_INT_ARR_START + (4 << LG_INIT_SET_SIZE);
  //    HllUtil.checkMemSize(minBytes, capBytes);
  //
  //    final Object memObj = dstMem.getArray();
  //    final long memAdd = dstMem.getCumulativeOffset(0L);
  //
  //    insertPreInts(memObj, memAdd, HASH_SET_PREINTS);
  //    insertSerVer(memObj, memAdd);
  //    insertFamilyId(memObj, memAdd);
  //    insertLgK(memObj, memAdd, lgConfigK);
  //    insertLgArr(memObj, memAdd, LG_INIT_SET_SIZE);
  //    insertFlags(memObj, memAdd, EMPTY_FLAG_MASK);
  //    insertListCount(memObj, memAdd, 0); //zero out for SET also
  //    insertModes(memObj, memAdd, tgtHllType, CurMode.SET);
  //    insertHashSetCount(memObj, memAdd, 0);
  //    return new DirectCouponHashSet(lgConfigK, tgtHllType, dstMem);
  //  }

  @Override //returns on-heap Set
  CouponHashSet copy() {
    return CouponHashSet.heapifySet(mem);
  }

  @Override //returns on-heap Set
  CouponHashSet copyAs(final TgtHllType tgtHllType) {
    final CouponHashSet clist = CouponHashSet.heapifySet(mem);
    return new CouponHashSet(clist, tgtHllType);
  }

  @Override //get Coupons from internal Mem to dstMem
  //Called by CouponList.insertList()
  //Called by CouponList.insertSet()
  void getCouponsToMemoryInts(final WritableMemory dstWmem, final int lenInts) {
    mem.copyTo(HASH_SET_INT_ARR_START, dstWmem, HASH_SET_INT_ARR_START, lenInts << 2);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    if (wmem == null) { noWriteAccess(); }
    //avoid array copy
    final int index = find(memObj, memAdd, getLgCouponArrInts(), coupon);
    if (index >= 0) {
      return this; //found duplicate, ignore
    }
    insertInt(memObj, memAdd, HASH_SET_INT_ARR_START + (~index << 2), coupon);
    incCouponCount();
    final boolean promote = checkGrowOrPromote();
    if (!promote) { return this; }
    return promoteListOrSetToHll(this);
  }

  @Override
  int getCompactSerializationBytes() {
    return HASH_SET_INT_ARR_START +  (getCouponCount() << 2);
  }

  @Override
  int getCouponCount() {
    return extractHashSetCount(memObj, memAdd);
  }

  @Override
  int getMemArrStart() {
    return HASH_SET_INT_ARR_START;
  }

  @Override
  int getPreInts() {
    return HASH_SET_PREINTS;
  }

  void incCouponCount() {
    assert wmem != null;
    int count = extractHashSetCount(memObj, memAdd);
    insertHashSetCount(memObj, memAdd, ++count);
  }

  private boolean checkGrowOrPromote() {
    int lgCouponArrInts = getLgCouponArrInts();
    if ((RESIZE_DENOM * getCouponCount()) > (RESIZE_NUMER * (1 << lgCouponArrInts))) {
      if (lgCouponArrInts == (getLgConfigK() - 3)) {
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
        final int idx = find(tgtCouponIntArr, tgtLgCouponArrSize, fetched);
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

  //Searches the Coupon hash table (embedded in Memory) for an empty slot
  // or a duplicate depending on the context.
  //If entire entry is empty, returns one's complement of index = found empty.
  //If entry equals given coupon, returns its index = found duplicate coupon
  //Continues searching
  //If the probe comes back to original index, throws an exception.
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
