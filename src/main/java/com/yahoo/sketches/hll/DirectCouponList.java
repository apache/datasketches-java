/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_LIST_SIZE;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_SET_SIZE;
import static com.yahoo.sketches.hll.HllUtil.noWriteAccess;
import static com.yahoo.sketches.hll.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.OUT_OF_ORDER_FLAG_MASK;
import static com.yahoo.sketches.hll.PreambleUtil.computeLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.extractCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractInt;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.extractListCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMode;
import static com.yahoo.sketches.hll.PreambleUtil.insertEmptyFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.insertFlags;
import static com.yahoo.sketches.hll.PreambleUtil.insertInt;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgK;
import static com.yahoo.sketches.hll.PreambleUtil.insertListCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertModes;
import static com.yahoo.sketches.hll.PreambleUtil.insertNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.insertSerVer;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
class DirectCouponList extends AbstractCoupons {
  WritableMemory wmem;
  Memory mem;
  Object memObj;
  long memAdd;
  final boolean compact;

  //called from newInstance, writableWrap and DirectCouponHashSet
  DirectCouponList(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode,
      final WritableMemory wmem) {
    super(lgConfigK, tgtHllType, curMode);
    this.wmem = wmem;
    mem = wmem;
    memObj = wmem.getArray();
    memAdd = wmem.getCumulativeOffset(0L);
    compact = extractCompactFlag(wmem);
    assert !compact;
  }

  //called from HllSketch.wrap and from DirectCouponHashSet constructor, may be compact
  DirectCouponList(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode,
      final Memory mem) {
    super(lgConfigK, tgtHllType, curMode);
    wmem = null;
    this.mem = mem;
    memObj = ((WritableMemory) mem).getArray();
    memAdd = mem.getCumulativeOffset(0L);
    compact = extractCompactFlag(mem);
  }

  /**
   * Standard factory for new DirectCouponList.
   * This initializes the given WritableMemory.
   * @param lgConfigK the configured Lg K
   * @param tgtHllType the configured HLL target
   * @param dstMem the destination memory for the sketch.
   * @return a new DirectCouponList
   */
  static DirectCouponList newInstance(final int lgConfigK, final TgtHllType tgtHllType,
      final WritableMemory dstMem) {
    insertPreInts(dstMem, LIST_PREINTS);
    insertSerVer(dstMem);
    insertFamilyId(dstMem);
    insertLgK(dstMem, lgConfigK);
    insertLgArr(dstMem, LG_INIT_LIST_SIZE);
    insertFlags(dstMem, EMPTY_FLAG_MASK); //empty and not compact
    insertListCount(dstMem, 0);
    insertModes(dstMem, tgtHllType, CurMode.LIST);
    return new DirectCouponList(lgConfigK, tgtHllType, CurMode.LIST, dstMem);
  }

  @Override //returns on-heap List
  CouponList copy() {
    return CouponList.heapifyList(mem);
  }

  @Override //returns on-heap List
  CouponList copyAs(final TgtHllType tgtHllType) {
    final CouponList clist = CouponList.heapifyList(mem);
    return new CouponList(clist, tgtHllType);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    if (wmem == null) { noWriteAccess(); }
    final int len = 1 << getLgCouponArrInts();
    for (int i = 0; i < len; i++) { //search for empty slot
      final int couponAtIdx = extractInt(mem, LIST_INT_ARR_START + (i << 2));
      if (couponAtIdx == EMPTY) {
        insertInt(wmem, LIST_INT_ARR_START + (i << 2), coupon);
        int couponCount = extractListCount(mem);
        insertListCount(wmem, ++couponCount);
        insertEmptyFlag(wmem, false);
        if (couponCount >= len) { //array full
          if (lgConfigK < 8) {
            return promoteListOrSetToHll(this);//oooFlag = false
          }
          return promoteListToSet(this); //oooFlag = true
        }
        return this;
      }
      //cell not empty
      if (couponAtIdx == coupon) { return this; } //duplicate
      //cell not empty & not a duplicate, continue
    } //end for
    throw new SketchesStateException("Invalid State: no empties & no duplicates");
  }

  @Override
  int getCompactSerializationBytes() {
    return getMemDataStart() + (getCouponCount() << 2);
  }

  @Override //Overridden by DirectCouponHashSet
  int getCouponCount() {
    return extractListCount(mem);
  }

  @Override
  int[] getCouponIntArr() { //here only to satisfy the abstract, should not be used
    return null;
  }

  @Override
  PairIterator iterator() {
    final long dataStart = getMemDataStart();
    final int lenInts = (compact) ? getCouponCount() : 1 << getLgCouponArrInts();
    return new IntMemoryPairIterator(mem, dataStart, lenInts, lgConfigK);
  }

  @Override
  int getLgCouponArrInts() {
    final int lgArr = extractLgArr(mem);
    if (lgArr >= LG_INIT_LIST_SIZE) { return lgArr; }
    //early versions of compact images did not use this lgArr field
    final int coupons = getCouponCount();
    return computeLgArr(mem, coupons, lgConfigK);
  }

  @Override
  int getMemDataStart() {
    return LIST_INT_ARR_START;
  }

  @Override
  Memory getMemory() {
    return mem;
  }

  @Override
  int getPreInts() {
    return LIST_PREINTS;
  }

  @Override
  WritableMemory getWritableMemory() {
    return wmem;
  }

  @Override
  boolean isCompact() {
    return compact;
  }

  @Override
  boolean isMemory() {
    return true;
  }

  @Override
  boolean isOffHeap() {
    return mem.isDirect();
  }

  @Override
  boolean isOutOfOrderFlag() {
    return extractOooFlag(mem);
  }

  @Override
  boolean isSameResource(final Memory mem) {
    return this.mem.isSameResource(mem);
  }

  @Override //not used on the direct side
  void putOutOfOrderFlag(final boolean oooFlag) {
    assert wmem != null;
    insertOooFlag(wmem, oooFlag);
  }

  @Override
  DirectCouponList reset() {
    if (wmem == null) {
      throw new SketchesArgumentException("Cannot reset a read-only sketch");
    }
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    wmem.clear(0, bytes);
    return DirectCouponList.newInstance(lgConfigK, tgtHllType, wmem);
  }

  //Called by DirectCouponList.couponUpdate()
  static final DirectCouponHashSet promoteListToSet(final DirectCouponList src) {
    final WritableMemory wmem = src.wmem;

    //get the data from the current memory
    HllUtil.checkPreamble(wmem); //sanity check
    final int lgConfigK = src.lgConfigK;
    final TgtHllType tgtHllType = src.tgtHllType;
    final int srcOffset = LIST_INT_ARR_START;
    final int couponArrInts = 1 << src.getLgCouponArrInts();
    final int[] couponArr = new int[couponArrInts]; //buffer
    wmem.getIntArray(srcOffset, couponArr, 0, couponArrInts);

    //rewrite the memory image as a SET:
    insertPreInts(wmem, HASH_SET_PREINTS);
    //SerVer, FamID, LgK  should be OK
    insertLgArr(wmem, LG_INIT_SET_SIZE);
    insertFlags(wmem, (byte) OUT_OF_ORDER_FLAG_MASK); //set oooFlag
    insertCurMin(wmem, 0); //was list count
    insertCurMode(wmem, CurMode.SET);
    //tgtHllType should already be set
    final int maxBytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    wmem.clear(LIST_INT_ARR_START, maxBytes - LIST_INT_ARR_START); //clear all past first 8

    //create the tgt
    final DirectCouponHashSet dchSet
        = new DirectCouponHashSet(src.lgConfigK, src.tgtHllType, src.wmem);

    //now reload the coupon data into the set
    for (int i = 0; i < couponArrInts; i++) {
      final int coupon = couponArr[i];
      if (coupon != EMPTY) {
        dchSet.couponUpdate(coupon);
      }
    }
    return dchSet;
  }

  static final DirectHllArray promoteListOrSetToHll(final DirectCouponList src) {
    final WritableMemory wmem = src.wmem;

    //get the data from the current list or set memory
    HllUtil.checkPreamble(wmem); //sanity check
    final int lgConfigK = src.lgConfigK;
    final TgtHllType tgtHllType = src.tgtHllType;
    final int srcMemDataStart = src.getMemDataStart();
    final double est = src.getEstimate();
    final int couponArrInts = 1 << src.getLgCouponArrInts();
    final int[] couponArr = new int[couponArrInts]; //buffer
    wmem.getIntArray(srcMemDataStart, couponArr, 0, couponArrInts);

    //rewrite the memory image as an HLL
    insertPreInts(wmem, HLL_PREINTS);
    //SerVer, FamID, LgK should be OK
    insertLgArr(wmem, 0); //no Aux possible yet
    insertFlags(wmem, 0); //clear all flags
    insertCurMin(wmem, 0);
    insertCurMode(wmem, CurMode.HLL);
    //tgtHllType should already be set
    //we update HipAccum at the end
    //clear KxQ0, KxQ1, NumAtCurMin, AuxCount, hllArray, auxArr
    final int maxBytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    wmem.clear(LIST_INT_ARR_START, maxBytes - LIST_INT_ARR_START); //clear all past first 8
    insertNumAtCurMin(wmem, 1 << lgConfigK); //set numAtCurMin
    insertKxQ0(wmem, 1 << lgConfigK);

    //choose the tgt
    final DirectHllArray dirHllArr;
    if (tgtHllType == TgtHllType.HLL_4) {
      dirHllArr = new DirectHll4Array(lgConfigK, wmem);
    } else if (tgtHllType == TgtHllType.HLL_6) {
      dirHllArr = new DirectHll6Array(lgConfigK, wmem);
    } else { //Hll_8
      dirHllArr = new DirectHll8Array(lgConfigK, wmem);
    }

    //now load the coupon data into HLL
    for (int i = 0; i < couponArrInts; i++) {
      final int coupon = couponArr[i];
      if (coupon != EMPTY) {
        dirHllArr.couponUpdate(coupon);
      }
    }
    dirHllArr.putHipAccum(est);
    return dirHllArr;
  }



}
