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
import static org.apache.datasketches.hll.HllUtil.noWriteAccess;
import static org.apache.datasketches.hll.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.hll.PreambleUtil.HASH_SET_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.HLL_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.LIST_INT_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.LIST_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.computeLgArr;
import static org.apache.datasketches.hll.PreambleUtil.extractCompactFlag;
import static org.apache.datasketches.hll.PreambleUtil.extractInt;
import static org.apache.datasketches.hll.PreambleUtil.extractLgArr;
import static org.apache.datasketches.hll.PreambleUtil.extractListCount;
import static org.apache.datasketches.hll.PreambleUtil.insertCurMin;
import static org.apache.datasketches.hll.PreambleUtil.insertCurMode;
import static org.apache.datasketches.hll.PreambleUtil.insertEmptyFlag;
import static org.apache.datasketches.hll.PreambleUtil.insertFamilyId;
import static org.apache.datasketches.hll.PreambleUtil.insertFlags;
import static org.apache.datasketches.hll.PreambleUtil.insertInt;
import static org.apache.datasketches.hll.PreambleUtil.insertKxQ0;
import static org.apache.datasketches.hll.PreambleUtil.insertLgArr;
import static org.apache.datasketches.hll.PreambleUtil.insertLgK;
import static org.apache.datasketches.hll.PreambleUtil.insertListCount;
import static org.apache.datasketches.hll.PreambleUtil.insertModes;
import static org.apache.datasketches.hll.PreambleUtil.insertNumAtCurMin;
import static org.apache.datasketches.hll.PreambleUtil.insertPreInts;
import static org.apache.datasketches.hll.PreambleUtil.insertSerVer;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

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
    for (int i = 0; i < len; i++) { //search for empty slot and duplicates
      final int couponAtIdx = extractInt(mem, LIST_INT_ARR_START + (i << 2));
      if (couponAtIdx == EMPTY) {
        insertInt(wmem, LIST_INT_ARR_START + (i << 2), coupon);
        int couponCount = extractListCount(mem);
        insertListCount(wmem, ++couponCount);
        insertEmptyFlag(wmem, false); //TODO only first time
        if (couponCount >= len) { //array full
          if (lgConfigK < 8) {
            return promoteListOrSetToHll(this);//oooFlag = false
          }
          return promoteListToSet(this); //oooFlag = true
        }
        return this;
      }
      //cell not empty
      if (couponAtIdx == coupon) { return this; } //return if duplicate
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
  boolean isSameResource(final Memory mem) {
    return this.mem.isSameResource(mem);
  }

  @Override
  PairIterator iterator() {
    final long dataStart = getMemDataStart();
    final int lenInts = (compact) ? getCouponCount() : 1 << getLgCouponArrInts();
    return new IntMemoryPairIterator(mem, dataStart, lenInts, lgConfigK);
  }

  @Override
  void mergeTo(final HllSketch that) {
    final int lenInts = (compact) ? getCouponCount() : 1 << getLgCouponArrInts();
    final int dataStart = getMemDataStart();
    for (int i = 0; i < lenInts; i++) {
      final int pair = mem.getInt(dataStart + (i << 2));
      if (pair == 0) { continue; }
      that.couponUpdate(pair);
    }
  }

  @Override
  DirectCouponList reset() {
    if (wmem == null) {
      throw new SketchesArgumentException("Cannot reset a read-only sketch");
    }
    insertEmptyFlag(wmem, true);
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
    insertCurMin(wmem, 0); //was list count
    insertCurMode(wmem, CurMode.SET);
    //tgtHllType should already be ok
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
