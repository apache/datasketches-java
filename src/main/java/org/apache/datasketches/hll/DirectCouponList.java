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

import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.common.Util.clear;
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

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;

/**
 * @author Lee Rhodes
 */
class DirectCouponList extends AbstractCoupons {
  MemorySegment wseg;
  MemorySegment seg;
  final boolean compact;

  private static int checkSegCompactFlag(final MemorySegment wseg, final int lgConfigK) {
    assert !extractCompactFlag(wseg) : "Compact Flag must not be set.";
    return lgConfigK;
  }

  //called from newInstance, writableWrap and DirectCouponHashSet, must not be compact
  DirectCouponList(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode, final MemorySegment wseg) {
    super(checkSegCompactFlag(wseg, lgConfigK), tgtHllType, curMode);
    this.wseg = wseg;
    seg = wseg;
    compact = extractCompactFlag(wseg);
  }

  //called from HllSketch.wrap and from DirectCouponHashSet constructor, may or may not be compact
  DirectCouponList(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode, final MemorySegment seg,
      final boolean readOnly) {
    super(lgConfigK, tgtHllType, curMode);
    wseg = null;
    this.seg = readOnly ? seg.asReadOnly() : seg;
    compact = extractCompactFlag(seg);
  }

  /**
   * Standard factory for new DirectCouponList.
   * This initializes the given MemorySegment.
   * @param lgConfigK the configured Lg K
   * @param tgtHllType the configured HLL target
   * @param dstSeg the destination MemorySegment for the sketch.
   * @return a new DirectCouponList
   */
  static DirectCouponList newInstance(final int lgConfigK, final TgtHllType tgtHllType,
      final MemorySegment dstSeg) {
    insertPreInts(dstSeg, LIST_PREINTS);
    insertSerVer(dstSeg);
    insertFamilyId(dstSeg);
    insertLgK(dstSeg, lgConfigK);
    insertLgArr(dstSeg, LG_INIT_LIST_SIZE);
    insertFlags(dstSeg, EMPTY_FLAG_MASK); //empty and not compact
    insertListCount(dstSeg, 0);
    insertModes(dstSeg, tgtHllType, CurMode.LIST);
    return new DirectCouponList(lgConfigK, tgtHllType, CurMode.LIST, dstSeg);
  }

  @Override //returns on-heap List
  CouponList copy() {
    return CouponList.heapifyList(seg);
  }

  @Override //returns on-heap List
  CouponList copyAs(final TgtHllType tgtHllType) {
    final CouponList clist = CouponList.heapifyList(seg);
    return new CouponList(clist, tgtHllType);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    if (wseg == null) { noWriteAccess(); }
    final int len = 1 << getLgCouponArrInts();
    for (int i = 0; i < len; i++) { //search for empty slot and duplicates
      final int couponAtIdx = extractInt(seg, LIST_INT_ARR_START + (i << 2));
      if (couponAtIdx == EMPTY) {
        insertInt(wseg, LIST_INT_ARR_START + (i << 2), coupon);
        int couponCount = extractListCount(seg);
        insertListCount(wseg, ++couponCount);
        insertEmptyFlag(wseg, false); //only first time
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
    return getSegDataStart() + (getCouponCount() << 2);
  }

  @Override //Overridden by DirectCouponHashSet
  int getCouponCount() {
    return extractListCount(seg);
  }

  @Override
  int[] getCouponIntArr() { //here only to satisfy the abstract, should not be used
    return null;
  }

  @Override
  int getLgCouponArrInts() {
    final int lgArr = extractLgArr(seg);
    if (lgArr >= LG_INIT_LIST_SIZE) { return lgArr; }
    //early versions of compact images did not use this lgArr field
    final int coupons = getCouponCount();
    return computeLgArr(seg, coupons, lgConfigK);
  }

  @Override
  int getSegDataStart() {
    return LIST_INT_ARR_START;
  }

  @Override
  MemorySegment getMemorySegment() {
    return seg;
  }

  @Override
  int getPreInts() {
    return LIST_PREINTS;
  }

  @Override
  boolean isCompact() {
    return compact;
  }

  @Override
  boolean hasMemorySegment() {
    return seg.scope().isAlive();
  }

  @Override
  boolean isOffHeap() {
    return seg.isNative();
  }

  @Override
  boolean isSameResource(final MemorySegment seg) {
    return MemorySegmentStatus.isSameResource(this.seg, seg);
  }

  @Override
  PairIterator iterator() {
    final long dataStart = getSegDataStart();
    final int lenInts = (compact) ? getCouponCount() : 1 << getLgCouponArrInts();
    return new IntMemorySegmentPairIterator(seg, dataStart, lenInts, lgConfigK);
  }

  @Override
  void mergeTo(final HllSketch that) {
    final int lenInts = (compact) ? getCouponCount() : 1 << getLgCouponArrInts();
    final int dataStart = getSegDataStart();
    for (int i = 0; i < lenInts; i++) {
      final int pair = seg.get(JAVA_INT_UNALIGNED, dataStart + (i << 2));
      if (pair == 0) { continue; }
      that.couponUpdate(pair);
    }
  }

  @Override
  DirectCouponList reset() {
    if (wseg == null) {
      throw new SketchesArgumentException("Cannot reset a read-only sketch");
    }
    insertEmptyFlag(wseg, true);
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    clear(wseg, 0, bytes);

    return DirectCouponList.newInstance(lgConfigK, tgtHllType, wseg);
  }

  //Called by DirectCouponList.couponUpdate()
  static final DirectCouponHashSet promoteListToSet(final DirectCouponList src) {
    final MemorySegment wseg = src.wseg;

    //get the data from the current MemorySegment
    HllUtil.checkPreamble(wseg); //sanity check
    final int lgConfigK = src.lgConfigK;
    final TgtHllType tgtHllType = src.tgtHllType;
    final int srcOffset = LIST_INT_ARR_START;
    final int couponArrInts = 1 << src.getLgCouponArrInts();
    final int[] couponArr = new int[couponArrInts]; //buffer
    MemorySegment.copy(wseg, JAVA_INT_UNALIGNED, srcOffset, couponArr, 0, couponArrInts);

    //rewrite the MemorySegment image as a SET:
    insertPreInts(wseg, HASH_SET_PREINTS);
    //SerVer, FamID, LgK  should be OK
    insertLgArr(wseg, LG_INIT_SET_SIZE);
    insertCurMin(wseg, 0); //was list count
    insertCurMode(wseg, CurMode.SET);
    //tgtHllType should already be ok
    final int maxBytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    clear(wseg, LIST_INT_ARR_START, maxBytes - LIST_INT_ARR_START); //clear all past first 8

    //create the tgt
    final DirectCouponHashSet dchSet = new DirectCouponHashSet(src.lgConfigK, src.tgtHllType, src.wseg);

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
    final MemorySegment wseg = src.wseg;

    //get the data from the current list or set MemorySegment
    HllUtil.checkPreamble(wseg); //sanity check
    final int lgConfigK = src.lgConfigK;
    final TgtHllType tgtHllType = src.tgtHllType;
    final int srcSegDataStart = src.getSegDataStart();
    final double est = src.getEstimate();
    final int couponArrInts = 1 << src.getLgCouponArrInts();
    final int[] couponArr = new int[couponArrInts]; //buffer
    MemorySegment.copy(wseg, JAVA_INT_UNALIGNED, srcSegDataStart, couponArr, 0, couponArrInts);

    //rewrite the MemorySegment image as an HLL
    insertPreInts(wseg, HLL_PREINTS);
    //SerVer, FamID, LgK should be OK
    insertLgArr(wseg, 0); //no Aux possible yet
    insertFlags(wseg, 0); //clear all flags
    insertCurMin(wseg, 0);
    insertCurMode(wseg, CurMode.HLL);
    //tgtHllType should already be set
    //we update HipAccum at the end
    //clear KxQ0, KxQ1, NumAtCurMin, AuxCount, hllArray, auxArr
    final int maxBytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    clear(wseg, LIST_INT_ARR_START, maxBytes - LIST_INT_ARR_START); //clear all past first 8
    insertNumAtCurMin(wseg, 1 << lgConfigK); //set numAtCurMin
    insertKxQ0(wseg, 1 << lgConfigK);

    //choose the tgt
    final DirectHllArray dirHllArr;
    if (tgtHllType == TgtHllType.HLL_4) {
      dirHllArr = new DirectHll4Array(lgConfigK, wseg);
    } else if (tgtHllType == TgtHllType.HLL_6) {
      dirHllArr = new DirectHll6Array(lgConfigK, wseg);
    } else { //Hll_8
      dirHllArr = new DirectHll8Array(lgConfigK, wseg);
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
