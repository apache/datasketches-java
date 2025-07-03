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

package org.apache.datasketches.hll2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.common.Util.clear;
import static org.apache.datasketches.hll2.HllUtil.EMPTY;
import static org.apache.datasketches.hll2.HllUtil.KEY_MASK_26;
import static org.apache.datasketches.hll2.HllUtil.RESIZE_DENOM;
import static org.apache.datasketches.hll2.HllUtil.RESIZE_NUMER;
import static org.apache.datasketches.hll2.HllUtil.noWriteAccess;
import static org.apache.datasketches.hll2.PreambleUtil.HASH_SET_INT_ARR_START;
import static org.apache.datasketches.hll2.PreambleUtil.HASH_SET_PREINTS;
import static org.apache.datasketches.hll2.PreambleUtil.LG_K_BYTE;
import static org.apache.datasketches.hll2.PreambleUtil.extractHashSetCount;
import static org.apache.datasketches.hll2.PreambleUtil.extractInt;
import static org.apache.datasketches.hll2.PreambleUtil.extractLgArr;
import static org.apache.datasketches.hll2.PreambleUtil.insertHashSetCount;
import static org.apache.datasketches.hll2.PreambleUtil.insertInt;
import static org.apache.datasketches.hll2.PreambleUtil.insertLgArr;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesException;
import org.apache.datasketches.common.SketchesStateException;

/**
 * @author Lee Rhodes
 */
final class DirectCouponHashSet extends DirectCouponList {

  //Constructs this sketch with data.
  DirectCouponHashSet(final int lgConfigK, final TgtHllType tgtHllType, final MemorySegment wseg) {
    super(lgConfigK, tgtHllType, CurMode.SET, wseg);
    assert wseg.get(JAVA_BYTE, LG_K_BYTE) > 7;
  }

  //Constructs this sketch with read-only data, may be compact.
  DirectCouponHashSet(final int lgConfigK, final TgtHllType tgtHllType, final MemorySegment seg,
      final boolean readOnly) {
    super(lgConfigK, tgtHllType, CurMode.SET, seg, true);
    assert seg.get(JAVA_BYTE, LG_K_BYTE) > 7;
  }

  @Override //returns on-heap Set
  CouponHashSet copy() {
    return CouponHashSet.heapifySet(seg);
  }

  @Override //returns on-heap Set
  CouponHashSet copyAs(final TgtHllType tgtHllType) {
    final CouponHashSet clist = CouponHashSet.heapifySet(seg);
    return new CouponHashSet(clist, tgtHllType);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    if (wseg == null) { noWriteAccess(); }
    //avoid array copy
    final int index = find(seg, getLgCouponArrInts(), coupon);
    if (index >= 0) {
      return this; //found duplicate, ignore
    }
    insertInt(wseg, HASH_SET_INT_ARR_START + (~index << 2), coupon);
    insertHashSetCount(wseg, getCouponCount() + 1);
    final boolean promote = checkGrowOrPromote();
    if (!promote) { return this; }
    return promoteListOrSetToHll(this);
  }

  @Override
  int getCouponCount() {
    return extractHashSetCount(seg);
  }

  @Override
  int getSegDataStart() {
    return HASH_SET_INT_ARR_START;
  }

  @Override
  int getPreInts() {
    return HASH_SET_PREINTS;
  }

  private boolean checkGrowOrPromote() {
    int lgCouponArrInts = getLgCouponArrInts();
    if ((RESIZE_DENOM * getCouponCount()) > (RESIZE_NUMER * (1 << lgCouponArrInts))) {
      if (lgCouponArrInts == (getLgConfigK() - 3)) {
        return true; // promote
      }
      insertLgArr(wseg, ++lgCouponArrInts);
      growHashSet(wseg, lgCouponArrInts);
    }
    return false;
  }

  //This could fail if the user has undersized the given MemorySegment
  //  and not used the public methods for sizing the MemorySegment.  See exception.
  private static void growHashSet(final MemorySegment wseg, final int tgtLgCouponArrSize) {
    final int tgtArrSize = 1 << tgtLgCouponArrSize;
    final int[] tgtCouponIntArr = new int[tgtArrSize];
    final int oldLen = 1 << extractLgArr(wseg);
    for (int i = 0; i < oldLen; i++) {
      final int fetched = extractInt(wseg, HASH_SET_INT_ARR_START + (i << 2));
      if (fetched != EMPTY) {
        final int idx = find(tgtCouponIntArr, tgtLgCouponArrSize, fetched);
        if (idx < 0) { //found EMPTY
          tgtCouponIntArr[~idx] = fetched; //insert
          continue;
        }
        throw new SketchesStateException("Error: found duplicate.");
      }
    }
    clear(wseg, HASH_SET_INT_ARR_START, tgtArrSize << 2);
    try {  MemorySegment.copy(tgtCouponIntArr, 0, wseg, JAVA_INT_UNALIGNED, HASH_SET_INT_ARR_START, tgtArrSize); }
    catch (final IndexOutOfBoundsException e) {
      throw new SketchesException(
          "The MemorySegment is undersized. Use the public methods for properly sizing MemorySegment.", e);
    }
  }

  //Searches the Coupon hash table (embedded in MemorySegment) for an empty slot
  // or a duplicate depending on the context.
  //If entire entry is empty, returns one's complement of index = found empty.
  //If entry equals given coupon, returns its index = found duplicate coupon
  //Continues searching
  //If the probe comes back to original index, throws an exception.
  private static int find(final MemorySegment seg, final int lgArr,
      final int coupon) {
    final int arrMask = (1 << lgArr) - 1;
    int probe = coupon & arrMask;
    final int loopIndex = probe;
    do {
      final int couponAtIndex = extractInt(seg, HASH_SET_INT_ARR_START + (probe << 2));
      if (couponAtIndex == EMPTY) { return ~probe; } //empty
      else if (coupon == couponAtIndex) { return probe; } //duplicate
      final int stride = ((coupon & KEY_MASK_26) >>> lgArr) | 1;
      probe = (probe + stride) & arrMask;
    } while (probe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

}
