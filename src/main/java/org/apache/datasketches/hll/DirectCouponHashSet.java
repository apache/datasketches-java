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
import static org.apache.datasketches.hll.HllUtil.KEY_MASK_26;
import static org.apache.datasketches.hll.HllUtil.RESIZE_DENOM;
import static org.apache.datasketches.hll.HllUtil.RESIZE_NUMER;
import static org.apache.datasketches.hll.HllUtil.noWriteAccess;
import static org.apache.datasketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.HASH_SET_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.LG_K_BYTE;
import static org.apache.datasketches.hll.PreambleUtil.extractHashSetCount;
import static org.apache.datasketches.hll.PreambleUtil.extractInt;
import static org.apache.datasketches.hll.PreambleUtil.extractLgArr;
import static org.apache.datasketches.hll.PreambleUtil.insertHashSetCount;
import static org.apache.datasketches.hll.PreambleUtil.insertInt;
import static org.apache.datasketches.hll.PreambleUtil.insertLgArr;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

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

  //Constructs this sketch with read-only data, may be compact.
  DirectCouponHashSet(final int lgConfigK, final TgtHllType tgtHllType,
      final Memory mem) {
    super(lgConfigK, tgtHllType, CurMode.SET, mem);
    assert mem.getByte(LG_K_BYTE) > 7;
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
    if (wmem == null) { noWriteAccess(); }
    //avoid array copy
    final int index = find(mem, getLgCouponArrInts(), coupon);
    if (index >= 0) {
      return this; //found duplicate, ignore
    }
    insertInt(wmem, HASH_SET_INT_ARR_START + (~index << 2), coupon);
    insertHashSetCount(wmem, getCouponCount() + 1);
    final boolean promote = checkGrowOrPromote();
    if (!promote) { return this; }
    return promoteListOrSetToHll(this);
  }

  @Override
  int getCouponCount() {
    return extractHashSetCount(mem);
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
    int lgCouponArrInts = getLgCouponArrInts();
    if ((RESIZE_DENOM * getCouponCount()) > (RESIZE_NUMER * (1 << lgCouponArrInts))) {
      if (lgCouponArrInts == (getLgConfigK() - 3)) {
        return true; // promote
      }
      //TODO if direct, ask for more memory
      insertLgArr(wmem, ++lgCouponArrInts);
      growHashSet(wmem, lgCouponArrInts);
    }
    return false;
  }

  private static final void growHashSet(final WritableMemory wmem, final int tgtLgCouponArrSize) {
    final int tgtArrSize = 1 << tgtLgCouponArrSize;
    final int[] tgtCouponIntArr = new int[tgtArrSize];
    final int oldLen = 1 << extractLgArr(wmem);
    for (int i = 0; i < oldLen; i++) {
      final int fetched = extractInt(wmem, HASH_SET_INT_ARR_START + (i << 2));
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
  private static final int find(final Memory mem, final int lgArr,
      final int coupon) {
    final int arrMask = (1 << lgArr) - 1;
    int probe = coupon & arrMask;
    final int loopIndex = probe;
    do {
      final int couponAtIndex = extractInt(mem, HASH_SET_INT_ARR_START + (probe << 2));
      if (couponAtIndex == EMPTY) { return ~probe; } //empty
      else if (coupon == couponAtIndex) { return probe; } //duplicate
      final int stride = ((coupon & KEY_MASK_26) >>> lgArr) | 1;
      probe = (probe + stride) & arrMask;
    } while (probe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

}
