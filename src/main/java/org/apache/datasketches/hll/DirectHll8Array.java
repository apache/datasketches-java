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

import static org.apache.datasketches.hll.HllUtil.KEY_BITS_26;
import static org.apache.datasketches.hll.HllUtil.KEY_MASK_26;
import static org.apache.datasketches.hll.HllUtil.VAL_MASK_6;
import static org.apache.datasketches.hll.HllUtil.noWriteAccess;
import static org.apache.datasketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.insertEmptyFlag;

import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Uses 8 bits per slot in a byte array.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectHll8Array extends DirectHllArray {

  //Called by HllSketch.writableWrap(), DirectCouponList.promoteListOrSetToHll
  DirectHll8Array(final int lgConfigK, final WritableMemory wmem) {
    super(lgConfigK, TgtHllType.HLL_8, wmem);
  }

  //Called by HllSketch.wrap(Memory)
  DirectHll8Array(final int lgConfigK, final Memory mem) {
    super(lgConfigK, TgtHllType.HLL_8, mem);
  }

  @Override
  HllSketchImpl copy() {
    return Hll8Array.heapify(mem);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    if (wmem == null) { noWriteAccess(); }
    insertEmptyFlag(wmem, false);
    final int configKmask = (1 << getLgConfigK()) - 1;
    final int slotNo = HllUtil.getPairLow26(coupon) & configKmask;
    final int newVal = HllUtil.getPairValue(coupon);
    assert newVal > 0;

    final int curVal = getSlotValue(slotNo);
    if (newVal > curVal) {
      putSlotValue(slotNo, newVal);
      hipAndKxQIncrementalUpdate(this, curVal, newVal);
      if (curVal == 0) {
        decNumAtCurMin(); //overloaded as num zeros
        assert getNumAtCurMin() >= 0;
      }
    }
    return this;
  }

  @Override
  int getHllByteArrBytes() {
    return hll8ArrBytes(lgConfigK);
  }

  @Override
  int getNibble(final int slotNo) {
    throw new SketchesStateException("Improper access.");
  }

  @Override
  final int getSlotValue(final int slotNo) {
    return mem.getByte(HLL_BYTE_ARR_START + slotNo) & VAL_MASK_6;
  }

  @Override
  PairIterator iterator() {
    return new DirectHll8Iterator(1 << lgConfigK);
  }

  @Override
  void mergeTo(final HllSketch that) {
    final int slots = 1 << lgConfigK;
    for (int i = 0; i < slots; i++ ) {
      final int value = mem.getByte(HLL_BYTE_ARR_START + i) & VAL_MASK_6;
      if (value == 0) { continue; }
      that.couponUpdate((value << KEY_BITS_26) | (i & KEY_MASK_26));
    }
  }

  @Override
  void putNibble(final int slotNo, final int nibValue) {
    throw new SketchesStateException("Improper access.");
  }

  @Override
  final void putSlotValue(final int slotNo, final int value) {
    wmem.putByte(HLL_BYTE_ARR_START + slotNo, (byte) (value & VAL_MASK_6));
  }

  //ITERATOR

  final class DirectHll8Iterator extends HllPairIterator {

    DirectHll8Iterator(final int lengthPairs) {
      super(lengthPairs);
    }

    @Override
    int value() {
      final int tmp = mem.getByte(HLL_BYTE_ARR_START + index);
      return tmp & VAL_MASK_6;
    }
  }

}
