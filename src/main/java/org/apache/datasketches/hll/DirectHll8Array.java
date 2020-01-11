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

import static org.apache.datasketches.hll.HllUtil.VAL_MASK_6;
import static org.apache.datasketches.hll.HllUtil.noWriteAccess;
import static org.apache.datasketches.hll.PreambleUtil.HLL_BYTE_ARR_START;

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
    final int newValue = HllUtil.getPairValue(coupon);
    final int configKmask = (1 << getLgConfigK()) - 1;
    final int slotNo = HllUtil.getPairLow26(coupon) & configKmask;

    updateSlotWithKxQ(slotNo, newValue);
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
  void putNibble(final int slotNo, final int nibValue) {
    throw new SketchesStateException("Improper access.");
  }

  @Override
  //Used by Union when source is not HLL8
  final void updateSlotNoKxQ(final int slotNo, final int newValue) {
    final int oldValue = getSlotValue(slotNo);
    if (newValue > oldValue) {
      wmem.putByte(HLL_BYTE_ARR_START + slotNo, (byte) (newValue & VAL_MASK_6));
    }
  }

  @Override
  //Used by this couponUpdate()
  //updates HipAccum, CurMin, NumAtCurMin, KxQs and checks newValue > oldValue
  final void updateSlotWithKxQ(final int slotNo, final int newValue) {
    final int oldValue = getSlotValue(slotNo);
    if (newValue > oldValue) {
      wmem.putByte(HLL_BYTE_ARR_START + slotNo, (byte) (newValue & VAL_MASK_6));
      hipAndKxQIncrementalUpdate(this, oldValue, newValue);
      if (oldValue == 0) {
        decNumAtCurMin();
        assert getNumAtCurMin() >= 0;
      }
    }
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
