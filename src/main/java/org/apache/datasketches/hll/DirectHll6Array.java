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
 * @author Lee Rhodes
 */
final class DirectHll6Array extends DirectHllArray {

  //Called by HllSketch.writableWrap(), DirectCouponList.promoteListOrSetToHll
  DirectHll6Array(final int lgConfigK, final WritableMemory wmem) {
    super(lgConfigK, TgtHllType.HLL_6, wmem);
  }

  //Called by HllSketch.wrap(Memory)
  DirectHll6Array(final int lgConfigK, final Memory mem) {
    super(lgConfigK, TgtHllType.HLL_6, mem);
  }

  @Override
  HllSketchImpl copy() {
    return Hll6Array.heapify(mem);
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
    return hll6ArrBytes(lgConfigK);
  }

  @Override
  int getNibble(final int slotNo) {
    throw new SketchesStateException("Improper access.");
  }

  @Override
  final int getSlotValue(final int slotNo) {
    return get6Bit(mem, HLL_BYTE_ARR_START, slotNo);
  }

  @Override
  PairIterator iterator() {
    return new DirectHll6Iterator(1 << lgConfigK);
  }

  @Override
  void putNibble(final int slotNo, final int nibValue) {
    throw new SketchesStateException("Improper access.");
  }

  @Override
  //Would be used by Union, but not used because the gadget is always HLL8 type
  final void updateSlotNoKxQ(final int slotNo, final int newValue) {
    throw new SketchesStateException("Improper access.");
  }

  @Override
  //Used by this couponUpdate()
  //updates HipAccum, CurMin, NumAtCurMin, KxQs and checks newValue > oldValue
  final void updateSlotWithKxQ(final int slotNo, final int newValue) {
    final int oldValue = getSlotValue(slotNo);
    if (newValue > oldValue) {
      put6Bit(wmem, HLL_BYTE_ARR_START, slotNo, newValue);
      hipAndKxQIncrementalUpdate(this, oldValue, newValue);
      if (oldValue == 0) {
        decNumAtCurMin(); //overloaded as num zeros
        assert getNumAtCurMin() >= 0;
      }
    }
  }

  //off-heap / direct
  private static final void put6Bit(final WritableMemory wmem, final int offsetBytes, final int slotNo,
      final int newValue) {
    final int startBit = slotNo * 6;
    final int shift = startBit & 0X7;
    final int byteIdx = (startBit >>> 3) + offsetBytes;
    final int valShifted = (newValue & 0X3F) << shift;
    final int curMasked = wmem.getShort(byteIdx) & (~(VAL_MASK_6 << shift));
    final short insert = (short) (curMasked | valShifted);
    wmem.putShort(byteIdx, insert);
  }

  //off-heap / direct
  private static final int get6Bit(final Memory mem, final int offsetBytes, final int slotNo) {
    final int startBit = slotNo * 6;
    final int shift = startBit & 0X7;
    final int byteIdx = (startBit >>> 3) + offsetBytes;
    return (byte) ((mem.getShort(byteIdx) >>> shift) & 0X3F);
  }

  //ITERATOR

  final class DirectHll6Iterator extends HllPairIterator {
    int bitOffset;

    DirectHll6Iterator(final int lengthPairs) {
      super(lengthPairs);
      bitOffset = -6;
    }

    @Override
    int value() {
      bitOffset += 6;
      final int tmp = mem.getShort(HLL_BYTE_ARR_START + (bitOffset / 8));
      final int shift = (bitOffset % 8) & 0X7;
      return (tmp >>> shift) & VAL_MASK_6;
    }
  }

}
