/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.VAL_MASK_6;
import static com.yahoo.sketches.hll.HllUtil.noWriteAccess;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
class DirectHll6Array extends DirectHllArray {

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
    final int configKmask = (1 << getLgConfigK()) - 1;
    final int slotNo = HllUtil.getLow26(coupon) & configKmask;
    final int newVal = HllUtil.getValue(coupon);
    assert newVal > 0;

    final int curVal = getSlot(slotNo);
    if (newVal > curVal) {
      putSlot(slotNo, newVal);
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
    return hll6ArrBytes(lgConfigK);
  }

  @Override
  PairIterator iterator() {
    return new DirectHll6Iterator(1 << lgConfigK);
  }

  @Override
  final int getSlot(final int slotNo) {
    return Hll6Array.get6Bit(mem, HLL_BYTE_ARR_START, slotNo);
  }

  @Override
  final void putSlot(final int slotNo, final int value) {
    Hll6Array.put6Bit(wmem, HLL_BYTE_ARR_START, slotNo, value);
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
