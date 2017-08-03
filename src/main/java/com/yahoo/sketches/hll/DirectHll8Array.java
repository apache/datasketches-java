/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.memory.UnsafeUtil.unsafe;
import static com.yahoo.sketches.hll.HllUtil.VAL_MASK_6;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARRAY_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
class DirectHll8Array extends DirectHllArray {

  DirectHll8Array(final WritableMemory wmem) {
    super(wmem);
  }

  DirectHll8Array(final Memory mem) {
    super(mem);
  }

  @Override
  HllSketchImpl copy() {
    return Hll8Array.heapify(mem);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    final int configKmask = (1 << getLgConfigK()) - 1;
    final int slotNo = BaseHllSketch.getLow26(coupon) & configKmask;
    final int newVal = BaseHllSketch.getValue(coupon);
    assert newVal > 0;
    final long byteOffset = HLL_BYTE_ARRAY_START + slotNo;
    final int curVal = unsafe.getByte(memObj, memAdd + byteOffset);
    if (newVal > curVal) {
      unsafe.putByte(memObj, memAdd + byteOffset, (byte) (newVal & VAL_MASK_6));
      hipAndKxQIncrementalUpdate(curVal, newVal);
      if (curVal == 0) {
        decNumAtCurMin(); //overloaded as num zeros
        assert getNumAtCurMin() >= 0;
      }
    }
    return this;
  }

  @Override
  int getHllByteArrBytes() {
    return 1 << extractLgK(memObj, memAdd);
  }

  @Override
  PairIterator getAuxIterator() {
    // TODO Auto-generated method stub
    return null;
  }


}
