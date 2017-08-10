/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.VAL_MASK_6;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * Uses 6 bits per slot in a packed byte array.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class Hll6Array extends HllArray {
  final WritableMemory mem;

  /**
   * Standard constructor
   * @param lgConfigK the configured Lg K
   */
  Hll6Array(final int lgConfigK) {
    super(lgConfigK, TgtHllType.HLL_6);
    hllByteArr = new byte[hll6ArrBytes(lgConfigK)];
    mem = WritableMemory.wrap(hllByteArr);
  }

  /**
   * Copy constructor
   * @param that another Hll6Array
   */
  Hll6Array(final Hll6Array that) {
    super(that);
    mem = WritableMemory.wrap(hllByteArr); //hllByteArr already cloned.
  }

  static final Hll6Array heapify(final Memory mem) {
    final Object memObj = ((WritableMemory) mem).getArray();
    final long memAdd = mem.getCumulativeOffset(0);
    final int lgConfigK = extractLgK(memObj, memAdd);
    final Hll6Array hll6Array = new Hll6Array(lgConfigK);
    HllArray.extractCommonHll(hll6Array, mem, memObj, memAdd);
    return hll6Array;
  }

  @Override
  Hll6Array copy() {
    return new Hll6Array(this);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    final int configKmask = (1 << getLgConfigK()) - 1;
    final int slotNo = HllUtil.getLow26(coupon) & configKmask;
    final int newVal = HllUtil.getValue(coupon);
    assert newVal > 0;
    final int curVal = get6Bit(mem, 0, slotNo);
    if (newVal > curVal) {
      put6Bit(mem, 0, slotNo, newVal);
      hipAndKxQIncrementalUpdate(curVal, newVal);
      if (curVal == 0) {
        decNumAtCurMin(); //overloaded as num zeros
        assert getNumAtCurMin() >= 0;
      }
    }
    return this;
  }

  static final void put6Bit(final WritableMemory mem, final int offsetBytes,
      final int slotNo, final int val) {
    final int startBit = slotNo * 6;
    final int shift = (startBit & 0X7) << 32;
    final int byteIdx = (startBit >>> 3) + offsetBytes;
    final int valShifted = (val & 0X3F) << shift;
    final int curMasked = mem.getShort(byteIdx) & (~(VAL_MASK_6 << shift));
    final short insert = (short) (curMasked | valShifted);
    mem.putShort(byteIdx, insert);
  }

  static final int get6Bit(final Memory mem, final int offsetBytes, final int slotNo) {
    final int startBit = slotNo * 6;
    final int shift = (startBit & 0X7) << 32;
    final int byteIdx = (startBit >>> 3) + offsetBytes;
    return (byte) ((mem.getShort(byteIdx) >>> shift) & 0X3F);
  }

  static final Hll6Array convertToHll6(final AbstractHllArray srcHllArr) {
    final Hll6Array hll6Array = new Hll6Array(srcHllArr.getLgConfigK());
    hll6Array.putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());
    final PairIterator itr = srcHllArr.getIterator();
    while (itr.nextValid()) {
      hll6Array.couponUpdate(itr.getPair());
    }
    hll6Array.putHipAccum(srcHllArr.getHipAccum());
    return hll6Array;
  }

  //ITERATOR
  @Override
  PairIterator getIterator() {
    return new HeapHll6Iterator(hllByteArr, 1 << lgConfigK);
  }

  final class HeapHll6Iterator extends HllArrayPairIterator {
    int bitOffset;

    HeapHll6Iterator(final byte[] array, final int lengthPairs) {
      super(array, lengthPairs);
      bitOffset = - 6;
    }

    @Override
    int value() {
      bitOffset += 6;
      final int tmp = mem.getShort(bitOffset / 8);
      final int shift = (bitOffset % 8) & 0X7;
      return (tmp >>> shift) & VAL_MASK_6;
    }
  }

}
