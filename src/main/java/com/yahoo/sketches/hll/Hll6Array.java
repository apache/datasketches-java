/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_BITS_26;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
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
    hllByteArr = new byte[byteArrBytes(lgConfigK)];
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
    final int slotNo = BaseHllSketch.getLow26(coupon) & configKmask;
    final int newVal = BaseHllSketch.getValue(coupon);
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

  @Override
  PairIterator getIterator() {
    return new Hll6Iterator();
  }

  static final void put6Bit(final WritableMemory mem, final long offsetBytes,
      final int slotNo, final int val) {
    final long idxAndShift = byteIdxAndShift(slotNo);
    final long byteIdx = idxAndShift & (0XFFFFFFFFL + offsetBytes);
    final int shift = (int)(idxAndShift >>> 32);
    final int valShifted = (val & 0X3F) << shift;
    final int curMasked = mem.getShort(byteIdx) & (~(VAL_MASK_6 << shift));
    final short insert = (short) (curMasked | valShifted);
    mem.putShort(byteIdx, insert);
  }

  static final int get6Bit(final Memory mem, final long offsetBytes, final int slotNo) {
    final long idxAndShift = byteIdxAndShift(slotNo);
    final long byteIdx = idxAndShift & (0XFFFFFFFFL + offsetBytes);
    final int shift = (int)(idxAndShift >>> 32);
    return (byte) ((mem.getShort(byteIdx) >> shift) & 0X3F);
  }

  private static final long byteIdxAndShift(final int slotIdx) {
    final long startBit = slotIdx * 6;
    //shift in upper 32, byte index in lower 32
    return (startBit >> 3) | ((startBit & 0X7L) << 32);
  }


  static final int byteArrBytes(final int lgConfigK) {
    final int numSlots = 1 << lgConfigK;
    return ((numSlots * 3) >> 2) + 1;
  }

  //Iterator

  final class Hll6Iterator implements PairIterator {
    int lengthBits;
    int slotNum;
    int bitOffset;

    Hll6Iterator() {
      lengthBits = (1 << getLgConfigK()) * 6;
      assert lengthBits <= (getHllByteArr().length * 8);
      slotNum = -1;
      bitOffset = -6;
    }

    @Override
    public boolean nextValid() {
      slotNum++;
      bitOffset += 6;
      while (bitOffset < lengthBits) {
        if (getValue() != EMPTY) {
          return true;
        }
        slotNum++;
        bitOffset += 6;
      }
      return false;
    }

    @Override
    public boolean nextAll() {
      slotNum++;
      bitOffset += 6;
      return bitOffset < lengthBits;
    }

    @Override
    public int getPair() {
      return (getValue() << KEY_BITS_26) | (slotNum & KEY_MASK_26);
    }

    @Override
    public int getKey() {
      return slotNum;
    }

    @Override
    public int getValue() {
      final int tmp = mem.getShort(bitOffset / 8);
      final int shift = (bitOffset % 8) & 0X7;
      return (tmp >>> shift) & VAL_MASK_6;
    }

    @Override
    public int getIndex() {
      return slotNum;
    }
  }

  static final Hll6Array convertToHll6(final HllArray srcHllArr) {
    final Hll6Array hll6Array = new Hll6Array(srcHllArr.getLgConfigK());
    hll6Array.putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());
    final PairIterator itr = srcHllArr.getIterator();
    while (itr.nextValid()) {
      hll6Array.couponUpdate(itr.getPair());
    }
    hll6Array.putHipAccum(srcHllArr.getHipAccum());
    return hll6Array;
  }

}
