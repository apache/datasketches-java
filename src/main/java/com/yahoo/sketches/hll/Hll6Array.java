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
 */
class Hll6Array extends HllArray {
  final WritableMemory mem;

  /**
   * Standard constructor for new instance
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
    final int lgConfigK = extractLgK(mem);
    final Hll6Array hll6Array = new Hll6Array(lgConfigK);
    HllArray.extractCommonHll(mem, hll6Array);
    return hll6Array;
  }

  @Override
  Hll6Array copy() {
    return new Hll6Array(this);
  }

  @Override
  PairIterator iterator() {
    return new HeapHll6Iterator(1 << lgConfigK);
  }

  @Override
  final int getSlot(final int slotNo) {
    return Hll6Array.get6Bit(mem, 0, slotNo);
  }

  @Override
  final void putSlot(final int slotNo, final int value) {
    Hll6Array.put6Bit(mem, 0, slotNo, value);
  }

  //works for both heap and direct
  static final void put6Bit(final WritableMemory wmem, final int offsetBytes, final int slotNo,
      final int newValue) {
    final int startBit = slotNo * 6;
    final int shift = startBit & 0X7;
    final int byteIdx = (startBit >>> 3) + offsetBytes;
    final int valShifted = (newValue & 0X3F) << shift;
    final int curMasked = wmem.getShort(byteIdx) & (~(VAL_MASK_6 << shift));
    final short insert = (short) (curMasked | valShifted);
    wmem.putShort(byteIdx, insert);
  }

  //works for both heap and direct
  static final int get6Bit(final Memory mem, final int offsetBytes, final int slotNo) {
    final int startBit = slotNo * 6;
    final int shift = startBit & 0X7;
    final int byteIdx = (startBit >>> 3) + offsetBytes;
    return (byte) ((mem.getShort(byteIdx) >>> shift) & 0X3F);
  }

  //ITERATOR

  final class HeapHll6Iterator extends HllPairIterator {
    int bitOffset;

    HeapHll6Iterator(final int lengthPairs) {
      super(lengthPairs);
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
