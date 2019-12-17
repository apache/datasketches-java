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
import static org.apache.datasketches.hll.PreambleUtil.extractLgK;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

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
  HllSketchImpl couponUpdate(final int coupon) {
    final int configKmask = (1 << lgConfigK) - 1;
    final int slotNo = coupon & configKmask;
    final int newVal = coupon >>> KEY_BITS_26;
    assert newVal > 0;

    final int curVal = get6Bit(mem, 0, slotNo);
    if (newVal > curVal) {
      put6Bit(mem, 0, slotNo, newVal);
      hipAndKxQIncrementalUpdate(this, curVal, newVal);
      if (curVal == 0) {
        numAtCurMin--; //interpret numAtCurMin as num Zeros
        assert getNumAtCurMin() >= 0;
      }
    }
    return this;
  }

  @Override
  final int getSlot(final int slotNo) {
    return Hll6Array.get6Bit(mem, 0, slotNo);
  }

  @Override
  PairIterator iterator() {
    return new HeapHll6Iterator(1 << lgConfigK);
  }

  @Override
  void mergeTo(final HllSketch that) {
    final int slots = 1 << lgConfigK;
    for (int slotNo = 0, bitOffset = 0; slotNo < slots; slotNo++, bitOffset += 6) {
      final int tmp = mem.getShort(bitOffset / 8);
      final int shift = (bitOffset % 8) & 0X7;
      final int value = (tmp >>> shift) & VAL_MASK_6;
      if (value == 0) { continue; }
      that.couponUpdate((value << KEY_BITS_26) | (slotNo & KEY_MASK_26));
    }
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
