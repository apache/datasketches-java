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

import static org.apache.datasketches.ByteArrayUtil.getShortLE;
import static org.apache.datasketches.ByteArrayUtil.putShortLE;
import static org.apache.datasketches.hll.HllUtil.KEY_BITS_26;
import static org.apache.datasketches.hll.HllUtil.VAL_MASK_6;
import static org.apache.datasketches.hll.PreambleUtil.extractLgK;

import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.Memory;

/**
 * Uses 6 bits per slot in a packed byte array.
 * @author Lee Rhodes
 */
class Hll6Array extends HllArray {


  /**
   * Standard constructor for new instance
   * @param lgConfigK the configured Lg K
   */
  Hll6Array(final int lgConfigK) {
    super(lgConfigK, TgtHllType.HLL_6);
    hllByteArr = new byte[hll6ArrBytes(lgConfigK)];
  }

  /**
   * Copy constructor
   * @param that another Hll6Array
   */
  Hll6Array(final Hll6Array that) {
    super(that);
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
    final int newValue = coupon >>> KEY_BITS_26;
    final int configKmask = (1 << lgConfigK) - 1;
    final int slotNo = coupon & configKmask;
    updateSlotWithKxQ(slotNo, newValue);
    return this;
  }

  @Override
  int getNibble(final int slotNo) {
    throw new SketchesStateException("Improper access.");
  }

  @Override
  final int getSlotValue(final int slotNo) {
    return get6Bit(hllByteArr, 0, slotNo);
  }

  @Override
  PairIterator iterator() {
    return new HeapHll6Iterator(1 << lgConfigK);
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
      put6Bit(hllByteArr, 0, slotNo, newValue);
      hipAndKxQIncrementalUpdate(this, oldValue, newValue);
      if (oldValue == 0) {
        numAtCurMin--; //interpret numAtCurMin as num Zeros
        assert getNumAtCurMin() >= 0;
      }
    }
  }

  //on-heap
  private static final void put6Bit(final byte[] arr, final int offsetBytes, final int slotNo,
      final int newValue) {
    final int startBit = slotNo * 6;
    final int shift = startBit & 0X7;
    final int byteIdx = (startBit >>> 3) + offsetBytes;
    final int valShifted = (newValue & 0X3F) << shift;
    final int curMasked = getShortLE(arr, byteIdx) & (~(VAL_MASK_6 << shift));
    final short insert = (short) (curMasked | valShifted);
    putShortLE(arr, byteIdx, insert);
  }

  //on-heap
  private static final int get6Bit(final byte[] arr, final int offsetBytes, final int slotNo) {
    final int startBit = slotNo * 6;
    final int shift = startBit & 0X7;
    final int byteIdx = (startBit >>> 3) + offsetBytes;
    return (byte) ((getShortLE(arr, byteIdx) >>> shift) & 0X3F);
  }



  //ITERATOR

  private final class HeapHll6Iterator extends HllPairIterator {
    int bitOffset;

    HeapHll6Iterator(final int lengthPairs) {
      super(lengthPairs);
      bitOffset = - 6;
    }

    @Override
    int value() {
      bitOffset += 6;
      final int tmp = getShortLE(hllByteArr, bitOffset / 8);
      final int shift = (bitOffset % 8) & 0X7;
      return (tmp >>> shift) & VAL_MASK_6;
    }
  }

}
