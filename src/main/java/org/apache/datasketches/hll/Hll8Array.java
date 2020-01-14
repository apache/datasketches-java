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

import static org.apache.datasketches.hll.HllUtil.EMPTY;
import static org.apache.datasketches.hll.HllUtil.KEY_BITS_26;
import static org.apache.datasketches.hll.HllUtil.VAL_MASK_6;
import static org.apache.datasketches.hll.PreambleUtil.extractLgK;

import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.Memory;

/**
 * Uses 8 bits per slot in a byte array.
 * @author Lee Rhodes
 */
class Hll8Array extends HllArray {

  /**
   * Standard constructor for new instance
   * @param lgConfigK the configured Lg K
   */
  Hll8Array(final int lgConfigK) {
    super(lgConfigK, TgtHllType.HLL_8);
    hllByteArr = new byte[hll8ArrBytes(lgConfigK)];
  }

  /**
   * Copy constructor
   * @param that another Hll8Array
   */
  Hll8Array(final Hll8Array that) {
    super(that);
  }

  static final Hll8Array heapify(final Memory mem) {
    final int lgConfigK = extractLgK(mem);
    final Hll8Array hll8Array = new Hll8Array(lgConfigK);
    HllArray.extractCommonHll(mem, hll8Array);
    return hll8Array;
  }

  @Override
  Hll8Array copy() {
    return new Hll8Array(this);
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
    return hllByteArr[slotNo] & VAL_MASK_6;
  }

  @Override
  PairIterator iterator() {
    return new HeapHll8Iterator(1 << lgConfigK);
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
      hllByteArr[slotNo] = (byte) (newValue & VAL_MASK_6);
    }
  }

  @Override
  //Used by this couponUpdate()
  //updates HipAccum, CurMin, NumAtCurMin, KxQs and checks newValue > oldValue
  final void updateSlotWithKxQ(final int slotNo, final int newValue) {
    final int oldValue = getSlotValue(slotNo);
    if (newValue > oldValue) {
      hllByteArr[slotNo] = (byte) (newValue & VAL_MASK_6);
      hipAndKxQIncrementalUpdate(this, oldValue, newValue);
      if (oldValue == 0) {
        numAtCurMin--; //interpret numAtCurMin as num Zeros
        assert getNumAtCurMin() >= 0;
      }
    }
  }

  //ITERATOR

  final class HeapHll8Iterator extends HllPairIterator {

    HeapHll8Iterator(final int lengthPairs) {
      super(lengthPairs);
    }

    @Override
    int value() {
      return hllByteArr[index] & VAL_MASK_6;
    }

    @Override
    public boolean nextValid() {
      while (++index < lengthPairs) {
        value = hllByteArr[index] & VAL_MASK_6;
        if (value != EMPTY) {
          return true;
        }
      }
      return false;
    }

  }

}
