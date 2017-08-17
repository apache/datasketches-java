/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.VAL_MASK_6;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * Uses 8 bits per slot in a byte array.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class Hll8Array extends HllArray {

  /**
   * Standard constructor for new instance
   * @param lgConfigK the configured Lg K
   */
  Hll8Array(final int lgConfigK) {
    super(lgConfigK, TgtHllType.HLL_8);
    hllByteArr = new byte[1 << lgConfigK];
  }

  /**
   * Copy constructor
   * @param that another Hll8Array
   */
  Hll8Array(final Hll8Array that) {
    super(that);
  }

  static final Hll8Array heapify(final Memory mem) {
    final Object memArr = ((WritableMemory) mem).getArray();
    final long memAdd = mem.getCumulativeOffset(0);
    final int lgConfigK = extractLgK(memArr, memAdd);
    final Hll8Array hll8Array = new Hll8Array(lgConfigK);
    HllArray.extractCommonHll(hll8Array, mem, memArr, memAdd);
    return hll8Array;
  }

  @Override
  Hll8Array copy() {
    return new Hll8Array(this);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    final int configKmask = (1 << getLgConfigK()) - 1;
    final int slotNo = HllUtil.getLow26(coupon) & configKmask;
    final int newVal = HllUtil.getValue(coupon);
    assert newVal > 0;
    final byte[] hllByteArr = getHllByteArr();
    final int curVal = hllByteArr[slotNo] & VAL_MASK_6;
    if (newVal > curVal) {
      hllByteArr[slotNo] = (byte) (newVal & VAL_MASK_6);
      hipAndKxQIncrementalUpdate(this, curVal, newVal);
      if (curVal == 0) {
        decNumAtCurMin(); //overloaded as num zeros
        assert getNumAtCurMin() >= 0;
      }
    }
    return this;
  }

  @Override
  int getSlot(final int slotNo) {
    return hllByteArr[slotNo];
  }

  @Override
  void putSlot(final int slotNo, final int newValue) {
    hllByteArr[slotNo] = (byte) newValue;
  }

  static final Hll8Array convertToHll8(final AbstractHllArray srcHllArr) {
    final int lgConfigK = srcHllArr.lgConfigK;
    final Hll8Array hll8Array = new Hll8Array(lgConfigK);
    hll8Array.putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());
    int numZeros = 1 << lgConfigK;
    final PairIterator itr = srcHllArr.getIterator();
    while (itr.nextAll()) {
      if (itr.getValue() != EMPTY) {
        numZeros--;
        hll8Array.couponUpdate(itr.getPair());
      }
    }
    hll8Array.putNumAtCurMin(numZeros);
    hll8Array.putHipAccum(srcHllArr.getHipAccum());
    return hll8Array;
  }

  //ITERATOR
  @Override
  PairIterator getIterator() {
    return new HeapHll8Iterator(1 << lgConfigK);
  }

  final class HeapHll8Iterator extends HllPairIterator {

    HeapHll8Iterator(final int lengthPairs) {
      super(lengthPairs);
    }

    @Override
    int value() {
      return hllByteArr[index] & VAL_MASK_6;
    }
  }

}
