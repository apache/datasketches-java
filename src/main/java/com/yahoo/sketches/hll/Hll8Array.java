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
   * Standard constructor.
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
    return new HeapHll8Iterator(hllByteArr, 1 << lgConfigK);
  }

  final class HeapHll8Iterator extends ByteArrayPairIterator {

    HeapHll8Iterator(final byte[] array, final int lengthPairs) {
      super(array, lengthPairs);
    }

    @Override
    public boolean nextValid() {
      while (++index < lengthPairs) {
        value = array[index] & 0XFF;
        if (value != EMPTY) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean nextAll() {
      if (++index < lengthPairs) {
        value = array[index] & 0XFF;
        return true;
      }
      return false;
    }
  }


  static final Hll8Array convertToHll8(final HllArray srcHllArr) {
    final Hll8Array hll8Array = new Hll8Array(srcHllArr.getLgConfigK());
    hll8Array.putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());
    final PairIterator itr = srcHllArr.getIterator();
    while (itr.nextValid()) {
      hll8Array.couponUpdate(itr.getPair());
    }
    hll8Array.putHipAccum(srcHllArr.getHipAccum());
    return hll8Array;
  }

}
