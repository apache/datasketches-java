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
    hll8Array.extractCommon(hll8Array, mem, memArr, memAdd);
    return hll8Array;
  }

  @Override
  Hll8Array copy() {
    return new Hll8Array(this);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    final int configKmask = (1 << lgConfigK) - 1;
    final int slotNo = BaseHllSketch.getLow26(coupon) & configKmask;
    final int newVal = BaseHllSketch.getValue(coupon);
    assert newVal > 0;
    final int curVal = hllByteArr[slotNo] & VAL_MASK_6;
    if (newVal > curVal) {
      hllByteArr[slotNo] = (byte) (newVal & VAL_MASK_6);
      hipAndKxQIncrementalUpdate(curVal, newVal);
      if (curVal == 0) { numAtCurMin--; } //overloaded as num zeros
      assert numAtCurMin >= 0;
    }
    return this;
  }

  @Override
  PairIterator getIterator() {
    return new Hll8Iterator();
  }

  final class Hll8Iterator implements PairIterator {
    byte[] array;
    int slots;
    int slotNum;

    Hll8Iterator() {
      array = hllByteArr;
      slots = hllByteArr.length;
      slotNum = -1;
    }

    @Override
    public boolean nextValid() {
      slotNum++;
      while (slotNum < slots) {
        if (getValue() != EMPTY) {
          return true;
        }
        slotNum++;
      }
      return false;
    }

    @Override
    public boolean nextAll() {
      slotNum++;
      return slotNum < slots;
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
      return array[slotNum] & VAL_MASK_6;
    }

    @Override
    public int getIndex() {
      return slotNum;
    }
  }

  static final Hll8Array convertToHll8(final HllArray srcHllArr) {
    final Hll8Array hll8Array = new Hll8Array(srcHllArr.getLgConfigK());
    hll8Array.putOooFlag(srcHllArr.getOooFlag());
    final PairIterator itr = srcHllArr.getIterator();
    while (itr.nextValid()) {
      hll8Array.couponUpdate(itr.getPair());
    }
    hll8Array.putHipAccum(srcHllArr.getHipAccum());
    return hll8Array;
  }

}
