/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.VAL_MASK_6;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;

import com.yahoo.memory.Memory;

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
  PairIterator iterator() {
    return new HeapHll8Iterator(1 << lgConfigK);
  }

  @Override
  final int getSlot(final int slotNo) {
    return hllByteArr[slotNo] & VAL_MASK_6;
  }

  @Override
  final void putSlot(final int slotNo, final int value) {
    hllByteArr[slotNo] = (byte) (value & VAL_MASK_6);
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
  }

}
