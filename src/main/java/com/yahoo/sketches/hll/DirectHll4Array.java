/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
class DirectHll4Array extends DirectHllArray {

  DirectHll4Array(final WritableMemory wmem) {
    super(wmem);
  }

  DirectHll4Array(final Memory mem) {
    super(mem);
  }

  @Override
  HllSketchImpl copy() {
    return Hll4Array.heapify(mem);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  int getHllByteArrBytes() {
    return 1 << (extractLgK(memObj, memAdd) - 1);
  }

  @Override
  PairIterator getAuxIterator() {
    // TODO Auto-generated method stub
    return null;
  }

}
