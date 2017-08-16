/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.AUX_TOKEN;
import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static com.yahoo.sketches.hll.HllUtil.hiNibbleMask;
import static com.yahoo.sketches.hll.HllUtil.loNibbleMask;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractAuxCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * Uses 4 bits per slot in a packed byte array.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class Hll4Array extends HllArray {

  /**
   * Standard constructor for new instance
   * @param lgConfigK the configured Lg K
   */
  Hll4Array(final int lgConfigK) {
    super(lgConfigK, TgtHllType.HLL_4);
    hllByteArr = new byte[1 << (lgConfigK - 1)];
    auxHashMap = null;
  }

  /**
   * Copy constructor
   * @param that another Hll4Array
   */
  Hll4Array(final Hll4Array that) {
    super(that);
  }

  static final Hll4Array heapify(final Memory mem) {
    final Object memArr = ((WritableMemory) mem).getArray();
    final long memAdd = mem.getCumulativeOffset(0);
    final int lgConfigK = extractLgK(memArr, memAdd);
    final Hll4Array hll4Array = new Hll4Array(lgConfigK);
    HllArray.extractCommonHll(hll4Array, mem, memArr, memAdd);

    //load AuxHashMap
    final int offset = HLL_BYTE_ARR_START + hll4Array.getHllByteArrBytes();
    final int auxCount = extractAuxCount(memArr, memAdd);
    final boolean compact = extractCompactFlag(memArr, memAdd);
    HeapAuxHashMap auxHashMap = null;
    if (auxCount > 0) {
      auxHashMap = HeapAuxHashMap.heapify(mem, offset, lgConfigK, auxCount, compact);
    }
    hll4Array.putAuxHashMap(auxHashMap);
    return hll4Array;
  }

  @Override
  Hll4Array copy() {
    return new Hll4Array(this);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    final int newValue = HllUtil.getValue(coupon);
    if (newValue <= getCurMin()) {
      return this; // super quick rejection; only works for large N
    }
    final int configKmask = (1 << getLgConfigK()) - 1;
    final int slotNo = HllUtil.getLow26(coupon) & configKmask;
    internalUpdate(this, slotNo, newValue);
    return this;
  }

  @Override
  int getSlot(final int slotNo) {
    int theByte = hllByteArr[slotNo >>> 1];
    if ((slotNo & 1) > 0) { //odd?
      theByte >>>= 4;
    }
    return theByte & loNibbleMask;
  }

  @Override
  void putSlot(final int slotNo, final int newValue) {
    final int byteno = slotNo >>> 1;
    final int oldValue = hllByteArr[byteno];
    if ((slotNo & 1) == 0) { // set low nibble
      hllByteArr[byteno] = (byte) ((oldValue & hiNibbleMask) | (newValue & loNibbleMask));
    } else { //set high nibble
      hllByteArr[byteno] = (byte) ((oldValue & loNibbleMask) | ((newValue << 4) & hiNibbleMask));
    }
  }

  static final Hll4Array convertToHll4(final AbstractHllArray srcHllArr) {
    final int lgConfigK = srcHllArr.getLgConfigK();
    final Hll4Array hll4Array = new Hll4Array(lgConfigK);
    hll4Array.putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());

    //1st pass: compute starting curMin and numAtCurMin:
    final int pair = curMinAndNum(srcHllArr);
    final int curMin = HllUtil.getValue(pair);
    final int numAtCurMin = HllUtil.getLow26(pair);

    //2nd pass: Populate KxQ registers, build AuxHashMap if needed
    final PairIterator itr = srcHllArr.getIterator();
    AuxHashMap auxHashMap = hll4Array.getAuxHashMap(); //may be null
    while (itr.nextValid()) {
      final int slotNo = itr.getIndex();
      final int actualValue = itr.getValue();
      hipAndKxQIncrementalUpdate(srcHllArr, 0, actualValue);
      if (actualValue >= (curMin + 15)) {
        hll4Array.putSlot(slotNo, AUX_TOKEN);
        if (auxHashMap == null) {
          auxHashMap = new HeapAuxHashMap(LG_AUX_ARR_INTS[lgConfigK], lgConfigK);
          hll4Array.putAuxHashMap(auxHashMap);
        }
        auxHashMap.mustAdd(slotNo, actualValue);
      } else {
        hll4Array.putSlot(slotNo, actualValue - curMin);
      }
    }

    hll4Array.putCurMin(curMin);
    hll4Array.putNumAtCurMin(numAtCurMin);
    hll4Array.putHipAccum(srcHllArr.getHipAccum());
    return hll4Array;
  }

  //ITERATOR
  @Override
  PairIterator getIterator() {
    return new HeapHll4Iterator(1 << lgConfigK);
  }

  final class HeapHll4Iterator extends HllPairIterator {

    HeapHll4Iterator(final int lengthPairs) {
      super(lengthPairs);
    }

    @Override
    int value() {
      final int nib = Hll4Array.this.getSlot(index);
      return (nib == AUX_TOKEN)
          ? auxHashMap.mustFindValueFor(index) //auxHashMap cannot be null here
          : nib + getCurMin();
    }
  }

}
