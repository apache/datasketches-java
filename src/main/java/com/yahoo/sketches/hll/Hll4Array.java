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

/**
 * Uses 4 bits per slot in a packed byte array.
 * @author Lee Rhodes
 */
class Hll4Array extends HllArray {

  /**
   * Standard constructor for new instance
   * @param lgConfigK the configured Lg K
   */
  Hll4Array(final int lgConfigK) {
    super(lgConfigK, TgtHllType.HLL_4);
    hllByteArr = new byte[hll4ArrBytes(lgConfigK)];
  }

  /**
   * Copy constructor
   * @param that another Hll4Array
   */
  Hll4Array(final Hll4Array that) {
    super(that);
  }

  static final Hll4Array heapify(final Memory mem) {
    final int lgConfigK = extractLgK(mem);
    final Hll4Array hll4Array = new Hll4Array(lgConfigK);
    HllArray.extractCommonHll(mem, hll4Array);

    //load AuxHashMap
    final int auxStart = hll4Array.auxStart;
    final int auxCount = extractAuxCount(mem);
    final boolean compact = extractCompactFlag(mem);
    HeapAuxHashMap auxHashMap = null;
    if (auxCount > 0) {
      auxHashMap = HeapAuxHashMap.heapify(mem, auxStart, lgConfigK, auxCount, compact);
    }
    hll4Array.putAuxHashMap(auxHashMap, false);
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
    Hll4Update.internalHll4Update(this, slotNo, newValue);
    return this;
  }

  @Override
  PairIterator iterator() {
    return new HeapHll4Iterator(1 << lgConfigK);
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
  int getUpdatableSerializationBytes() {
    final AuxHashMap auxHashMap = getAuxHashMap();
    final int auxBytes;
    if (auxHashMap == null) {
      auxBytes = 4 << LG_AUX_ARR_INTS[lgConfigK];
    } else {
      auxBytes = 4 << auxHashMap.getLgAuxArrInts();
    }
    return HLL_BYTE_ARR_START + getHllByteArrBytes() + auxBytes;
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

  @Override
  byte[] toCompactByteArray() {
    return ToByteArrayImpl.toHllByteArray(this, true);
  }

  //ITERATOR

  final class HeapHll4Iterator extends HllPairIterator {

    HeapHll4Iterator(final int lengthPairs) {
      super(lengthPairs);
    }

    @Override
    int value() {
      final int nib = Hll4Array.this.getSlot(index);
      if (nib == AUX_TOKEN) {
        final AuxHashMap auxHashMap = getAuxHashMap();
        return auxHashMap.mustFindValueFor(index); //auxHashMap cannot be null here
      } else {
        return nib + getCurMin();
      }
    }
  }

}
