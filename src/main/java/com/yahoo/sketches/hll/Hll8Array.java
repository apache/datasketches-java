/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_BITS_26;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static com.yahoo.sketches.hll.HllUtil.VAL_MASK_6;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARRAY_START;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.extractCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;
import static com.yahoo.sketches.hll.PreambleUtil.extractNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMode;
import static com.yahoo.sketches.hll.PreambleUtil.insertEmptyFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.insertHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgK;
import static com.yahoo.sketches.hll.PreambleUtil.insertNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.hll.PreambleUtil.insertTgtHllType;

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
    checkPreamble(mem, memArr, memAdd);
    final int lgConfigK = extractLgK(memArr, memAdd);
    final Hll8Array hll8Array = new Hll8Array(lgConfigK);

    hll8Array.oooFlag = extractOooFlag(memArr, memAdd);
    hll8Array.curMin = extractCurMin(memArr, memAdd);
    hll8Array.hipAccum = extractHipAccum(memArr, memAdd);
    hll8Array.kxq0 = extractKxQ0(memArr, memAdd);
    hll8Array.kxq1 = extractKxQ1(memArr, memAdd);
    hll8Array.numAtCurMin = extractNumAtCurMin(memArr, memAdd);

    //load Hll array
    final int hllArrLen = hll8Array.hllByteArr.length;
    mem.getByteArray(HLL_BYTE_ARRAY_START, hll8Array.hllByteArr, 0, hllArrLen);

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

  @Override
  byte[] toCompactByteArray() {
    final int hllBytes = hllByteArr.length;
    final int totBytes = HLL_BYTE_ARRAY_START + hllBytes;
    final byte[] memArr = new byte[totBytes];
    final WritableMemory wmem = WritableMemory.wrap(memArr);
    final long memAdd = wmem.getCumulativeOffset(0);

    insertPreInts(memArr, memAdd, HLL_PREINTS);
    insertSerVer(memArr, memAdd);
    insertFamilyId(memArr, memAdd);
    insertLgK(memArr, memAdd, lgConfigK);
    insertLgArr(memArr, memAdd, 0); //not used by HLL
    insertEmptyFlag(memArr, memAdd, isEmpty());
    insertCompactFlag(memArr, memAdd, true);
    insertOooFlag(memArr, memAdd, oooFlag);
    insertCurMin(memArr, memAdd, curMin);
    insertCurMode(memArr, memAdd, curMode);
    insertTgtHllType(memArr, memAdd, tgtHllType);
    insertHipAccum(memArr, memAdd, hipAccum);
    insertKxQ0(memArr, memAdd, kxq0);
    insertKxQ1(memArr, memAdd, kxq1);
    insertNumAtCurMin(memArr, memAdd, numAtCurMin);
    wmem.putByteArray(HLL_BYTE_ARRAY_START, hllByteArr, 0, hllBytes);
    return memArr;
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
