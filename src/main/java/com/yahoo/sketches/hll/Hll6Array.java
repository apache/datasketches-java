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
 * Uses 6 bits per slot in a packed byte array.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class Hll6Array extends HllArray {
  final WritableMemory mem;

  /**
   * Standard constructor
   * @param lgConfigK the configured Lg K
   */
  Hll6Array(final int lgConfigK) {
    super(lgConfigK, TgtHllType.HLL_6);
    hllByteArr = new byte[byteArrBytes(lgConfigK)];
    mem = WritableMemory.wrap(hllByteArr);
  }

  /**
   * Copy constructor
   * @param that another Hll6Array
   */
  Hll6Array(final Hll6Array that) {
    super(that);
    mem = WritableMemory.wrap(hllByteArr); //hllByteArr already cloned.
  }

  static final Hll6Array heapify(final Memory mem) {
    final Object memArr = ((WritableMemory) mem).getArray();
    final long memAdd = mem.getCumulativeOffset(0);
    checkPreamble(mem, memArr, memAdd);
    final int lgConfigK = extractLgK(memArr, memAdd);
    final Hll6Array hll6Array = new Hll6Array(lgConfigK);

    hll6Array.oooFlag = extractOooFlag(memArr, memAdd);
    hll6Array.curMin = extractCurMin(memArr, memAdd);
    hll6Array.hipAccum = extractHipAccum(memArr, memAdd);
    hll6Array.kxq0 = extractKxQ0(memArr, memAdd);
    hll6Array.kxq1 = extractKxQ1(memArr, memAdd);
    hll6Array.numAtCurMin = extractNumAtCurMin(memArr, memAdd);

    //load Hll array
    final int hllArrLen = hll6Array.hllByteArr.length;
    mem.getByteArray(HLL_BYTE_ARRAY_START, hll6Array.hllByteArr, 0, hllArrLen);

    return hll6Array;
  }

  @Override
  Hll6Array copy() {
    return new Hll6Array(this);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    final int configKmask = (1 << lgConfigK) - 1;
    final int slotNo = BaseHllSketch.getLow26(coupon) & configKmask;
    final int newVal = BaseHllSketch.getValue(coupon);
    assert newVal > 0;
    final int curVal = get6Bit(mem, 0, slotNo);
    if (newVal > curVal) {
      put6Bit(mem, 0, slotNo, newVal);
      hipAndKxQIncrementalUpdate(curVal, newVal);
      if (curVal == 0) { numAtCurMin--; } //overloaded as num zeros
      assert numAtCurMin >= 0;
    }
    return this;
  }

  @Override
  PairIterator getIterator() {
    return new Hll6Iterator();
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
    insertLgArr(memArr, memAdd, 0); //not used by HLL mode
    insertEmptyFlag(memArr, memAdd, isEmpty());
    insertCompactFlag(memArr, memAdd, true); //
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

  @Override
  byte[] toUpdatableByteArray() {
    return toCompactByteArray(); //For HLL_6, it is the same
  }

  static final void put6Bit(final WritableMemory mem, final long offsetBytes,
      final int slotNo, final int val) {
    final long idxAndShift = byteIdxAndShift(slotNo);
    final long byteIdx = idxAndShift & (0XFFFFFFFFL + offsetBytes);
    final int shift = (int)(idxAndShift >>> 32);
    final int valShifted = (val & 0X3F) << shift;
    final int curMasked = mem.getShort(byteIdx) & (~(VAL_MASK_6 << shift));
    final short insert = (short) (curMasked | valShifted);
    mem.putShort(byteIdx, insert);
  }

  static final int get6Bit(final Memory mem, final long offsetBytes, final int slotNo) {
    final long idxAndShift = byteIdxAndShift(slotNo);
    final long byteIdx = idxAndShift & (0XFFFFFFFFL + offsetBytes);
    final int shift = (int)(idxAndShift >>> 32);
    return (byte) ((mem.getShort(byteIdx) >> shift) & 0X3F);
  }

  private static final long byteIdxAndShift(final int slotIdx) {
    final long startBit = slotIdx * 6;
    //shift in upper 32, byte index in lower 32
    return (startBit >> 3) | ((startBit & 0X7L) << 32);
  }


  static final int byteArrBytes(final int lgConfigK) {
    final int numSlots = 1 << lgConfigK;
    return ((numSlots * 3) >> 2) + 1;
  }

  //Iterator

  final class Hll6Iterator implements PairIterator {
    byte[] array;
    int lengthBits;
    int slotNum;
    int bitOffset;

    Hll6Iterator() {
      array = hllByteArr;
      lengthBits = (1 << lgConfigK) * 6;
      assert lengthBits <= (array.length * 8);
      slotNum = -1;
      bitOffset = -6;
    }

    @Override
    public boolean nextValid() {
      slotNum++;
      bitOffset += 6;
      while (bitOffset < lengthBits) {
        if (getValue() != EMPTY) {
          return true;
        }
        slotNum++;
        bitOffset += 6;
      }
      return false;
    }

    @Override
    public boolean nextAll() {
      slotNum++;
      bitOffset += 6;
      return bitOffset < lengthBits;
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
      final int tmp = mem.getShort(bitOffset / 8);
      final int shift = (bitOffset % 8) & 0X7;
      return (tmp >>> shift) & VAL_MASK_6;
    }

    @Override
    public int getIndex() {
      return slotNum;
    }
  }

  static final Hll6Array convertToHll6(final HllArray srcHllArr) {
    final Hll6Array hll6Array = new Hll6Array(srcHllArr.getLgConfigK());
    hll6Array.putOooFlag(srcHllArr.getOooFlag());
    final PairIterator itr = srcHllArr.getIterator();
    while (itr.nextValid()) {
      hll6Array.couponUpdate(itr.getPair());
    }
    hll6Array.putHipAccum(srcHllArr.getHipAccum());
    return hll6Array;
  }

}
