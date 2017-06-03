/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.simpleIntLog2;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_BITS_26;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_DENOM;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_NUMER;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARRAY_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractAuxCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class AuxHashMap {

  /**
   * Log2 table sizes for exceptions based on lgK from 0 to 26.
   * However, only lgK from 7 to 21 are used.
   */
  private static final int[] LG_AUX_SIZE = new int[] {
    0, 2, 2, 2, 2, 2, 2, 3, 3, 3,   //0 - 9
    4, 4, 5, 5, 6, 7, 8, 9, 10, 11, //10 - 19
    12, 13, 14, 15, 16, 17, 18      //20 - 26
  };

  final int lgConfigK;
  int lgAuxArrSize;
  int auxCount;
  int[] auxIntArr; //used by Hll4Array

  /**
   * Standard constructor
   * @param lgConfigK must be 7 to 21
   */
  AuxHashMap(final int lgConfigK) {
    this.lgConfigK = lgConfigK;
    lgAuxArrSize = getExpectedLgAuxInts(lgConfigK);
    auxIntArr = new int[1 << lgAuxArrSize];
  }

  /**
   * Copy constructor
   * @param that another AuxHashMap
   */
  AuxHashMap(final AuxHashMap that) {
    lgConfigK = that.lgConfigK;
    lgAuxArrSize = that.lgAuxArrSize;
    auxCount = that.auxCount;
    auxIntArr = that.auxIntArr.clone();
  }

  AuxHashMap copy() {
    return new AuxHashMap(this);
  }

  static final AuxHashMap heapify(final Memory mem, final Object memArr, final long memAdd) {
    final int lgConfigK = extractLgK(memArr, memAdd);
    final int auxCount = extractAuxCount(memArr, memAdd);

    final AuxHashMap auxMap = new AuxHashMap(lgConfigK);
    final int expectedLgSize = getExpectedLgAuxInts(lgConfigK);
    final int expectedFill = ((1 << expectedLgSize) * 3) >> 2;
    if (auxCount > expectedFill) {
      final int reqFill = (int) ((auxCount << 2) / 3.0);
      final int newSize = ceilingPowerOf2(reqFill);
      auxMap.lgAuxArrSize = simpleIntLog2(newSize);
      auxMap.auxIntArr = new int[newSize];
    }
    final int hllArrLen = 1 << (lgConfigK - 1);
    final int auxStart = HLL_BYTE_ARRAY_START + hllArrLen;
    final int[] packedArr = new int[auxCount];
    final int configKmask = (1 << lgConfigK) - 1;
    mem.getIntArray(auxStart, packedArr, 0, auxCount);
    for (int i = 0; i < auxCount; i++) {
      final int pair = packedArr[i];
      final int slotNo = BaseHllSketch.getLow26(pair) & configKmask;
      final int value = BaseHllSketch.getValue(pair);
      auxMap.update(slotNo, value);
    }
    return auxMap;
  }

  /**
   * Returns value given slotNo. If this fails an exception is thrown.
   * @param slotNo the index from the HLL array
   * @return value the HLL value at the slotNo
   */
  int findValueFor(final int slotNo) { //was mustFind
    final int index = find(auxIntArr, lgAuxArrSize, lgConfigK, slotNo);
    if (index < 0) {
      throw new SketchesStateException("SlotNo not found: " + slotNo);
    }
    return BaseHllSketch.getValue(auxIntArr[index]);
  }

  PairIterator getIterator() {
    return new AuxIterator();
  }

  static final int getExpectedLgAuxInts(final int lgConfigK) {
    return LG_AUX_SIZE[lgConfigK];
  }

  int getCompactedSizeBytes() {
    return auxCount << 2;
  }

  byte[] toByteArray() {
    final byte[] out = new byte[auxCount << 2];
    final WritableMemory wmem = WritableMemory.wrap(out);
    final PairIterator itr = getIterator();
    int cnt = 0;
    while (itr.nextValid()) {
      wmem.putInt(cnt++ << 2, itr.getPair());
    }
    return out;
  }

  /**
   * Replaces the entry at slotNo with the given value
   * @param slotNo the index from the HLL array
   * @param value the HLL value at the slotNo
   */
  void replace(final int slotNo, final int value) { //was mustReplace
    final int idx = find(auxIntArr, lgAuxArrSize, lgConfigK, slotNo);
    if (auxIntArr[idx] == EMPTY) {
      final String pairStr = pairString(pair(slotNo, value));
      throw new SketchesStateException("Pair not found: " + pairStr);
    }
    auxIntArr[idx] = pair(slotNo, value);
  }

  /**
   * Adds the slotNo and value to the aux array. If this fails an exception is thrown.
   * @param slotNo the index from the HLL array
   * @param value the HLL value at the slotNo
   */
  void update(final int slotNo, final int value) { //was mustAdd
    final int idx = find(auxIntArr, lgAuxArrSize, lgConfigK, slotNo);
    if (idx < 0) {
      auxIntArr[~idx] = pair(slotNo, value);
      auxCount++;
      checkGrow();
      return;
    }
    final String pairStr = pairString(pair(slotNo, value));
    throw new SketchesStateException("Found a valid Aux pair that should not be there: " + pairStr);
  }

  final class AuxIterator implements PairIterator {
    final int len;
    int index;
    final int[] array;

    AuxIterator() {
      array = auxIntArr;
      len = array.length;

      index = - 1;
    }

    @Override
    public boolean nextValid() {
      while (++index < len) {
        if (array[index] != EMPTY) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean nextAll() {
      return ++index < len;
    }

    @Override
    public int getPair() {
      return array[index];
    }

    @Override
    public int getKey() {
      return BaseHllSketch.getLow26(array[index]);
    }

    @Override
    public int getValue() {
      return BaseHllSketch.getValue(array[index]);
    }

    @Override
    public int getIndex() {
      return index;
    }
  }

  //returns one's complement if slot is empty.
  private static final int find(final int[] array, final int lgArr, final int lgConfigK,
      final int slotNo) {
    final int auxArrMask = (1 << lgArr) - 1;
    final int configKmask = (1 << lgConfigK) - 1;
    int probe = slotNo & auxArrMask;
    final int loopIndex = probe;
    do {
      if (array[probe] == EMPTY) {
        return ~probe; //empty
      }
      else if (slotNo == (array[probe] & configKmask)) { return probe; }
      final int stride = (slotNo >>> lgArr) | 1;
      probe = (probe + stride) & auxArrMask;
    } while (probe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

  private void checkGrow() {
    if ((RESIZE_DENOM * auxCount) > (RESIZE_NUMER * auxIntArr.length)) {
      growAuxSpace();
      //TODO ask for more memory
    }
  }

  private void growAuxSpace() {
    final int[] oldArray = auxIntArr;
    final int size = 1 << ++lgAuxArrSize;
    final int configKmask = (1 << lgConfigK) - 1;
    auxIntArr = new int[size];
    for (int i = 0; i < oldArray.length; i++) {
      final int fetched = oldArray[i];
      if (fetched != EMPTY) {
        final int idx = find(auxIntArr, lgAuxArrSize, lgConfigK, fetched & configKmask);
        auxIntArr[idx] = fetched;
      }
    }
  }

  //Pairs
  private static final int pair(final int slotNo, final int value) {
    return (value << KEY_BITS_26) | (slotNo & KEY_MASK_26);
  }

  //used for thrown exceptions
  private static final String pairString(final int pair) {
    return "SlotNo: " + BaseHllSketch.getLow26(pair) + ", Value: " + BaseHllSketch.getValue(pair);
  }
  //End Basic Pairs

}
