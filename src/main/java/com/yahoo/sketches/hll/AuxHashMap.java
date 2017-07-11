/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_BITS_26;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_DENOM;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_NUMER;
import static com.yahoo.sketches.hll.PreambleUtil.extractInt;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class AuxHashMap {

  final int lgConfigK; //required for #slot bits
  int lgAuxArrSize;
  int auxCount;
  int[] auxIntArr; //used by Hll4Array

  /**
   * Standard constructor
   * @param lgConfigK must be 7 to 21
   */
  AuxHashMap(final int lgAuxArrSize, final int lgConfigK) {
    this.lgConfigK = lgConfigK;
    this.lgAuxArrSize = lgAuxArrSize;
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

  static final AuxHashMap heapify(final Memory mem, final long offset, final int lgConfigK,
      final int auxCount, final boolean compact) {
    final int auxArrInts =
        Math.max(4, ceilingPowerOf2((auxCount * RESIZE_DENOM) / RESIZE_NUMER));
    final int lgAuxArrInts = Util.simpleIntLog2(auxArrInts);
    final AuxHashMap auxMap = new AuxHashMap(lgAuxArrInts, lgConfigK);
    final Object memArr = ((WritableMemory) mem).getArray();
    final long memAdd = mem.getCumulativeOffset(0);
    final int configKmask = (1 << lgConfigK) - 1;

    if (compact) {
      for (int i = 0; i < auxCount; i++) {
        final int pair = extractInt(memArr, memAdd, offset + (i << 2));
        final int slotNo = BaseHllSketch.getLow26(pair) & configKmask;
        final int value = BaseHllSketch.getValue(pair);
        auxMap.mustAdd(slotNo, value);
      }
    } else { //updatable
      for (int i = 0; i < auxArrInts; i++) {
        final int pair = extractInt(memArr, memAdd, offset + (i << 2));
        if (pair == EMPTY) { continue; }
        final int slotNo = BaseHllSketch.getLow26(pair) & configKmask;
        final int value = BaseHllSketch.getValue(pair);
        auxMap.mustAdd(slotNo, value);
      }
    }
    return auxMap;
  }

  /**
   * Returns value given slotNo. If this fails an exception is thrown.
   * @param slotNo the index from the HLL array
   * @return value the HLL value at the slotNo
   * @throws SketchesStateException if valid slotNo and value is not found.
   */
  //In C: two-registers.c Line 205
  int mustFindValueFor(final int slotNo) {
    final int index = find(auxIntArr, lgAuxArrSize, lgConfigK, slotNo);
    if (index >= 0) {
      return BaseHllSketch.getValue(auxIntArr[index]);
    }
    throw new SketchesStateException("SlotNo not found: " + slotNo);
  }

  PairIterator getIterator() {
    return new AuxIterator();
  }

  int getCompactedSizeBytes() {
    return auxCount << 2;
  }

  int getUpdatableSizeBytes() {
    return 4 << lgAuxArrSize;
  }

  byte[] toCompactByteArray() {
    final byte[] out = new byte[auxCount << 2];
    final WritableMemory wmem = WritableMemory.wrap(out);
    final PairIterator itr = getIterator();
    int cnt = 0;
    while (itr.nextValid()) {
      wmem.putInt(cnt++ << 2, itr.getPair());
    }
    return out;
  }

  byte[] toUpdatableByteArray() {
    final byte[] out = new byte[4 << lgAuxArrSize];
    final WritableMemory wmem = WritableMemory.wrap(out);
    final PairIterator itr = getIterator();
    int cnt = 0;
    while (itr.nextAll()) {
      wmem.putInt(cnt++ << 2, itr.getPair());
    }
    return out;
  }

  /**
   * Replaces the entry at slotNo with the given value.
   * @param slotNo the index from the HLL array
   * @param value the HLL value at the slotNo
   * @throws SketchesStateException if a valid slotNo, value is not found.
   */
  //In C: two-registers.c Line 321.
  void mustReplace(final int slotNo, final int value) {
    final int idx = find(auxIntArr, lgAuxArrSize, lgConfigK, slotNo);
    if (idx >= 0) {
      auxIntArr[idx] = pair(slotNo, value); //replace
      return;
    }
    final String pairStr = pairString(pair(slotNo, value));
    throw new SketchesStateException("Pair not found: " + pairStr);
  }

  /**
   * Adds the slotNo and value to the aux array.
   * @param slotNo the index from the HLL array
   * @param value the HLL value at the slotNo.
   * @throws SketchesStateException if this slotNo already exists in the aux array.
   */
  //In C: two-registers.c Line 300.
  void mustAdd(final int slotNo, final int value) {
    final int index = find(auxIntArr, lgAuxArrSize, lgConfigK, slotNo);
    if (index >= 0) {
      final String pairStr = pairString(pair(slotNo, value));
      throw new SketchesStateException("Found a slotNo that should not be there: " + pairStr);
    }
      //Found empty entry
      auxIntArr[~index] = pair(slotNo, value);
      auxCount++;
      checkGrow();
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

  //Searches the Aux arr hash table
  //If entry is empty, returns one's complement of aux index.
  //If entry contains given slotNo, returns its aux array index.
  //Else throws an exception.
  private static final int find(final int[] auxArr, final int lgAuxInts, final int lgConfigK,
      final int slotNo) {
    assert lgAuxInts < lgConfigK;
    final int auxInts = 1 << lgAuxInts;
    final int auxArrMask = auxInts - 1;
    final int configKmask = (1 << lgConfigK) - 1;
    int probe = slotNo & auxArrMask;
    final int loopIndex = probe;
    do {
      if (auxArr[probe] == EMPTY) {
        return ~probe; //empty
      }
      else if (slotNo == (auxArr[probe] & configKmask)) { //found given slotNo
        return probe; //return aux array index
      }
      final int stride = (slotNo >>> lgAuxInts) | 1;
      probe = (probe + stride) & auxArrMask;
    } while (probe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

  private void checkGrow() {
    if ((RESIZE_DENOM * auxCount) > (RESIZE_NUMER * auxIntArr.length)) {
      growAuxSpace();
      //TODO if direct, ask for more memory
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
        auxIntArr[~idx] = fetched;
      }
    }
  }

  //Pairs
  private static final int pair(final int slotNo, final int value) {
    return (value << KEY_BITS_26) | (slotNo & KEY_MASK_26);
  }

  //used for thrown exceptions
  private static final String pairString(final int pair) {
    return "SlotNo: " + BaseHllSketch.getLow26(pair) + ", Value: "
        + BaseHllSketch.getValue(pair);
  }
  //End Basic Pairs

}
