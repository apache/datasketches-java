/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_BITS_26;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARRAY_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractAuxCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;
import static com.yahoo.sketches.hll.PreambleUtil.insertAuxCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertInt;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesStateException;

/**
 * Uses 4 bits per slot in a packed byte array.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class Hll4Array extends HllArray {
  private static final int loNibbleMask = 0x0f;
  private static final int hiNibbleMask = 0xf0;
  private static final int AUX_TOKEN = 0xf;
  /**
   * Log2 table sizes for exceptions based on lgK from 0 to 26.
   * However, only lgK from 7 to 21 are used.
   */
  private static final int[] LG_AUX_SIZE = new int[] {
    0, 2, 2, 2, 2, 2, 2, 3, 3, 3,   //0 - 9
    4, 4, 5, 5, 6, 7, 8, 9, 10, 11, //10 - 19
    12, 13, 14, 15, 16, 17, 18      //20 - 26
  };


  /**
   * Standard constructor
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
    hll4Array.extractCommon(hll4Array, mem, memArr, memAdd);

    //load AuxHashMap
    final int offset = HLL_BYTE_ARRAY_START + hll4Array.hllByteArr.length;
    final int auxCount = extractAuxCount(memArr, memAdd);
    final boolean compact = extractCompactFlag(memArr, memAdd);
    hll4Array.auxHashMap = (auxCount == 0)
        ? null
        : AuxHashMap.heapify(mem, offset, lgConfigK, auxCount, compact);
    return hll4Array;
  }

  @Override
  Hll4Array copy() {
    return new Hll4Array(this);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    final int newValue = BaseHllSketch.getValue(coupon);
    if (newValue <= curMin) {
      return this; // super quick rejection; only works for large N
    }
    final int configKmask = (1 << lgConfigK) - 1;
    final int slotNo = BaseHllSketch.getLow26(coupon) & configKmask;
    internalUpdate(slotNo, newValue);
    return this;
  }

  static final int getExpectedLgAuxInts(final int lgConfigK) {
    return LG_AUX_SIZE[lgConfigK];
  }

  @Override
  PairIterator getIterator() {
    return new Hll4Iterator();
  }

  @Override
  byte[] toByteArray(final boolean compact) {
    final int hllBytes = hllByteArr.length;
    final int auxBytes = (auxHashMap == null) //HLL_4
        ? 0
        : (compact) ? auxHashMap.getCompactedSizeBytes() : auxHashMap.getUpdatableSizeBytes();
    final int totBytes = HLL_BYTE_ARRAY_START + hllBytes + auxBytes; //HLL_4
    final byte[] memArr = new byte[totBytes];
    final WritableMemory wmem = WritableMemory.wrap(memArr);
    final long memAdd = wmem.getCumulativeOffset(0);
    insertCommon(memArr, memAdd, compact);
    wmem.putByteArray(HLL_BYTE_ARRAY_START, hllByteArr, 0, hllBytes);

    //AuxHashMap
    final int auxCount = (auxHashMap != null) ? auxHashMap.auxCount : 0;
    insertAuxCount(memArr, memAdd, auxCount);

    if (auxCount > 0) {
      if (compact) {
        final PairIterator itr = auxHashMap.getIterator();
        int cnt = 0;
        final long auxStart = HLL_BYTE_ARRAY_START + hllBytes;
        while (itr.nextValid()) {
          insertInt(memArr, memAdd, auxStart + (cnt++ << 2), itr.getPair());
        }
        assert cnt == auxCount;
      } else { //updatable
        final int[] arr = auxHashMap.auxIntArr;
        final int len = auxHashMap.auxIntArr.length;
        wmem.putIntArray(HLL_BYTE_ARRAY_START + hllBytes, arr, 0, len);
      }
    }
    return memArr;
  }

  //Iterator

  final class Hll4Iterator implements PairIterator {
    int slots;
    int slotNum;

    Hll4Iterator() {
      slots = hllByteArr.length << 1; //X2
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
      final int nib = getNibble(hllByteArr, slotNum);
      if (nib == AUX_TOKEN) {
        return auxHashMap.mustFindValueFor(slotNum); //auxHashMap cannot be null here
      }
      return nib + curMin;
    }

    @Override
    public int getIndex() {
      return slotNum;
    }
  }

  static final int getNibble(final byte[] array, final int slotNo) {
    int theByte = array[slotNo >> 1];
    if ((slotNo & 1) > 0) { //odd?
      theByte >>>= 4;
    }
    return theByte & loNibbleMask;
  }

  static final void setNibble(final byte[] array, final int slotNo , final int newValue) {
    final int byteno = slotNo >> 1;
    final int oldValue = array[byteno];
    if ((slotNo & 1) == 0) { // set low nibble
      array[byteno] = (byte) ((oldValue & hiNibbleMask) | (newValue & loNibbleMask));
    } else { //set high nibble
      array[byteno] = (byte) ((oldValue & loNibbleMask) | ((newValue << 4) & hiNibbleMask));
    }
  }

  // in C: two-registers.c Line 836 in "hhb_abstract_set_slot_if_new_value_bigger" non-sparse
  //Uses lgConfigK, curMin, numAtCurMin, auxMap,
  private void internalUpdate(final int slotNo, final int newValue) {
    assert ((0 <= slotNo) && (slotNo < (1 << lgConfigK)));
    assert (newValue > 0);
    final int rawStoredOldValue = getNibble(hllByteArr, slotNo);  //could be 0
    //This is provably a LB:
    final int lbOnOldValue =  rawStoredOldValue + curMin; //lower bound, could be 0

    if (newValue > lbOnOldValue) { //842 //newValue <= lbOnOldValue -> return no need to update array
      final int actualOldValue = (rawStoredOldValue < AUX_TOKEN)
          ? lbOnOldValue
          : auxHashMap.mustFindValueFor(slotNo); //846 rawStoredOldValue == AUX_TOKEN

      if (newValue > actualOldValue) { //848 //actualOldValue could still be 0; newValue > 0

        //We know that the array will be changed
        hipAndKxQIncrementalUpdate(actualOldValue, newValue); //haven't actually updated hllByteArr yet

        assert (newValue >= curMin)
          : "New value " + newValue + " is less than current minimum " + curMin;

        //newValue >= curMin

        final int shiftedNewValue = newValue - curMin; //874
        assert (shiftedNewValue >= 0);

        if (rawStoredOldValue == AUX_TOKEN) { //879

          //Given that we have an AUX_TOKEN, there are four cases for how to
          //  actually modify the data structure

          if (shiftedNewValue >= AUX_TOKEN) { //CASE 1: //881
            //the byte array already contains aux token
            //This is the case where old and new values are both exceptions.
            //Therefore, the 4-bit array already is AUX_TOKEN. Only need to update auxMap
            auxHashMap.mustReplace(slotNo, newValue);
          }
          else {                              //CASE 2: //885
            //This is the (hypothetical) case where old value is an exception and the new one is not.
            // which is impossible given that curMin has not changed here and the newValue > oldValue.
            throw new SketchesStateException("Impossible case");
          }
        }

        else { //rawStoredOldValue != AUX_TOKEN

          if (shiftedNewValue >= AUX_TOKEN) { //CASE 3: //892
            //This is the case where the old value is not an exception and the new value is.
            //Therefore the AUX_TOKEN must be stored in the 4-bit array and the new value
            // added to the exception table.
            setNibble(hllByteArr, slotNo, AUX_TOKEN);
            if (auxHashMap == null) {
              auxHashMap = new AuxHashMap(LG_AUX_SIZE[lgConfigK], lgConfigK);
            }
            auxHashMap.mustAdd(slotNo, newValue);
          }
          else {                             // CASE 4: //897
            //This is the case where neither the old value nor the new value is an exception.
            //Therefore we just overwrite the 4-bit array with the shifted new value.
            setNibble(hllByteArr, slotNo, shiftedNewValue);
          }
        }

        // we just increased a pair value, so it might be time to change curMin
        if (actualOldValue == curMin) { //908
          assert (numAtCurMin >= 1);
          numAtCurMin--;
          while (numAtCurMin == 0) {
            shiftToBiggerCurMin(); //increases curMin by 1, and builds a new aux table,
            //shifts values in 4-bit table, and recounts curMin
          }
        }
      } //end newValue <= actualOldValue
    } //end newValue <= lbOnOldValue
  }

  //This scheme only works with two registers (2 kxq values).
  //  Both hipAccum and kxq0 and kxq1 remain untouched.
  //  Changes curMin, numAtCurMin, hllByteArr and auxMap
  //In C: two-registers.c Lines 710 "hhb_shift_to_bigger_curmin"
  private void shiftToBiggerCurMin() {
    final int oldCurMin = curMin;
    final int newCurMin = oldCurMin + 1;
    final int configK = 1 << lgConfigK;
    final int configKmask = configK - 1;
    int numAtNewCurMin = 0;
    assert numAtCurMin == 0;

    // walk through the slots decrementing stored values of 4-bit array
    for (int i = 0; i < configK; i++) { //723
      final int oldStoredValue = getNibble(hllByteArr, i);
      if (oldStoredValue < 1) {
        throw new SketchesStateException("Bug 1: oldStoredValue < 1");
      }
      if (oldStoredValue < AUX_TOKEN) {
        final int newStoredValue = oldStoredValue - 1;
        if (newStoredValue == 0) {
          numAtNewCurMin++;
        }
        setNibble(hllByteArr, i, newStoredValue);
      }

      else if (oldStoredValue > AUX_TOKEN) {
        throw new SketchesStateException("Bug 2: oldStoredValue > AUX_TOKEN");
      }
      // else oldStoredValue == AUX_TOKEN, we leave it in place
    }

    // if old AuxHashMap exists, walk through it updating some slots and
    //   building new AuxHashMap if needed.
    AuxHashMap newAuxMap = null;
    if (auxHashMap != null ) {
      final int[] oldAuxArr = auxHashMap.auxIntArr;
      final int oldAuxArrLen = auxHashMap.auxIntArr.length;

      for (int i = 0; i < oldAuxArrLen; i++) {
        final int oldPair = oldAuxArr[i];
        if (oldPair == 0) { continue; } //not a valid pair
        final int slotNo = BaseHllSketch.getLow26(oldPair) & configKmask;
        final int oldActualValue = BaseHllSketch.getValue(oldPair);
        final int newShiftedValue = oldActualValue - newCurMin;

        assert newShiftedValue >= 0;

        assert getNibble(hllByteArr, slotNo) == AUX_TOKEN : "Nibble: " + getNibble(hllByteArr, slotNo);

        if (newShiftedValue < AUX_TOKEN) { //756
          assert (newShiftedValue == 14);
          // the former exception value isn't one anymore, so it stays out of new AuxHashMap;
          // so set it in the HLL array.
          setNibble(hllByteArr, slotNo, newShiftedValue);
          // the following actually cannot happen since new shifted value == 14
          //   if (shifted_value == 0) {numAtNewCurmin += 1;}
        }
        else { //newShiftedValue >= AUX_TOKEN
          // we just verified this above
          // setNibble(hllByteArr, slotNo, AUX_TOKEN);
          if (newAuxMap == null) {
            newAuxMap = new AuxHashMap(LG_AUX_SIZE[lgConfigK], lgConfigK);
          }
          newAuxMap.mustAdd(slotNo, oldActualValue);
        }
      } //end for
    } //end if (auxHashMap != null)
    auxHashMap = newAuxMap;

    curMin = newCurMin;
    numAtCurMin = numAtNewCurMin;
  }

  static final Hll4Array convertToHll4(final HllArray srcHllArr) {
    final int lgConfigK = srcHllArr.getLgConfigK();
    final Hll4Array hll4Array = new Hll4Array(lgConfigK);
    hll4Array.putOooFlag(srcHllArr.getOooFlag());
    final int[] hist = new int[64];

    PairIterator itr = srcHllArr.getIterator();
    while (itr.nextAll()) {
      hist[itr.getValue()]++;
    }
    int curMin = 0;
    while (hist[curMin] == 0) {
      curMin++;
    }
    final int numAtCurMin = hist[curMin];
    itr = srcHllArr.getIterator();
    while (itr.nextValid()) {
      final int slotNo = itr.getIndex();
      final int actualValue = itr.getValue();
      hll4Array.hipAndKxQIncrementalUpdate(0, actualValue);
      if (actualValue >= (curMin + 15)) {
        Hll4Array.setNibble(hll4Array.hllByteArr, slotNo, AUX_TOKEN);
        if (hll4Array.auxHashMap == null) {
          hll4Array.auxHashMap = new AuxHashMap(LG_AUX_SIZE[lgConfigK], lgConfigK);
        }
        hll4Array.auxHashMap.mustAdd(slotNo, actualValue);
      } else {
        Hll4Array.setNibble(hll4Array.hllByteArr, slotNo, actualValue - curMin);
      }
    }
    hll4Array.curMin = curMin;
    hll4Array.numAtCurMin = numAtCurMin;
    hll4Array.putHipAccum(srcHllArr.getHipAccum());
    return hll4Array;
  }

}
