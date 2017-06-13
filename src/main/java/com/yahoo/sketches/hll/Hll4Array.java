/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.KEY_BITS_26;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARRAY_START;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.extractAuxCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;
import static com.yahoo.sketches.hll.PreambleUtil.extractNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertAuxCount;
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
import com.yahoo.sketches.SketchesStateException;

/**
 * Uses 4 bits per slot in a packed byte array.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class Hll4Array extends HllArray {
  private static final int loNibbleMask = 0x0f;
  private static final int hiNibbleMask = 0xf0;

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

    auxHashMap = new AuxHashMap(LG_AUX_SIZE[lgConfigK], lgConfigK);
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
    checkPreamble(mem, memArr, memAdd);
    final int lgConfigK = extractLgK(memArr, memAdd);
    final Hll4Array hll4Array = new Hll4Array(lgConfigK);

    hll4Array.oooFlag = extractOooFlag(memArr, memAdd);
    hll4Array.curMin = extractCurMin(memArr, memAdd);
    hll4Array.hipAccum = extractHipAccum(memArr, memAdd);
    hll4Array.kxq0 = extractKxQ0(memArr, memAdd);
    hll4Array.kxq1 = extractKxQ1(memArr, memAdd);
    hll4Array.numAtCurMin = extractNumAtCurMin(memArr, memAdd);

    //load Hll array
    final int hllArrLen = hll4Array.hllByteArr.length;
    mem.getByteArray(HLL_BYTE_ARRAY_START, hll4Array.hllByteArr, 0, hllArrLen);

    //load AuxHashMap
    final int offset = HLL_BYTE_ARRAY_START + hllArrLen;
    final int auxCount = extractAuxCount(memArr, memAdd);
    hll4Array.auxHashMap = AuxHashMap.heapify(mem, offset, lgConfigK, auxCount);
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
  int getCurrentSerializationBytes() {
    return HLL_BYTE_ARRAY_START + (1 << (lgConfigK - 1)) + ((auxHashMap.auxCount) << 2);
  }

  @Override
  PairIterator getIterator() {
    return new Hll4Iterator();
  }

  @Override
  byte[] toCompactByteArray() {
    final int hllBytes = hllByteArr.length;
    final int auxBytes = auxHashMap.getCompactedSizeBytes();            //Hll4
    final int totBytes = HLL_BYTE_ARRAY_START + hllBytes + auxBytes;    //Hll4
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

    //remainder only for Hll4
    final int auxCount = auxHashMap.auxCount;
    insertAuxCount(memArr, memAdd, auxCount);
    if (auxCount > 0) {
      final int auxStart = HLL_BYTE_ARRAY_START + hllBytes;
      wmem.putByteArray(auxStart, auxHashMap.toByteArray(), 0, auxCount << 2);
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
        return auxHashMap.mustFindValueFor(slotNum);
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

    // walk through old AuxMap updating some slots and building new AuxMap
    final AuxHashMap newAuxMap = new AuxHashMap(LG_AUX_SIZE[lgConfigK], lgConfigK);
    final int[] auxArr = auxHashMap.auxIntArr;
    final int auxArrLen = auxHashMap.auxIntArr.length;

    for (int i = 0; i < auxArrLen; i++) {
      final int pair = auxArr[i];
      if (pair == 0) { continue; } //not a valid pair
      final int slotNo = BaseHllSketch.getLow26(pair) & configKmask;
      final int actualValue = BaseHllSketch.getValue(pair);
      final int shiftedValue = actualValue - newCurMin;

      assert shiftedValue >= 0;

      assert getNibble(hllByteArr, slotNo) == AUX_TOKEN : "Nibble: " + getNibble(hllByteArr, slotNo);

      if (shiftedValue < AUX_TOKEN) { //756
        assert (shiftedValue == 14);
        // this former exception value isn't one anymore, so it stays out of new AuxMap
        setNibble(hllByteArr, slotNo, shiftedValue);
        // the following actually cannot happen since shifted value == 14
        // if (shifted_value == 0) {numAtNewCurmin += 1;}
      }

      else {
        // we just verified this above
        // setNibble(hllByteArr, slotNo, AUX_TOKEN);
        newAuxMap.mustAdd(slotNo, actualValue);
      }
    } //end for
    auxHashMap = newAuxMap;
    curMin = newCurMin;
    numAtCurMin = numAtNewCurMin;
  }

}
