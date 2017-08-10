/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.memory.UnsafeUtil.unsafe;
import static com.yahoo.sketches.hll.HllUtil.AUX_TOKEN;
import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static com.yahoo.sketches.hll.HllUtil.hiNibbleMask;
import static com.yahoo.sketches.hll.HllUtil.loNibbleMask;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
class DirectHll4Array extends DirectHllArray {

  //Called by HllSketch.writableWrap()
  DirectHll4Array(final int lgConfigK, final WritableMemory wmem) {
    super(lgConfigK, TgtHllType.HLL_4, wmem);
  }

  //Called by HllSketch.wrap(Memory)
  DirectHll4Array(final int lgConfigK, final Memory mem) {
    super(lgConfigK, TgtHllType.HLL_4, mem);
  }



  @Override
  HllSketchImpl copy() {
    return Hll4Array.heapify(mem);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    final int newValue = HllUtil.getValue(coupon);
    if (newValue <= getCurMin()) {
      return this; // super quick rejection; only works for large N
    }
    final int configKmask = (1 << getLgConfigK()) - 1;
    final int slotNo = HllUtil.getLow26(coupon) & configKmask;
    internalUpdate(slotNo, newValue);
    return this;
  }

  @Override
  int getHllByteArrBytes() {
    return 1 << (extractLgK(memObj, memAdd) - 1);
  }

  final int getNibble(final int slotNo) {
    final long unsafeOffset = memAdd + HLL_BYTE_ARR_START + (slotNo >>> 1);
    int theByte = unsafe.getByte(memObj, unsafeOffset);
    if ((slotNo & 1) > 0) { //odd?
      theByte >>>= 4;
    }
    return theByte & loNibbleMask;
  }

  final void setNibble(final int slotNo , final int newValue) {
    final long unsafeOffset = memAdd + HLL_BYTE_ARR_START + (slotNo >>> 1);
    final int oldValue = unsafe.getByte(memObj, unsafeOffset);
    final byte value = ((slotNo & 1) == 0) //even?
        ? (byte) ((oldValue & hiNibbleMask) | (newValue & loNibbleMask)) //set low nibble
        : (byte) ((oldValue & loNibbleMask) | ((newValue << 4) & hiNibbleMask)); //set high nibble
    unsafe.putByte(memObj, unsafeOffset, value);
  }

  @Override
  PairIterator getIterator() {
    return new DirectHll4Iterator(mem, HLL_BYTE_ARR_START, 1 << lgConfigK);
  }

  private void internalUpdate(final int slotNo, final int newValue) {
    assert ((0 <= slotNo) && (slotNo < (1 << getLgConfigK())));
    assert (newValue > 0);
    final int lgConfigK = getLgConfigK();
    final int curMin = getCurMin();

    AuxHashMap auxHashMap = getAuxHashMap(); //may be null
    final int rawStoredOldValue = getNibble(slotNo);  //could be 0
    //This is provably a LB:
    final int lbOnOldValue =  rawStoredOldValue + curMin; //lower bound, could be 0

    if (newValue > lbOnOldValue) { //842: newValue <= lbOnOldValue -> return no need to update array
      final int actualOldValue = (rawStoredOldValue < AUX_TOKEN)
          ? lbOnOldValue
          : auxHashMap.mustFindValueFor(slotNo); //846 rawStoredOldValue == AUX_TOKEN

      if (newValue > actualOldValue) { //848: actualOldValue could still be 0; newValue > 0

        //We know that the array will be changed
        hipAndKxQIncrementalUpdate(actualOldValue, newValue); //haven't actually updated yet

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
            setNibble(slotNo, AUX_TOKEN);
            if (auxHashMap == null) {
              auxHashMap = new HeapAuxHashMap(LG_AUX_ARR_INTS[lgConfigK], lgConfigK);
              putAuxHashMap(auxHashMap);
            }
            auxHashMap.mustAdd(slotNo, newValue);
          }
          else {                             // CASE 4: //897
            //This is the case where neither the old value nor the new value is an exception.
            //Therefore we just overwrite the 4-bit array with the shifted new value.
            setNibble(slotNo, shiftedNewValue);
          }
        }

        // we just increased a pair value, so it might be time to change curMin
        if (actualOldValue == curMin) { //908
          assert (getNumAtCurMin() >= 1);
          decNumAtCurMin();
          while (getNumAtCurMin() == 0) {
            shiftToBiggerCurMin(); //increases curMin by 1, and builds a new aux table,
            //shifts values in 4-bit table, and recounts curMin
          }
        }
      } //end newValue <= actualOldValue
    } //end newValue <= lbOnOldValue
  }

  //This scheme only works with two double registers (2 kxq values).
  //  HipAccum, kxq0 and kxq1 remain untouched.
  //  This changes curMin, numAtCurMin, hllByteArr and auxMap.
  //Entering this routine assumes that all slots have valid values > 0 and <= 15.
  //An AuxHashMap must exist if any values in the current hllByteArray are already 15.
  //In C: again-two-registers.c Lines 710 "hhb_shift_to_bigger_curmin"
  private void shiftToBiggerCurMin() {
    final int oldCurMin = getCurMin();
    final int newCurMin = oldCurMin + 1;
    final int lgConfigK = getLgConfigK();
    final int configK = 1 << lgConfigK;
    final int configKmask = configK - 1;

    int numAtNewCurMin = 0;
    int numAuxTokens = 0;

    // Walk through the slots of 4-bit array decrementing stored values by one unless it
    // equals AUX_TOKEN, where it is left alone but counted to be checked later.
    // If oldStoredValue is 0 it is an error.
    // If the decremented value is 0, we increment numAtNewCurMin.
    // Because getNibble is masked to 4 bits oldStoredValue can never be > 15 or negative
    for (int i = 0; i < configK; i++) { //724
      int oldStoredValue = getNibble(i);
      if (oldStoredValue == 0) {
        throw new SketchesStateException("Array slots cannot be 0 at this point.");
      }
      if (oldStoredValue < AUX_TOKEN) {
        setNibble(i, --oldStoredValue);
        if (oldStoredValue == 0) { numAtNewCurMin++; }
      } else { //oldStoredValue == AUX_TOKEN
        numAuxTokens++;
        assert getAuxHashMap() != null : "AuxHashMap cannot be null at this point.";
      }
    }

    //If old AuxHashMap exists, walk through it updating some slots and build a new AuxHashMap
    // if needed.
    HeapAuxHashMap newAuxMap = null;
    final AuxHashMap oldAuxMap = getAuxHashMap();
    if (oldAuxMap != null) {
      int slotNum;
      int oldActualVal;
      int newShiftedVal;

      final PairIterator itr = oldAuxMap.getIterator();
      while (itr.nextValid()) {
        slotNum = itr.getKey() & configKmask;
        oldActualVal = itr.getValue();
        newShiftedVal = oldActualVal - newCurMin;
        assert newShiftedVal >= 0;

        assert getNibble(slotNum) == AUX_TOKEN
            : "Array slot != AUX_TOKEN: " + getNibble(slotNum);
        if (newShiftedVal < AUX_TOKEN) { //756
          assert (newShiftedVal == 14);
          // The former exception value isn't one anymore, so it stays out of new AuxHashMap.
          // Correct the AUX_TOKEN value in the HLL array to the newShiftedVal (14).
          setNibble(slotNum, newShiftedVal);
          numAuxTokens--;
        }
        else { //newShiftedVal >= AUX_TOKEN
          // the former exception remains an exception, so must be added to the newAuxMap
          if (newAuxMap == null) {
            newAuxMap = new HeapAuxHashMap(LG_AUX_ARR_INTS[lgConfigK], lgConfigK);
          }
          newAuxMap.mustAdd(slotNum, oldActualVal);
        }
      } //end scan of oldAuxMap
    } //end if (auxHashMap != null)
    else { //oldAuxMap == null
      assert numAuxTokens == 0;
    }

    if (newAuxMap != null) {
      assert newAuxMap.getAuxCount() == numAuxTokens;
    }
    putAuxHashMap(newAuxMap);

    putCurMin(newCurMin);
    putNumAtCurMin(numAtNewCurMin);
  } //end of shiftToBiggerCurMin

  //ITERATOR
  final class DirectHll4Iterator extends HllMemoryPairIterator {

    DirectHll4Iterator(final Memory mem, final long offsetBytes, final int lengthPairs) {
      super(mem, offsetBytes, lengthPairs);
    }

    @Override
    int value() {
      final int nib = getNibble(index);
      return (nib == AUX_TOKEN)
          ? directAuxHashMap.mustFindValueFor(index) //directAuxHashMap cannot be null here
          : nib + getCurMin();
    }
  }

}
