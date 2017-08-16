/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.invPow2;
import static com.yahoo.sketches.hll.HllUtil.AUX_TOKEN;
import static com.yahoo.sketches.hll.HllUtil.HLL_HIP_RSE_FACTOR;
import static com.yahoo.sketches.hll.HllUtil.HLL_NON_HIP_RSE_FACTOR;
import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
abstract class AbstractHllArray extends HllSketchImpl {

  AbstractHllArray(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode) {
    super(lgConfigK, tgtHllType, curMode);
  }

  abstract void addToHipAccum(double delta);

  abstract void decNumAtCurMin();

  abstract AuxHashMap getAuxHashMap();

  abstract PairIterator getAuxIterator();

  @Override
  int getCompactSerializationBytes() {
    final AuxHashMap auxHashMap = getAuxHashMap();
    final int auxBytes = (auxHashMap == null) ? 0 : auxHashMap.getAuxCount() << 2;
    return HLL_BYTE_ARR_START + getHllByteArrBytes() + auxBytes;
  }

  abstract int getCurMin();

  abstract double getHipAccum();

  abstract byte[] getHllByteArr();

  abstract int getHllByteArrBytes();

  //abstract void getHllBytesToMemory(WritableMemory dstWmem, int lenBytes);

  abstract double getKxQ0();

  abstract double getKxQ1();

  @Override
  int getMemArrStart() {
    return HLL_BYTE_ARR_START;
  }

  abstract Memory getMemory();

  abstract AuxHashMap getNewAuxHashMap();

  abstract int getNumAtCurMin();

  @Override
  int getPreInts() {
    return HLL_PREINTS;
  }

  @Override
  double getRelErr(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    final int lgConfigK = getLgConfigK();
    final boolean oooFlag = isOutOfOrderFlag();
    if (lgConfigK <= 12) {
      return RelativeErrorTables.getRelErr(true, oooFlag, lgConfigK, numStdDev);
    }
    final double rseFactor =  (oooFlag) ? HLL_NON_HIP_RSE_FACTOR : HLL_HIP_RSE_FACTOR;
    return (rseFactor * numStdDev) / Math.sqrt(1 << lgConfigK);
  }

  @Override
  double getRelErrFactor(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    final int lgConfigK = getLgConfigK();
    final boolean oooFlag = isOutOfOrderFlag();
    if (lgConfigK <= 12) {
      return RelativeErrorTables.getRelErr(true, oooFlag, lgConfigK, numStdDev)
          * Math.sqrt(1 << lgConfigK);
    }
    final double rseFactor =  (oooFlag) ? HLL_NON_HIP_RSE_FACTOR : HLL_HIP_RSE_FACTOR;
    return rseFactor * numStdDev;
  }

  abstract int getSlot(int slotNo);

  @Override
  int getUpdatableSerializationBytes() {
    final AuxHashMap auxHashMap = getAuxHashMap();
    final int auxBytes = (auxHashMap == null) ? 0 : 4 << auxHashMap.getLgAuxArrInts();
    return HLL_BYTE_ARR_START + getHllByteArrBytes() + auxBytes;
  }

  abstract void putAuxHashMap(AuxHashMap auxHashMap);

  abstract void putCurMin(int curMin);

  abstract void putHipAccum(double hipAccum);

  abstract void putKxQ0(double kxq0);

  abstract void putKxQ1(double kxq1);

  abstract void putNumAtCurMin(int numAtCurMin);

  abstract void putSlot(int slotNo, int value);

  static final int getExpectedLgAuxInts(final int lgConfigK) {
    return LG_AUX_ARR_INTS[lgConfigK];
  }

  static final int hll4ArrBytes(final int lgConfigK) {
    return 1 << (lgConfigK - 1);
  }

  static final int hll6ArrBytes(final int lgConfigK) {
    final int numSlots = 1 << lgConfigK;
    return ((numSlots * 3) >>> 2) + 1;
  }

  static final int hll8ArrBytes(final int lgConfigK) {
    return 1 << lgConfigK;
  }

  static final int curMinAndNum(final AbstractHllArray hllArr) {
    int curMin = 64;
    int numAtCurMin = 0;
    final PairIterator itr = hllArr.getIterator();
    while (itr.nextAll()) {
      final int v = itr.getValue();
      if (v < curMin) {
        curMin = v;
        numAtCurMin = 1;
      }
      if (v == curMin) {
        numAtCurMin++;
      }
    }
    return HllUtil.pair(numAtCurMin, curMin);
  }

  /**
   * HIP and KxQ incremental update.
   * @param oldValue old value
   * @param newValue new value
   */
  //In C: again-two-registers.c Lines 851 to 871
  static final void hipAndKxQIncrementalUpdate(final AbstractHllArray host, final int oldValue,
      final int newValue) {
    assert newValue > oldValue;
    final int configK = 1 << host.getLgConfigK();
    //update hipAccum BEFORE updating kxq0 and kxq1
    double kxq0 = host.getKxQ0();
    double kxq1 = host.getKxQ1();
    host.addToHipAccum(configK / (kxq0 + kxq1));
    //update kxq0 and kxq1; subtract first, then add.
    if (oldValue < 32) { host.putKxQ0(kxq0 -= invPow2(oldValue)); }
    else               { host.putKxQ1(kxq1 -= invPow2(oldValue)); }
    if (newValue < 32) { host.putKxQ0(kxq0 += invPow2(newValue)); }
    else               { host.putKxQ1(kxq1 += invPow2(newValue)); }
  }

  // in C: two-registers.c Line 836 in "hhb_abstract_set_slot_if_new_value_bigger" non-sparse
  //Uses lgConfigK, curMin, numAtCurMin, auxMap,
  static final void internalUpdate(final AbstractHllArray host, final int slotNo, final int newValue) {
    assert ((0 <= slotNo) && (slotNo < (1 << host.getLgConfigK())));
    assert (newValue > 0);
    final int curMin = host.getCurMin();

    AuxHashMap auxHashMap = host.getAuxHashMap(); //may be null
    final int rawStoredOldValue = host.getSlot(slotNo);  //could be 0
    //This is provably a LB:
    final int lbOnOldValue =  rawStoredOldValue + curMin; //lower bound, could be 0

    if (newValue > lbOnOldValue) { //842:
      final int actualOldValue = (rawStoredOldValue < AUX_TOKEN)
          ? lbOnOldValue
          : auxHashMap.mustFindValueFor(slotNo); //846 rawStoredOldValue == AUX_TOKEN

      if (newValue > actualOldValue) { //848: actualOldValue could still be 0; newValue > 0

        //We know that the array will be changed
        hipAndKxQIncrementalUpdate(host, actualOldValue, newValue); //haven't actually updated yet

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
            host.putSlot(slotNo, AUX_TOKEN);
            if (auxHashMap == null) {
              auxHashMap = host.getNewAuxHashMap();
              host.putAuxHashMap(auxHashMap);
            }
            auxHashMap.mustAdd(slotNo, newValue);
          }
          else {                             // CASE 4: //897
            //This is the case where neither the old value nor the new value is an exception.
            //Therefore we just overwrite the 4-bit array with the shifted new value.
            host.putSlot(slotNo, shiftedNewValue);
          }
        }

        // we just increased a pair value, so it might be time to change curMin
        if (actualOldValue == curMin) { //908
          assert (host.getNumAtCurMin() >= 1);
          host.decNumAtCurMin();
          while (host.getNumAtCurMin() == 0) {
            shiftToBiggerCurMin(host); //increases curMin by 1, and builds a new aux table,
            //shifts values in 4-bit table, and recounts curMin
          }
        }
      } //end newValue <= actualOldValue
    } //end newValue <= lbOnOldValue -> return, no need to update array
  }

  //This scheme only works with two double registers (2 kxq values).
  //  HipAccum, kxq0 and kxq1 remain untouched.
  //  This changes curMin, numAtCurMin, hllByteArr and auxMap.
  //Entering this routine assumes that all slots have valid values > 0 and <= 15.
  //An AuxHashMap must exist if any values in the current hllByteArray are already 15.
  //In C: again-two-registers.c Lines 710 "hhb_shift_to_bigger_curmin"
  static final void shiftToBiggerCurMin(final AbstractHllArray host) {
    final int oldCurMin = host.getCurMin();
    final int newCurMin = oldCurMin + 1;
    final int lgConfigK = host.getLgConfigK();
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
      int oldStoredValue = host.getSlot(i);
      if (oldStoredValue == 0) {
        throw new SketchesStateException("Array slots cannot be 0 at this point.");
      }
      if (oldStoredValue < AUX_TOKEN) {
        host.putSlot(i, --oldStoredValue);
        if (oldStoredValue == 0) { numAtNewCurMin++; }
      } else { //oldStoredValue == AUX_TOKEN
        numAuxTokens++;
        assert host.getAuxHashMap() != null : "AuxHashMap cannot be null at this point.";
      }
    }

    //If old AuxHashMap exists, walk through it updating some slots and build a new AuxHashMap
    // if needed.
    HeapAuxHashMap newAuxMap = null;
    final AuxHashMap oldAuxMap = host.getAuxHashMap();
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

        assert host.getSlot(slotNum) == AUX_TOKEN
            : "Array slot != AUX_TOKEN: " + host.getSlot(slotNum);
        if (newShiftedVal < AUX_TOKEN) { //756
          assert (newShiftedVal == 14);
          // The former exception value isn't one anymore, so it stays out of new AuxHashMap.
          // Correct the AUX_TOKEN value in the HLL array to the newShiftedVal (14).
          host.putSlot(slotNum, newShiftedVal);
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
    host.putAuxHashMap(newAuxMap);

    host.putCurMin(newCurMin);
    host.putNumAtCurMin(numAtNewCurMin);
  } //end of shiftToBiggerCurMin



}
