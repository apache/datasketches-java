/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hll;

import static org.apache.datasketches.hll.HllUtil.AUX_TOKEN;
import static org.apache.datasketches.hll.HllUtil.LG_AUX_ARR_INTS;

import org.apache.datasketches.SketchesStateException;

/**
 * Update process common to Heap Hll 4 and Direct Hll 4
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class Hll4Update {

  //Uses lgConfigK, curMin, numAtCurMin, auxMap
  //Only called by Hll4Array and DirectHll4Array
  //In C: two-registers.c Line 836 in "hhb_abstract_set_slot_if_new_value_bigger" non-sparse
  static final void internalHll4Update(final AbstractHllArray host, final int slotNo,
      final int newValue) {
    assert ((0 <= slotNo) && (slotNo < (1 << host.getLgConfigK())));

    final int curMin = host.getCurMin();
    final int rawStoredOldNibble = host.getNibble(slotNo);  //could be 0
    final int lbOnOldValue = rawStoredOldNibble + curMin; //provable lower bound, could be 0

    if (newValue <= lbOnOldValue) { return; }
    //Thus: newValue > lbOnOldValue AND newValue > curMin

    AuxHashMap auxHashMap; // = host.getAuxHashMap();
    final int actualOldValue;
    final int shiftedNewValue; //value - curMin

    //Based on whether we have an AUX_TOKEN and whether the shiftedNewValue is greater than
    // AUX_TOKEN, we have four cases for how to actually modify the data structure:
    // 1. (shiftedNewValue >= AUX_TOKEN) && (rawStoredOldNibble = AUX_TOKEN) //881:
    //    The byte array already contains aux token
    //    This is the case where old and new values are both exceptions.
    //    Therefore, the 4-bit array already is AUX_TOKEN. Only need to update auxMap
    // 2. (shiftedNewValue < AUX_TOKEN) && (rawStoredOldNibble = AUX_TOKEN) //885
    //    This is the (hypothetical) case where old value is an exception and the new one is not,
    //    which is impossible given that curMin has not changed here and the newValue > oldValue.
    // 3. (shiftedNewValue >= AUX_TOKEN) && (rawStoredOldNibble < AUX_TOKEN) //892
    //    This is the case where the old value is not an exception and the new value is.
    //    Therefore the AUX_TOKEN must be stored in the 4-bit array and the new value
    //    added to the exception table.
    // 4. (shiftedNewValue < AUX_TOKEN) && (rawStoredOldNibble < AUX_TOKEN) //897
    //    This is the case where neither the old value nor the new value is an exception.
    //    Therefore we just overwrite the 4-bit array with the shifted new value.

    if (rawStoredOldNibble == AUX_TOKEN) { //846 Note: This is rare and really hard to test!
      auxHashMap = host.getAuxHashMap(); //auxHashMap must already exist.
      assert auxHashMap != null;
      actualOldValue = auxHashMap.mustFindValueFor(slotNo);//lgtm [java/dereferenced-value-may-be-null]
      if (newValue <= actualOldValue) { return; }
      //We know that the array will be changed, but we haven't actually updated yet.
      AbstractHllArray.hipAndKxQIncrementalUpdate(host, actualOldValue, newValue);
      shiftedNewValue = newValue - curMin;
      assert (shiftedNewValue >= 0);

      if (shiftedNewValue >= AUX_TOKEN) { //CASE 1:
        auxHashMap.mustReplace(slotNo, newValue); //lgtm [java/dereferenced-value-may-be-null]
      }
      //else                              //CASE 2: impossible

    } else { //rawStoredOldNibble < AUX_TOKEN
      actualOldValue = lbOnOldValue;
      //We know that the array will be changed, but we haven't actually updated yet.
      AbstractHllArray.hipAndKxQIncrementalUpdate(host, actualOldValue, newValue);
      shiftedNewValue = newValue - curMin;
      assert (shiftedNewValue >= 0);

      if (shiftedNewValue >= AUX_TOKEN) { //CASE 3: //892
        host.putNibble(slotNo, AUX_TOKEN);
        auxHashMap = host.getAuxHashMap();
        if (auxHashMap == null) {
          auxHashMap = host.getNewAuxHashMap();
          host.putAuxHashMap(auxHashMap, false);
        }
        auxHashMap.mustAdd(slotNo, newValue);
      }
      else {                             // CASE 4: //897
        host.putNibble(slotNo, shiftedNewValue);
      }
    }

    // We just changed the HLL array, so it might be time to change curMin
    if (actualOldValue == curMin) { //908
      assert (host.getNumAtCurMin() >= 1);
      host.decNumAtCurMin();
      while (host.getNumAtCurMin() == 0) {
        //increases curMin by 1, and builds a new aux table,
        // shifts values in 4-bit table, and recounts curMin
        shiftToBiggerCurMin(host);
      }
    }
  }

  //This scheme only works with two double registers (2 kxq values).
  //  HipAccum, kxq0 and kxq1 remain untouched.
  //  This changes curMin, numAtCurMin, hllByteArr and auxMap.
  //Entering this routine assumes that all slots have valid nibbles > 0 and <= 15.
  //An AuxHashMap must exist if any values in the current hllByteArray are already 15.
  //In C: again-two-registers.c Lines 710 "hhb_shift_to_bigger_curmin"
  private static final void shiftToBiggerCurMin(final AbstractHllArray host) {
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
      int oldStoredNibble = host.getNibble(i);
      if (oldStoredNibble == 0) {
        throw new SketchesStateException("Array slots cannot be 0 at this point.");
      }
      if (oldStoredNibble < AUX_TOKEN) {
        host.putNibble(i, --oldStoredNibble);
        if (oldStoredNibble == 0) { numAtNewCurMin++; }
      } else { //oldStoredNibble == AUX_TOKEN
        numAuxTokens++;
        assert host.getAuxHashMap() != null : "AuxHashMap cannot be null at this point.";
      }
    }

    //If old AuxHashMap exists, walk through it updating some slots and build a new AuxHashMap
    // if needed.
    AuxHashMap newAuxMap = null;
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

        assert host.getNibble(slotNum) == AUX_TOKEN
            : "Array slot != AUX_TOKEN: " + host.getNibble(slotNum);
        if (newShiftedVal < AUX_TOKEN) { //756
          assert (newShiftedVal == 14);
          // The former exception value isn't one anymore, so it stays out of new AuxHashMap.
          // Correct the AUX_TOKEN value in the HLL array to the newShiftedVal (14).
          host.putNibble(slotNum, newShiftedVal);
          numAuxTokens--;
        }
        else { //newShiftedVal >= AUX_TOKEN
          // the former exception remains an exception, so must be added to the newAuxMap
          if (newAuxMap == null) {
            //Note: even in the direct case we use a heap aux map temporarily
            newAuxMap = new HeapAuxHashMap(LG_AUX_ARR_INTS[lgConfigK], lgConfigK);
          }
          newAuxMap.mustAdd(slotNum, oldActualVal);
        }
      } //end scan of oldAuxMap
    } //end if (auxHashMap != null)
    else { //oldAuxMap == null
      assert numAuxTokens == 0 : "auxTokens: " + numAuxTokens;
    }

    if (newAuxMap != null) {
      assert newAuxMap.getAuxCount() == numAuxTokens : "auxCount: " + newAuxMap.getAuxCount()
        + ", HLL tokens: " + numAuxTokens;
    }
    host.putAuxHashMap(newAuxMap, false); //if we are direct, this will do the right thing

    host.putCurMin(newCurMin);
    host.putNumAtCurMin(numAtNewCurMin);
  } //end of shiftToBiggerCurMin

}
