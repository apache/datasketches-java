/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.invPow2;
import static com.yahoo.sketches.hll.HllUtil.AUX_TOKEN;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.HLL_HIP_RSE_FACTOR;
import static com.yahoo.sketches.hll.HllUtil.HLL_NON_HIP_RSE_FACTOR;
import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static com.yahoo.sketches.hll.HllUtil.MIN_LOG_K;
import static com.yahoo.sketches.hll.PreambleUtil.AUX_COUNT_INT;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.insertAuxCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMode;
import static com.yahoo.sketches.hll.PreambleUtil.insertEmptyFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.insertHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.insertInt;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgK;
import static com.yahoo.sketches.hll.PreambleUtil.insertNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.hll.PreambleUtil.insertTgtHllType;
import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
abstract class AbstractHllArray extends HllSketchImpl {
  AuxHashMap auxHashMap = null;

  AbstractHllArray(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode) {
    super(lgConfigK, tgtHllType, curMode);
  }

  abstract void addToHipAccum(double delta);

  @Override
  HllArray copyAs(final TgtHllType tgtHllType) {
    if (tgtHllType == getTgtHllType()) {
      return (HllArray) copy();
    }
    if (tgtHllType == HLL_4) {
      return convertToHll4(this);
    }
    if (tgtHllType == HLL_6) {
      return convertToHll6(this);
    }
    return convertToHll8(this);
  }


  abstract void decNumAtCurMin();

  AuxHashMap getAuxHashMap() {
    return auxHashMap;
  }

  PairIterator getAuxIterator() {
    return (auxHashMap == null) ? null : auxHashMap.getIterator();
  }

  @Override
  int getCompactSerializationBytes() {
    final AuxHashMap auxHashMap = getAuxHashMap();
    final int auxBytes = (auxHashMap == null) ? 0 : auxHashMap.getAuxCount() << 2;
    return HLL_BYTE_ARR_START + getHllByteArrBytes() + auxBytes;
  }

  /**
   * This is the (non-HIP) estimator.
   * It is called "composite" because multiple estimators are pasted together.
   * @return the composite estimate
   */
  //In C: again-two-registers.c hhb_get_composite_estimate L1489
  @Override
  double getCompositeEstimate() {
    return compositeEstimate(this);
  }

  abstract int getCurMin();

  @Override
  double getEstimate() {
    if (isOutOfOrderFlag()) {
      return getCompositeEstimate();
    }
    return getHipAccum();
  }

  abstract double getHipAccum();

  abstract int getHllByteArrBytes();

  @Override
  abstract PairIterator getIterator();

  abstract double getKxQ0();

  abstract double getKxQ1();

  @Override
  double getLowerBound(final int numStdDev) {
    return lowerBound(this, numStdDev);
  }

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

  @SuppressWarnings("unused")
  int getSlot(final int slotNo) {
    return -1;//intentional, overridden only by heap and direct HLL4
  }

  @Override
  int getUpdatableSerializationBytes() {
    final AuxHashMap auxHashMap = getAuxHashMap();
    final int auxBytes = (auxHashMap == null) ? 0 : 4 << auxHashMap.getLgAuxArrInts();
    return HLL_BYTE_ARR_START + getHllByteArrBytes() + auxBytes;
  }

  @Override
  double getUpperBound(final int numStdDev) {
    return upperBound(this, numStdDev);
  }

  abstract void putAuxHashMap(AuxHashMap auxHashMap);

  abstract void putCurMin(int curMin);

  abstract void putHipAccum(double hipAccum);

  abstract void putKxQ0(double kxq0);

  abstract void putKxQ1(double kxq1);

  abstract void putNumAtCurMin(int numAtCurMin);

  @SuppressWarnings("unused")
  void putSlot(final int slotNo, final int value) {
    //intentional no-op, overridden only by heap and direct HLL4
  }

  @Override
  byte[] toCompactByteArray() {
    return toByteArray(this, true);
  }

  @Override
  byte[] toUpdatableByteArray() {
    return toByteArray(this, false);
  }

  //COMPUTING HLL BYTE ARRAY LENGTHS

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

  //TO BYTE ARRAY PROCESS FOR ALL HLL

  private static final byte[] toByteArray(final AbstractHllArray impl, final boolean compact) {
    int auxBytes = 0;
    final AuxHashMap auxHashMap = impl.getAuxHashMap();
    if (auxHashMap != null) { //only relevant for HLL_4
      auxBytes = (compact)
          ? auxHashMap.getCompactSizeBytes()
          : auxHashMap.getUpdatableSizeBytes();
    }
    final int totBytes = HLL_BYTE_ARR_START + impl.getHllByteArrBytes() + auxBytes;
    final byte[] byteArr = new byte[totBytes];
    final WritableMemory wmem = WritableMemory.wrap(byteArr);
    insertHll(impl, wmem, compact);
    return byteArr;
  }

  private static final void insertHll(final AbstractHllArray impl, final WritableMemory wmem,
      final boolean compact) {
    insertCommonHll(impl, wmem, compact);

    if (impl.isMemory()) {
      final Memory mem = impl.getMemory();
      mem.copyTo(HLL_BYTE_ARR_START, wmem, HLL_BYTE_ARR_START, impl.getHllByteArrBytes());
    } else { //Heap
      final byte[] hllByteArr = ((HllArray)impl).hllByteArr;
      wmem.putByteArray(HLL_BYTE_ARR_START, hllByteArr, 0, hllByteArr.length);
    }

    if (impl.getAuxHashMap() != null) {
      insertAux(impl, wmem, compact);
    } else {
      wmem.putInt(AUX_COUNT_INT, 0);
    }
  }

  private static final void insertCommonHll(final AbstractHllArray impl, final WritableMemory wmem,
      final boolean compact) {
    final Object memObj = wmem.getArray();
    final long memAdd = wmem.getCumulativeOffset(0L);
    insertPreInts(memObj, memAdd, HLL_PREINTS);
    insertSerVer(memObj, memAdd);
    insertFamilyId(memObj, memAdd);
    insertLgK(memObj, memAdd, impl.getLgConfigK());
    insertEmptyFlag(memObj, memAdd, impl.isEmpty());
    insertCompactFlag(memObj, memAdd, compact);
    insertOooFlag(memObj, memAdd, impl.isOutOfOrderFlag());
    insertCurMin(memObj, memAdd, impl.getCurMin());
    insertCurMode(memObj, memAdd, impl.getCurMode());
    insertTgtHllType(memObj, memAdd, impl.getTgtHllType());
    insertHipAccum(memObj, memAdd, impl.getHipAccum());
    insertKxQ0(memObj, memAdd, impl.getKxQ0());
    insertKxQ1(memObj, memAdd, impl.getKxQ1());
    insertNumAtCurMin(memObj, memAdd, impl.getNumAtCurMin());
  }

  private static final void insertAux(final AbstractHllArray impl, final WritableMemory wmem,
      final boolean compact) {
    final Object memObj = wmem.getArray();
    final long memAdd = wmem.getCumulativeOffset(0L);
    final AuxHashMap auxHashMap = impl.getAuxHashMap();
    final int auxCount = auxHashMap.getAuxCount();
    insertAuxCount(memObj, memAdd, auxCount);
    insertLgArr(memObj, memAdd, auxHashMap.getLgAuxArrInts()); //only used for direct HLL
    final long auxStart = HLL_BYTE_ARR_START + impl.getHllByteArrBytes();
    if (compact) {
      final PairIterator itr = auxHashMap.getIterator();
      int cnt = 0;
      while (itr.nextValid()) {
        insertInt(memObj, memAdd, auxStart + (cnt++ << 2), itr.getPair());
      }
      assert cnt == auxCount;
    } else { //updatable
      final int auxInts = 1 << auxHashMap.getLgAuxArrInts();
      if (impl.isMemory()) {
        impl.getMemory().copyTo(auxStart, wmem, auxStart, auxInts << 2);
      } else {
        final int[] auxArr = auxHashMap.getAuxIntArr();
        wmem.putIntArray(auxStart, auxArr, 0, auxInts);
      }
    }
  }

  //UPDATE HIP AND KXQ COMMON TO ALL HLL

  /**
   * HIP and KxQ incremental update.
   * @param oldValue old value
   * @param newValue new value
   */
  //In C: again-two-registers.c Lines 851 to 871
  //Called here and by Heap and Direct 6 and 8 bit implementations
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

  //UPDATE PROCESS COMMON TO HEAP AND DIRECT HLL_4

  //In C: two-registers.c Line 836 in "hhb_abstract_set_slot_if_new_value_bigger" non-sparse
  //Uses lgConfigK, curMin, numAtCurMin, auxMap
  //Only called by Hll4Array and DirectHll4Array
  static final void internalHll4Update(final AbstractHllArray host, final int slotNo, final int newValue) {
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

  //UPPER AND LOWER BOUNDS

  private static final double lowerBound(final AbstractHllArray absHllArr, final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    final int lgConfigK = absHllArr.lgConfigK;
    final int configK = 1 << lgConfigK;
    final boolean oooFlag = absHllArr.isOutOfOrderFlag();
    final double compositeEstimate = absHllArr.getCompositeEstimate();
    final double hipAccum = absHllArr.getHipAccum();
    if (lgConfigK > 12) {
      final double tmp;
      if (oooFlag) {
        final double hllNonHipEps =
            (numStdDev * HLL_NON_HIP_RSE_FACTOR) / Math.sqrt(configK);
        tmp = compositeEstimate / (1.0 + hllNonHipEps);
      } else {
        final double hllHipEps = (numStdDev * HLL_HIP_RSE_FACTOR) / Math.sqrt(configK);
        tmp =  hipAccum / (1.0 + hllHipEps);
      }
      double numNonZeros = configK;
      if (absHllArr.getCurMin() == 0) {
        numNonZeros -= absHllArr.getNumAtCurMin();
      }
      return Math.max(tmp, numNonZeros);
    }
    //lgConfigK <= 12
    final double re = RelativeErrorTables.getRelErr(false, oooFlag, lgConfigK, numStdDev);
    return ((oooFlag) ? compositeEstimate : hipAccum) / (1.0 + re);
  }

  private static final double upperBound(final AbstractHllArray absHllArr, final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    final int lgConfigK = absHllArr.lgConfigK;
    final int configK = 1 << lgConfigK;
    final boolean oooFlag = absHllArr.isOutOfOrderFlag();
    final double compositeEstimate = absHllArr.getCompositeEstimate();
    final double hipAccum = absHllArr.getHipAccum();
    if (lgConfigK > 12) {
      if (oooFlag) {
        final double hllNonHipEps =
            (numStdDev * HLL_NON_HIP_RSE_FACTOR) / Math.sqrt(configK);
        return compositeEstimate / (1.0 - hllNonHipEps);
      }
      final double hllHipEps = (numStdDev * HLL_HIP_RSE_FACTOR) / Math.sqrt(configK);
      return hipAccum / (1.0 - hllHipEps);
    }
    //lgConfigK <= 12
    final double re = RelativeErrorTables.getRelErr(true, oooFlag, lgConfigK, numStdDev);
    return ((oooFlag) ? compositeEstimate : hipAccum) / (1.0 + re);
  }

  //THE COMPOSITE ESTIMATOR

  /**
   * This is the (non-HIP) estimator.
   * It is called "composite" because multiple estimators are pasted together.
   * @param absHllArr an instance of the AbstractHllArray class.
   * @return the composite estimate
   */
  //In C: again-two-registers.c hhb_get_composite_estimate L1489
  private static final double compositeEstimate(final AbstractHllArray absHllArr) {
    final int lgConfigK = absHllArr.getLgConfigK();
    final double rawEst = getRawEstimate(lgConfigK, absHllArr.getKxQ0() + absHllArr.getKxQ1());

    final double[] xArr = CompositeInterpolationXTable.xArrs[lgConfigK - MIN_LOG_K];
    final double yStride = CompositeInterpolationXTable.yStrides[lgConfigK - MIN_LOG_K];
    final int xArrLen = xArr.length;

    if (rawEst < xArr[0]) { return 0; }

    final int xArrLenM1 = xArrLen - 1;

    if (rawEst > xArr[xArrLenM1]) {
      final double finalY = yStride * (xArrLenM1);
      final double factor = finalY / xArr[xArrLenM1];
      return rawEst * factor;
    }

    final double adjEst =
        CubicInterpolation.usingXArrAndYStride(xArr, yStride, rawEst);

    // We need to completely avoid the linear_counting estimator if it might have a crazy value.
    // Empirical evidence suggests that the threshold 3*k will keep us safe if 2^4 <= k <= 2^21.

    if (adjEst > (3 << lgConfigK)) { return adjEst; }
    //Alternate call
    //if ((adjEst > (3 << lgConfigK)) || ((curMin != 0) || (numAtCurMin == 0)) ) { return adjEst; }

    final double linEst =
        getHllBitMapEstimate(lgConfigK, absHllArr.getCurMin(), absHllArr.getNumAtCurMin());

    // Bias is created when the value of an estimator is compared with a threshold to decide whether
    // to use that estimator or a different one.
    // We conjecture that less bias is created when the average of the two estimators
    // is compared with the threshold. Empirical measurements support this conjecture.

    final double avgEst = (adjEst + linEst) / 2.0;

    // The following constants comes from empirical measurements of the crossover point
    // between the average error of the linear estimator and the adjusted hll estimator
    double crossOver = 0.64;
    if (lgConfigK == 4)      { crossOver = 0.718; }
    else if (lgConfigK == 5) { crossOver = 0.672; }

    return (avgEst > (crossOver * (1 << lgConfigK))) ? adjEst : linEst;
  }

  /**
   * Estimator when N is small, roughly less than k log(k).
   * Refer to Wikipedia: Coupon Collector Problem
   * @return the very low range estimate
   */
  //In C: again-two-registers.c hhb_get_improved_linear_counting_estimate L1274
  private static final double getHllBitMapEstimate(
      final int lgConfigK, final int curMin, final int numAtCurMin) {
    final int configK = 1 << lgConfigK;
    final int numUnhitBuckets =  (curMin == 0) ? numAtCurMin : 0;

    //This will eventually go away.
    if (numUnhitBuckets == 0) {
      return configK * Math.log(configK / 0.5);
    }

    final int numHitBuckets = configK - numUnhitBuckets;
    return HarmonicNumbers.getBitMapEstimate(configK, numHitBuckets);
  }

  //In C: again-two-registers.c hhb_get_raw_estimate L1167
  private static final double getRawEstimate(final int lgConfigK, final double kxqSum) {
    final int configK = 1 << lgConfigK;
    final double correctionFactor;
    if (lgConfigK == 4) { correctionFactor = 0.673; }
    else if (lgConfigK == 5) { correctionFactor = 0.697; }
    else if (lgConfigK == 6) { correctionFactor = 0.709; }
    else { correctionFactor = 0.7213 / (1.0 + (1.079 / configK)); }
    final double hyperEst = (correctionFactor * configK * configK) / kxqSum;
    return hyperEst;
  }

  //CONVERSIONS

  private static final Hll4Array convertToHll4(final AbstractHllArray srcHllArr) {
    final int lgConfigK = srcHllArr.getLgConfigK();
    final Hll4Array hll4Array = new Hll4Array(lgConfigK);
    hll4Array.putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());

    //1st pass: compute starting curMin and numAtCurMin:
    final int pair = curMinAndNum(srcHllArr);
    final int curMin = HllUtil.getValue(pair);
    final int numAtCurMin = HllUtil.getLow26(pair);

    //2nd pass: Must know curMin. Populate KxQ registers, build AuxHashMap if needed
    final PairIterator itr = srcHllArr.getIterator();
    AuxHashMap auxHashMap = hll4Array.getAuxHashMap(); //may be null
    while (itr.nextValid()) {
      final int slotNo = itr.getIndex();
      final int actualValue = itr.getValue();
      hipAndKxQIncrementalUpdate(srcHllArr, 0, actualValue);
      if (actualValue >= (curMin + 15)) {
        hll4Array.putSlot(slotNo, AUX_TOKEN);
        if (auxHashMap == null) {
          auxHashMap = new HeapAuxHashMap(LG_AUX_ARR_INTS[lgConfigK], lgConfigK);
          hll4Array.putAuxHashMap(auxHashMap);
        }
        auxHashMap.mustAdd(slotNo, actualValue);
      } else {
        hll4Array.putSlot(slotNo, actualValue - curMin);
      }
    }

    hll4Array.putCurMin(curMin);
    hll4Array.putNumAtCurMin(numAtCurMin);
    hll4Array.putHipAccum(srcHllArr.getHipAccum());
    return hll4Array;
  }

  private static final int curMinAndNum(final AbstractHllArray hllArr) {
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

  private static final Hll6Array convertToHll6(final AbstractHllArray srcHllArr) {
    final int lgConfigK = srcHllArr.lgConfigK;
    final Hll6Array hll6Array = new Hll6Array(lgConfigK);
    hll6Array.putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());
    int numZeros = 1 << lgConfigK;
    final PairIterator itr = srcHllArr.getIterator();
    while (itr.nextAll()) {
      if (itr.getValue() != EMPTY) {
        numZeros--;
        hll6Array.couponUpdate(itr.getPair()); //creates KxQ registers
      }
    }
    hll6Array.putNumAtCurMin(numZeros);
    hll6Array.putHipAccum(srcHllArr.getHipAccum());
    return hll6Array;
  }

  private static final Hll8Array convertToHll8(final AbstractHllArray srcHllArr) {
    final int lgConfigK = srcHllArr.lgConfigK;
    final Hll8Array hll8Array = new Hll8Array(lgConfigK);
    hll8Array.putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());
    int numZeros = 1 << lgConfigK;
    final PairIterator itr = srcHllArr.getIterator();
    while (itr.nextAll()) {
      if (itr.getValue() != EMPTY) {
        numZeros--;
        hll8Array.couponUpdate(itr.getPair());
      }
    }
    hll8Array.putNumAtCurMin(numZeros);
    hll8Array.putHipAccum(srcHllArr.getHipAccum());
    return hll8Array;
  }

}
