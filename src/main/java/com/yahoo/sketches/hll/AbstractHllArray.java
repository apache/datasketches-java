/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.invPow2;
import static com.yahoo.sketches.hll.HllUtil.AUX_TOKEN;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
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

/**
 * @author Lee Rhodes
 */
abstract class AbstractHllArray extends HllSketchImpl {
  AuxHashMap auxHashMap = null; //used for both heap and direct HLL4
  final int auxStart; //used for direct HLL4

  AbstractHllArray(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode) {
    super(lgConfigK, tgtHllType, curMode);
    auxStart = HLL_BYTE_ARR_START + hll4ArrBytes(lgConfigK);
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
    final int auxCountBytes = (auxHashMap == null) ? 0 : auxHashMap.getAuxCount() << 2;
    return HLL_BYTE_ARR_START + getHllByteArrBytes() + auxCountBytes;
  }

  /**
   * This is the (non-HIP) estimator.
   * It is called "composite" because multiple estimators are pasted together.
   * @return the composite estimate
   */
  //In C: again-two-registers.c hhb_get_composite_estimate L1489
  @Override
  double getCompositeEstimate() {
    return HllEstimators.hllCompositeEstimate(this);
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
    HllUtil.checkNumStdDev(numStdDev);
    return HllEstimators.hllLowerBound(this, numStdDev);
  }

  @Override
  int getMemDataStart() {
    return HLL_BYTE_ARR_START;
  }

  abstract Memory getMemory();

  abstract AuxHashMap getNewAuxHashMap();

  abstract int getNumAtCurMin();

  @Override
  int getPreInts() {
    return HLL_PREINTS;
  }

  abstract int getSlot(int slotNo);

  @Override
  int getUpdatableSerializationBytes() {
    final AuxHashMap auxHashMap = getAuxHashMap();
    final int auxBytes = (auxHashMap == null) ? 0 : 4 << auxHashMap.getLgAuxArrInts();
    return HLL_BYTE_ARR_START + getHllByteArrBytes() + auxBytes;
  }

  @Override
  double getUpperBound(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    return HllEstimators.hllUpperBound(this, numStdDev);
  }

  abstract void putAuxHashMap(AuxHashMap auxHashMap, boolean compact);

  abstract void putCurMin(int curMin);

  abstract void putHipAccum(double hipAccum);

  abstract void putKxQ0(double kxq0);

  abstract void putKxQ1(double kxq1);

  abstract void putNumAtCurMin(int numAtCurMin);

  abstract void putSlot(final int slotNo, final int value);

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

  //To byte array for Heap HLL
  static final byte[] toByteArray(final AbstractHllArray impl, final boolean compact) {
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
    final byte[] hllByteArr = ((HllArray)impl).hllByteArr;
    wmem.putByteArray(HLL_BYTE_ARR_START, hllByteArr, 0, hllByteArr.length);
    if (impl.getAuxHashMap() != null) {
      insertAux(impl, wmem, compact);
    } else {
      wmem.putInt(AUX_COUNT_INT, 0);
    }
  }

  private static final void insertCommonHll(final AbstractHllArray srcImpl,
      final WritableMemory tgtWmem, final boolean compact) {
    final Object tgtMemObj = tgtWmem.getArray();
    final long tgtMemAdd = tgtWmem.getCumulativeOffset(0L);
    insertPreInts(tgtMemObj, tgtMemAdd, HLL_PREINTS);
    insertSerVer(tgtMemObj, tgtMemAdd);
    insertFamilyId(tgtMemObj, tgtMemAdd);
    insertLgK(tgtMemObj, tgtMemAdd, srcImpl.getLgConfigK());
    insertEmptyFlag(tgtMemObj, tgtMemAdd, srcImpl.isEmpty());
    insertCompactFlag(tgtMemObj, tgtMemAdd, compact);
    insertOooFlag(tgtMemObj, tgtMemAdd, srcImpl.isOutOfOrderFlag());
    insertCurMin(tgtMemObj, tgtMemAdd, srcImpl.getCurMin());
    insertCurMode(tgtMemObj, tgtMemAdd, srcImpl.getCurMode());
    insertTgtHllType(tgtMemObj, tgtMemAdd, srcImpl.getTgtHllType());
    insertHipAccum(tgtMemObj, tgtMemAdd, srcImpl.getHipAccum());
    insertKxQ0(tgtMemObj, tgtMemAdd, srcImpl.getKxQ0());
    insertKxQ1(tgtMemObj, tgtMemAdd, srcImpl.getKxQ1());
    insertNumAtCurMin(tgtMemObj, tgtMemAdd, srcImpl.getNumAtCurMin());
  }

  //used by insertHll for heap
  private static final void insertAux(final AbstractHllArray srcImpl, final WritableMemory tgtWmem,
      final boolean tgtCompact) {
    final Object memObj = tgtWmem.getArray();
    final long memAdd = tgtWmem.getCumulativeOffset(0L);
    final AuxHashMap auxHashMap = srcImpl.getAuxHashMap();
    final int auxCount = auxHashMap.getAuxCount();
    insertAuxCount(memObj, memAdd, auxCount);
    insertLgArr(memObj, memAdd, auxHashMap.getLgAuxArrInts()); //only used for direct HLL
    final long auxStart = srcImpl.auxStart;
    if (tgtCompact) {
      final PairIterator itr = auxHashMap.getIterator();
      int cnt = 0;
      while (itr.nextValid()) { //works whether src has compact memory or not
        insertInt(memObj, memAdd, auxStart + (cnt++ << 2), itr.getPair());
      }
      assert cnt == auxCount;
    } else { //updatable
      final int auxInts = 1 << auxHashMap.getLgAuxArrInts();
      final int[] auxArr = auxHashMap.getAuxIntArr();
      tgtWmem.putIntArray(auxStart, auxArr, 0, auxInts);
    }
  }

  /**
   * Common HIP and KxQ incremental update for all heap and direct Hll.
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
          hll4Array.putAuxHashMap(auxHashMap, false);
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
