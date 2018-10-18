/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.invPow2;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;
import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;

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
      return Conversions.convertToHll4(this);
    }
    if (tgtHllType == HLL_6) {
      return Conversions.convertToHll6(this);
    }
    return Conversions.convertToHll8(this);
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
  abstract PairIterator iterator();

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

  abstract AuxHashMap getNewAuxHashMap();

  abstract int getNumAtCurMin();

  @Override
  int getPreInts() {
    return HLL_PREINTS;
  }

  abstract int getSlot(int slotNo);

  @Override //used by HLL6 and HLL8, Overridden by HLL4
  int getUpdatableSerializationBytes() {
    return HLL_BYTE_ARR_START + getHllByteArrBytes();
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

  /**
   * Common HIP and KxQ incremental update for all heap and direct Hll.
   * @param host the origin implementation
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

}
