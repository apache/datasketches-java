/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.invPow2;
import static com.yahoo.sketches.hll.PreambleUtil.FAMILY_ID;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.SER_VER;
import static com.yahoo.sketches.hll.PreambleUtil.extractFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.extractPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;

import com.yahoo.memory.Memory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
abstract class HllArray extends HllSketchImpl {
  //Derived using some formulas in Ting's paper. In C: giant-file.c 1077
  static final double HLL_HIP_RSE_FACTOR = 0.836083874576235;
  //In C: giant-file.c Line 1421
  static final double HLL_NON_HIP_RSE_FACTOR = 1.04; //From Flajolet
  static final int AUX_TOKEN = 0xf;

  int curMin; //only changed by Hll4Array
  int numAtCurMin;
  double hipAccum;
  double kxq0;
  double kxq1;
  byte[] hllByteArr = null; //init by sub-classes
  AuxHashMap auxHashMap = null; //init only by Hll4Array

  /**
   * Standard constructor
   * @param lgConfigK the configured Lg K
   * @param tgtHllType the type of target HLL sketch
   */
  HllArray(final int lgConfigK, final TgtHllType tgtHllType) {
    super(lgConfigK, tgtHllType, CurMode.HLL);
    final int configK = 1 << lgConfigK;
    curMin = 0;
    numAtCurMin = configK;
    hipAccum = 0;
    kxq0 = configK;
    kxq1 = 0;
  }

  /**
   * Copy constructor
   * @param that another HllArray
   */
  HllArray(final HllArray that) {
    super(that);
    curMin = that.curMin;
    numAtCurMin = that.numAtCurMin;
    hipAccum = that.hipAccum;
    kxq0 = that.kxq0;
    kxq1 = that.kxq1;
    hllByteArr = that.hllByteArr.clone(); //that.hllByteArr should never be null.
    auxHashMap = (that.auxHashMap != null) ? that.auxHashMap.copy() : null;
  }

  @Override
  HllArray copyAs(final TgtHllType tgtHllType) {
    if (tgtHllType == this.tgtHllType) {
      return (HllArray)copy();
    }
    if (tgtHllType == HLL_4) {
      return convertToHll4(this);
    }
    if (tgtHllType == HLL_6) {
      return convertToHll6(this);
    }
    return convertToHll8(this);
  }

  @Override
  PairIterator getAuxIterator() {
    if (auxHashMap != null) { return auxHashMap.getIterator(); }
    return null;
  }

  @Override
  int getCount() {
    return -1;
  }

  @Override
  int getCurMin() {
    return curMin;
  }

  @Override
  abstract int getCurrentSerializationBytes();

  @Override
  double getEstimate() {
    if (oooFlag) {
      return getCompositeEstimate();
    }
    return hipAccum;
  }

  @Override
  double getHipAccum() {
    return hipAccum;
  }

  @Override
  abstract PairIterator getIterator();

  @Override
  double getLowerBound(final double numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    final double tmp;
    if (oooFlag) {
      tmp = getCompositeEstimate() / (1.0 + hllNonHipEps(numStdDev));
    } else {
      tmp =  hipAccum / (1.0 + hllHipEps(numStdDev));
    }
    double numNonZeros = 1 << lgConfigK;
    if (curMin == 0) {
      numNonZeros -= numAtCurMin;
    }
    return Math.max(tmp, numNonZeros);
  }

  @Override
  int getMaxCouponArrInts() {
    return -1;
  }

  @Override
  int getNumAtCurMin() {
    return numAtCurMin;
  }

  @Override
  double getUpperBound(final double numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    if (oooFlag) {
      //In C: two-registers.c Line 1495
      return getCompositeEstimate() / (1.0 - hllNonHipEps(numStdDev));
    }
    return hipAccum / (1.0 - hllHipEps(numStdDev));
  }

  @Override
  boolean isEmpty() {
    return (curMin == 0) && (numAtCurMin == (1 << lgConfigK));
  }

  @Override
  void putHipAccum(final double value) {
    hipAccum = value;
  }

  @Override
  abstract byte[] toByteArray();

  /**
   * HIP and KxQ incremental update.
   * @param oldValue old value
   * @param newValue new value
   */
  //In C: two-registers.c Lines 851 to 871
  void hipAndKxQIncrementalUpdate(final int oldValue, final int newValue) {
    assert newValue > oldValue;
    final int configK = 1 << lgConfigK;
    //update hipAccum BEFORE updating kxq0 and kxq1
    hipAccum += configK / (kxq0 + kxq1);
    //update kxq0 and kxq1; subtract first, then add.
    if (oldValue < 32) { kxq0 -= invPow2(oldValue); }
    else               { kxq1 -= invPow2(oldValue); }
    if (newValue < 32) { kxq0 += invPow2(newValue); }
    else               { kxq1 += invPow2(newValue); }
  }

  static final void checkPreamble(final Memory mem, final Object memArr, final long memAdd) {
    final int memPreInts = extractPreInts(memArr, memAdd);
    final int serVer = extractSerVer(memArr, memAdd);
    final int famId = extractFamilyId(memArr, memAdd);
    if ( (memPreInts != HLL_PREINTS) || (serVer != SER_VER) || (famId != FAMILY_ID) ) {
      badPreambleState(mem);
    }
  }

  //In C: two-registers.c Lines: 1156
  private double getRawEstimate() {
    final int configK = 1 << lgConfigK;
    final double correctionFactor = 0.7213 / (1.0 + (1.079 / configK));
    return (correctionFactor * configK * configK) / (kxq0 + kxq1);
  }

  /**
   * This is the (non-HIP) estimator that is exported to users.
   * It is called "composite" because multiple estimators are pasted together.
   * @return the composite estimate
   */
  //In C: two-registers.c Line 1433
  private double getCompositeEstimate() {
    final double rawEst = getRawEstimate();
    final int configK = 1 << lgConfigK;

    Tables.checkK(lgConfigK);

    final double[] x_arr = Tables.getXarr(lgConfigK);
    final double[] y_arr = Tables.getYarr(lgConfigK);

    if (rawEst < x_arr[0]) {
      return 0;
    }
    if (rawEst > x_arr[x_arr.length - 1]) {
      return rawEst;
    }
    final double adjEst = Tables.cubicInterpolateUsingTable(x_arr, y_arr, rawEst);
    if (adjEst > (3.0 * configK)) {
      return adjEst;
    }
    final double linEst = getHllBitMapEstimate();
    final double avgEst = (adjEst + linEst) / 2.0;
    // The following constant 0.64 comes from empirical measurements of the crossover
    //    point between the average error of the linear estimator and the adjusted hll estimator
    if (avgEst > (0.64 * configK)) {
      return adjEst;
    }
    return linEst;
  }

  private double getHllBitMapEstimate() { //estimator for when N is small
    final int configK = 1 << lgConfigK;
    return ((curMin != 0) || (numAtCurMin == 0))
        ? configK * Math.log(configK / 0.5)
        : Tables.getBitMapEstimate(configK, configK - numAtCurMin);
  }

  //In C: two-registers.c lines 1136-1137
  private double hllHipEps(final double numStdDevs) {
    return (numStdDevs * HLL_HIP_RSE_FACTOR) / Math.sqrt(1 << lgConfigK);
  }

  //In C: giant-file.c lines 1500-1501
  private double hllNonHipEps(final double numStdDevs) {
    return (numStdDevs * HLL_NON_HIP_RSE_FACTOR) / Math.sqrt(1 << lgConfigK);
  }

  private static final Hll4Array convertToHll4(final HllArray srcHllArr) {
    final Hll4Array hll4Array = new Hll4Array(srcHllArr.getLgConfigK());
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

  private static final Hll6Array convertToHll6(final HllArray srcHllArr) {
    final Hll6Array hll6Array = new Hll6Array(srcHllArr.getLgConfigK());
    hll6Array.putOooFlag(srcHllArr.getOooFlag());
    final PairIterator itr = srcHllArr.getIterator();
    while (itr.nextValid()) {
      hll6Array.couponUpdate(itr.getPair());
    }
    hll6Array.putHipAccum(srcHllArr.getHipAccum());
    return hll6Array;
  }

  private static final Hll8Array convertToHll8(final HllArray srcHllArr) {
    final Hll8Array hll8Array = new Hll8Array(srcHllArr.getLgConfigK());
    hll8Array.putOooFlag(srcHllArr.getOooFlag());
    final PairIterator itr = srcHllArr.getIterator();
    while (itr.nextValid()) {
      hll8Array.couponUpdate(itr.getPair());
    }
    hll8Array.putHipAccum(srcHllArr.getHipAccum());
    return hll8Array;
  }

}
