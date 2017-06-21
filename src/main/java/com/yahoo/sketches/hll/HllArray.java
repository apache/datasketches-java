/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.invPow2;
import static com.yahoo.sketches.hll.PreambleUtil.FAMILY_ID;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARRAY_START;
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
  //From Kevin's extensive analysis for low values of lgConfigK.
  private static final double[] HLL_HIP_RSE_FACTORS = {0.862, 0.8467, 0.8397, 0.8360};
  private static final double[] HLL_NON_HIP_RSE_FACTORS = {1.1059, 1.0706, 1.0545, 1.0464};
  final double HLL_HIP_RSE_FACTOR;
  final double HLL_NON_HIP_RSE_FACTOR;
  int curMin; //only changed by Hll4Array
  int numAtCurMin;
  double hipAccum;
  double kxq0;
  double kxq1;
  byte[] hllByteArr = null; //init by sub-classes
  AuxHashMap auxHashMap = null; //used only by Hll4Array

  /**
   * Standard constructor
   * @param lgConfigK the configured Lg K
   * @param tgtHllType the type of target HLL sketch
   */
  HllArray(final int lgConfigK, final TgtHllType tgtHllType) {
    super(lgConfigK, tgtHllType, CurMode.HLL);
    HLL_HIP_RSE_FACTOR = (lgConfigK < 7)
        ? HLL_HIP_RSE_FACTORS[lgConfigK - 4]
        : HLL_HIP_RSE_FACTORS[3];
    HLL_NON_HIP_RSE_FACTOR = (lgConfigK < 7)
        ? HLL_NON_HIP_RSE_FACTORS[lgConfigK - 4]
        : HLL_NON_HIP_RSE_FACTORS[3];
    curMin = 0;
    numAtCurMin = 1 << lgConfigK;
    hipAccum = 0;
    kxq0 = 1 << lgConfigK;
    kxq1 = 0;
  }

  /**
   * Copy constructor
   * @param that another HllArray
   */
  HllArray(final HllArray that) {
    super(that);
    HLL_HIP_RSE_FACTOR = (lgConfigK < 7)
        ? HLL_HIP_RSE_FACTORS[lgConfigK - 4]
        : HLL_HIP_RSE_FACTORS[3];
    HLL_NON_HIP_RSE_FACTOR = (lgConfigK < 7)
        ? HLL_NON_HIP_RSE_FACTORS[lgConfigK - 4]
        : HLL_NON_HIP_RSE_FACTORS[3];
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
      return Hll4Array.convertToHll4(this);
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
  int getCurrentSerializationBytes() {
    final int auxBytes = (auxHashMap == null) ? 0 : auxHashMap.auxCount << 2;
    return HLL_BYTE_ARRAY_START + hllByteArr.length + auxBytes;
  }

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
  abstract byte[] toCompactByteArray();

  /**
   * HIP and KxQ incremental update.
   * @param oldValue old value
   * @param newValue new value
   */
  //In C: again-two-registers.c Lines 851 to 871
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

  //In C: again-two-registers.c hhb_get_raw_estimate Lines: 1167
  private double getRawEstimate() {
    final int configK = 1 << lgConfigK;
    final double correctionFactor;
    if (lgConfigK == 4) { correctionFactor = 0.673; }
    else if (lgConfigK == 5) { correctionFactor = 0.697; }
    else if (lgConfigK == 6) { correctionFactor = 0.709; }
    else { correctionFactor = 0.7213 / (1.0 + (1.079 / configK)); }
    return (correctionFactor * configK * configK) / (kxq0 + kxq1);
  }

  /**
   * This is the (non-HIP) estimator that is exported to users.
   * It is called "composite" because multiple estimators are pasted together.
   * @return the composite estimate
   */
  //In C: again-two-registers.c hhb_get_composite_estimate Line 1489
  private double getCompositeEstimate() {
    final double rawEst = getRawEstimate();
    final int configK = 1 << lgConfigK;

    HllUtil.checkLgK(lgConfigK);

    final double[] x_arr = Interpolation.getXarr(lgConfigK);
    final double[] y_arr = Interpolation.getYarr(lgConfigK);

    if (rawEst < x_arr[0]) {
      return 0;
    }
    if (rawEst > x_arr[x_arr.length - 1]) {
      final double factor = y_arr[y_arr.length - 1] / x_arr[x_arr.length - 1];
      return (rawEst * factor);
    }
    final double adjEst = Interpolation.cubicInterpolateUsingTable(x_arr, y_arr, rawEst);
    if (adjEst > (3.0 * configK)) {
      return adjEst;
    }
    final double linEst = getHllBitMapEstimate();
    final double avgEst = (adjEst + linEst) / 2.0;

    // The following constants comes from empirical measurements of the crossover point
    // between the average error of the linear estimator and the adjusted hll estimator
    double crossOver = 0.64;
    if (configK == 16)      { crossOver = 0.718; }
    else if (configK == 32) { crossOver = 0.672; }

    return (avgEst > (crossOver * configK)) ? adjEst : linEst;
  }

  private double getHllBitMapEstimate() { //estimator for when N is small
    final int configK = 1 << lgConfigK;
    return ((curMin != 0) || (numAtCurMin == 0))
        ? configK * Math.log(configK / 0.5)
        : HarmonicNumbers.getBitMapEstimate(configK, configK - numAtCurMin);
  }

  //In C: again-two-registers.c lines hhb_get_hip_estimate_and_bounds L1136-1137
  private double hllHipEps(final double numStdDevs) {
    return (numStdDevs * HLL_HIP_RSE_FACTOR) / Math.sqrt(1 << lgConfigK);
  }

  //In C: giant-file.c lines 1500-1501
  private double hllNonHipEps(final double numStdDevs) {
    return (numStdDevs * HLL_NON_HIP_RSE_FACTOR) / Math.sqrt(1 << lgConfigK);
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
