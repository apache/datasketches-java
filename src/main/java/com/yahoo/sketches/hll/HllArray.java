/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.invPow2;
import static com.yahoo.sketches.hll.HllUtil.MIN_LOG_K;
import static com.yahoo.sketches.hll.PreambleUtil.FAMILY_ID;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARRAY_START;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.SER_VER;
import static com.yahoo.sketches.hll.PreambleUtil.extractCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.extractHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.extractNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.extractSerVer;
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
import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;
import static java.lang.Math.log;
import static java.lang.Math.sqrt;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
abstract class HllArray extends HllSketchImpl {
  private static final double HLL_HIP_RSE_FACTOR = sqrt(log(2.0)); //.8325546
  private static final double HLL_NON_HIP_RSE_FACTOR = sqrt((3.0 * log(2.0)) - 1.0); //1.03896

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
    curMin = that.getCurMin();
    numAtCurMin = that.getNumAtCurMin();
    hipAccum = that.getHipAccum();
    kxq0 = that.kxq0;
    kxq1 = that.kxq1;
    hllByteArr = that.hllByteArr.clone(); //that.hllByteArr should never be null.
    final AuxHashMap thatAuxMap = that.getAuxHashMap();
    auxHashMap = (thatAuxMap != null) ? thatAuxMap.copy() : null;
  }

  @Override
  HllArray copyAs(final TgtHllType tgtHllType) {
    if (tgtHllType == getTgtHllType()) {
      return (HllArray)copy();
    }
    if (tgtHllType == HLL_4) {
      return Hll4Array.convertToHll4(this);
    }
    if (tgtHllType == HLL_6) {
      return Hll6Array.convertToHll6(this);
    }
    return Hll8Array.convertToHll8(this);
  }

  void addToHipAccum(final double delta) {
    hipAccum += delta;
  }

  void decNumAtCurMin() {
    numAtCurMin--;
  }

  AuxHashMap getAuxHashMap() {
    return auxHashMap;
  }

  @Override
  PairIterator getAuxIterator() {
    final AuxHashMap auxHashMap = getAuxHashMap();
    if (auxHashMap != null) { return auxHashMap.getIterator(); }
    return null;
  }

  /**
   * This is the (non-HIP) estimator.
   * It is called "composite" because multiple estimators are pasted together.
   * @return the composite estimate
   */
  //In C: again-two-registers.c hhb_get_composite_estimate L1489
  // Make package private to allow testing.
  @Override
  double getCompositeEstimate() {
    final int lgConfigK = getLgConfigK();
    final double rawEst = getRawEstimate(lgConfigK, getKxQ0() + getKxQ1());

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

    final double linEst = getHllBitMapEstimate(lgConfigK, getCurMin(), getNumAtCurMin());

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

  @Override
  int getCouponCount() {
    return -1;
  }

  @Override
  int getCurMin() {
    return curMin;
  }

  @Override
  int getCurrentSerializationBytes() {
    final AuxHashMap auxHashMap = getAuxHashMap();
    final int auxBytes = (auxHashMap == null) ? 0 : auxHashMap.auxCount << 2;
    return HLL_BYTE_ARRAY_START + getHllByteArr().length + auxBytes;
  }

  @Override
  double getEstimate() {
    if (isOutOfOrderFlag()) {
      return getCompositeEstimate();
    }
    return getHipAccum();
  }

  @Override
  double getHipAccum() {
    return hipAccum;
  }

  byte[] getHllByteArr() {
    return hllByteArr;
  }

  @Override
  abstract PairIterator getIterator();

  @Override
  double getLowerBound(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    final int lgConfigK = getLgConfigK();
    final int configK = 1 << lgConfigK;
    final boolean oooFlag = isOutOfOrderFlag();
    if (lgConfigK > 12) {
      final double tmp;
      if (isOutOfOrderFlag()) {
        final double hllNonHipEps =
            (numStdDev * HLL_NON_HIP_RSE_FACTOR) / Math.sqrt(configK);
        tmp = getCompositeEstimate() / (1.0 + hllNonHipEps);
      } else {
        final double hllHipEps = (numStdDev * HLL_HIP_RSE_FACTOR) / Math.sqrt(configK);
        tmp =  getHipAccum() / (1.0 + hllHipEps);
      }
      double numNonZeros = configK;
      if (getCurMin() == 0) {
        numNonZeros -= getNumAtCurMin();
      }
      return Math.max(tmp, numNonZeros);
    }
    //lgConfigK <= 12
    final double re = RelativeErrorTables.getRelErr(false, oooFlag, lgConfigK, numStdDev);
    return ((oooFlag) ? getCompositeEstimate() : getHipAccum()) / (1.0 + re);
  }

  double getKxQ0() {
    return kxq0;
  }

  double getKxQ1() {
    return kxq1;
  }

  @Override
  int getLgMaxCouponArrInts() {
    return -1; //intentionally illegal value
  }

  @Override
  int getNumAtCurMin() {
    return numAtCurMin;
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

  @Override
  double getUpperBound(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    final int lgConfigK = getLgConfigK();
    final boolean oooFlag = isOutOfOrderFlag();
    if (lgConfigK > 12) {
      if (oooFlag) {
        final double hllNonHipEps =
            (numStdDev * HLL_NON_HIP_RSE_FACTOR) / Math.sqrt(1 << lgConfigK);
        return getCompositeEstimate() / (1.0 - hllNonHipEps);
      }
      final double hllHipEps = (numStdDev * HLL_HIP_RSE_FACTOR) / Math.sqrt(1 << lgConfigK);
      return getHipAccum() / (1.0 - hllHipEps);
    }
    //lgConfigK <= 12
    final double re = RelativeErrorTables.getRelErr(true, oooFlag, lgConfigK, numStdDev);
    return ((oooFlag) ? getCompositeEstimate() : getHipAccum()) / (1.0 + re);
  }

  @Override
  boolean isEmpty() {
    final int configK = 1 << getLgConfigK();
    return (getCurMin() == 0) && (getNumAtCurMin() == configK);
  }

  void putAuxHashMap(final AuxHashMap auxHashMap) {
    this.auxHashMap = auxHashMap;
  }

  void putCurMin(final int curMin) {
    this.curMin = curMin;
  }

  void putHipAccum(final double value) {
    hipAccum = value;
  }

  void putHllByteArr(final byte[] hllByteArr) {
    this.hllByteArr = hllByteArr;
  }

  void putKxQ0(final double kxq0) {
    this.kxq0 = kxq0;
  }

  void putKxQ1(final double kxq1) {
    this.kxq1 = kxq1;
  }

  void putNumAtCurMin(final int numAtCurMin) {
    this.numAtCurMin = numAtCurMin;
  }

  @Override
  byte[] toCompactByteArray() {
    return toByteArray(true);
  }

  @Override
  byte[] toUpdatableByteArray() {
    return toByteArray(false);
  }

  void extractCommon(final HllArray hllArray, final Memory mem, final Object memArr,
      final long memAdd) {
    checkPreamble(mem, memArr, memAdd);
    hllArray.putOutOfOrderFlag(extractOooFlag(memArr, memAdd));
    hllArray.putCurMin(extractCurMin(memArr, memAdd));
    hllArray.putHipAccum(extractHipAccum(memArr, memAdd));
    hllArray.putKxQ0(extractKxQ0(memArr, memAdd));
    hllArray.putKxQ1(extractKxQ1(memArr, memAdd));
    hllArray.putNumAtCurMin(extractNumAtCurMin(memArr, memAdd));

    //load Hll array
    final byte[] hllByteArr = getHllByteArr();
    final int hllArrLen = hllByteArr.length;
    mem.getByteArray(HLL_BYTE_ARRAY_START, hllByteArr, 0, hllArrLen);
  }

  void insertCommon(final byte[] memArr, final long memAdd, final boolean compact) {
    insertPreInts(memArr, memAdd, HLL_PREINTS);
    insertSerVer(memArr, memAdd);
    insertFamilyId(memArr, memAdd);
    insertLgK(memArr, memAdd, getLgConfigK());
    insertLgArr(memArr, memAdd, 0); //not used for HLL mode
    insertEmptyFlag(memArr, memAdd, isEmpty());
    insertCompactFlag(memArr, memAdd, compact);
    insertOooFlag(memArr, memAdd, isOutOfOrderFlag());
    insertCurMin(memArr, memAdd, getCurMin());
    insertCurMode(memArr, memAdd, getCurMode());
    insertTgtHllType(memArr, memAdd, getTgtHllType());
    insertHipAccum(memArr, memAdd, getHipAccum());
    insertKxQ0(memArr, memAdd, getKxQ0());
    insertKxQ1(memArr, memAdd, getKxQ1());
    insertNumAtCurMin(memArr, memAdd, getNumAtCurMin());
  }

  byte[] toByteArray(final boolean compact) {
    final byte[] hllByteArr = getHllByteArr();
    final int hllBytes = hllByteArr.length;
    final int totBytes = HLL_BYTE_ARRAY_START + hllBytes;
    final byte[] memArr = new byte[totBytes];
    final WritableMemory wmem = WritableMemory.wrap(memArr);
    final long memAdd = wmem.getCumulativeOffset(0);
    insertCommon(memArr, memAdd, compact);
    wmem.putByteArray(HLL_BYTE_ARRAY_START, hllByteArr, 0, hllBytes);
    return memArr;
  }

  /**
   * HIP and KxQ incremental update.
   * @param oldValue old value
   * @param newValue new value
   */
  //In C: again-two-registers.c Lines 851 to 871
  void hipAndKxQIncrementalUpdate(final int oldValue, final int newValue) {
    assert newValue > oldValue;
    final int configK = 1 << getLgConfigK();
    //update hipAccum BEFORE updating kxq0 and kxq1
    double kxq0 = getKxQ0();
    double kxq1 = getKxQ1();
    addToHipAccum(configK / (kxq0 + kxq1));
    //update kxq0 and kxq1; subtract first, then add.
    if (oldValue < 32) { putKxQ0(kxq0 -= invPow2(oldValue)); }
    else               { putKxQ1(kxq1 -= invPow2(oldValue)); }
    if (newValue < 32) { putKxQ0(kxq0 += invPow2(newValue)); }
    else               { putKxQ1(kxq1 += invPow2(newValue)); }
  }

  static final void checkPreamble(final Memory mem, final Object memArr, final long memAdd) {
    final int memPreInts = extractPreInts(memArr, memAdd);
    final int serVer = extractSerVer(memArr, memAdd);
    final int famId = extractFamilyId(memArr, memAdd);
    if ( (memPreInts != HLL_PREINTS) || (serVer != SER_VER) || (famId != FAMILY_ID) ) {
      badPreambleState(mem);
    }
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

}
