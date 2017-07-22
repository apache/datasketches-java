/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.invPow2;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.HLL_HIP_RSE_FACTOR;
import static com.yahoo.sketches.hll.HllUtil.HLL_NON_HIP_RSE_FACTOR;
import static com.yahoo.sketches.hll.HllUtil.MIN_LOG_K;
import static com.yahoo.sketches.hll.PreambleUtil.AUX_COUNT_INT;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARRAY_START;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractCurMode;
import static com.yahoo.sketches.hll.PreambleUtil.extractHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.extractInt;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.extractListCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractOooFlag;
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
 * @author Kevin Lang
 */
abstract class HllArray extends AbstractHllArray {
  final int lgConfigK;
  final TgtHllType tgtHllType;
  final CurMode curMode;
  boolean oooFlag = false; //Out-Of-Order Flag
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
    this.lgConfigK = lgConfigK;
    this.tgtHllType = tgtHllType;
    curMode = CurMode.HLL;
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
    lgConfigK = that.getLgConfigK();
    tgtHllType = that.getTgtHllType();
    curMode = that.getCurMode();
    oooFlag = that.isOutOfOrderFlag();
    curMin = that.getCurMin();
    numAtCurMin = that.getNumAtCurMin();
    hipAccum = that.getHipAccum();
    kxq0 = that.getKxQ0();
    kxq1 = that.getKxQ1();
    hllByteArr = that.getHllByteArr().clone(); //that.hllByteArr should never be null.
    final AuxHashMap thatAuxMap = that.getAuxHashMap();
    auxHashMap = (thatAuxMap != null) ? thatAuxMap.copy() : null;
  }

  static final void heapifyMemCouponsToHll(final Memory mem, final HllArray hllArray) {
    final Object memObj = ((WritableMemory) mem).getArray();
    final long memAdd = mem.getCumulativeOffset(0);
    final CurMode curMode = extractCurMode(memObj, memAdd);
    final int couponCount = extractListCount(memObj, memAdd);
    final int couponArrInts = 1 << extractLgArr(memObj, memAdd);
    final int start = (curMode == CurMode.LIST) ? LIST_INT_ARR_START : HASH_SET_INT_ARR_START;
    int cnt = 0;
    for (int i = 0; i < couponArrInts; i++) {
      final int coupon = extractInt(memObj, memAdd, start + (i << 2));
      if (coupon == EMPTY) { continue; }
      hllArray.couponUpdate(coupon);
      cnt++;
    }
    hllArray.putHipAccum(CouponList.getEstimate(couponCount));
    hllArray.putOutOfOrderFlag(false);
    assert cnt == couponCount;
  }

  //Src can be either list or set.
  //Used by CouponList, CouponHashSet
  static final HllSketchImpl morphHeapCouponsToHll(final CouponList src) {
    final HllArray tgtHllArr = HllArray.newHll(src.lgConfigK, src.tgtHllType);
    final PairIterator srcItr = src.getIterator();
    while (srcItr.nextValid()) {
      tgtHllArr.couponUpdate(srcItr.getPair());
    }
    tgtHllArr.putHipAccum(src.getEstimate());
    tgtHllArr.putOutOfOrderFlag(false);
    return tgtHllArr;
  }

  static final HllArray newHll(final int lgConfigK, final TgtHllType tgtHllType) {
    if (tgtHllType == HLL_4) { return new Hll4Array(lgConfigK); }
    if (tgtHllType == HLL_6) { return new Hll6Array(lgConfigK); }
    return new Hll8Array(lgConfigK);
  }

  @Override
  HllArray copyAs(final TgtHllType tgtHllType) {
    if (tgtHllType == getTgtHllType()) {
      return (HllArray) copy();
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

  @Override
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
  int getCurMin() {
    return curMin;
  }

  @Override
  CurMode getCurMode() {
    return curMode;
  }

  @Override
  int getCompactSerializationBytes() {
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

  @Override
  byte[] getHllByteArr() {
    return hllByteArr;
  }

  @Override
  abstract PairIterator getIterator();

  @Override
  double getKxQ0() {
    return kxq0;
  }

  @Override
  double getKxQ1() {
    return kxq1;
  }

  @Override
  int getLgConfigK() {
    return lgConfigK;
  }

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
  TgtHllType getTgtHllType() {
    return tgtHllType;
  }

  @Override
  int getUpdatableSerializationBytes() {
    final AuxHashMap auxHashMap = getAuxHashMap();
    final int auxBytes = (auxHashMap == null) ? 0 : 4 << auxHashMap.lgAuxArrSize;
    return HLL_BYTE_ARRAY_START + hllByteArr.length + auxBytes;
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

  @Override
  boolean isOutOfOrderFlag() {
    return oooFlag;
  }

  @Override
  void populateHllByteArrFromMem(final Memory srcMem, final int lenBytes) {
    //TODO
  }

  @Override
  void populateMemFromHllByteArr(final WritableMemory dstWmem, final int lenBytes) {
    //TODO
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
  void putOutOfOrderFlag(final boolean oooFlag) {
    this.oooFlag = oooFlag;
  }

  @Override
  byte[] toCompactByteArray() {
    return toByteArray(this, true);
  }

  @Override
  byte[] toUpdatableByteArray() {
    return toByteArray(this, false);
  }

  static final byte[] toByteArray(final AbstractHllArray impl, final boolean compact) {
    int auxBytes = 0;
    final AuxHashMap auxHashMap = impl.getAuxHashMap();
    if (auxHashMap != null) { //only relevant for HLL_4
      auxBytes = (compact) ? auxHashMap.getCompactedSizeBytes()
          : auxHashMap.getUpdatableSizeBytes();
    }
    final int totBytes = HLL_BYTE_ARRAY_START + impl.getHllByteArr().length + auxBytes;
    final byte[] byteArr = new byte[totBytes];
    final WritableMemory wmem = WritableMemory.wrap(byteArr);
    insertHll(impl, wmem, compact);
    return byteArr;
  }

  // used by toByteArray and DirectHllArray
  static final void insertHll(final AbstractHllArray impl, final WritableMemory wmem,
      final boolean compact) {
    insertCommonHll(impl, wmem, compact);
    final byte[] hllByteArr = impl.getHllByteArr();
    wmem.putByteArray(HLL_BYTE_ARRAY_START, hllByteArr, 0, hllByteArr.length);

    if (impl.getAuxHashMap() != null) {
      insertAux(impl, wmem, compact);
    } else {
      wmem.putInt(AUX_COUNT_INT, 0);
    }
  }

  static final void insertAux(final AbstractHllArray impl, final WritableMemory wmem,
      final boolean compact) {
    final Object memObj = wmem.getArray();
    final long memAdd = wmem.getCumulativeOffset(0L);
    final AuxHashMap auxHashMap = impl.getAuxHashMap();
    final int auxCount = auxHashMap.auxCount;
    insertAuxCount(memObj, memAdd, auxCount);
    final long auxStart = HLL_BYTE_ARRAY_START + impl.getHllByteArr().length;
    if (compact) {
      final PairIterator itr = auxHashMap.getIterator();
      int cnt = 0;
      while (itr.nextValid()) {
        insertInt(memObj, memAdd, auxStart + (cnt++ << 2), itr.getPair());
      }
      assert cnt == auxCount;
    } else { //updatable
      wmem.putIntArray(auxStart, auxHashMap.auxIntArr, 0, auxHashMap.auxIntArr.length);
    }
  }

  static final void insertCommonHll(final AbstractHllArray impl, final WritableMemory wmem,
      final boolean compact) {
    final Object memObj = wmem.getArray();
    final long memAdd = wmem.getCumulativeOffset(0L);
    insertPreInts(memObj, memAdd, HLL_PREINTS);
    insertSerVer(memObj, memAdd);
    insertFamilyId(memObj, memAdd);
    insertLgK(memObj, memAdd, impl.getLgConfigK());
    insertLgArr(memObj, memAdd, 0); //not used for HLL mode
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

  static final void extractCommonHll(final HllArray hllArray, final Memory srcMem,
      final Object memArr, final long memAdd) {
    hllArray.putOutOfOrderFlag(extractOooFlag(memArr, memAdd));
    hllArray.putCurMin(extractCurMin(memArr, memAdd));
    hllArray.putHipAccum(extractHipAccum(memArr, memAdd));
    hllArray.putKxQ0(extractKxQ0(memArr, memAdd));
    hllArray.putKxQ1(extractKxQ1(memArr, memAdd));
    hllArray.putNumAtCurMin(extractNumAtCurMin(memArr, memAdd));

    //load Hll array
    final byte[] hllByteArr = hllArray.getHllByteArr();
    final int hllArrLen = hllByteArr.length;
    srcMem.getByteArray(HLL_BYTE_ARRAY_START, hllByteArr, 0, hllArrLen);
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

  //Used by union operator.  Always copies or downsamples to HLL_8.
  //Caller must ultimately manage oooFlag, as caller has more info
  static final HllSketchImpl copyOrDownsampleHll(
      final HllSketchImpl srcSketch, final int tgtLgK) {
    final HllArray src = (HllArray) srcSketch;
    final int srcLgK = src.getLgConfigK();
    if ((srcLgK <= tgtLgK) && (src.getTgtHllType() == TgtHllType.HLL_8)) {
      return src.copy();
    }
    final int minLgK = Math.min(srcLgK, tgtLgK);
    final HllArray tgtHllArr = HllArray.newHll(minLgK, TgtHllType.HLL_8);
    final PairIterator srcItr = src.getIterator();
    while (srcItr.nextValid()) {
      tgtHllArr.couponUpdate(srcItr.getPair());
    }
    //both of these are required for isomorphism
    tgtHllArr.putHipAccum(src.getHipAccum());
    tgtHllArr.putOutOfOrderFlag(src.isOutOfOrderFlag());
    return tgtHllArr;
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
