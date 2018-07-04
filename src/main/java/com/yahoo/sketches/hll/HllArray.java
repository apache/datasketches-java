/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.extractNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractOooFlag;
import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
abstract class HllArray extends AbstractHllArray {
  boolean oooFlag = false; //Out-Of-Order Flag
  int curMin; //always zero for Hll6 and Hll8, only used / tracked by Hll4Array
  int numAtCurMin; //interpreted as num zeros when curMin == 0
  double hipAccum;
  double kxq0;
  double kxq1;
  byte[] hllByteArr = null; //init by sub-classes

  /**
   * Standard constructor for new instance
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
    super(that.getLgConfigK(), that.getTgtHllType(), CurMode.HLL);
    oooFlag = that.isOutOfOrderFlag();
    curMin = that.getCurMin();
    numAtCurMin = that.getNumAtCurMin();
    hipAccum = that.getHipAccum();
    kxq0 = that.getKxQ0();
    kxq1 = that.getKxQ1();
    hllByteArr = that.hllByteArr.clone(); //that.hllByteArr should never be null.
    final AuxHashMap thatAuxMap = that.getAuxHashMap();
    if (thatAuxMap != null) {
      putAuxHashMap(thatAuxMap.copy(), false);
    } else {
      putAuxHashMap(null, false);
    }
  }

  static final HllArray newHeapHll(final int lgConfigK, final TgtHllType tgtHllType) {
    if (tgtHllType == HLL_4) { return new Hll4Array(lgConfigK); }
    if (tgtHllType == HLL_6) { return new Hll6Array(lgConfigK); }
    return new Hll8Array(lgConfigK);
  }

  @Override
  void addToHipAccum(final double delta) {
    hipAccum += delta;
  }

  @Override //used by HLL6 and HLL8, overridden by HLL4
  HllSketchImpl couponUpdate(final int coupon) {
    final int configKmask = (1 << getLgConfigK()) - 1;
    final int slotNo = HllUtil.getLow26(coupon) & configKmask;
    final int newVal = HllUtil.getValue(coupon);
    assert newVal > 0;

    final int curVal = getSlot(slotNo);
    if (newVal > curVal) {
      putSlot(slotNo, newVal);
      hipAndKxQIncrementalUpdate(this, curVal, newVal);
      if (curVal == 0) {
        decNumAtCurMin(); //interpret numAtCurMin as num Zeros
        assert getNumAtCurMin() >= 0;
      }
    }
    return this;
  }

  @Override
  void decNumAtCurMin() {
    numAtCurMin--;
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
  double getHipAccum() {
    return hipAccum;
  }

  @Override
  int getHllByteArrBytes() {
    return hllByteArr.length;
  }

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
  AuxHashMap getNewAuxHashMap() {
    return new HeapAuxHashMap(LG_AUX_ARR_INTS[lgConfigK], lgConfigK);
  }

  @Override
  int getNumAtCurMin() {
    return numAtCurMin;
  }

  @Override
  WritableMemory getWritableMemory() {
    return null;
  }

  @Override
  boolean isCompact() {
    return false;
  }

  @Override
  boolean isEmpty() {
    final int configK = 1 << getLgConfigK();
    return (getCurMin() == 0) && (getNumAtCurMin() == configK);
  }

  @Override
  boolean isMemory() {
    return false;
  }

  @Override
  boolean isOffHeap() {
    return false;
  }

  @Override
  boolean isOutOfOrderFlag() {
    return oooFlag;
  }

  @Override
  boolean isSameResource(final Memory mem) {
    return false;
  }

  @Override
  void putAuxHashMap(final AuxHashMap auxHashMap, final boolean compact) {
    this.auxHashMap = auxHashMap;
  }

  @Override
  void putCurMin(final int curMin) {
    this.curMin = curMin;
  }

  @Override
  void putHipAccum(final double value) {
    hipAccum = value;
  }

  @Override
  void putKxQ0(final double kxq0) {
    this.kxq0 = kxq0;
  }

  @Override
  void putKxQ1(final double kxq1) {
    this.kxq1 = kxq1;
  }

  @Override
  void putNumAtCurMin(final int numAtCurMin) {
    this.numAtCurMin = numAtCurMin;
  }

  @Override
  void putOutOfOrderFlag(final boolean oooFlag) {
    this.oooFlag = oooFlag;
  }

  @Override
  HllSketchImpl reset() {
    return new CouponList(lgConfigK, tgtHllType, CurMode.LIST);
  }

  @Override //used by HLL6 and HLL8, overridden by HLL4
  byte[] toCompactByteArray() {
    return toUpdatableByteArray(); //indistinguishable for HLL6 and HLL8
  }

  @Override //used by HLL4, HLL6 and HLL8
  byte[] toUpdatableByteArray() {
    return ToByteArrayImpl.toHllByteArray(this, false);
  }


  //used by heapify by all Heap HLL
  static final void extractCommonHll(final Memory srcMem, final HllArray hllArray) {
    hllArray.putOutOfOrderFlag(extractOooFlag(srcMem));
    hllArray.putCurMin(extractCurMin(srcMem));
    hllArray.putHipAccum(extractHipAccum(srcMem));
    hllArray.putKxQ0(extractKxQ0(srcMem));
    hllArray.putKxQ1(extractKxQ1(srcMem));
    hllArray.putNumAtCurMin(extractNumAtCurMin(srcMem));

    //load Hll array
    srcMem.getByteArray(HLL_BYTE_ARR_START, hllArray.hllByteArr, 0, hllArray.hllByteArr.length);
  }

}
