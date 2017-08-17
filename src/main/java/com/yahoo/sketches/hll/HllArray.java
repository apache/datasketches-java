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

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
abstract class HllArray extends AbstractHllArray {
  boolean oooFlag = false; //Out-Of-Order Flag
  int curMin; //only changed by Hll4Array
  int numAtCurMin;
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
    hllByteArr = that.getHllByteArr().clone(); //that.hllByteArr should never be null.
    final AuxHashMap thatAuxMap = that.getAuxHashMap();
    auxHashMap = (thatAuxMap != null) ? thatAuxMap.copy() : null;
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
  byte[] getHllByteArr() {
    return hllByteArr;
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
  Memory getMemory() {
    return null;
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
  void putAuxHashMap(final AuxHashMap auxHashMap) {
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

  void putHllByteArr(final byte[] hllByteArr) {
    this.hllByteArr = hllByteArr;
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

  //used by heapify by all Heap HLL
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
    srcMem.getByteArray(HLL_BYTE_ARR_START, hllByteArr, 0, hllArrLen);
  }

}
