/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.memory.UnsafeUtil.unsafe;
import static com.yahoo.sketches.Util.invPow2;
import static com.yahoo.sketches.hll.PreambleUtil.CUR_MIN_COUNT_INT;
import static com.yahoo.sketches.hll.PreambleUtil.HIP_ACCUM_DOUBLE;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractCurMode;
import static com.yahoo.sketches.hll.PreambleUtil.extractEmptyFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.extractKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;
import static com.yahoo.sketches.hll.PreambleUtil.extractNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.extractOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractTgtHllType;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.insertNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertOooFlag;
import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
abstract class DirectHllArray extends AbstractHllArray {
  final long auxArrOffset;
  WritableMemory wmem;
  Memory mem;
  Object memObj;
  long memAdd;
  AuxHashMap directAuxHashMap = null;


  //Memory must be initialized, may have data
  DirectHllArray(final int lgConfigK, final TgtHllType tgtHllType, final WritableMemory wmem) {
    super(lgConfigK, tgtHllType, CurMode.HLL);
    this.wmem = wmem;
    mem = wmem;
    memObj = wmem.getArray();
    memAdd = wmem.getCumulativeOffset(0L);
    auxArrOffset = HLL_BYTE_ARR_START + (1 << (lgConfigK - 1));
  }

  //Memory must be initialized, should have data
  DirectHllArray(final int lgConfigK, final TgtHllType tgtHllType, final Memory mem) {
    super(lgConfigK, tgtHllType, CurMode.HLL);
    wmem = null;
    this.mem = mem;
    memObj = ((WritableMemory) mem).getArray();
    memAdd = mem.getCumulativeOffset(0L);
    auxArrOffset = HLL_BYTE_ARR_START + (1 << (lgConfigK - 1));
  }

  //only called by DirectAuxHashMap
  final void updateMemory(final WritableMemory newWmem) {
    wmem = newWmem;
    mem = newWmem;
    memObj = wmem.getArray();
    memAdd = wmem.getCumulativeOffset(0L);
  }

  @Override
  HllArray copyAs(final TgtHllType tgtHllType) {
    if (tgtHllType == getTgtHllType()) {
      return (HllArray) copy();
    }
    if (tgtHllType == HLL_4) {
      return Hll4Array.convertToHll4(Hll4Array.heapify(mem));
    }
    if (tgtHllType == HLL_6) {
      return Hll6Array.convertToHll6(Hll6Array.heapify(mem));
    }
    return Hll8Array.convertToHll8(Hll8Array.heapify(mem));
  }

  @Override
  void addToHipAccum(final double delta) {
    final double hipAccum = unsafe.getDouble(memObj, memAdd + HIP_ACCUM_DOUBLE);
    unsafe.putDouble(memObj, memAdd + HIP_ACCUM_DOUBLE, hipAccum + delta);
  }

  @Override
  void decNumAtCurMin() {
    int numAtCurMin = unsafe.getInt(memObj, memAdd + CUR_MIN_COUNT_INT);
    unsafe.putInt(memObj, memAdd + CUR_MIN_COUNT_INT, --numAtCurMin);
  }

  @Override
  AuxHashMap getAuxHashMap() {
    return directAuxHashMap;
  }

  @Override
  PairIterator getAuxIterator() {
    return (directAuxHashMap == null) ? null : directAuxHashMap.getIterator();
  }

  @Override
  int getCurMin() {
    return extractCurMin(memObj, memAdd);
  }

  @Override
  CurMode getCurMode() {
    return extractCurMode(memObj, memAdd);
  }

  @Override
  double getCompositeEstimate() {
    return HllArray.compositeEstimate(this);
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
    return extractHipAccum(memObj, memAdd);
  }

  @Override
  byte[] getHllByteArr() { //not allowed
    return null;
  }

  @Override
  abstract int getHllByteArrBytes();

  @Override
  abstract PairIterator getIterator();

  @Override
  double getKxQ0() {
    return extractKxQ0(memObj, memAdd);
  }

  @Override
  double getKxQ1() {
    return extractKxQ1(memObj, memAdd);
  }

  @Override
  int getLgConfigK() {
    return extractLgK(memObj, memAdd);
  }

  @Override
  double getLowerBound(final int numStdDev) {
    return HllArray.lowerBound(this, numStdDev);
  }

  @Override
  Memory getMemory() {
    return mem;
  }

  @Override
  int getNumAtCurMin() {
    return extractNumAtCurMin(memObj, memAdd);
  }

  @Override
  TgtHllType getTgtHllType() {
    return extractTgtHllType(memObj, memAdd);
  }

  @Override
  double getUpperBound(final int numStdDev) {
    return HllArray.upperBound(this, numStdDev);
  }

  @Override
  boolean isEmpty() {
    return extractEmptyFlag(memObj, memAdd);
  }

  @Override
  boolean isMemory() {
    return true;
  }

  @Override
  boolean isOffHeap() {
    return mem.isDirect();
  }

  @Override
  boolean isOutOfOrderFlag() {
    return extractOooFlag(memObj, memAdd);
  }

  //  @Override
  //  void putHllBytesFromMemory(final Memory srcMem, final int lenBytes) {
  //    // TODO Auto-generated method stub
  //  }
  //
  //  @Override
  //  void getHllBytesToMemory(final WritableMemory dstWmem, final int lenBytes) {
  //    // TODO Auto-generated method stub
  //  }

  @Override
  void putAuxHashMap(final AuxHashMap auxHashMap) {
    directAuxHashMap = auxHashMap;
  }

  @Override
  void putCurMin(final int curMin) {
    insertCurMin(memObj, memAdd, curMin);
  }

  @Override
  void putHipAccum(final double hipAccum) {
    insertHipAccum(memObj, memAdd, hipAccum);
  }

  @Override
  void putKxQ0(final double kxq0) {
    insertKxQ0(memObj, memAdd, kxq0);
  }

  @Override
  void putKxQ1(final double kxq1) {
    insertKxQ1(memObj, memAdd, kxq1);
  }

  @Override
  void putNumAtCurMin(final int numAtCurMin) {
    insertNumAtCurMin(memObj, memAdd, numAtCurMin);
  }

  @Override
  void putOutOfOrderFlag(final boolean oooFlag) {
    insertOooFlag(memObj, memAdd, oooFlag);
  }

  @Override
  byte[] toCompactByteArray() {
    return HllArray.toByteArray(this, true);
  }

  @Override
  byte[] toUpdatableByteArray() {
    return HllArray.toByteArray(this, false);
  }

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

}
