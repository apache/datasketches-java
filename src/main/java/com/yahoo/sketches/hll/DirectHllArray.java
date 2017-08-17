/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.memory.UnsafeUtil.unsafe;
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
  int getCurMin() {
    return extractCurMin(memObj, memAdd);
  }

  @Override
  CurMode getCurMode() {
    return extractCurMode(memObj, memAdd);
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
  Memory getMemory() {
    return mem;
  }

  @Override
  AuxHashMap getNewAuxHashMap() {
    return new DirectAuxHashMap(this, true);
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

  @Override
  void putAuxHashMap(final AuxHashMap auxHashMap) {
    this.auxHashMap = auxHashMap;
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
}
