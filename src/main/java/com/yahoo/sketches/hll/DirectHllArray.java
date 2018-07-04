/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.PreambleUtil.CUR_MIN_COUNT_INT;
import static com.yahoo.sketches.hll.PreambleUtil.HIP_ACCUM_DOUBLE;
import static com.yahoo.sketches.hll.PreambleUtil.extractCompactFlag;
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
import static com.yahoo.sketches.hll.PreambleUtil.insertAuxCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.insertNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertOooFlag;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
abstract class DirectHllArray extends AbstractHllArray {
  WritableMemory wmem;
  Memory mem;
  Object memObj;
  long memAdd;
  final boolean compact;

  //Memory must be already initialized and may have data
  DirectHllArray(final int lgConfigK, final TgtHllType tgtHllType, final WritableMemory wmem) {
    super(lgConfigK, tgtHllType, CurMode.HLL);
    this.wmem = wmem;
    mem = wmem;
    memObj = wmem.getArray();
    memAdd = wmem.getCumulativeOffset(0L);
    compact = extractCompactFlag(mem);
    assert !compact;
  }

  //Memory must already be initialized and should have data
  DirectHllArray(final int lgConfigK, final TgtHllType tgtHllType, final Memory mem) {
    super(lgConfigK, tgtHllType, CurMode.HLL);
    wmem = null;
    this.mem = mem;
    memObj = ((WritableMemory) mem).getArray();
    memAdd = mem.getCumulativeOffset(0L);
    compact = extractCompactFlag(mem);
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
    final double hipAccum = mem.getDouble(HIP_ACCUM_DOUBLE);
    wmem.putDouble(HIP_ACCUM_DOUBLE, hipAccum + delta);
  }

  @Override
  void decNumAtCurMin() {
    int numAtCurMin = mem.getInt(CUR_MIN_COUNT_INT);
    wmem.putInt(CUR_MIN_COUNT_INT, --numAtCurMin);
  }

  @Override
  int getCurMin() {
    return extractCurMin(mem);
  }

  @Override
  CurMode getCurMode() {
    return extractCurMode(mem);
  }

  @Override
  double getHipAccum() {
    return extractHipAccum(mem);
  }

  @Override
  double getKxQ0() {
    return extractKxQ0(mem);
  }

  @Override
  double getKxQ1() {
    return extractKxQ1(mem);
  }

  @Override
  int getLgConfigK() {
    return extractLgK(mem);
  }

  @Override
  AuxHashMap getNewAuxHashMap() {
    return new DirectAuxHashMap(this, true);
  }

  @Override
  int getNumAtCurMin() {
    return extractNumAtCurMin(mem);
  }

  @Override
  TgtHllType getTgtHllType() {
    return extractTgtHllType(mem);
  }

  @Override
  WritableMemory getWritableMemory() {
    return wmem;
  }

  @Override
  boolean isCompact() {
    return compact;
  }

  @Override
  boolean isEmpty() {
    return extractEmptyFlag(mem);
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
    return extractOooFlag(mem);
  }

  @Override
  boolean isSameResource(final Memory mem) {
    return this.mem.isSameResource(mem);
  }

  @Override
  void putAuxHashMap(final AuxHashMap auxHashMap, final boolean compact) {
    if (auxHashMap instanceof HeapAuxHashMap) {
      if (compact) {
        this.auxHashMap = auxHashMap; //heap and compact
      } else { //heap and not compact
        final int[] auxArr = auxHashMap.getAuxIntArr();
        wmem.putIntArray(auxStart, auxArr, 0, auxArr.length);
        insertLgArr(wmem, auxHashMap.getLgAuxArrInts());
        insertAuxCount(wmem, auxHashMap.getAuxCount());
        this.auxHashMap = new DirectAuxHashMap(this, false);
      }
    } else { //DirectAuxHashMap
      assert !compact; //must not be compact
      this.auxHashMap = auxHashMap;
    }
  }

  @Override
  void putCurMin(final int curMin) {
    insertCurMin(wmem, curMin);
  }

  @Override
  void putHipAccum(final double hipAccum) {
    insertHipAccum(wmem, hipAccum);
  }

  @Override
  void putKxQ0(final double kxq0) {
    insertKxQ0(wmem, kxq0);
  }

  @Override //called very very very rarely
  void putKxQ1(final double kxq1) {
    insertKxQ1(wmem, kxq1);
  }

  @Override
  void putNumAtCurMin(final int numAtCurMin) {
    insertNumAtCurMin(wmem, numAtCurMin);
  }

  @Override //not used on the direct side
  void putOutOfOrderFlag(final boolean oooFlag) {
    insertOooFlag(wmem, oooFlag);
  }

  @Override //used by HLL6 and HLL8, overridden by HLL4
  byte[] toCompactByteArray() {
    return toUpdatableByteArray(); //indistinguishable for HLL6 and HLL8
  }

  @Override //used by HLL6 and HLL8, overridden by HLL4
  byte[] toUpdatableByteArray() {
    final int totBytes = getCompactSerializationBytes();
    final byte[] byteArr = new byte[totBytes];
    final WritableMemory memOut = WritableMemory.wrap(byteArr);
    mem.copyTo(0, memOut, 0, totBytes);
    insertCompactFlag(memOut, false);
    return byteArr;
  }

  @Override
  HllSketchImpl reset() {
    if (wmem == null) {
      throw new SketchesArgumentException("Cannot reset a read-only sketch");
    }
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    wmem.clear(0, bytes);
    return DirectCouponList.newInstance(lgConfigK, tgtHllType, wmem);
  }
}
