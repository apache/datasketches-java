/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hll;

import static org.apache.datasketches.hll.PreambleUtil.CUR_MIN_COUNT_INT;
import static org.apache.datasketches.hll.PreambleUtil.HIP_ACCUM_DOUBLE;
import static org.apache.datasketches.hll.PreambleUtil.extractCompactFlag;
import static org.apache.datasketches.hll.PreambleUtil.extractCurMin;
import static org.apache.datasketches.hll.PreambleUtil.extractCurMode;
import static org.apache.datasketches.hll.PreambleUtil.extractEmptyFlag;
import static org.apache.datasketches.hll.PreambleUtil.extractHipAccum;
import static org.apache.datasketches.hll.PreambleUtil.extractKxQ0;
import static org.apache.datasketches.hll.PreambleUtil.extractKxQ1;
import static org.apache.datasketches.hll.PreambleUtil.extractLgK;
import static org.apache.datasketches.hll.PreambleUtil.extractNumAtCurMin;
import static org.apache.datasketches.hll.PreambleUtil.extractOooFlag;
import static org.apache.datasketches.hll.PreambleUtil.extractRebuildCurMinNumKxQFlag;
import static org.apache.datasketches.hll.PreambleUtil.extractTgtHllType;
import static org.apache.datasketches.hll.PreambleUtil.insertAuxCount;
import static org.apache.datasketches.hll.PreambleUtil.insertCompactFlag;
import static org.apache.datasketches.hll.PreambleUtil.insertCurMin;
import static org.apache.datasketches.hll.PreambleUtil.insertEmptyFlag;
import static org.apache.datasketches.hll.PreambleUtil.insertHipAccum;
import static org.apache.datasketches.hll.PreambleUtil.insertKxQ0;
import static org.apache.datasketches.hll.PreambleUtil.insertKxQ1;
import static org.apache.datasketches.hll.PreambleUtil.insertLgArr;
import static org.apache.datasketches.hll.PreambleUtil.insertNumAtCurMin;
import static org.apache.datasketches.hll.PreambleUtil.insertOooFlag;
import static org.apache.datasketches.hll.PreambleUtil.insertRebuildCurMinNumKxQFlag;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

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
    insertEmptyFlag(wmem, false);
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
    checkReadOnly(wmem);
    final double hipAccum = mem.getDouble(HIP_ACCUM_DOUBLE);
    wmem.putDouble(HIP_ACCUM_DOUBLE, hipAccum + delta);
  }

  @Override
  void decNumAtCurMin() {
    checkReadOnly(wmem);
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
  Memory getMemory() {
    return mem;
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
  boolean isOutOfOrder() {
    return extractOooFlag(mem);
  }

  @Override
  boolean isSameResource(final Memory mem) {
    return this.mem.isSameResource(mem);
  }

  @Override
  boolean isRebuildCurMinNumKxQFlag() {
    return extractRebuildCurMinNumKxQFlag(mem);
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
      this.auxHashMap = auxHashMap; //In case of read-only this works.
    }
  }

  @Override
  void putCurMin(final int curMin) {
    checkReadOnly(wmem);
    insertCurMin(wmem, curMin);
  }

  @Override
  void putEmptyFlag(final boolean empty) {
    checkReadOnly(wmem);
    insertEmptyFlag(wmem, empty);
  }

  @Override
  void putHipAccum(final double hipAccum) {
    checkReadOnly(wmem);
    insertHipAccum(wmem, hipAccum);
  }

  @Override
  void putKxQ0(final double kxq0) {
    checkReadOnly(wmem);
    insertKxQ0(wmem, kxq0);
  }

  @Override //called very very very rarely
  void putKxQ1(final double kxq1) {
    checkReadOnly(wmem);
    insertKxQ1(wmem, kxq1);
  }

  @Override
  void putNumAtCurMin(final int numAtCurMin) {
    checkReadOnly(wmem);
    insertNumAtCurMin(wmem, numAtCurMin);
  }

  @Override //not used on the direct side
  void putOutOfOrder(final boolean oooFlag) {
    if (oooFlag) { putHipAccum(0); }
    checkReadOnly(wmem);
    insertOooFlag(wmem, oooFlag);
  }

  @Override
  void putRebuildCurMinNumKxQFlag(final boolean rebuild) {
    checkReadOnly(wmem);
    insertRebuildCurMinNumKxQFlag(wmem, rebuild);
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
    checkReadOnly(wmem);
    insertEmptyFlag(wmem, true);
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    wmem.clear(0, bytes);
    return DirectCouponList.newInstance(lgConfigK, tgtHllType, wmem);
  }

  private static final void checkReadOnly(final WritableMemory wmem) {
    if (wmem == null) {
      throw new SketchesArgumentException("Cannot modify a read-only sketch");
    }
  }
}
