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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.common.Util.clear;
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

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
abstract class DirectHllArray extends AbstractHllArray {
  MemorySegment wseg; //used for writable direct
  MemorySegment seg;  //used for compact, read-only direct
  Object segObj;      //used temporarily for byte-array
  final boolean compact;

  private static int checkSegCompactFlag(final MemorySegment wseg, final int lgConfigK) {
    assert !extractCompactFlag(wseg);
    return lgConfigK;
  }

  //Memory must be already initialized and may have data. Writable, must not be Compact.
  DirectHllArray(final int lgConfigK, final TgtHllType tgtHllType, final MemorySegment wseg) {
    super(checkSegCompactFlag(wseg, lgConfigK), tgtHllType, CurMode.HLL);
    this.wseg = wseg;
    seg = wseg;
    segObj = wseg.toArray(JAVA_BYTE);
    compact = extractCompactFlag(seg);
    insertEmptyFlag(wseg, false);
  }

  //Memory must already be initialized and should have data. Read-only. May be Compact or not
  DirectHllArray(final int lgConfigK, final TgtHllType tgtHllType, final MemorySegment seg, final boolean readOnly) {
    super(lgConfigK, tgtHllType, CurMode.HLL);
    wseg = null;
    this.seg = seg;
    segObj = seg.toArray(JAVA_BYTE);
    compact = extractCompactFlag(seg);
  }

  //only called by DirectAuxHashMap
  final void updateMemorySegment(final MemorySegment newWseg) {
    wseg = newWseg;
    seg = newWseg;
    segObj = wseg.toArray(JAVA_BYTE);
  }

  @Override
  void addToHipAccum(final double delta) {
    checkReadOnly(wseg);
    final double hipAccum = seg.get(JAVA_DOUBLE_UNALIGNED, HIP_ACCUM_DOUBLE);
    wseg.set(JAVA_DOUBLE_UNALIGNED, HIP_ACCUM_DOUBLE, hipAccum + delta);
  }

  @Override
  void decNumAtCurMin() {
    checkReadOnly(wseg);
    int numAtCurMin = seg.get(JAVA_INT_UNALIGNED, CUR_MIN_COUNT_INT);
    wseg.set(JAVA_INT_UNALIGNED, CUR_MIN_COUNT_INT, --numAtCurMin);
  }

  @Override
  int getCurMin() {
    return extractCurMin(seg);
  }

  @Override
  CurMode getCurMode() {
    return extractCurMode(seg);
  }

  @Override
  double getHipAccum() {
    return extractHipAccum(seg);
  }

  @Override
  double getKxQ0() {
    return extractKxQ0(seg);
  }

  @Override
  double getKxQ1() {
    return extractKxQ1(seg);
  }

  @Override
  int getLgConfigK() {
    return extractLgK(seg);
  }

  @Override
  MemorySegment getMemorySegment() {
    return seg;
  }

  @Override
  AuxHashMap getNewAuxHashMap() {
    return new DirectAuxHashMap(this, true);
  }

  @Override
  int getNumAtCurMin() {
    return extractNumAtCurMin(seg);
  }

  @Override
  TgtHllType getTgtHllType() {
    return extractTgtHllType(seg);
  }

  @Override
  boolean isCompact() {
    return compact;
  }

  @Override
  boolean isEmpty() {
    return extractEmptyFlag(seg);
  }

  @Override
  boolean hasMemorySegment() {
    return seg.scope().isAlive();
  }

  @Override
  boolean isOffHeap() {
    return seg.isNative();
  }

  @Override
  boolean isOutOfOrder() {
    return extractOooFlag(seg);
  }

  @Override
  boolean isSameResource(final MemorySegment seg) {
    return MemorySegmentStatus.isSameResource(this.seg, seg);
  }

  @Override
  boolean isRebuildCurMinNumKxQFlag() {
    return extractRebuildCurMinNumKxQFlag(seg);
  }

  @Override
  void putAuxHashMap(final AuxHashMap auxHashMap, final boolean compact) {
    if (auxHashMap instanceof HeapAuxHashMap) {
      if (compact) {
        this.auxHashMap = auxHashMap; //heap and compact
      } else { //heap and not compact
        final int[] auxArr = auxHashMap.getAuxIntArr();
        MemorySegment.copy(auxArr, 0, wseg, JAVA_INT_UNALIGNED, auxStart, auxArr.length);
        insertLgArr(wseg, auxHashMap.getLgAuxArrInts());
        insertAuxCount(wseg, auxHashMap.getAuxCount());
        this.auxHashMap = new DirectAuxHashMap(this, false);
      }
    } else { //DirectAuxHashMap
      assert !compact; //must not be compact
      this.auxHashMap = auxHashMap; //In case of read-only this works.
    }
  }

  @Override
  void putCurMin(final int curMin) {
    checkReadOnly(wseg);
    insertCurMin(wseg, curMin);
  }

  @Override
  void putEmptyFlag(final boolean empty) {
    checkReadOnly(wseg);
    insertEmptyFlag(wseg, empty);
  }

  @Override
  void putHipAccum(final double hipAccum) {
    checkReadOnly(wseg);
    insertHipAccum(wseg, hipAccum);
  }

  @Override
  void putKxQ0(final double kxq0) {
    checkReadOnly(wseg);
    insertKxQ0(wseg, kxq0);
  }

  @Override //called very very very rarely
  void putKxQ1(final double kxq1) {
    checkReadOnly(wseg);
    insertKxQ1(wseg, kxq1);
  }

  @Override
  void putNumAtCurMin(final int numAtCurMin) {
    checkReadOnly(wseg);
    insertNumAtCurMin(wseg, numAtCurMin);
  }

  @Override //not used on the direct side
  void putOutOfOrder(final boolean oooFlag) {
    if (oooFlag) { putHipAccum(0); }
    checkReadOnly(wseg);
    insertOooFlag(wseg, oooFlag);
  }

  @Override
  void putRebuildCurMinNumKxQFlag(final boolean rebuild) {
    checkReadOnly(wseg);
    insertRebuildCurMinNumKxQFlag(wseg, rebuild);
  }

  @Override //used by HLL6 and HLL8, overridden by HLL4
  byte[] toCompactByteArray() {
    return toUpdatableByteArray(); //indistinguishable for HLL6 and HLL8
  }

  @Override //used by HLL6 and HLL8, overridden by HLL4
  byte[] toUpdatableByteArray() {
    final int totBytes = getCompactSerializationBytes();
    final byte[] byteArr = new byte[totBytes];
    final MemorySegment segOut = MemorySegment.ofArray(byteArr);
    MemorySegment.copy(seg, 0, segOut, 0, totBytes);
    insertCompactFlag(segOut, false);
    return byteArr;
  }

  @Override
  HllSketchImpl reset() {
    checkReadOnly(wseg);
    insertEmptyFlag(wseg, true);
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    clear(wseg, 0, bytes);
    return DirectCouponList.newInstance(lgConfigK, tgtHllType, wseg);
  }

  private static final void checkReadOnly(final MemorySegment wseg) {
    if (wseg == null) {
      throw new SketchesArgumentException("Cannot modify a read-only sketch");
    }
  }
}
