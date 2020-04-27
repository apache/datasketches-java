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

import static org.apache.datasketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static org.apache.datasketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.extractCurMin;
import static org.apache.datasketches.hll.PreambleUtil.extractEmptyFlag;
import static org.apache.datasketches.hll.PreambleUtil.extractHipAccum;
import static org.apache.datasketches.hll.PreambleUtil.extractKxQ0;
import static org.apache.datasketches.hll.PreambleUtil.extractKxQ1;
import static org.apache.datasketches.hll.PreambleUtil.extractNumAtCurMin;
import static org.apache.datasketches.hll.PreambleUtil.extractOooFlag;
import static org.apache.datasketches.hll.PreambleUtil.extractRebuildCurMinNumKxQFlag;
import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_6;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
abstract class HllArray extends AbstractHllArray {
  boolean oooFlag = false; //Out-Of-Order Flag
  boolean rebuildCurMinNumKxQ = false;
  int curMin; //always zero for Hll6 and Hll8, only used by Hll4Array
  int numAtCurMin; //# of values at curMin. If curMin = 0, it is # of zeros
  double hipAccum;
  double kxq0;
  double kxq1;
  byte[] hllByteArr = null; //init by sub-classes
  final int configKmask;

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
    configKmask = (1 << lgConfigK) - 1;
  }

  /**
   * Copy constructor
   * @param that another HllArray
   */
  HllArray(final HllArray that) {
    super(that.getLgConfigK(), that.getTgtHllType(), CurMode.HLL);
    oooFlag = that.isOutOfOrder();
    rebuildCurMinNumKxQ = that.isRebuildCurMinNumKxQFlag();
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
    configKmask = (1 << lgConfigK) - 1;
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
  WritableMemory getWritableMemory() {
    return null;
  }

  @Override
  boolean isCompact() {
    return false;
  }

  @Override
  boolean isEmpty() {
    return false; //because there should be no normal way to create an HllArray that is empty
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
  boolean isOutOfOrder() {
    return oooFlag;
  }

  @Override
  boolean isSameResource(final Memory mem) {
    return false;
  }

  @Override
  boolean isRebuildCurMinNumKxQFlag() {
    return rebuildCurMinNumKxQ;
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
  void putEmptyFlag(final boolean empty) { }

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
  void putOutOfOrder(final boolean oooFlag) {
    if (oooFlag) { putHipAccum(0); }
    this.oooFlag = oooFlag;
  }

  @Override
  void putRebuildCurMinNumKxQFlag(final boolean rebuild) {
    rebuildCurMinNumKxQ = rebuild;
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
    hllArray.putOutOfOrder(extractOooFlag(srcMem));
    hllArray.putEmptyFlag(extractEmptyFlag(srcMem));
    hllArray.putCurMin(extractCurMin(srcMem));
    hllArray.putHipAccum(extractHipAccum(srcMem));
    hllArray.putKxQ0(extractKxQ0(srcMem));
    hllArray.putKxQ1(extractKxQ1(srcMem));
    hllArray.putNumAtCurMin(extractNumAtCurMin(srcMem));
    hllArray.putRebuildCurMinNumKxQFlag(extractRebuildCurMinNumKxQFlag(srcMem));

    //load Hll array
    srcMem.getByteArray(HLL_BYTE_ARR_START, hllArray.hllByteArr, 0, hllArray.hllByteArr.length);
  }

}
