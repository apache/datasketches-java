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
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.hll.AbstractCoupons.find;
import static org.apache.datasketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static org.apache.datasketches.hll.PreambleUtil.AUX_COUNT_INT;
import static org.apache.datasketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.insertAuxCount;
import static org.apache.datasketches.hll.PreambleUtil.insertCompactFlag;
import static org.apache.datasketches.hll.PreambleUtil.insertCurMin;
import static org.apache.datasketches.hll.PreambleUtil.insertCurMode;
import static org.apache.datasketches.hll.PreambleUtil.insertEmptyFlag;
import static org.apache.datasketches.hll.PreambleUtil.insertFamilyId;
import static org.apache.datasketches.hll.PreambleUtil.insertHashSetCount;
import static org.apache.datasketches.hll.PreambleUtil.insertHipAccum;
import static org.apache.datasketches.hll.PreambleUtil.insertInt;
import static org.apache.datasketches.hll.PreambleUtil.insertKxQ0;
import static org.apache.datasketches.hll.PreambleUtil.insertKxQ1;
import static org.apache.datasketches.hll.PreambleUtil.insertLgArr;
import static org.apache.datasketches.hll.PreambleUtil.insertLgK;
import static org.apache.datasketches.hll.PreambleUtil.insertListCount;
import static org.apache.datasketches.hll.PreambleUtil.insertNumAtCurMin;
import static org.apache.datasketches.hll.PreambleUtil.insertOooFlag;
import static org.apache.datasketches.hll.PreambleUtil.insertPreInts;
import static org.apache.datasketches.hll.PreambleUtil.insertRebuildCurMinNumKxQFlag;
import static org.apache.datasketches.hll.PreambleUtil.insertSerVer;
import static org.apache.datasketches.hll.PreambleUtil.insertTgtHllType;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesStateException;

/**
 * @author Lee Rhodes
 */
final class ToByteArrayImpl {

  // To byte array used by the heap HLL modes.
  static byte[] toHllByteArray(final AbstractHllArray impl, final boolean compact) {
    int auxBytes = 0;
    if (impl.tgtHllType == TgtHllType.HLL_4) {
      final AuxHashMap auxHashMap = impl.getAuxHashMap();
      if (auxHashMap != null) {
        auxBytes = (compact)
            ? auxHashMap.getCompactSizeBytes()
            : auxHashMap.getUpdatableSizeBytes();
      } else {
        auxBytes = (compact) ? 0 : 4 << LG_AUX_ARR_INTS[impl.lgConfigK];
      }
    }
    final int totBytes = HLL_BYTE_ARR_START + impl.getHllByteArrBytes() + auxBytes;
    final byte[] byteArr = new byte[totBytes];
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    insertHll(impl, wseg, compact);
    return byteArr;
  }

  private static void insertHll(final AbstractHllArray impl, final MemorySegment wseg,
      final boolean compact) {
    insertCommonHll(impl, wseg, compact);
    final byte[] hllByteArr = ((HllArray)impl).hllByteArr;
    MemorySegment.copy(hllByteArr, 0, wseg, JAVA_BYTE, HLL_BYTE_ARR_START, hllByteArr.length);
    if (impl.getAuxHashMap() != null) {
      insertAux(impl, wseg, compact);
    } else {
      wseg.set(JAVA_INT_UNALIGNED, AUX_COUNT_INT, 0);
    }
  }

  private static void insertCommonHll(final AbstractHllArray srcImpl,
      final MemorySegment tgtWseg, final boolean compact) {
    insertPreInts(tgtWseg, srcImpl.getPreInts());
    insertSerVer(tgtWseg);
    insertFamilyId(tgtWseg);
    insertLgK(tgtWseg, srcImpl.getLgConfigK());
    insertEmptyFlag(tgtWseg, srcImpl.isEmpty());
    insertCompactFlag(tgtWseg, compact);
    insertOooFlag(tgtWseg, srcImpl.isOutOfOrder());
    insertCurMin(tgtWseg, srcImpl.getCurMin());
    insertCurMode(tgtWseg, srcImpl.getCurMode());
    insertTgtHllType(tgtWseg, srcImpl.getTgtHllType());
    insertHipAccum(tgtWseg, srcImpl.getHipAccum());
    insertKxQ0(tgtWseg, srcImpl.getKxQ0());
    insertKxQ1(tgtWseg, srcImpl.getKxQ1());
    insertNumAtCurMin(tgtWseg, srcImpl.getNumAtCurMin());
    insertRebuildCurMinNumKxQFlag(tgtWseg, srcImpl.isRebuildCurMinNumKxQFlag());
  }

  private static void insertAux(final AbstractHllArray srcImpl, final MemorySegment tgtWseg,
      final boolean tgtCompact) {
    final AuxHashMap auxHashMap = srcImpl.getAuxHashMap();
    final int auxCount = auxHashMap.getAuxCount();
    insertAuxCount(tgtWseg, auxCount);
    insertLgArr(tgtWseg, auxHashMap.getLgAuxArrInts()); //only used for direct HLL
    final long auxStart = srcImpl.auxStart;
    if (tgtCompact) {
      final PairIterator itr = auxHashMap.getIterator();
      int cnt = 0;
      while (itr.nextValid()) { //works whether src has compact MemorySegment or not
        insertInt(tgtWseg, auxStart + (cnt++ << 2), itr.getPair());
      }
      assert cnt == auxCount;
    } else { //updatable
      final int auxInts = 1 << auxHashMap.getLgAuxArrInts();
      final int[] auxArr = auxHashMap.getAuxIntArr();
      MemorySegment.copy(auxArr, 0, tgtWseg, JAVA_INT_UNALIGNED, auxStart, auxInts);
    }
  }

  //To byte array for coupons
  static byte[] toCouponByteArray(final AbstractCoupons impl, final boolean dstCompact) {
    final int srcCouponCount = impl.getCouponCount();
    final int srcLgCouponArrInts = impl.getLgCouponArrInts();
    final int srcCouponArrInts = 1 << srcLgCouponArrInts;
    final byte[] byteArrOut;
    final boolean list = impl.getCurMode() == CurMode.LIST;
    //prepare switch
    final int sw = (impl.hasMemorySegment() ? 0 : 4) | (impl.isCompact() ? 0 : 2) | (dstCompact ? 0 : 1);
    switch (sw) {
      case 0: { //Src MemorySegment, Src Compact, Dst Compact
        final MemorySegment srcSeg = impl.getMemorySegment();
        final int bytesOut = impl.getSegDataStart() + (srcCouponCount << 2);
        byteArrOut = new byte[bytesOut];
        MemorySegment.copy(srcSeg, JAVA_BYTE, 0, byteArrOut, 0, bytesOut);
        break;
      }
      case 1: { //Src MemorySegment, Src Compact, Dst Updatable
        final int dataStart = impl.getSegDataStart();
        final int bytesOut = dataStart + (srcCouponArrInts << 2);
        byteArrOut = new byte[bytesOut];
        final MemorySegment segOut = MemorySegment.ofArray(byteArrOut);
        copyCommonListAndSet(impl, segOut);
        insertCompactFlag(segOut, dstCompact);

        final int[] tgtCouponIntArr = new int[srcCouponArrInts];
        final PairIterator itr = impl.iterator();
        while (itr.nextValid()) {
          final int pair = itr.getPair();
          final int idx = find(tgtCouponIntArr, srcLgCouponArrInts, pair);
          if (idx < 0) { //found EMPTY
            tgtCouponIntArr[~idx] = pair; //insert
            continue;
          }
          throw new SketchesStateException("Error: found duplicate.");
        }
        MemorySegment.copy(tgtCouponIntArr, 0, segOut, JAVA_INT_UNALIGNED, dataStart, srcCouponArrInts);

        if (list) {
          insertListCount(segOut, srcCouponCount);
        } else {
          insertHashSetCount(segOut, srcCouponCount);
        }
        break;
      }

      case 6:   //Src Heap,   Src Updatable, Dst Compact
      case 2: { //Src MemorySegment, Src Updatable, Dst Compact
        final int dataStart = impl.getSegDataStart();
        final int bytesOut = dataStart + (srcCouponCount << 2);
        byteArrOut = new byte[bytesOut];
        final MemorySegment segOut = MemorySegment.ofArray(byteArrOut);
        copyCommonListAndSet(impl, segOut);
        insertCompactFlag(segOut, dstCompact);

        final PairIterator itr = impl.iterator();
        int cnt = 0;
        while (itr.nextValid()) {
          insertInt(segOut, dataStart + (cnt++ << 2), itr.getPair());
        }
        if (list) {
          insertListCount(segOut, srcCouponCount);
        } else {
          insertHashSetCount(segOut, srcCouponCount);
        }
        break;
      }
      case 3: { //Src MemorySegment, Src Updatable, Dst Updatable
        final MemorySegment srcSeg = impl.getMemorySegment();
        final int bytesOut = impl.getSegDataStart() + (srcCouponArrInts << 2);
        byteArrOut = new byte[bytesOut];
        MemorySegment.copy(srcSeg, JAVA_BYTE, 0, byteArrOut, 0, bytesOut);
        break;
      }
      case 7: { //Src Heap, Src Updatable, Dst Updatable
        final int dataStart = impl.getSegDataStart();
        final int bytesOut = dataStart + (srcCouponArrInts << 2);
        byteArrOut = new byte[bytesOut];
        final MemorySegment segOut = MemorySegment.ofArray(byteArrOut);
        copyCommonListAndSet(impl, segOut);

        MemorySegment.copy(impl.getCouponIntArr(), 0, segOut, JAVA_INT_UNALIGNED, dataStart, srcCouponArrInts);
        if (list) {
          insertListCount(segOut, srcCouponCount);
        } else {
          insertHashSetCount(segOut, srcCouponCount);
        }
        break;
      }
      default: throw new SketchesStateException("Corruption, should not happen: " + sw);
    }
    return byteArrOut;
  }

  private static void copyCommonListAndSet(final AbstractCoupons impl,
      final MemorySegment wseg) {
    insertPreInts(wseg, impl.getPreInts());
    insertSerVer(wseg);
    insertFamilyId(wseg);
    insertLgK(wseg, impl.getLgConfigK());
    insertLgArr(wseg, impl.getLgCouponArrInts());
    insertEmptyFlag(wseg, impl.isEmpty());
    insertOooFlag(wseg, impl.isOutOfOrder());
    insertCurMode(wseg, impl.getCurMode());
    insertTgtHllType(wseg, impl.getTgtHllType());
  }

}
