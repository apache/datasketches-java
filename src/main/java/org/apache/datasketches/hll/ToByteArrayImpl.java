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

import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
class ToByteArrayImpl {

  // To byte array used by the heap HLL modes.
  static final byte[] toHllByteArray(final AbstractHllArray impl, final boolean compact) {
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
    final WritableMemory wmem = WritableMemory.wrap(byteArr);
    insertHll(impl, wmem, compact);
    return byteArr;
  }

  private static final void insertHll(final AbstractHllArray impl, final WritableMemory wmem,
      final boolean compact) {
    insertCommonHll(impl, wmem, compact);
    final byte[] hllByteArr = ((HllArray)impl).hllByteArr;
    wmem.putByteArray(HLL_BYTE_ARR_START, hllByteArr, 0, hllByteArr.length);
    if (impl.getAuxHashMap() != null) {
      insertAux(impl, wmem, compact);
    } else {
      wmem.putInt(AUX_COUNT_INT, 0);
    }
  }

  private static final void insertCommonHll(final AbstractHllArray srcImpl,
      final WritableMemory tgtWmem, final boolean compact) {
    insertPreInts(tgtWmem, srcImpl.getPreInts());
    insertSerVer(tgtWmem);
    insertFamilyId(tgtWmem);
    insertLgK(tgtWmem, srcImpl.getLgConfigK());
    insertEmptyFlag(tgtWmem, srcImpl.isEmpty());
    insertCompactFlag(tgtWmem, compact);
    insertOooFlag(tgtWmem, srcImpl.isOutOfOrder());
    insertCurMin(tgtWmem, srcImpl.getCurMin());
    insertCurMode(tgtWmem, srcImpl.getCurMode());
    insertTgtHllType(tgtWmem, srcImpl.getTgtHllType());
    insertHipAccum(tgtWmem, srcImpl.getHipAccum());
    insertKxQ0(tgtWmem, srcImpl.getKxQ0());
    insertKxQ1(tgtWmem, srcImpl.getKxQ1());
    insertNumAtCurMin(tgtWmem, srcImpl.getNumAtCurMin());
    insertRebuildCurMinNumKxQFlag(tgtWmem, srcImpl.isRebuildCurMinNumKxQFlag());
  }

  private static final void insertAux(final AbstractHllArray srcImpl, final WritableMemory tgtWmem,
      final boolean tgtCompact) {
    final AuxHashMap auxHashMap = srcImpl.getAuxHashMap();
    final int auxCount = auxHashMap.getAuxCount();
    insertAuxCount(tgtWmem, auxCount);
    insertLgArr(tgtWmem, auxHashMap.getLgAuxArrInts()); //only used for direct HLL
    final long auxStart = srcImpl.auxStart;
    if (tgtCompact) {
      final PairIterator itr = auxHashMap.getIterator();
      int cnt = 0;
      while (itr.nextValid()) { //works whether src has compact memory or not
        insertInt(tgtWmem, auxStart + (cnt++ << 2), itr.getPair());
      }
      assert cnt == auxCount;
    } else { //updatable
      final int auxInts = 1 << auxHashMap.getLgAuxArrInts();
      final int[] auxArr = auxHashMap.getAuxIntArr();
      tgtWmem.putIntArray(auxStart, auxArr, 0, auxInts);
    }
  }

  //To byte array for coupons
  static final byte[] toCouponByteArray(final AbstractCoupons impl, final boolean dstCompact) {
    final int srcCouponCount = impl.getCouponCount();
    final int srcLgCouponArrInts = impl.getLgCouponArrInts();
    final int srcCouponArrInts = 1 << srcLgCouponArrInts;
    final byte[] byteArrOut;
    final boolean list = impl.getCurMode() == CurMode.LIST;
    //prepare switch
    final int sw = (impl.isMemory() ? 0 : 4) | (impl.isCompact() ? 0 : 2) | (dstCompact ? 0 : 1);
    switch (sw) {
      case 0: { //Src Memory, Src Compact, Dst Compact
        final Memory srcMem = impl.getMemory();
        final int bytesOut = impl.getMemDataStart() + (srcCouponCount << 2);
        byteArrOut = new byte[bytesOut];
        srcMem.getByteArray(0, byteArrOut, 0, bytesOut);
        break;
      }
      case 1: { //Src Memory, Src Compact, Dst Updatable
        final int dataStart = impl.getMemDataStart();
        final int bytesOut = dataStart + (srcCouponArrInts << 2);
        byteArrOut = new byte[bytesOut];
        final WritableMemory memOut = WritableMemory.wrap(byteArrOut);
        copyCommonListAndSet(impl, memOut);
        insertCompactFlag(memOut, dstCompact);

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
        memOut.putIntArray(dataStart, tgtCouponIntArr, 0, srcCouponArrInts);

        if (list) {
          insertListCount(memOut, srcCouponCount);
        } else {
          insertHashSetCount(memOut, srcCouponCount);
        }
        break;
      }
      case 2: { //Src Memory, Src Updatable, Dst Compact
        final int dataStart = impl.getMemDataStart();
        final int bytesOut = dataStart + (srcCouponCount << 2);
        byteArrOut = new byte[bytesOut];
        final WritableMemory memOut = WritableMemory.wrap(byteArrOut);
        copyCommonListAndSet(impl, memOut);
        insertCompactFlag(memOut, dstCompact);

        final PairIterator itr = impl.iterator();
        int cnt = 0;
        while (itr.nextValid()) {
          insertInt(memOut, dataStart + (cnt++ << 2), itr.getPair());
        }
        if (list) {
          insertListCount(memOut, srcCouponCount);
        } else {
          insertHashSetCount(memOut, srcCouponCount);
        }
        break;
      }
      case 3: { //Src Memory, Src Updatable, Dst Updatable
        final Memory srcMem = impl.getMemory();
        final int bytesOut = impl.getMemDataStart() + (srcCouponArrInts << 2);
        byteArrOut = new byte[bytesOut];
        srcMem.getByteArray(0, byteArrOut, 0, bytesOut);
        break;
      }
      case 6: { //Src Heap, Src Updatable, Dst Compact
        final int dataStart = impl.getMemDataStart();
        final int bytesOut = dataStart + (srcCouponCount << 2);
        byteArrOut = new byte[bytesOut];
        final WritableMemory memOut = WritableMemory.wrap(byteArrOut);
        copyCommonListAndSet(impl, memOut);
        insertCompactFlag(memOut, dstCompact);

        final PairIterator itr = impl.iterator();
        int cnt = 0;
        while (itr.nextValid()) {
          insertInt(memOut, dataStart + (cnt++ << 2), itr.getPair());
        }
        if (list) {
          insertListCount(memOut, srcCouponCount);
        } else {
          insertHashSetCount(memOut, srcCouponCount);
        }
        break;
      }
      case 7: { //Src Heap, Src Updatable, Dst Updatable
        final int dataStart = impl.getMemDataStart();
        final int bytesOut = dataStart + (srcCouponArrInts << 2);
        byteArrOut = new byte[bytesOut];
        final WritableMemory memOut = WritableMemory.wrap(byteArrOut);
        copyCommonListAndSet(impl, memOut);

        memOut.putIntArray(dataStart, impl.getCouponIntArr(), 0, srcCouponArrInts);
        if (list) {
          insertListCount(memOut, srcCouponCount);
        } else {
          insertHashSetCount(memOut, srcCouponCount);
        }
        break;
      }
      default: throw new SketchesStateException("Corruption, should not happen: " + sw);
    }
    return byteArrOut;
  }

  private static final void copyCommonListAndSet(final AbstractCoupons impl,
      final WritableMemory wmem) {
    insertPreInts(wmem, impl.getPreInts());
    insertSerVer(wmem);
    insertFamilyId(wmem);
    insertLgK(wmem, impl.getLgConfigK());
    insertLgArr(wmem, impl.getLgCouponArrInts());
    insertEmptyFlag(wmem, impl.isEmpty());
    insertOooFlag(wmem, impl.isOutOfOrder());
    insertCurMode(wmem, impl.getCurMode());
    insertTgtHllType(wmem, impl.getTgtHllType());
  }

}
