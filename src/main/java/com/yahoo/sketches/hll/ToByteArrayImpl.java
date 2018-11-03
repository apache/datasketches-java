/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.AbstractCoupons.find;
import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static com.yahoo.sketches.hll.PreambleUtil.AUX_COUNT_INT;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.insertAuxCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMode;
import static com.yahoo.sketches.hll.PreambleUtil.insertEmptyFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.insertHashSetCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.insertInt;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgK;
import static com.yahoo.sketches.hll.PreambleUtil.insertListCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.hll.PreambleUtil.insertTgtHllType;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
class ToByteArrayImpl {

  // To byte array used by the heap HLL types.
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
    insertOooFlag(tgtWmem, srcImpl.isOutOfOrderFlag());
    insertCurMin(tgtWmem, srcImpl.getCurMin());
    insertCurMode(tgtWmem, srcImpl.getCurMode());
    insertTgtHllType(tgtWmem, srcImpl.getTgtHllType());
    insertHipAccum(tgtWmem, srcImpl.getHipAccum());
    insertKxQ0(tgtWmem, srcImpl.getKxQ0());
    insertKxQ1(tgtWmem, srcImpl.getKxQ1());
    insertNumAtCurMin(tgtWmem, srcImpl.getNumAtCurMin());
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
    insertOooFlag(wmem, impl.isOutOfOrderFlag());
    insertCurMode(wmem, impl.getCurMode());
    insertTgtHllType(wmem, impl.getTgtHllType());
  }

}
