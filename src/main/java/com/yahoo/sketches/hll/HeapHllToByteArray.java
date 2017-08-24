/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.PreambleUtil.AUX_COUNT_INT;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.insertAuxCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMode;
import static com.yahoo.sketches.hll.PreambleUtil.insertEmptyFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.insertHipAccum;
import static com.yahoo.sketches.hll.PreambleUtil.insertInt;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ0;
import static com.yahoo.sketches.hll.PreambleUtil.insertKxQ1;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgK;
import static com.yahoo.sketches.hll.PreambleUtil.insertNumAtCurMin;
import static com.yahoo.sketches.hll.PreambleUtil.insertOooFlag;
import static com.yahoo.sketches.hll.PreambleUtil.insertPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.hll.PreambleUtil.insertTgtHllType;

import com.yahoo.memory.WritableMemory;

/**
 * To byte array for all the heap HLL types.
 * @author Lee Rhodes
 */
class HeapHllToByteArray {

  static final byte[] toByteArray(final AbstractHllArray impl, final boolean compact) {
    int auxBytes = 0;
    final AuxHashMap auxHashMap = impl.getAuxHashMap();
    if (auxHashMap != null) { //only relevant for HLL_4
      auxBytes = (compact)
          ? auxHashMap.getCompactSizeBytes()
          : auxHashMap.getUpdatableSizeBytes();
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
    final Object tgtMemObj = tgtWmem.getArray();
    final long tgtMemAdd = tgtWmem.getCumulativeOffset(0L);
    insertPreInts(tgtMemObj, tgtMemAdd, HLL_PREINTS);
    insertSerVer(tgtMemObj, tgtMemAdd);
    insertFamilyId(tgtMemObj, tgtMemAdd);
    insertLgK(tgtMemObj, tgtMemAdd, srcImpl.getLgConfigK());
    insertEmptyFlag(tgtMemObj, tgtMemAdd, srcImpl.isEmpty());
    insertCompactFlag(tgtMemObj, tgtMemAdd, compact);
    insertOooFlag(tgtMemObj, tgtMemAdd, srcImpl.isOutOfOrderFlag());
    insertCurMin(tgtMemObj, tgtMemAdd, srcImpl.getCurMin());
    insertCurMode(tgtMemObj, tgtMemAdd, srcImpl.getCurMode());
    insertTgtHllType(tgtMemObj, tgtMemAdd, srcImpl.getTgtHllType());
    insertHipAccum(tgtMemObj, tgtMemAdd, srcImpl.getHipAccum());
    insertKxQ0(tgtMemObj, tgtMemAdd, srcImpl.getKxQ0());
    insertKxQ1(tgtMemObj, tgtMemAdd, srcImpl.getKxQ1());
    insertNumAtCurMin(tgtMemObj, tgtMemAdd, srcImpl.getNumAtCurMin());
  }

  private static final void insertAux(final AbstractHllArray srcImpl, final WritableMemory tgtWmem,
      final boolean tgtCompact) {
    final Object memObj = tgtWmem.getArray();
    final long memAdd = tgtWmem.getCumulativeOffset(0L);
    final AuxHashMap auxHashMap = srcImpl.getAuxHashMap();
    final int auxCount = auxHashMap.getAuxCount();
    insertAuxCount(memObj, memAdd, auxCount);
    insertLgArr(memObj, memAdd, auxHashMap.getLgAuxArrInts()); //only used for direct HLL
    final long auxStart = srcImpl.auxStart;
    if (tgtCompact) {
      final PairIterator itr = auxHashMap.getIterator();
      int cnt = 0;
      while (itr.nextValid()) { //works whether src has compact memory or not
        insertInt(memObj, memAdd, auxStart + (cnt++ << 2), itr.getPair());
      }
      assert cnt == auxCount;
    } else { //updatable
      final int auxInts = 1 << auxHashMap.getLgAuxArrInts();
      final int[] auxArr = auxHashMap.getAuxIntArr();
      tgtWmem.putIntArray(auxStart, auxArr, 0, auxInts);
    }
  }

}
