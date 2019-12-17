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

import static org.apache.datasketches.hll.HllUtil.AUX_TOKEN;
import static org.apache.datasketches.hll.HllUtil.KEY_BITS_26;
import static org.apache.datasketches.hll.HllUtil.KEY_MASK_26;
import static org.apache.datasketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static org.apache.datasketches.hll.HllUtil.hiNibbleMask;
import static org.apache.datasketches.hll.HllUtil.loNibbleMask;
import static org.apache.datasketches.hll.HllUtil.noWriteAccess;
import static org.apache.datasketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.extractAuxCount;
import static org.apache.datasketches.hll.PreambleUtil.extractCompactFlag;
import static org.apache.datasketches.hll.PreambleUtil.insertAuxCount;
import static org.apache.datasketches.hll.PreambleUtil.insertCompactFlag;
import static org.apache.datasketches.hll.PreambleUtil.insertEmptyFlag;
import static org.apache.datasketches.hll.PreambleUtil.insertInt;
import static org.apache.datasketches.hll.PreambleUtil.insertLgArr;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
class DirectHll4Array extends DirectHllArray {

  //Called by HllSketch.writableWrap(), DirectCouponList.promoteListOrSetToHll
  DirectHll4Array(final int lgConfigK, final WritableMemory wmem) {
    super(lgConfigK, TgtHllType.HLL_4, wmem);
    if (extractAuxCount(mem) > 0) {
      putAuxHashMap(new DirectAuxHashMap(this, false), false);
    }
  }

  //Called by HllSketch.wrap(Memory)
  DirectHll4Array(final int lgConfigK, final Memory mem) {
    super(lgConfigK, TgtHllType.HLL_4, mem);
    final int auxCount = extractAuxCount(mem);
    if (auxCount > 0) {
      final boolean compact = extractCompactFlag(mem);
      final AuxHashMap auxHashMap;
      if (compact) {
        auxHashMap = HeapAuxHashMap.heapify(mem, auxStart, lgConfigK, auxCount, compact);
      } else {
        auxHashMap =  new DirectAuxHashMap(this, false); //not compact
      }
      putAuxHashMap(auxHashMap, compact);
    }
  }

  @Override
  HllSketchImpl copy() {
    return Hll4Array.heapify(mem);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    if (wmem == null) { noWriteAccess(); }
    insertEmptyFlag(wmem, false);
    final int newValue = HllUtil.getValue(coupon);
    if (newValue <= getCurMin()) {
      return this; // super quick rejection; only works for large N, HLL4
    }
    final int configKmask = (1 << getLgConfigK()) - 1;
    final int slotNo = HllUtil.getLow26(coupon) & configKmask;
    Hll4Update.internalHll4Update(this, slotNo, newValue);
    return this;
  }

  @Override
  int getHllByteArrBytes() {
    return hll4ArrBytes(lgConfigK);
  }

  @Override
  final int getSlot(final int slotNo) {
    final long offset = HLL_BYTE_ARR_START + (slotNo >>> 1);
    int theByte = mem.getByte(offset);
    if ((slotNo & 1) > 0) { //odd?
      theByte >>>= 4;
    }
    return theByte & loNibbleMask;
  }

  @Override
  int getUpdatableSerializationBytes() {
    final AuxHashMap auxHashMap = getAuxHashMap();
    final int auxBytes;
    if (auxHashMap == null) {
      auxBytes = 4 << LG_AUX_ARR_INTS[lgConfigK];
    } else {
      auxBytes = 4 << auxHashMap.getLgAuxArrInts();
    }
    return HLL_BYTE_ARR_START + getHllByteArrBytes() + auxBytes;
  }

  @Override
  PairIterator iterator() {
    return new DirectHll4Iterator(1 << lgConfigK);
  }

  @Override
  void mergeTo(final HllSketch that) {
    final int slots = 1 << lgConfigK;
    for (int slotNo = 0; slotNo < slots; slotNo++) {
      int value = mem.getByte(HLL_BYTE_ARR_START + (slotNo >>> 1)) & 0xFF;
      final int nib = ((slotNo & 1) > 0) ? value >>> 4 : value & loNibbleMask;
      if (nib == AUX_TOKEN) {
        final AuxHashMap auxHashMap = getAuxHashMap();
        value = auxHashMap.mustFindValueFor(slotNo); //auxHashMap cannot be null here
      } else {
        value = nib;
      }
      if (value == 0) { continue; }
      that.couponUpdate((value << KEY_BITS_26) | (slotNo & KEY_MASK_26));
    }
  }

  @Override
  final void putSlot(final int slotNo, final int newValue) {
    final long offset = HLL_BYTE_ARR_START + (slotNo >>> 1);
    final int oldValue = mem.getByte(offset);
    final byte value = ((slotNo & 1) == 0) //even?
        ? (byte) ((oldValue & hiNibbleMask) | (newValue & loNibbleMask)) //set low nibble
        : (byte) ((oldValue & loNibbleMask) | ((newValue << 4) & hiNibbleMask)); //set high nibble
    wmem.putByte(offset, value);
  }

  @Override
  byte[] toCompactByteArray() {
    final boolean srcMemIsCompact = extractCompactFlag(mem);
    final int totBytes = getCompactSerializationBytes();
    final byte[] byteArr = new byte[totBytes];
    final WritableMemory memOut = WritableMemory.wrap(byteArr);
    if (srcMemIsCompact) { //mem is already consistent with result
      mem.copyTo(0, memOut, 0, totBytes);
      return byteArr;
    }
    //everything but the aux array is consistent
    mem.copyTo(0, memOut, 0, auxStart);
    if (auxHashMap != null) {
      final int auxCount = auxHashMap.getAuxCount();
      insertAuxCount(memOut, auxCount);
      insertLgArr(memOut, auxHashMap.getLgAuxArrInts()); //only used for direct HLL
      final PairIterator itr = auxHashMap.getIterator();
      int cnt = 0;
      while (itr.nextValid()) { //works whether src has compact memory or not
        insertInt(memOut, auxStart + (cnt++ << 2), itr.getPair());
      }
      assert cnt == auxCount;
    }
    insertCompactFlag(memOut, true);
    return byteArr;
  }

  @Override
  byte[] toUpdatableByteArray() {
    final boolean memIsCompact = extractCompactFlag(mem);
    final int totBytes = getUpdatableSerializationBytes();
    final byte[] byteArr = new byte[totBytes];
    final WritableMemory memOut = WritableMemory.wrap(byteArr);

    if (!memIsCompact) { //both mem and target are updatable
      mem.copyTo(0, memOut, 0, totBytes);
      return byteArr;
    }
    //mem is compact, need to handle auxArr. Easiest way:
    final HllSketch heapSk = HllSketch.heapify(mem);
    return heapSk.toUpdatableByteArray();
  }

  //ITERATOR

  final class DirectHll4Iterator extends HllPairIterator {

    DirectHll4Iterator(final int lengthPairs) {
      super(lengthPairs);
    }

    @Override
    int value() {
      final int nib = DirectHll4Array.this.getSlot(index);
      if (nib == AUX_TOKEN) {
        final AuxHashMap auxHashMap = getAuxHashMap();
        return auxHashMap.mustFindValueFor(index); //auxHashMap cannot be null here
      } else {
        return nib + getCurMin();
      }
    }
  }

}
