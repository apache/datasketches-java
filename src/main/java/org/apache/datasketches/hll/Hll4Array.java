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
import static org.apache.datasketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static org.apache.datasketches.hll.HllUtil.hiNibbleMask;
import static org.apache.datasketches.hll.HllUtil.loNibbleMask;
import static org.apache.datasketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.extractAuxCount;
import static org.apache.datasketches.hll.PreambleUtil.extractCompactFlag;
import static org.apache.datasketches.hll.PreambleUtil.extractLgK;

import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.Memory;

/**
 * Uses 4 bits per slot in a packed byte array.
 * @author Lee Rhodes
 */
final class Hll4Array extends HllArray {

  /**
   * Standard constructor for new instance
   * @param lgConfigK the configured Lg K
   */
  Hll4Array(final int lgConfigK) {
    super(lgConfigK, TgtHllType.HLL_4);
    hllByteArr = new byte[hll4ArrBytes(lgConfigK)];
  }

  /**
   * Copy constructor
   * @param that another Hll4Array
   */
  Hll4Array(final Hll4Array that) {
    super(that);
  }

  static final Hll4Array heapify(final Memory mem) {
    final int lgConfigK = extractLgK(mem);
    final Hll4Array hll4Array = new Hll4Array(lgConfigK);
    HllArray.extractCommonHll(mem, hll4Array);

    //load AuxHashMap
    final int auxStart = hll4Array.auxStart;
    final int auxCount = extractAuxCount(mem);
    final boolean compact = extractCompactFlag(mem);
    HeapAuxHashMap auxHashMap = null;
    if (auxCount > 0) {
      auxHashMap = HeapAuxHashMap.heapify(mem, auxStart, lgConfigK, auxCount, compact);
    }
    hll4Array.putAuxHashMap(auxHashMap, false);
    return hll4Array;
  }

  @Override
  Hll4Array copy() {
    return new Hll4Array(this);
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    final int newValue = coupon >>> KEY_BITS_26;
    final int configKmask = (1 << getLgConfigK()) - 1;
    final int slotNo = coupon & configKmask;
    updateSlotWithKxQ(slotNo, newValue);
    return this;
  }

  @Override
  int getNibble(final int slotNo) {
    int theByte = hllByteArr[slotNo >>> 1];
    if ((slotNo & 1) > 0) { //odd?
      theByte >>>= 4;
    }
    return theByte & loNibbleMask;
  }

  @Override
  int getSlotValue(final int slotNo) {
    final int nib = getNibble(slotNo);
    if (nib == AUX_TOKEN) {
      final AuxHashMap auxHashMap = getAuxHashMap();
      return auxHashMap.mustFindValueFor(slotNo); //auxHashMap cannot be null here
    } else {
      return nib + getCurMin();
    }
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
    return new HeapHll4Iterator(1 << lgConfigK);
  }

  @Override
  void putNibble(final int slotNo, final int nibValue) {
    final int byteno = slotNo >>> 1;
    final int oldValue = hllByteArr[byteno];
    if ((slotNo & 1) == 0) { // set low nibble
      hllByteArr[byteno] = (byte) ((oldValue & hiNibbleMask) | (nibValue & loNibbleMask));
    } else { //set high nibble
      hllByteArr[byteno] = (byte) ((oldValue & loNibbleMask) | ((nibValue << 4) & hiNibbleMask));
    }
  }

  @Override
  //Would be used by Union, but not used because the gadget is always HLL8 type
  void updateSlotNoKxQ(final int slotNo, final int newValue) {
    throw new SketchesStateException("Improper access.");
  }

  @Override
  //Used by this couponUpdate()
  //updates HipAccum, CurMin, NumAtCurMin, KxQs and checks newValue > oldValue
  void updateSlotWithKxQ(final int slotNo, final int newValue) {
    Hll4Update.internalHll4Update(this, slotNo, newValue);
  }

  @Override
  byte[] toCompactByteArray() {
    return ToByteArrayImpl.toHllByteArray(this, true);
  }

  //ITERATOR

  final class HeapHll4Iterator extends HllPairIterator {

    HeapHll4Iterator(final int lengthPairs) {
      super(lengthPairs);
    }

    @Override
    int value() {
     return getSlotValue(index);
    }
  }

}
