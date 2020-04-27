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
import static org.apache.datasketches.hll.HllUtil.EMPTY;
import static org.apache.datasketches.hll.HllUtil.LG_AUX_ARR_INTS;

/**
 * Converters for one TgtHllType to another.  The source can be heap or direct, but the result is
 * always on heap. These conversions only apply to sketches in HLL (dense) mode.
 *
 * @author Lee Rhodes
 */
class Conversions {

  static final Hll4Array convertToHll4(final AbstractHllArray srcAbsHllArr) {
    final int lgConfigK = srcAbsHllArr.getLgConfigK();
    final Hll4Array hll4Array = new Hll4Array(lgConfigK);
    hll4Array.putOutOfOrder(srcAbsHllArr.isOutOfOrder());

    //1st pass: compute starting curMin and numAtCurMin:
    final int pair = curMinAndNum(srcAbsHllArr);
    final int curMin = HllUtil.getPairValue(pair);
    final int numAtCurMin = HllUtil.getPairLow26(pair);

    //2nd pass: Must know curMin to create AuxHashMap.
    //Populate KxQ registers, build AuxHashMap if needed
    final PairIterator itr = srcAbsHllArr.iterator();
    AuxHashMap auxHashMap = hll4Array.getAuxHashMap(); //may be null
    while (itr.nextValid()) {
      final int slotNo = itr.getIndex();
      final int actualValue = itr.getValue();
      AbstractHllArray.hipAndKxQIncrementalUpdate(hll4Array, 0, actualValue);
      if (actualValue >= (curMin + 15)) {
        hll4Array.putNibble(slotNo, AUX_TOKEN);
        if (auxHashMap == null) {
          auxHashMap = new HeapAuxHashMap(LG_AUX_ARR_INTS[lgConfigK], lgConfigK);
          hll4Array.putAuxHashMap(auxHashMap, false);
        }
        auxHashMap.mustAdd(slotNo, actualValue);
      } else {
        hll4Array.putNibble(slotNo, actualValue - curMin);
      }
    }

    hll4Array.putCurMin(curMin);
    hll4Array.putNumAtCurMin(numAtCurMin);
    hll4Array.putHipAccum(srcAbsHllArr.getHipAccum()); //intentional overwrite
    hll4Array.putRebuildCurMinNumKxQFlag(false);
    return hll4Array;
  }

  /**
   * This returns curMin and numAtCurMin as a pair and will be correct independent of the TgtHllType
   * of the input AbstractHllArray.
   *
   * <p>In general, it is always true that for HLL_6 and HLL_8, curMin is always 0, and numAtCurMin
   * is the number of zero slots. For these two types there is no need to track curMin nor to track
   * numAtCurMin once all the slots are filled.
   *
   * @param absHllArr an instance of AbstractHllArray
   * @return pair values representing numAtCurMin and curMin
   */
  static final int curMinAndNum(final AbstractHllArray absHllArr) {
    int curMin = 64;
    int numAtCurMin = 0;
    final PairIterator itr = absHllArr.iterator();
    while (itr.nextAll()) {
      final int v = itr.getValue();
      if (v > curMin) { continue; }
      if (v < curMin) {
        curMin = v;
        numAtCurMin = 1;
      } else {
        numAtCurMin++;
      }
    }
    return HllUtil.pair(numAtCurMin, curMin);
  }

  static final Hll6Array convertToHll6(final AbstractHllArray srcAbsHllArr) {
    final int lgConfigK = srcAbsHllArr.lgConfigK;
    final Hll6Array hll6Array = new Hll6Array(lgConfigK);
    hll6Array.putOutOfOrder(srcAbsHllArr.isOutOfOrder());
    int numZeros = 1 << lgConfigK;
    final PairIterator itr = srcAbsHllArr.iterator();
    while (itr.nextAll()) {
      if (itr.getValue() != EMPTY) {
        numZeros--;
        hll6Array.couponUpdate(itr.getPair()); //couponUpdate creates KxQ registers
      }
    }
    hll6Array.putNumAtCurMin(numZeros);
    hll6Array.putHipAccum(srcAbsHllArr.getHipAccum()); //intentional overwrite
    hll6Array.putRebuildCurMinNumKxQFlag(false);
    return hll6Array;
  }

  static final Hll8Array convertToHll8(final AbstractHllArray srcAbsHllArr) {
    final int lgConfigK = srcAbsHllArr.lgConfigK;
    final Hll8Array hll8Array = new Hll8Array(lgConfigK);
    hll8Array.putOutOfOrder(srcAbsHllArr.isOutOfOrder());
    int numZeros = 1 << lgConfigK;
    final PairIterator itr = srcAbsHllArr.iterator();
    while (itr.nextAll()) {
      if (itr.getValue() != EMPTY) {
        numZeros--;
        hll8Array.couponUpdate(itr.getPair()); //creates KxQ registers
      }
    }
    hll8Array.putNumAtCurMin(numZeros);
    hll8Array.putHipAccum(srcAbsHllArr.getHipAccum()); //intentional overwrite
    hll8Array.putRebuildCurMinNumKxQFlag(false);
    return hll8Array;
  }

}
