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

import static java.lang.Math.log;
import static java.lang.Math.sqrt;
import static org.apache.datasketches.hll.PreambleUtil.HASH_SET_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.HLL_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.LIST_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.extractCurMode;
import static org.apache.datasketches.hll.PreambleUtil.extractFamilyId;
import static org.apache.datasketches.hll.PreambleUtil.extractPreInts;
import static org.apache.datasketches.hll.PreambleUtil.extractSerVer;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesReadOnlyException;
import org.apache.datasketches.memory.Memory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class HllUtil {
  static final int KEY_BITS_26 = 26;
  static final int VAL_BITS_6 = 6;
  static final int KEY_MASK_26 = (1 << KEY_BITS_26) - 1;
  static final int VAL_MASK_6 = (1 << VAL_BITS_6) - 1;
  static final int EMPTY = 0;
  static final int MIN_LOG_K = 4;
  static final int MAX_LOG_K = 21;

  static final double HLL_HIP_RSE_FACTOR = sqrt(log(2.0)); //.8325546
  static final double HLL_NON_HIP_RSE_FACTOR = sqrt((3.0 * log(2.0)) - 1.0); //1.03896
  static final double COUPON_RSE_FACTOR = .409; //at transition point not the asymptote

  static final double COUPON_RSE = COUPON_RSE_FACTOR / (1 << 13);

  static final int LG_INIT_LIST_SIZE = 3;
  static final int LG_INIT_SET_SIZE = 5;
  static final int RESIZE_NUMER = 3;
  static final int RESIZE_DENOM = 4;

  static final int loNibbleMask = 0x0f;
  static final int hiNibbleMask = 0xf0;
  static final int AUX_TOKEN = 0xf;

  /**
   * Log2 table sizes for exceptions based on lgK from 0 to 26.
   * However, only lgK from 4 to 21 are used.
   */
  static final int[] LG_AUX_ARR_INTS = new int[] {
    0, 2, 2, 2, 2, 2, 2, 3, 3, 3,   //0 - 9
    4, 4, 5, 5, 6, 7, 8, 9, 10, 11, //10 - 19
    12, 13, 14, 15, 16, 17, 18      //20 - 26
  };

  //Checks
  static final int checkLgK(final int lgK) {
    if ((lgK >= MIN_LOG_K) && (lgK <= MAX_LOG_K)) { return lgK; }
    throw new SketchesArgumentException(
      "Log K must be between 4 and 21, inclusive: " + lgK);
  }

  static void checkMemSize(final long minBytes, final long capBytes) {
    if (capBytes < minBytes) {
      throw new SketchesArgumentException(
          "Given WritableMemory is not large enough: " + capBytes);
    }
  }

  static final void checkNumStdDev(final int numStdDev) {
    if ((numStdDev < 1) || (numStdDev > 3)) {
      throw new SketchesArgumentException(
          "NumStdDev may not be less than 1 or greater than 3.");
    }
  }

  static CurMode checkPreamble(final Memory mem) {
    final int preInts = extractPreInts(mem);
    final int serVer = extractSerVer(mem);
    final int famId = extractFamilyId(mem);
    final CurMode curMode = extractCurMode(mem);
    if (
      (famId != Family.HLL.getID())
      || (serVer != 1)
      || ((preInts != LIST_PREINTS) && (preInts != HASH_SET_PREINTS) && (preInts != HLL_PREINTS))
      || ((curMode == CurMode.LIST) && (preInts != LIST_PREINTS))
      || ((curMode == CurMode.SET) && (preInts != HASH_SET_PREINTS))
      || ((curMode == CurMode.HLL) && (preInts != HLL_PREINTS))
    ) {
      throw new SketchesArgumentException("Possible Corruption, Invalid Preamble:"
          + PreambleUtil.toString(mem));
    }
    return curMode;
  }

  //Exceptions
  static final void noWriteAccess() {
    throw new SketchesReadOnlyException(
        "This sketch is compact or does not have write access to the underlying resource.");
  }

  //Used for thrown exceptions
  static String pairString(final int pair) {
    return "SlotNo: " + getPairLow26(pair) + ", Value: "
        + getPairValue(pair);
  }

  //Pairs
  static int pair(final int slotNo, final int value) {
    return (value << KEY_BITS_26) | (slotNo & KEY_MASK_26);
  }

  static final int getPairLow26(final int coupon) {
    return coupon & KEY_MASK_26;
  }

  static final int getPairValue(final int coupon) {
    return coupon >>> KEY_BITS_26;
  }

}
