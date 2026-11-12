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

import static org.apache.datasketches.common.Util.checkBounds;
import static org.apache.datasketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.HASH_SET_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.HLL_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.LIST_PREINTS;
import static org.apache.datasketches.hll.PreambleUtil.computeLgArr;
import static org.apache.datasketches.hll.PreambleUtil.extractAuxCount;
import static org.apache.datasketches.hll.PreambleUtil.extractCompactFlag;
import static org.apache.datasketches.hll.PreambleUtil.extractCurMode;
import static org.apache.datasketches.hll.PreambleUtil.extractFamilyId;
import static org.apache.datasketches.hll.PreambleUtil.extractHashSetCount;
import static org.apache.datasketches.hll.PreambleUtil.extractInt;
import static org.apache.datasketches.hll.PreambleUtil.extractLgArr;
import static org.apache.datasketches.hll.PreambleUtil.extractLgK;
import static org.apache.datasketches.hll.PreambleUtil.extractListCount;
import static org.apache.datasketches.hll.PreambleUtil.extractPreInts;
import static org.apache.datasketches.hll.PreambleUtil.extractSerVer;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;

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

  static final double HLL_HIP_RSE_FACTOR = 0.8325546111576977; //sqrt(log(2.0))
  static final double HLL_NON_HIP_RSE_FACTOR = 1.0389617614136892; //sqrt((3.0 * log(2.0)) - 1.0)
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
  static final int[] LG_AUX_ARR_INTS = {
    0, 2, 2, 2, 2, 2, 2, 3, 3, 3,   //0 - 9
    4, 4, 5, 5, 6, 7, 8, 9, 10, 11, //10 - 19
    12, 13, 14, 15, 16, 17, 18      //20 - 26
  };

  //Checks
  static int checkLgK(final int lgK) {
    if ((lgK >= MIN_LOG_K) && (lgK <= MAX_LOG_K)) { return lgK; }
    throw new SketchesArgumentException(
      "Log K must be between 4 and 21, inclusive: " + lgK);
  }

  static void checkSegSize(final long minBytes, final long capBytes) {
    if (capBytes < minBytes) {
      throw new SketchesArgumentException(
          "Given MemorySegment is not large enough: " + capBytes);
    }
  }

  static void checkNumStdDev(final int numStdDev) {
    if ((numStdDev < 1) || (numStdDev > 3)) {
      throw new SketchesArgumentException(
          "NumStdDev may not be less than 1 or greater than 3.");
    }
  }

  static CurMode checkPreamble(final MemorySegment seg) {
    return checkPreamble(seg, true);
  }

  /**
   * Checks the preamble of an image.
   * @param seg the image
   * @param checkSetLimits true for an image from outside. False only for the sketch's own segment
   * while it is being promoted, when a coupon list or set is one coupon past its limit.
   * @return the current mode of the image
   */
  static CurMode checkPreamble(final MemorySegment seg, final boolean checkSetLimits) {
    checkBounds(0, 8, seg.byteSize()); //need min 8 bytes
    final int preInts = extractPreInts(seg);
    checkBounds(0, (long)preInts * Integer.BYTES, seg.byteSize());
    final int serVer = extractSerVer(seg);
    final int famId = extractFamilyId(seg);
    final CurMode curMode = extractCurMode(seg);
    if (
      (famId != Family.HLL.getID())
      || (serVer != 1)
      || ((preInts != LIST_PREINTS) && (preInts != HASH_SET_PREINTS) && (preInts != HLL_PREINTS))
      || ((curMode == CurMode.LIST) && (preInts != LIST_PREINTS))
      || ((curMode == CurMode.SET) && (preInts != HASH_SET_PREINTS))
      || ((curMode == CurMode.HLL) && (preInts != HLL_PREINTS))
    ) {
      throw new SketchesArgumentException("Possible Corruption, Invalid Preamble:"
          + PreambleUtil.toString(seg));
    }
    checkSizes(seg, curMode, checkSetLimits);
    return curMode;
  }

  /**
   * Checks lgK and the sizes declared by the image.
   * LgK must be in the documented range. The coupon hash set and the auxiliary (exception) hash map
   * are never larger than K, and a coupon list never holds more than its initial capacity.
   * @param seg the image
   * @param curMode the current mode of the image
   * @param checkSetLimits if true, also check a coupon hash set image with checkSet()
   */
  private static void checkSizes(final MemorySegment seg, final CurMode curMode,
      final boolean checkSetLimits) {
    final int lgK = extractLgK(seg);
    checkLgK(lgK);
    if (curMode == CurMode.LIST) {
      final int count = extractListCount(seg);
      if (count > (1 << LG_INIT_LIST_SIZE)) {
        throw new SketchesArgumentException("Possible Corruption: list count " + count);
      }
    } else if (curMode == CurMode.SET) {
      final int count = extractHashSetCount(seg);
      final int lgArr = extractLgArr(seg);
      if ((count < 0) || (count > (1 << lgK)) || (lgArr > lgK)) {
        throw new SketchesArgumentException("Possible Corruption: set count " + count + ", lgArr " + lgArr);
      }
      if (checkSetLimits) { checkSet(seg, lgK, count, lgArr); }
    } else { //HLL
      final int auxCount = extractAuxCount(seg);
      final int lgArr = extractLgArr(seg);
      if ((auxCount < 0) || (auxCount > (1 << lgK)) || ((auxCount > 0) && (lgArr > lgK))) {
        throw new SketchesArgumentException("Possible Corruption: aux count " + auxCount + ", lgArr " + lgArr);
      }
    }
  }

  /**
   * Checks a coupon hash set image against the limits a valid sketch keeps.
   * A sketch uses a hash set only when lgK is at least 8. The table starts at 2^LG_INIT_SET_SIZE
   * ints, grows up to 2^(lgK - 3) ints, and is never more than 3/4 full; the sketch is promoted to
   * HLL mode before that would be exceeded. In an updatable image the count must equal the number
   * of non-empty slots in the table.
   * @param seg the image
   * @param lgK the checked lgK of the image
   * @param count the coupon count of the image, already checked to be in [0, K]
   * @param lgArr the lgArr field of the image, already checked to be at most lgK
   */
  private static void checkSet(final MemorySegment seg, final int lgK, final int count, final int lgArr) {
    if (lgK < (LG_INIT_SET_SIZE + 3)) {
      throw new SketchesArgumentException("Possible Corruption: set mode with lgK " + lgK);
    }
    final int maxLgArr = lgK - 3;
    if (extractCompactFlag(seg)) {
      if ((RESIZE_DENOM * (long) count) > (RESIZE_NUMER * (1L << maxLgArr))) {
        throw new SketchesArgumentException("Possible Corruption: set count " + count + " for lgK " + lgK);
      }
      checkBounds(0, HASH_SET_INT_ARR_START + ((long) count << 2), seg.byteSize());
      return;
    }
    //updatable: early images may not have the lgArr field, then it is computed from the count
    final int effLgArr = (lgArr < LG_INIT_SET_SIZE) ? computeLgArr(seg, count, lgK) : lgArr;
    if ((effLgArr < LG_INIT_SET_SIZE) || (effLgArr > maxLgArr)
        || ((RESIZE_DENOM * (long) count) > (RESIZE_NUMER * (1L << effLgArr)))) {
      throw new SketchesArgumentException("Possible Corruption: set count " + count + ", lgArr " + effLgArr
          + " for lgK " + lgK);
    }
    final int arrInts = 1 << effLgArr;
    checkBounds(0, HASH_SET_INT_ARR_START + ((long) arrInts << 2), seg.byteSize());
    int nonEmpty = 0;
    for (int i = 0; i < arrInts; i++) {
      if (extractInt(seg, HASH_SET_INT_ARR_START + (i << 2)) != EMPTY) { nonEmpty++; }
    }
    if (nonEmpty != count) {
      throw new SketchesArgumentException("Possible Corruption: set count " + count + ", non-empty slots "
          + nonEmpty);
    }
  }

  //Exceptions
  static void noWriteAccess() {
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

  static int getPairLow26(final int coupon) {
    return coupon & KEY_MASK_26;
  }

  static int getPairValue(final int coupon) {
    return coupon >>> KEY_BITS_26;
  }

}
