/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static java.lang.Math.log;
import static java.lang.Math.sqrt;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

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

  static final void badPreambleState(final Memory mem) {
    throw new SketchesArgumentException("Possible Corruption, Invalid Preamble:"
        + PreambleUtil.toString(mem));
  }

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

  static final void noWriteAccess() {
    throw new SketchesArgumentException(
        "This sketch does not have write access to the underlying resource.");
  }

  //Pairs

  public static int pair(final int slotNo, final int value) {
    return (value << KEY_BITS_26) | (slotNo & KEY_MASK_26);
  }

  //used for thrown exceptions
  public static String pairString(final int pair) {
    return "SlotNo: " + getLow26(pair) + ", Value: "
        + getValue(pair);
  }

  static final int getLow26(final int coupon) {
    return coupon & KEY_MASK_26;
  }

  static final int getValue(final int coupon) {
    return coupon >>> KEY_BITS_26;
  }

}
