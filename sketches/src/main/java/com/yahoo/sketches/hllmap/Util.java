/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import java.math.BigInteger;

import com.yahoo.sketches.SketchesArgumentException;

public final class Util {

  /**
   * Returns the next prime number that is greater than the given target. There will be
   * no prime numbers less than the returned prime number that are greater than the given target.
   * @param target the starting value to begin the search for the next prime
   * @return the next prime number that is greater than or equal to the given target.
   */
  static final int nextPrime(int target) {
    return BigInteger.valueOf(target).nextProbablePrime().intValueExact();
  }

  static final void checkK(int k) {
    if (!com.yahoo.sketches.Util.isPowerOf2(k) || (k > 1024) || (k < 16)) {
      throw new SketchesArgumentException("K must be power of 2 and (16 <= k <= 1024): " + k);
    }
  }

  static final void checkGrowthFactor(float growthFactor) {
    if (growthFactor <= 1.0) {
      throw new SketchesArgumentException("growthFactor must be > 1.0: " + growthFactor);
    }
  }

  static final void checkTgtEntries(int tgtEntries) {
    if (tgtEntries < 16) {
      throw new SketchesArgumentException("tgtEntries must be >= 16");
    }
  }

  static final void checkKeySizeBytes(int keySizeBytes) {
    if (keySizeBytes < 4) {
      throw new SketchesArgumentException("KeySizeBytes must be >= 4: " + keySizeBytes);
    }
  }

  //TODO move to sketches.Util eventually

  /**
   * Computes the inverse integer power of 2: 1/(2^e) = 2^(-e).
   * @param e a positive value between 0 and 1023 inclusive
   * @return  the inverse integer power of 2: 1/(2^e) = 2^(-e)
   */
  public static double invPow2(int e) {
    assert (e | (1024 - e - 1)) >= 0 : "e cannot be negative or greater than 1023: " + e;
    return Double.longBitsToDouble((1023L - e) << 52);
  }

  static String fmtLong(long value) {
    return String.format("%,d", value);
  }

  static String fmtDouble(double value) {
    return String.format("%,.3f", value);
  }

  //static void println(String s) { System.out.println(s); }
}
