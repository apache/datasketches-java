/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static com.yahoo.sketches.Util.zeroPad;

import java.math.BigInteger;

import com.yahoo.sketches.SketchesArgumentException;

final class Util {

  /**
   * Returns the next prime number that is greater than the given target. There will be
   * no prime numbers less than the returned prime number that are greater than the given target.
   * @param target the starting value to begin the search for the next prime
   * @return the next prime number that is greater than or equal to the given target.
   */
  static int nextPrime(int target) {
    return BigInteger.valueOf(target).nextProbablePrime().intValueExact();
  }

  static void checkK(int k) {
    if (!com.yahoo.sketches.Util.isPowerOf2(k) || (k > 1024) || (k < 16)) {
      throw new SketchesArgumentException("K must be power of 2 and (16 <= k <= 1024): " + k);
    }
  }

  static void checkGrowthFactor(float growthFactor) {
    if (growthFactor <= 1.0) {
      throw new SketchesArgumentException("growthFactor must be > 1.0: " + growthFactor);
    }
  }

  static void checkTgtEntries(int tgtEntries) {
    if (tgtEntries < 16) {
      throw new SketchesArgumentException("tgtEntries must be >= 16");
    }
  }

  static void checkKeySizeBytes(int keySizeBytes) {
    if (keySizeBytes < 4) {
      throw new SketchesArgumentException("KeySizeBytes must be >= 4: " + keySizeBytes);
    }
  }

  static String fmtLong(long value) {
    return String.format("%,d", value);
  }

  static String fmtDouble(double value) {
    return String.format("%,.3f", value);
  }

  static byte[] intToBytes(int v, byte[] arr) {
    for (int i = 0; i < 4; i++) {
      arr[i] = (byte) (v & 0XFF);
      v >>>= 8;
    }
    return arr;
  }

  static byte[] longToBytes(long v, byte[] arr) {
    for (int i = 0; i < 8; i++) {
      arr[i] = (byte) (v & 0XFFL);
      v >>>= 8;
    }
    return arr;
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

  /**
   * Returns the given time in milliseconds formatted as Hours:Min:Sec.mSec
   * @param mS the given nanoseconds
   * @return the given time in milliseconds formatted as Hours:Min:Sec.mSec
   */
  //temporarily copied from SNAPSHOT com.yahoo.sketches.TestingUtil (test branch)
  public static String milliSecToString(long mS) {
    long rem_mS = (long)(mS % 1000.0);
    long rem_sec = (long)((mS / 1000.0) % 60.0);
    long rem_min = (long)((mS / 60000.0) % 60.0);
    long hr  =     (long)(mS / 3600000.0);
    String mSstr = zeroPad(Long.toString(rem_mS), 3);
    String secStr = zeroPad(Long.toString(rem_sec), 2);
    String minStr = zeroPad(Long.toString(rem_min), 2);
    return String.format("%d:%2s:%2s.%3s", hr, minStr, secStr, mSstr);
  }

  static void println(String s) { System.out.println(s); }
}
