/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static com.yahoo.sketches.Util.zeroPad;

import org.testng.annotations.Test;

public class MapTestingUtil {
  public static final String LS = System.getProperty("line.separator");
  public static final char TAB = '\t';

  /**
   * Returns an int array of points that will be evenly spaced on a log axis.
   * This is designed for Log_base2 numbers.
   * @param lgStart the Log_base2 of the starting value. E.g., for 1 lgStart = 0.
   * @param lgEnd the Log_base2 of the ending value. E.g. for 1024 lgEnd = 10.
   * @param points the total number of points including the starting and ending values.
   * @return an int array of points that will be evenly spaced on a log axis.
   */
  public static int[] evenlyLgSpaced(int lgStart, int lgEnd, int points) {
    if (points <= 0) {
      throw new IllegalArgumentException("points must be > 0");
    }
    if ((lgEnd < 0) || (lgStart < 0)) {
      throw new IllegalArgumentException("lgStart and lgEnd must be >= 0.");
    }
    int[] out = new int[points];
    out[0] = 1 << lgStart;
    if (points == 1) { return out; }
    double delta = (lgEnd - lgStart)/(points -1.0);
    for (int i=1; i<points; i++) {
      double mXpY = mXplusY(delta, i, lgStart);
      out[i] = (int)Math.round(Math.pow(2, mXpY));
    }
    return out;
  }

  public static final double mXplusY(final double m, final double x, final double y) {
    return m * x + y;
  }

  /**
   * Returns a Little-Endian byte array extracted from the given int.
   * @param v the given int
   * @param arr a given array of 4 bytes that will be returned with the data
   * @return a Little-Endian byte array extracted from the given int.
   */
  public static final byte[] intToBytes(int v, byte[] arr) {
    for (int i=0; i<4; i++) {
      arr[i] = (byte) (v & 0XFF);
      v >>>= 8;
    }
    return arr;
  }

  /**
   * Returns an int extracted from a Little-Endian byte array.
   * @param arr the given byte array
   * @return an int extracted from a Little-Endian byte array.
   */
  public static final int bytesToInt(byte[] arr) {
    int v = 0;
    for (int i=0; i<4; i++) {
      v |= (arr[i] & 0XFF) << i * 8;
    }
    return v;
  }

  /**
   * Returns a Little-Endian byte array extracted from the given long.
   * @param v the given long
   * @param arr a given array of 8 bytes that will be returned with the data
   * @return a Little-Endian byte array extracted from the given long.
   */
  public static final byte[] longToBytes(long v, byte[] arr) {
    for (int i=0; i<8; i++) {
      arr[i] = (byte) (v & 0XFFL);
      v >>>= 8;
    }
    return arr;
  }

  /**
   * Returns a long extracted from a Little-Endian byte array.
   * @param arr the given byte array
   * @return a long extracted from a Little-Endian byte array.
   */
  public static final long bytesToLong(byte[] arr) {
    long v = 0;
    for (int i=0; i<8; i++) {
      v |= (arr[i] & 0XFFL) << i * 8;
    }
    return v;
  }

  /**
   * Returns a string view of a byte array
   * @param arr the given byte array
   * @param signed set true if you want the byte values signed.
   * @param littleEndian set true if you want Little-Endian order
   * @param sep the separator string between bytes
   * @return a string view of a byte array
   */
  public static final String bytesToString(
      byte[] arr, boolean signed, boolean littleEndian, String sep) {
    StringBuilder sb = new StringBuilder();
    int mask = (signed) ? 0XFFFFFFFF : 0XFF;
    int arrLen = arr.length;
    if (littleEndian) {
      for (int i = 0; i < arrLen-1; i++) {
        sb.append(arr[i] & mask).append(sep);
      }
      sb.append(arr[arrLen-1] & mask);
    } else {
      for (int i = arrLen; i-- > 1; ) {
        sb.append(arr[i] & mask).append(sep);
      }
      sb.append(arr[0] & mask);
    }
    return sb.toString();
  }

  /**
   * Returns the given time in nanoseconds formatted as Sec.mSec uSec nSec
   * @param nS the given nanoseconds
   * @return the given time in nanoseconds formatted as Sec.mSec uSec nSec
   */
  //temporarily copied from SNAPSHOT com.yahoo.sketches.TestingUtil (test branch)
  public static String nanoSecToString(long nS) {
    long rem_nS = (long)(nS % 1000.0);
    long rem_uS = (long)((nS / 1000.0) % 1000.0);
    long rem_mS = (long)((nS / 1000000.0) % 1000.0);
    long sec    = (long)(nS / 1000000000.0);
    String nSstr = zeroPad(Long.toString(rem_nS), 3);
    String uSstr = zeroPad(Long.toString(rem_uS), 3);
    String mSstr = zeroPad(Long.toString(rem_mS), 3);
    return String.format("%d.%3s %3s %3s", sec, mSstr, uSstr, nSstr);
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

  // TEST

  @Test
  public void checkEvenlyLgSpaced() {
    int lgStart = 0;
    int lgEnd = 10;
    int ppo = 4;
    int points = ppo * (lgEnd - lgStart) +1;
    int[] xPts = evenlyLgSpaced(lgStart, lgEnd, points);
    int lgStartTrials = 20;
    int lgEndTrials = 2;
    int[] tPts = evenlyLgSpaced(lgStartTrials, lgEndTrials, points);
    for (int i=0; i<points; i++) {
      println(""+xPts[i]+", "+tPts[i]);
    }
  }

  @Test
  public void checkBytesToString() {
    long lng = 0XF8F7F6F504030201L;
    println(Long.toHexString(lng));
    byte[] bytes = new byte[8];
    bytes = longToBytes(lng, bytes);
    String sep = ".";
    println("unsign, LE: "+bytesToString(bytes, false, true, sep));
    println("signed, LE: "+bytesToString(bytes, true,  true, sep));
    println("unsign, BE: "+bytesToString(bytes, false,  false, sep));
    println("signed, BE: "+bytesToString(bytes, true,  false, sep));
    long v = bytesToLong(bytes);
    println(Long.toHexString(v));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
