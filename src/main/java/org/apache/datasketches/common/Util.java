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

package org.apache.datasketches.common;

import static java.lang.Math.ceil;
import static java.lang.Math.floor;
import static java.lang.Math.log;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.hash.MurmurHash3.hash;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Comparator;

/**
 * Common utility functions.
 *
 * @author Lee Rhodes
 */
@SuppressWarnings("unchecked")
public final class Util {

  static {
    if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
      throw new SketchesNotSupportedException("Machine Native Endianness must be LITTLE_ENDIAN.");
    }
  }

  /**
   * The java line separator character as a String.
   */
  public static final String LS = System.getProperty("line.separator");

  /**
   * The tab character
   */
  public static final char TAB = '\t';

  /**
   * The natural logarithm of 2.0.
   */
  public static final double LOG2 = log(2.0);

  /**
   * The inverse golden ratio as an unsigned long.
   */
  public static final long INVERSE_GOLDEN_U64 = 0x9e3779b97f4a7c13L;

  /**
   * The inverse golden ratio as a fraction.
   * This has more precision than using the formula: (Math.sqrt(5.0) - 1.0) / 2.0.
   */
  public static final double INVERSE_GOLDEN = 0.6180339887498949025;

  /**
   * Long.MAX_VALUE as a double.
   */
  public static final double LONG_MAX_VALUE_AS_DOUBLE = Long.MAX_VALUE;

  /**
   * The seed 9001 used in the sketch update methods is a prime number that
   * was chosen very early on in experimental testing. Choosing a seed is somewhat arbitrary, and
   * the author cannot prove that this particular seed is somehow superior to other seeds.  There
   * was some early Internet discussion that a seed of 0 did not produce as clean avalanche diagrams
   * as non-zero seeds, but this may have been more related to the MurmurHash2 release, which did
   * have some issues. As far as the author can determine, MurmurHash3 does not have these problems.
   *
   * <p>In order to perform set operations on two sketches it is critical that the same hash
   * function and seed are identical for both sketches, otherwise the assumed 1:1 relationship
   * between the original source key value and the hashed bit string would be violated. Once
   * you have developed a history of stored sketches you are stuck with it.
   *
   * <p><b>WARNING:</b> This seed is used internally by library sketches in different
   * packages and thus must be declared public. However, this seed value must not be used by library
   * users with the MurmurHash3 function. It should be viewed as existing for exclusive, private
   * use by the library.
   *
   * <p><a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">See Default Update Seed</a>
   */
  public static final long DEFAULT_UPDATE_SEED = 9001L;

  private Util() {}

  //Byte Conversions

  /**
   * Returns an int extracted from a Little-Endian byte array.
   * @param arr the given byte array
   * @return an int extracted from a Little-Endian byte array.
   */
  public static int bytesToInt(final byte[] arr) {
    return (arr[3] << 24)
          | ((arr[2] & 0xff) << 16)
          | ((arr[1] & 0xff) <<  8)
          | (arr[0] & 0xff);
  }

  /**
   * Returns a long extracted from a Little-Endian byte array.
   * @param arr the given byte array
   * @return a long extracted from a Little-Endian byte array.
   */
  public static long bytesToLong(final byte[] arr) {
    return ((long)arr[7] << 56)
          | (((long)arr[6] & 0xff) << 48)
          | (((long)arr[5] & 0xff) << 40)
          | (((long)arr[4] & 0xff) << 32)
          | (((long)arr[3] & 0xff) << 24)
          | (((long)arr[2] & 0xff) << 16)
          | (((long)arr[1] & 0xff) <<  8)
          | ((long)arr[0] & 0xff);
  }

  /**
   * Returns a Little-Endian byte array extracted from the given int.
   * @param v the given int
   * @param arr a given array of 4 bytes that will be returned with the data
   * @return a Little-Endian byte array extracted from the given int.
   */
  public static byte[] intToBytes(final int v, final byte[] arr) {
    arr[3] = (byte) (v >>> 24);
    arr[2] = (byte) (v >>> 16);
    arr[1] = (byte) (v >>>  8);
    arr[0] = (byte) v;
    return arr;
  }

  /**
   * Returns a Little-Endian byte array extracted from the given long.
   * @param v the given long
   * @param arr a given array of 8 bytes that will be returned with the data
   * @return a Little-Endian byte array extracted from the given long.
   */
  public static byte[] longToBytes(final long v, final byte[] arr) {
    arr[7] = (byte) (v >>> 56);
    arr[6] = (byte) (v >>> 48);
    arr[5] = (byte) (v >>> 40);
    arr[4] = (byte) (v >>> 32);
    arr[3] = (byte) (v >>> 24);
    arr[2] = (byte) (v >>> 16);
    arr[1] = (byte) (v >>>  8);
    arr[0] = (byte) v;
    return arr;
  }

  //Byte array conversions

  static long[] convertToLongArray(final byte[] byteArr, final boolean littleEndian) {
    final int len = byteArr.length;
    final long[] longArr = new long[(len / 8) + ((len % 8) != 0 ? 1 : 0)];
    int off = 0;
    int longArrIdx = 0;
    while (off < len) {
      final int rem = Math.min(len - 1 - off, 7);
      long tgt = 0;
      if (littleEndian) {
        for (int j = off + rem, k = 0; j >= off; --j, k++) {
          tgt |= (byteArr[j] & 0XFFL) << (k * 8);
        }
      } else { //BE
        for (int j = off + rem, k = rem; j >= off; --j, k--) {
          tgt |= (byteArr[j] & 0XFFL) << (k * 8);
        }
      }
      off += 8;
      longArr[longArrIdx++] = tgt;
    }
    return longArr;
  }

  //String Related

  /**
   * Returns a string of spaced hex bytes in Big-Endian order.
   * @param v the given long
   * @return string of spaced hex bytes in Big-Endian order.
   */
  public static String longToHexBytes(final long v) {
    final long mask = 0XFFL;
    final StringBuilder sb = new StringBuilder();
    for (int i = 8; i-- > 0; ) {
      final String s = Long.toHexString((v >>> (i * 8)) & mask);
      sb.append(zeroPad(s, 2)).append(" ");
    }
    return sb.toString();
  }

  /**
   * Returns a string view of a byte array
   * @param arr the given byte array
   * @param signed set true if you want the byte values signed.
   * @param littleEndian set true if you want Little-Endian order
   * @param sep the separator string between bytes
   * @return a string view of a byte array
   */
  public static String bytesToString(
      final byte[] arr, final boolean signed, final boolean littleEndian, final String sep) {
    final StringBuilder sb = new StringBuilder();
    final int mask = signed ? 0XFFFFFFFF : 0XFF;
    final int arrLen = arr.length;
    if (littleEndian) {
      for (int i = 0; i < (arrLen - 1); i++) {
        sb.append(arr[i] & mask).append(sep);
      }
      sb.append(arr[arrLen - 1] & mask);
    } else {
      for (int i = arrLen; i-- > 1; ) {
        sb.append(arr[i] & mask).append(sep);
      }
      sb.append(arr[0] & mask);
    }
    return sb.toString();
  }

  /**
   * Returns the given time in nanoseconds formatted as Sec.mSec_uSec_nSec
   * @param nS the given nanoseconds
   * @return the given time in nanoseconds formatted as Sec.mSec_uSec_nSec
   */
  public static String nanoSecToString(final long nS) {
    final long rem_nS = (long)(nS % 1000.0);
    final long rem_uS = (long)((nS / 1000.0) % 1000.0);
    final long rem_mS = (long)((nS / 1000000.0) % 1000.0);
    final long sec    = (long)(nS / 1000000000.0);
    final String nSstr = zeroPad(Long.toString(rem_nS), 3);
    final String uSstr = zeroPad(Long.toString(rem_uS), 3);
    final String mSstr = zeroPad(Long.toString(rem_mS), 3);
    return String.format("%d.%3s_%3s_%3s", sec, mSstr, uSstr, nSstr);
  }

  /**
   * Returns the given time in milliseconds formatted as Hours:Min:Sec.mSec
   * @param mS the given milliseconds
   * @return the given time in milliseconds formatted as Hours:Min:Sec.mSec
   */
  public static String milliSecToString(final long mS) {
    final long rem_mS = (long)(mS % 1000.0);
    final long rem_sec = (long)((mS / 1000.0) % 60.0);
    final long rem_min = (long)((mS / 60000.0) % 60.0);
    final long hr  =     (long)(mS / 3600000.0);
    final String mSstr = zeroPad(Long.toString(rem_mS), 3);
    final String secStr = zeroPad(Long.toString(rem_sec), 2);
    final String minStr = zeroPad(Long.toString(rem_min), 2);
    return String.format("%d:%2s:%2s.%3s", hr, minStr, secStr, mSstr);
  }

  /**
   * Prepend the given string with zeros. If the given string is equal or greater than the given
   * field length, it will be returned without modification.
   * @param s the given string
   * @param fieldLength desired total field length including the given string
   * @return the given string prepended with zeros.
   */
  public static String zeroPad(final String s, final int fieldLength) {
    return characterPad(s, fieldLength, '0', false);
  }

  /**
   * Prepend or postpend the given string with the given character to fill the given field length.
   * If the given string is equal to or greater than the given field length, it will be returned
   * without modification.
   * @param s the given string
   * @param fieldLength the desired field length
   * @param padChar the desired pad character
   * @param postpend if true append the pacCharacters to the end of the string.
   * @return prepended or postpended given string with the given character to fill the given field length.
   */
  public static String characterPad(final String s, final int fieldLength, final char padChar, final boolean postpend) {
    final int sLen = s.length();
    if (sLen < fieldLength) {
      final char[] cArr = new char[fieldLength - sLen];
      java.util.Arrays.fill(cArr, padChar);
      final String addstr = String.valueOf(cArr);
      return (postpend) ? s.concat(addstr) : addstr.concat(s);
    }
    return s;
  }

  //Memory byte alignment

  /**
   * Checks if parameter v is a multiple of 8 and greater than zero.
   * @param v The parameter to check
   * @param argName This name will be part of the error message if the check fails.
   */
  public static void checkIfMultipleOf8AndGT0(final long v, final String argName) {
    if (((v & 0X7L) == 0L) && (v > 0L)) {
      return;
    }
    throw new SketchesArgumentException("The value of the parameter \"" + argName
      + "\" must be a positive multiple of 8 and greater than zero: " + v);
  }

  /**
   * Returns true if v is a multiple of 8 and greater than zero
   * @param v The parameter to check
   * @return true if v is a multiple of 8 and greater than zero
   */
  public static boolean isMultipleOf8AndGT0(final long v) {
    return ((v & 0X7L) == 0L) && (v > 0L);
  }

  //Powers of 2 or powers of base related

  /**
   * Returns true if given long argument is exactly a positive power of 2.
   *
   * @param n The input argument.
   * @return true if argument is exactly a positive power of 2.
   */
  public static boolean isPowerOf2(final long n) {
    return (n > 0) && ((n & (n - 1L)) == 0); //or (n > 0) && ((n & -n) == n)
  }

  /**
   * Checks the given long argument if it is a positive integer power of 2.
   * If not, it throws an exception with the user supplied local argument name, if not null.
   * @param n The input long argument must be a positive integer power of 2.
   * @param argName Used in the thrown exception. It may be null.
   * @throws SketchesArgumentException if not a positive integer power of 2.
   */
  public static void checkIfPowerOf2(final long n, String argName) {
    if (isPowerOf2(n)) { return; }
    argName = (argName == null) ? "" : argName;
    throw new SketchesArgumentException("The value of the argument \"" + argName + "\""
        + " must be a positive integer power of 2: " + n);
  }

  /**
   * Computes the int ceiling power of 2 within the range [1, 2^30]. This is the smallest positive power
   * of 2 that is equal to or greater than the given n and a positive integer.
   *
   * <p>For:
   * <ul>
   * <li>n &le; 1: returns 1</li>
   * <li>2^30 &le; n &le; 2^31 -1 : returns 2^30</li>
   * <li>n == an exact power of 2 : returns n</li>
   * <li>otherwise returns the smallest power of 2 &ge; n and equal to a positive integer</li>
   * </ul>
   *
   * @param n The input int argument.
   * @return the ceiling power of 2.
   */
  public static int ceilingPowerOf2(final int n) {
    if (n <= 1) { return 1; }
    final int topIntPwrOf2 = 1 << 30;
    return n >= topIntPwrOf2 ? topIntPwrOf2 : Integer.highestOneBit((n - 1) << 1);
  }

  /**
   * Computes the long ceiling power of 2 within the range [1, 2^62]. This is the smallest positive power
   * of 2 that is equal to or greater than the given n and a positive long.
   *
   * <p>For:
   * <ul>
   * <li>n &le; 1: returns 1</li>
   * <li>2^62 &le; n &le; 2^63 -1 : returns 2^62</li>
   * <li>n == an exact power of 2 : returns n</li>
   * <li>otherwise returns the smallest power of 2 &ge; n and equal to a positive long</li>
   * </ul>
   *
   * @param n The input long argument.
   * @return the ceiling power of 2.
   */
  public static long ceilingPowerOf2(final long n) {
    if (n <= 1L) { return 1L; }
    final long topIntPwrOf2 = 1L << 62;
    return n >= topIntPwrOf2 ? topIntPwrOf2 : Long.highestOneBit((n - 1L) << 1);
  }

  /**
   * Computes the floor power of 2 given <i>n</i> is in the range [1, 2^31-1].
   * This is the largest positive power of 2 that equal to or less than the given n and equal
   * to a positive integer.
   *
   * <p>For:
   * <ul>
   * <li>n &le; 1: returns 1</li>
   * <li>2^30 &le; n &le; 2^31 -1 : returns 2^30</li>
   * <li>n == a power of 2 : returns n</li>
   * <li>otherwise returns the largest power of 2 less than n and equal to a mathematical
   * integer.</li>
   * </ul>
   *
   * @param n The given int argument.
   * @return the floor power of 2 as an int.
   */
  public static int floorPowerOf2(final int n) {
    if (n <= 1) { return 1; }
    return Integer.highestOneBit(n);
  }

  /**
   * Computes the floor power of 2 given <i>n</i> is in the range [1, 2^63-1].
   * This is the largest positive power of 2 that is equal to or less than the given <i>n</i> and
   * equal to a positive integer.
   *
   * <p>For:
   * <ul>
   * <li>n &le; 1: returns 1</li>
   * <li>2^62 &le; n &le; 2^63 -1 : returns 2^62</li>
   * <li>n == a power of 2 : returns n</li>
   * <li>otherwise returns the largest power of 2 less than n and equal to a mathematical
   * integer.</li>
   * </ul>
   *
   * @param n The given long argument.
   * @return the floor power of 2 as a long
   */
  public static long floorPowerOf2(final long n) {
    if (n <= 1) { return 1; }
    return Long.highestOneBit(n);
  }

  /**
   * This is a long integer equivalent to <i>Math.ceil(n / (double)(1 << k))</i>
   * where: <i>0 &lt; k &le; 6</i> and <i>n</i> is a non-negative long.
   * These limits are not checked for speed reasons.
   * @param n the input dividend as a positive long greater than zero.
   * @param k the input divisor exponent of 2 as a positive integer where 0 &lt; k &le; 6.
   * @return the long integer equivalent to <i>Math.ceil(n / 2^k)</i>.
   */
  public static long ceilingMultiple2expK(final long n, final int k) {
    final long mask = (1L << k) - 1L;
    return (n & mask) > 0 ? (n >>> k) + 1 : n >>> k;
  }

  /**
   * Computes the inverse integer power of 2: 1/(2^e) = 2^(-e).
   * @param e a positive value between 0 and 1023 inclusive
   * @return  the inverse integer power of 2: 1/(2^e) = 2^(-e)
   */
  public static double invPow2(final int e) {
    assert (e | (1024 - e - 1)) >= 0 : "e cannot be negative or greater than 1023: " + e;
    return Double.longBitsToDouble((1023L - e) << 52);
  }

  /**
   * Computes the next larger integer point in the power series
   * <i>point = 2<sup>( i / ppo )</sup></i> given the current point in the series.
   * For illustration, this can be used in a loop as follows:
   *
   * <pre>{@code
   *     int maxP = 1024;
   *     int minP = 1;
   *     int ppo = 2;
   *
   *     for (int p = minP; p <= maxP; p = pwr2SeriesNext(ppo, p)) {
   *       System.out.print(p + " ");
   *     }
   *     //generates the following series:
   *     //1 2 3 4 6 8 11 16 23 32 45 64 91 128 181 256 362 512 724 1024
   * }</pre>
   *
   * @param ppo Points-Per-Octave, or the number of points per integer powers of 2 in the series.
   * @param curPoint the current point of the series. Must be &ge; 1.
   * @return the next point in the power series.
   */
  public static long pwr2SeriesNext(final int ppo, final long curPoint) {
    final long cur = curPoint < 1L ? 1L : curPoint;
    int gi = (int)round(log2(cur) * ppo); //current generating index
    long next;
    do {
      next = round(pow(2.0, (double) ++gi / ppo));
    } while ( next <= curPoint);
    return next;
  }

  /**
   * Computes the previous, smaller integer point in the power series
   * <i>point = 2<sup>( i / ppo )</sup></i> given the current point in the series.
   * For illustration, this can be used in a loop as follows:
   *
   * <pre>{@code
   *     int maxP = 1024;
   *     int minP = 1;
   *     int ppo = 2;
   *
   *     for (int p = maxP; p >= minP; p = pwr2SeriesPrev(ppo, p)) {
   *       System.out.print(p + " ");
   *     }
   *     //generates the following series:
   *     //1024 724 512 362 256 181 128 91 64 45 32 23 16 11 8 6 4 3 2 1
   * }</pre>
   *
   * @param ppo Points-Per-Octave, or the number of points per integer powers of 2 in the series.
   * @param curPoint the current point of the series. Must be &ge; 1.
   * @return the previous, smaller point in the power series.
   * A returned value of zero terminates the series.
   */
  public static int pwr2SeriesPrev(final int ppo, final int curPoint) {
    if (curPoint <= 1) { return 0; }
    int gi = (int)round(log2(curPoint) * ppo); //current generating index
    int prev;
    do {
      prev = (int)round(pow(2.0, (double) --gi / ppo));
    } while (prev >= curPoint);
    return prev;
  }

  /**
   * Computes the next larger double in the power series
   * <i>point = logBase<sup>( i / ppb )</sup></i> given the current point in the series.
   * For illustration, this can be used in a loop as follows:
   *
   * <pre>{@code
   *     double maxP = 1024.0;
   *     double minP = 1.0;
   *     int ppb = 2;
   *     double logBase = 2.0;
   *
   *     for (double p = minP; p <= maxP; p = powerSeriesNextDouble(ppb, p, true, logBase)) {
   *       System.out.print(p + " ");
   *     }
   *     //generates the following series:
   *     //1 2 3 4 6 8 11 16 23 32 45 64 91 128 181 256 362 512 724 1024
   * }</pre>
   *
   * @param ppb Points-Per-Base, or the number of points per integer powers of base in the series.
   * @param curPoint the current point of the series. Must be &ge; 1.0.
   * @param roundToLong if true the output will be rounded to the nearest long.
   * @param logBase the desired base of the logarithms
   * @return the next point in the power series.
   */
  public static double powerSeriesNextDouble(final int ppb, final double curPoint,
      final boolean roundToLong, final double logBase) {
    final double cur = curPoint < 1.0 ? 1.0 : curPoint;
    double gi = round(logBaseOfX(logBase, cur) * ppb ); //current generating index
    double next;
    do {
      final double n = pow(logBase, ++gi / ppb);
      next = roundToLong ? round(n) : n;
    } while (next <= cur);
    return next;
  }

  /**
   * Returns the ceiling of a given <i>n</i> given a <i>base</i>, where the ceiling is an integral power of the base.
   * This is the smallest positive power of <i>base</i> that is equal to or greater than the given <i>n</i>
   * and equal to a mathematical integer.
   * The result of this function is consistent with {@link #ceilingPowerOf2(int)} for values
   * less than one. I.e., if <i>n &lt; 1,</i> the result is 1.
   *
   * <p>The formula is: <i>base<sup>ceiling(log<sub>base</sub>(x))</sup></i></p>
   *
   * @param base The number in the expression &#8968;base<sup>n</sup>&#8969;.
   * @param n The input argument.
   * @return the ceiling power of <i>base</i> as a double and equal to a mathematical integer.
   */
  public static double ceilingPowerBaseOfDouble(final double base, final double n) {
    final double x = n < 1.0 ? 1.0 : n;
    return Math.round(pow(base, ceil(logBaseOfX(base, x))));
  }

  /**
   * Computes the floor of a given <i>n</i> given <i>base</i>, where the floor is an integral power of the base.
   * This is the largest positive power of <i>base</i> that is equal to or less than the given <i>n</i>
   * and equal to a mathematical integer.
   * The result of this function is consistent with {@link #floorPowerOf2(int)} for values
   * less than one. I.e., if <i>n &lt; 1,</i> the result is 1.
   *
   * <p>The formula is: <i>base<sup>floor(log<sub>base</sub>(x))</sup></i></p>
   *
   * @param base The number in the expression &#8970;base<sup>n</sup>&#8971;.
   * @param n The input argument.
   * @return the floor power of 2 and equal to a mathematical integer.
   */
  public static double floorPowerBaseOfDouble(final double base, final double n) {
    final double x = n < 1.0 ? 1.0 : n;
    return Math.round(pow(base, floor(logBaseOfX(base, x))));
  }

  // Logarithm related

  /**
   * The log<sub>2</sub>(value)
   * @param value the given value
   * @return log<sub>2</sub>(value)
   */
  public static double log2(final double value) {
    return log(value) / LOG2;
  }

  /**
   * Returns the log<sub>base</sub>(x). Example, if base = 2.0: logB(2.0, x) = log(x) / log(2.0).
   * @param base The number in the expression log(x) / log(base).
   * @param x the given value
   * @return the log<sub>base</sub>(x)
   */
  public static double logBaseOfX(final double base, final double x) {
    return log(x) / log(base);
  }

  /**
   * Returns the number of one bits following the lowest-order ("rightmost") zero-bit in the
   * two's complement binary representation of the specified long value, or 64 if the value is equal
   * to minus one.
   * @param v the value whose number of trailing ones is to be computed.
   * @return the number of one bits following the lowest-order ("rightmost") zero-bit in the
   * two's complement binary representation of the specified long value, or 64 if the value is equal
   * to minus one.
   */
  public static int numberOfTrailingOnes(final long v) {
    return Long.numberOfTrailingZeros(~v);
  }

  /**
   * Returns the number of one bits preceding the highest-order ("leftmost") zero-bit in the
   * two's complement binary representation of the specified long value, or 64 if the value is equal
   * to minus one.
   * @param v the value whose number of leading ones is to be computed.
   * @return the number of one bits preceding the lowest-order ("rightmost") zero-bit in the
   * two's complement binary representation of the specified long value, or 64 if the value is equal
   * to minus one.
   */
  public static int numberOfLeadingOnes(final long v) {
    return Long.numberOfLeadingZeros(~v);
  }

  /**
   * Returns the log2 of the given int value if it is an exact power of 2 and greater than zero.
   * If not, it throws an exception with the user supplied local argument name.
   * @param powerOf2 must be a power of 2 and greater than zero.
   * @param argName the argument name used in the exception if thrown.
   * @return the log2 of the given value if it is an exact power of 2 and greater than zero.
   * @throws SketchesArgumentException if not a power of 2 nor greater than zero.
   */
  public static int exactLog2OfInt(final int powerOf2, final String argName) {
    checkIfPowerOf2(powerOf2, argName);
    return Integer.numberOfTrailingZeros(powerOf2);
  }

  /**
   * Returns the log2 of the given long value if it is an exact power of 2 and greater than zero.
   * If not, it throws an exception with the user supplied local argument name.
   * @param powerOf2 must be a power of 2 and greater than zero.
   * @param argName the argument name used in the exception if thrown.
   * @return the log2 of the given value if it is an exact power of 2 and greater than zero.
   * @throws SketchesArgumentException if not a power of 2 nor greater than zero.
   */
  public static int exactLog2OfLong(final long powerOf2, final String argName) {
    checkIfPowerOf2(powerOf2, argName);
    return Long.numberOfTrailingZeros(powerOf2);
  }

  /**
   * Returns the log2 of the given int value if it is an exact power of 2 and greater than zero.
   * If not, it throws an exception.
   * @param powerOf2 must be a power of 2 and greater than zero.
   * @return the log2 of the given int value if it is an exact power of 2 and greater than zero.
   */
  public static int exactLog2OfInt(final int powerOf2) {
    if (!isPowerOf2(powerOf2)) {
      throw new SketchesArgumentException("Argument 'powerOf2' must be a positive power of 2.");
    }
    return Long.numberOfTrailingZeros(powerOf2);
  }

  /**
   * Returns the log2 of the given long value if it is an exact power of 2 and greater than zero.
   * If not, it throws an exception.
   * @param powerOf2 must be a power of 2 and greater than zero.
   * @return the log2 of the given long value if it is an exact power of 2 and greater than zero.
   */
  public static int exactLog2OfLong(final long powerOf2) {
    if (!isPowerOf2(powerOf2)) {
      throw new SketchesArgumentException("Argument 'powerOf2' must be a positive power of 2.");
    }
    return Long.numberOfTrailingZeros(powerOf2);
  }

  //Checks that throw

  /**
   * Check the requested offset and length against the allocated size.
   * The invariants equation is: {@code 0 <= reqOff <= reqLen <= reqOff + reqLen <= allocSize}.
   * If this equation is violated an {@link SketchesArgumentException} will be thrown.
   * @param reqOff the requested offset
   * @param reqLen the requested length
   * @param allocSize the allocated size.
   */
  public static void checkBounds(final long reqOff, final long reqLen, final long allocSize) {
    if ((reqOff | reqLen | (reqOff + reqLen) | (allocSize - (reqOff + reqLen))) < 0) {
      throw new SketchesArgumentException("Bounds Violation: "
          + "reqOffset: " + reqOff + ", reqLength: " + reqLen
              + ", (reqOff + reqLen): " + (reqOff + reqLen) + ", allocSize: " + allocSize);
    }
  }

  /**
   * Checks the given parameter to make sure it is positive and between 0.0 inclusive and 1.0
   * inclusive.
   *
   * @param p
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param argName Used in the thrown exception.
   */
  public static void checkProbability(final double p, final String argName) {
    if ((p >= 0.0) && (p <= 1.0)) {
      return;
    }
    throw new SketchesArgumentException("The value of the parameter \"" + argName
        + "\" must be between 0.0 inclusive and 1.0 inclusive: " + p);
  }

  //Boolean Checks

  /**
   * Unsigned compare with longs.
   * @param n1 A long to be treated as if unsigned.
   * @param n2 A long to be treated as if unsigned.
   * @return true if n1 &gt; n2.
   */
  public static boolean isLessThanUnsigned(final long n1, final long n2) {
    return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
  }

  /**
   * Returns true if given n is even.
   * @param n the given n
   * @return true if given n is even.
   */
  public static boolean isEven(final long n) {
    return (n & 1L) == 0;
  }

  /**
   * Returns true if given n is odd.
   * @param n the given n
   * @return true if given n is odd.
   */
  public static boolean isOdd(final long n) {
    return (n & 1L) == 1L;
  }

  //Other

  /**
   * Returns a one if the bit at bitPos is a one, otherwise zero.
   * @param number the number to examine
   * @param bitPos the given zero-based bit position, where the least significant
   * bit is at position zero.
   * @return a one if the bit at bitPos is a one, otherwise zero.
   */
  public static int bitAt(final long number, final int bitPos) {
    return (number & (1L << bitPos)) > 0 ? 1 : 0;
  }

  /**
   * Computes the number of decimal digits of the number n
   * @param n the given number
   * @return the number of decimal digits of the number n
   */
  public static int numDigits(long n) {
    if ((n % 10) == 0) { n++; }
    return (int) ceil(log(n) / log(10));
  }

  /**
   * Converts the given number to a string prepended with spaces, if necessary, to
   * match the given length.
   *
   * <p>For example, assume a sequence of integers from 1 to 1000. The largest value has
   * four decimal digits. Convert the entire sequence of strings to the form "   1" to "1000".
   * When these strings are sorted they will be in numerical sequence: "   1", "   2", ... "1000".</p>
   *
   * @param number the given number
   * @param length the desired string length.
   * @return the given number to a string prepended with spaces
   */
  public static String longToFixedLengthString(final long number, final int length) {
    final String num = Long.toString(number);
    return characterPad(num, length, ' ', false);
  }

  //Generic tests

  /**
   * Finds the minimum of two generic items
   * @param <T> the type
   * @param item1 item one
   * @param item2 item two
   * @param c the given comparator
   * @return the minimum value
   */
  public static <T> Object minT(final Object item1, final Object item2, final Comparator<? super T> c) {
    return  c.compare((T)item1, (T)item2) <= 0 ? item1 : item2;
  }

  /**
   * Finds the maximum of two generic items
   * @param <T> the type
   * @param item1 item one
   * @param item2 item two
   * @param c the given comparator
   * @return the maximum value
   */
  public static <T> Object maxT(final Object item1, final Object item2, final Comparator<? super T> c) {
    return  c.compare((T)item1, (T)item2) >= 0 ? item1 : item2;
  }

  /**
   * Is item1 Less-Than item2?
   * @param <T> the type
   * @param item1 item one
   * @param item2 item two
   * @param c the given comparator
   * @return true if item1 Less-Than item2
   */
  public static <T> boolean lt(final Object item1, final Object item2, final Comparator<? super T> c) {
    return c.compare((T)item1, (T)item2) < 0;
  }

  /**
   * Is item1 Less-Than-Or-Equal-To item2?
   * @param <T> the type
   * @param item1 item one
   * @param item2 item two
   * @param c the given comparator
   * @return true if item1 Less-Than-Or-Equal-To item2
   */
  public static <T> boolean le(final Object item1, final Object item2, final Comparator<? super T> c) {
    return c.compare((T)item1, (T)item2) <= 0;
  }

  //MemorySegment related

  /**
   * Clears all bytes of this MemorySegment to zero.
   * @param seg the given MemorySegment
   */
  public static void clear(final MemorySegment seg) {
    seg.fill((byte)0);
  }

  /**
   * Clears a portion of this MemorySegment to zero.
   * @param seg the given MemorySegment
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @param lengthBytes the length in bytes
   */
  public static void clear(final MemorySegment seg, final long offsetBytes, final long lengthBytes) {
    final MemorySegment slice = seg.asSlice(offsetBytes, lengthBytes);
    slice.fill((byte)0);
  }

  /**
   * Clears the bits defined by the bitMask
   * @param seg the given MemorySegment
   * @param offsetBytes offset bytes relative to this Memory start.
   * @param bitMask the bits set to one will be cleared
   */
  public static void clearBits(final MemorySegment seg, final long offsetBytes, final byte bitMask) {
    final byte b = seg.get(JAVA_BYTE, offsetBytes);
    seg.set(JAVA_BYTE, offsetBytes, (byte)(b & ~bitMask));
  }

  /**
   * Returns true if both segments have the same contents and the same length.
   * @param seg1 the given MemorySegment #1
   * @param seg2 the given MemorySegment #2
   * @return true if both segments have the same contents and the same length.
   */
  public static boolean equalContents(final MemorySegment seg1, final MemorySegment seg2) {
    if (seg1.byteSize() != seg2.byteSize()) { return false; }
    return equalContents(seg1, 0, seg2, 0, seg1.byteSize());
  }

  /**
   * Returns true if both segments have the same content for the specified region.
   * @param seg1 the given MemorySegment #1
   * @param seg1offsetBytes the starting offset for MemorySegment #1 in bytes.
   * @param seg2 the given MemorySegment #2
   * @param seg2offsetBytes the starting offset for MemorySegment #2 in bytes.
   * @param lengthBytes the length of the region to be compared, in bytes.
   * @return true, if both segments have the content for the specified region.
   */
  public static boolean equalContents(
      final MemorySegment seg1,
      final long seg1offsetBytes,
      final MemorySegment seg2,
      final long seg2offsetBytes,
      final long lengthBytes) {
    if (seg1.equals(seg2) && (seg1.byteSize() == seg2.byteSize())) { return true; } //identical segments
    final long seg1EndOff = seg1offsetBytes + lengthBytes;
    final long seg2EndOff = seg2offsetBytes + lengthBytes;
    return MemorySegment.mismatch(seg1, seg1offsetBytes, seg1EndOff, seg2, seg2offsetBytes, seg2EndOff) == -1;
  }

  /**
   * Fills a portion of this Memory region to the given byte value.
   * @param seg the given MemorySegment
   * @param offsetBytes offset bytes relative to this Memory start
   * @param lengthBytes the length in bytes
   * @param value the given byte value
   */
  public static void fill(final MemorySegment seg, final long offsetBytes, final long lengthBytes, final byte value) {
    final MemorySegment slice = seg.asSlice(offsetBytes, lengthBytes);
    slice.fill(value);
  }

  /**
   * Request a new heap MemorySegment with the given capacityBytes and either 8-byte aligned or one byte aligned.
   *
   * <p>If <i>aligned</i> is true, the returned MemorySegment will be constructed from a <i>long[]</i> array,
   * and, as a result, it will have a memory alignment of 8 bytes.
   * If the requested capacity is not exactly divisible by eight, the returned size
   * will be rolled up to the next multiple of eight bytes.</p>
   *
   * <p>If <i>aligned</i> is false, the returned MemorySegment will be constructed from a <i>byte[]</i> array,
   * and have a memory alignment of 1 byte.
   *
   * @param capacityBytes The new capacity being requested. It must not be negative and cannot exceed Integer.MAX_VALUE.
   * @param aligned if true, the new heap segment will have an alignment of 8 bytes, otherwise the alignment will be 1 byte.
   * @return a new MemorySegment with the requested capacity and alignment.
   */
  public static MemorySegment alignedHeapSegment(final int capacityBytes, final boolean aligned) {
    if (aligned) {
      final int lenLongs = capacityBytes >>> 3;
      final long[] array = ((capacityBytes & 0x7) == 0)
          ? new long[lenLongs]
          : new long[lenLongs + 1];
      return MemorySegment.ofArray(array);
    }
    return MemorySegment.ofArray(new byte[capacityBytes]);
  }

  /**
   * Sets the bits defined by the bitMask
   * @param seg the given MemorySegment
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @param bitMask the bits set to one will be set
   */
  public static void setBits(final MemorySegment seg, final long offsetBytes, final byte bitMask) {
    final byte b = seg.get(JAVA_BYTE, offsetBytes);
    seg.set(JAVA_BYTE, offsetBytes, (byte)(b | bitMask));
  }

  /**
   * Computes and checks the 16-bit seed hash from the given long seed.
   * The seed hash may not be zero in order to maintain compatibility with older serialized
   * versions that did not have this concept.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return the seed hash.
   */
  public static short computeSeedHash(final long seed) {
    final long[] seedArr = {seed};
    final short seedHash = (short)(hash(seedArr, 0L)[0] & 0xFFFFL);
    if (seedHash == 0) {
      throw new SketchesArgumentException(
          "The given seed: " + seed + " produced a seedHash of zero. "
              + "You must choose a different seed.");
    }
    return seedHash;
  }

  /**
   * Check if the two seed hashes are equal. If not, throw an SketchesArgumentException.
   * @param seedHashA the seedHash A
   * @param seedHashB the seedHash B
   * @return seedHashA if they are equal
   */
  public static short checkSeedHashes(final short seedHashA, final short seedHashB) {
    if (seedHashA != seedHashB) {
      throw new SketchesArgumentException(
          "Incompatible Seed Hashes. " + Integer.toHexString(seedHashA & 0XFFFF)
            + ", " + Integer.toHexString(seedHashB & 0XFFFF));
    }
    return seedHashA;
  }

}
