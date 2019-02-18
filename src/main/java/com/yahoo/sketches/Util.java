/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static java.lang.Math.ceil;
import static java.lang.Math.floor;
import static java.lang.Math.log;
import static java.lang.Math.pow;
import static java.lang.Math.round;

/**
 * Common utility functions.
 *
 * @author Lee Rhodes
 */
public final class Util {

  /**
   * The smallest Log2 cache size allowed: 5.
   */
  public static final int MIN_LG_ARR_LONGS = 5;

  /**
   * The smallest Log2 nom entries allowed: 4.
   */
  public static final int MIN_LG_NOM_LONGS = 4;

  /**
   * The largest Log2 nom entries allowed: 26.
   */
  public static final int MAX_LG_NOM_LONGS = 26;

  /**
   * The hash table rebuild threshold = 15.0/16.0.
   */
  public static final double REBUILD_THRESHOLD = 15.0 / 16.0;

  /**
   * The resize threshold = 0.5; tuned for speed.
   */
  public static final double RESIZE_THRESHOLD = 0.5;

  /**
   * The default nominal entries is provided as a convenience for those cases where the
   * nominal sketch size in number of entries is not provided.
   * A sketch of 4096 entries has a Relative Standard Error (RSE) of +/- 1.56% at a confidence of
   * 68%; or equivalently, a Relative Error of +/- 3.1% at a confidence of 95.4%.
   * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">See Default Nominal Entries</a>
   */
  public static final int DEFAULT_NOMINAL_ENTRIES = 4096;

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
  public static final long iGoldenU64 = 0x9e3779b97f4a7c13L;

  /**
   * The inverse golden ratio as a fraction.
   * This has more precision than using the formula: (Math.sqrt(5.0) - 1.0) / 2.0.
   */
  public static final double iGolden = 0.6180339887498949025; // the inverse golden ratio

  private Util() {}

  //Byte Conversions

  /**
   * Returns an int extracted from a Little-Endian byte array.
   * @param arr the given byte array
   * @return an int extracted from a Little-Endian byte array.
   */
  public static int bytesToInt(final byte[] arr) {
    int v = 0;
    for (int i = 0; i < 4; i++) {
      v |= (arr[i] & 0XFF) << (i * 8);
    }
    return v;
  }

  /**
   * Returns a long extracted from a Little-Endian byte array.
   * @param arr the given byte array
   * @return a long extracted from a Little-Endian byte array.
   */
  public static long bytesToLong(final byte[] arr) {
    long v = 0;
    for (int i = 0; i < 8; i++) {
      v |= (arr[i] & 0XFFL) << (i * 8);
    }
    return v;
  }

  /**
   * Returns a Little-Endian byte array extracted from the given int.
   * @param v the given int
   * @param arr a given array of 4 bytes that will be returned with the data
   * @return a Little-Endian byte array extracted from the given int.
   */
  public static byte[] intToBytes(int v, final byte[] arr) {
    for (int i = 0; i < 4; i++) {
      arr[i] = (byte) (v & 0XFF);
      v >>>= 8;
    }
    return arr;
  }

  /**
   * Returns a Little-Endian byte array extracted from the given long.
   * @param v the given long
   * @param arr a given array of 8 bytes that will be returned with the data
   * @return a Little-Endian byte array extracted from the given long.
   */
  public static byte[] longToBytes(long v, final byte[] arr) {
    for (int i = 0; i < 8; i++) {
      arr[i] = (byte) (v & 0XFFL);
      v >>>= 8;
    }
    return arr;
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
    final int mask = (signed) ? 0XFFFFFFFF : 0XFF;
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
   * Returns the given time in nanoseconds formatted as Sec.mSec uSec nSec
   * @param nS the given nanoseconds
   * @return the given time in nanoseconds formatted as Sec.mSec uSec nSec
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
   * @param mS the given nanoseconds
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
  public static final String zeroPad(final String s, final int fieldLength) {
    return characterPad(s, fieldLength, '0', false);
  }

  /**
   * Prepend or postpend the given string with the given character to fill the given field length.
   * If the given string is equal or greater than the given field length, it will be returned
   * without modification.
   * @param s the given string
   * @param fieldLength the desired field length
   * @param padChar the desired pad character
   * @param postpend if true append the pacCharacters to the end of the string.
   * @return prepended or postpended given string with the given character to fill the given field
   * length.
   */
  public static final String characterPad(final String s, final int fieldLength, final char padChar,
      final boolean postpend) {
    final char[] chArr = s.toCharArray();
    final int sLen = chArr.length;
    if (sLen < fieldLength) {
      final char[] out = new char[fieldLength];
      final int blanks = fieldLength - sLen;

      if (postpend) {
        for (int i = 0; i < sLen; i++) {
          out[i] = chArr[i];
        }
        for (int i = sLen; i < fieldLength; i++) {
          out[i] = padChar;
        }
      } else { //prepend
        for (int i = 0; i < blanks; i++) {
          out[i] = padChar;
        }
        for (int i = blanks; i < fieldLength; i++) {
          out[i] = chArr[i - blanks];
        }
      }

      return String.valueOf(out);
    }
    return s;
  }

  //Seed Hash

  /**
   * Check if the two seed hashes are equal. If not, throw an SketchesArgumentException.
   * @param seedHashA the seedHash A
   * @param seedHashB the seedHash B
   * @return seedHashA if they are equal
   */
  public static final short checkSeedHashes(final short seedHashA, final short seedHashB) {
    if (seedHashA != seedHashB) {
      throw new SketchesArgumentException(
          "Incompatible Seed Hashes. " + seedHashA + ", " + seedHashB);
    }
    return seedHashA;
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
    final short seedHash = (short)((hash(seedArr, 0L)[0]) & 0xFFFFL);
    if (seedHash == 0) {
      throw new SketchesArgumentException(
          "The given seed: " + seed + " produced a seedHash of zero. "
              + "You must choose a different seed.");
    }
    return seedHash;
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
    return (((v & 0X7L) == 0L) && (v > 0L));
  }

  //Powers of 2 related

  /**
   * Returns true if argument is exactly a positive power of 2 and greater than zero.
   *
   * @param v The input argument.
   * @return true if argument is exactly a positive power of 2 and greater than zero.
   */
  public static boolean isPowerOf2(final int v) {
    return (v > 0) && ((v & (v - 1)) == 0); //or (v > 0) && ((v & -v) == v)
  }

  /**
   * Checks the given parameter to make sure it is positive, an integer-power of 2 and greater than
   * zero.
   *
   * @param v The input argument.
   * @param argName Used in the thrown exception.
   */
  public static void checkIfPowerOf2(final int v, final String argName) {
    if ((v > 0) && ((v & (v - 1)) == 0)) {
      return;
    }
    throw new SketchesArgumentException("The value of the parameter \"" + argName
        + "\" must be a positive integer-power of 2" + " and greater than 0: " + v);
  }

  /**
   * Checks the given value if it is a power of 2. If not, it throws an exception.
   * Otherwise, returns the log-base2 of the given value.
   * @param value must be a power of 2 and greater than zero.
   * @param argName the argument name used in the exception if thrown.
   * @return the log-base2 of the given value
   */
  public static int toLog2(final int value, final String argName) {
    checkIfPowerOf2(value, argName);
    return Integer.numberOfTrailingZeros(value);
  }

  /**
   * Computes the ceiling power of 2 within the range [1, 2^30]. This is the smallest positive power
   * of 2 that equal to or greater than the given n and equal to a mathematical integer.
   *
   * <p>For:
   * <ul>
   * <li>n &le; 1: returns 1</li>
   * <li>2^30 &le; n &le; 2^31 -1 : returns 2^30</li>
   * <li>n == a power of 2 : returns n</li>
   * <li>otherwise returns the smallest power of 2 greater than n and equal to a mathematical
   * integer</li>
   * </ul>
   *
   * @param n The input argument.
   * @return the ceiling power of 2.
   */
  public static int ceilingPowerOf2(final int n) {
    if (n <= 1) { return 1; }
    final int topPwrOf2 = 1 << 30;
    return (n >= topPwrOf2) ? topPwrOf2 : Integer.highestOneBit((n - 1) << 1);
  }

  /**
   * Computes the floor power of 2 within the range [1, 2^30]. This is the largest positive power of
   * 2 that equal to or less than the given n and equal to a mathematical integer.
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
   * @param n The given argument.
   * @return the floor power of 2.
   */
  public static int floorPowerOf2(final int n) {
    if (n <= 1) { return 1; }
    return Integer.highestOneBit(n);
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
   * Returns an int array of points that will be evenly spaced on a log axis.
   * This is designed for Log_base2 numbers.
   * @param lgStart the Log_base2 of the starting value. E.g., for 1 lgStart = 0.
   * @param lgEnd the Log_base2 of the ending value. E.g. for 1024 lgEnd = 10.
   * @param points the total number of points including the starting and ending values.
   * @return an int array of points that will be evenly spaced on a log axis.
   */
  public static int[] evenlyLgSpaced(final int lgStart, final int lgEnd, final int points) {
    if (points <= 0) {
      throw new SketchesArgumentException("points must be > 0");
    }
    if ((lgEnd < 0) || (lgStart < 0)) {
      throw new SketchesArgumentException("lgStart and lgEnd must be >= 0.");
    }
    final int[] out = new int[points];
    out[0] = 1 << lgStart;
    if (points == 1) { return out; }
    final double delta = (lgEnd - lgStart) / (points - 1.0);
    for (int i = 1; i < points; i++) {
      final double mXpY = (delta * i) + lgStart;
      out[i] = (int)round(pow(2, mXpY));
    }
    return out;
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
   *     for (int p = minP; p <= maxP; p = pwr2LawNext(ppo, p)) {
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
  public static final int pwr2LawNext(final int ppo, final int curPoint) {
    final int cur = (curPoint < 1) ? 1 : curPoint;
    int gi = (int)round(log2(cur) * ppo); //current generating index
    int next;
    do {
      next = (int)round(pow(2.0, (double) ++gi / ppo));
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
   *     for (int p = maxP; p >= minP; p = pwr2LawPrev(ppo, p)) {
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
  public static final int pwr2LawPrev(final int ppo, final int curPoint) {
    if (curPoint <= 1) { return 0; }
    int gi = (int)round(log2(curPoint) * ppo); //current generating index
    int prev;
    do {
      prev = (int)round(pow(2.0, (double) --gi / ppo));
    } while (prev >= curPoint);
    return prev;
  }


  /**
   * The log base 2 of the value
   * @param value the given value
   * @return The log base 2 of the value
   */
  public static final double log2(final double value) {
    return log(value) / LOG2;
  }

  /**
   * Gives the log2 of an integer that is known to be a power of 2.
   *
   * @param x number that is greater than zero
   * @return the log2 of an integer that is known to be a power of 2.
   */
  public static int simpleIntLog2(final int x) {
    final int exp = Integer.numberOfTrailingZeros(x);
    if (x != (1 << exp)) {
      throw new SketchesArgumentException("Argument x cannot be negative or zero.");
    }
    return exp;
  }

  /**
   * Gets the smallest allowed exponent of 2 that it is a sub-multiple of the target by zero,
   * one or more resize factors.
   *
   * @param lgTarget Log2 of the target size
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param lgMin Log2 of the minimum allowed starting size
   * @return The Log2 of the starting size
   */
  public static final int startingSubMultiple(final int lgTarget, final ResizeFactor rf,
      final int lgMin) {
    final int lgRF = rf.lg();
    return (lgTarget <= lgMin) ? lgMin : (lgRF == 0) ? lgTarget : ((lgTarget - lgMin) % lgRF) + lgMin;
  }

  //log_base or power_base related

  /**
   * Computes the ceiling power of B as a double. This is the smallest positive power
   * of B that equal to or greater than the given n and equal to a mathematical integer.
   * The result of this function is consistent with {@link #ceilingPowerOf2(int)} for values
   * less than one. I.e., if <i>n &lt; 1,</i> the result is 1.
   *
   * @param b The base in the expression &#8968;b<sup>n</sup>&#8969;.
   * @param n The input argument.
   * @return the ceiling power of B as a double and equal to a mathematical integer.
   */
  public static double ceilingPowerOfBdouble(final double b, final double n) {
    final double x = (n < 1.0) ? 1.0 : n;
    return pow(b, ceil(logB(b, x)));
  }

  /**
   * Computes the floor power of B as a double. This is the largest positive power
   * of B that equal to or less than the given n and equal to a mathematical integer.
   * The result of this function is consistent with {@link #floorPowerOf2(int)} for values
   * less than one. I.e., if <i>n &lt; 1,</i> the result is 1.
   *
   * @param b The base in the expression &#8970;b<sup>n</sup>&#8971;.
   * @param n The input argument.
   * @return the floor power of 2 and equal to a mathematical integer.
   */
  public static double floorPowerOfBdouble(final double b, final double n) {
    final double x = (n < 1.0) ? 1.0 : n;
    return pow(b, floor(logB(b, x)));
  }

  /**
   * Returns the logarithm_logBase of x. Example: logB(2.0, x) = log(x) / log(2.0).
   * @param logBase the base of the logarithm used
   * @param x the given value
   * @return the logarithm_logBase of x: Example: logB(2.0, x) = log(x) / log(2.0).
   */
  public static final double logB(final double logBase, final double x) {
    return log(x) / log(logBase);
  }

  /**
   * Computes the next larger double in the power series
   * <i>point = logBase<sup>( i / ppo )</sup></i> given the current point in the series.
   * For illustration, this can be used in a loop as follows:
   *
   * <pre>{@code
   *     double maxP = 1024.0;
   *     double minP = 1.0;
   *     int ppo = 2;
   *     double logBase = 2.0;
   *
   *     for (double p = minP; p <= maxP; p = pwr2LawNextDouble(ppo, p, true, logBase)) {
   *       System.out.print(Math.round(p) + " ");
   *     }
   *     //generates the following series:
   *     //1 2 3 4 6 8 11 16 23 32 45 64 91 128 181 256 362 512 724 1024
   * }</pre>
   *
   * @param ppo Points-Per-Octave, or the number of points per integer powers of 2 in the series.
   * @param curPoint the current point of the series. Must be &ge; 1.0.
   * @param roundToInt if true the output will be rounded to the nearest integer.
   * @param logBase the desired base of the logarithms
   * @return the next point in the power series.
   */
  public static final double pwrLawNextDouble(final int ppo, final double curPoint,
      final boolean roundToInt, final double logBase) {
    final double cur = (curPoint < 1.0) ? 1.0 : curPoint;
    double gi = round((logB(logBase, cur) * ppo) ); //current generating index
    double next;
    do {
      final double n = pow(logBase, ++gi / ppo);
      next = roundToInt ? round(n) : n;
    } while (next <= cur);
    return next;
  }

  //Other checks

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

  /**
   * Unsigned compare with longs.
   * @param n1 A long to be treated as if unsigned.
   * @param n2 A long to be treated as if unsigned.
   * @return true if n1 &gt; n2.
   */
  public static boolean isLessThanUnsigned(final long n1, final long n2) {
    return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
  }

}
