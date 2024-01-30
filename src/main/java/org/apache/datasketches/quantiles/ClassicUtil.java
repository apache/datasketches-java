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

package org.apache.datasketches.quantiles;

import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.common.Util.isPowerOf2;
import static org.apache.datasketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.extractFlags;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;

/**
 * Utilities for the classic quantiles sketches and independent of the type.
 *
 * @author Lee Rhodes
 */
public final class ClassicUtil {
  static final int DOUBLES_SER_VER = 3;
  static final int MAX_PRELONGS = Family.QUANTILES.getMaxPreLongs();
  static final int MIN_K = 2;
  static final int MAX_K = 1 << 15;

  private ClassicUtil() {}

  static final String LS = System.getProperty("line.separator");
  static final char TAB = '\t';

  /**
   * Used by Classic Quantiles.
   * Gets the normalized rank error given k and pmf for the Quantiles DoubleSketch and ItemsSketch.
   * @param k the configuration parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   */
  // constants were derived as the best fit to 99 percentile empirically measured max error in
  // thousands of trials
  public static double getNormalizedRankError(final int k, final boolean pmf) {
    return pmf
        ? 1.854 / pow(k, 0.9657)
        : 1.576 / pow(k, 0.9726);
  }

  /**
   * Used by Classic Quantiles.
   * Gets the approximate <em>k</em> to use given epsilon, the normalized rank error
   * for the Quantiles DoubleSketch and ItemsSketch.
   * @param epsilon the normalized rank error between zero and one.
   * @param pmf if true, this function returns <em>k</em> assuming the input epsilon
   * is the desired "double-sided" epsilon for the getPMF() function. Otherwise, this function
   * returns <em>k</em> assuming the input epsilon is the desired "single-sided"
   * epsilon for all the other queries.
   * @return <i>k</i> given epsilon.
   */
  // constants were derived as the best fit to 99 percentile empirically measured max error in
  // thousands of trials
  public static int getKFromEpsilon(final double epsilon, final boolean pmf) {
    //Ensure that eps is >= than the lowest possible eps given MAX_K and pmf=false.
    final double eps = max(epsilon, 6.395E-5);
    final double kdbl = pmf
        ? exp(log(1.854 / eps) / 0.9657)
        : exp(log(1.576 / eps) / 0.9726);
    final double krnd = round(kdbl);
    final double del = abs(krnd - kdbl);
    //round to closest int if within 1 ppm of the int, otherwise use the ceiling.
    final int k = (int) ((del < 1E-6) ? krnd : ceil(kdbl));
    return max(MIN_K, min(MAX_K, k));
  }

  /**
   * Used by Classic Quantiles.
   * Checks the validity of the given k
   * @param k must be greater than 1 and less than 65536 and a power of 2.
   */
  static void checkK(final int k) {
    if ((k < MIN_K) || (k > MAX_K) || !isPowerOf2(k)) {
      throw new SketchesArgumentException(
          "K must be >= " + MIN_K + " and <= " + MAX_K + " and a power of 2: " + k);
    }
  }

  /**
   * Used by Classic Quantiles.
   * Checks the validity of the given family ID
   * @param familyID the given family ID
   */
  static void checkFamilyID(final int familyID) {
    final Family family = Family.idToFamily(familyID);
    if (!family.equals(Family.QUANTILES)) {
      throw new SketchesArgumentException(
          "Possible corruption: Invalid Family: " + family.toString());
    }
  }

  /**
   * Used by Classic Quantiles.
   * Checks the consistency of the flag bits and the state of preambleLong and the memory
   * capacity and returns the empty state.
   * @param preambleLongs the size of preamble in longs
   * @param flags the flags field
   * @param memCapBytes the memory capacity
   * @return the empty state
   */
  static boolean checkPreLongsFlagsCap(final int preambleLongs, final int flags, final long memCapBytes) {
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0; //Preamble flags empty state
    final int minPre = Family.QUANTILES.getMinPreLongs(); //1
    final int maxPre = Family.QUANTILES.getMaxPreLongs(); //2
    final boolean valid = ((preambleLongs == minPre) && empty) || ((preambleLongs == maxPre) && !empty);
    if (!valid) {
      throw new SketchesArgumentException(
          "Possible corruption: PreambleLongs inconsistent with empty state: " + preambleLongs);
    }
    checkHeapFlags(flags);
    if (memCapBytes < (preambleLongs << 3)) {
      throw new SketchesArgumentException(
          "Possible corruption: Insufficient capacity for preamble: " + memCapBytes);
    }
    return empty;
  }

  /**
   * Used by Classic Quantiles.
   * Checks just the flags field of the preamble. Allowed flags are Read Only, Empty, Compact, and
   * ordered.
   * @param flags the flags field
   */
  static void checkHeapFlags(final int flags) {  //only used by checkPreLongsFlagsCap and test
    final int allowedFlags =
        READ_ONLY_FLAG_MASK | EMPTY_FLAG_MASK | COMPACT_FLAG_MASK | ORDERED_FLAG_MASK;
    final int flagsMask = ~allowedFlags;
    if ((flags & flagsMask) > 0) {
      throw new SketchesArgumentException(
        "Possible corruption: Invalid flags field: " + Integer.toBinaryString(flags));
    }
  }

  /**
   * Used by Classic Quantiles.
   * Checks just the flags field of an input Memory object. Returns true for a compact
   * sketch, false for an update sketch. Does not perform additional checks, including sketch
   * family.
   * @param srcMem the source Memory containing a sketch
   * @return true if flags indicate a compact sketch, otherwise false
   */
  static boolean checkIsCompactMemory(final Memory srcMem) {
    // only reading so downcast is ok
    final int flags = extractFlags(srcMem);
    final int compactFlags = READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK;
    return (flags & compactFlags) > 0;
  }

  /**
   * Used by Classic Quantiles.
   * Returns the number of retained valid items in the sketch given k and n.
   * @param k the given configured k of the sketch
   * @param n the current number of items seen by the sketch
   * @return the number of retained items in the sketch given k and n.
   */
  static int computeRetainedItems(final int k, final long n) {
    final int bbCnt = computeBaseBufferItems(k, n);
    final long bitPattern = computeBitPattern(k, n);
    final int validLevels = computeValidLevels(bitPattern);
    return bbCnt + (validLevels * k);
  }

  /**
   * Used by Classic Quantiles.
   * Returns the total item capacity of an updatable, non-compact combined buffer
   * given <i>k</i> and <i>n</i>.  If total levels = 0, this returns the ceiling power of 2
   * size for the base buffer or the MIN_BASE_BUF_SIZE, whichever is larger.
   *
   * @param k sketch parameter. This determines the accuracy of the sketch and the
   * size of the updatable data structure, which is a function of <i>k</i> and <i>n</i>.
   *
   * @param n The number of items in the input stream
   * @return the current item capacity of the combined buffer
   */
  static int computeCombinedBufferItemCapacity(final int k, final long n) {
    final int totLevels = computeNumLevelsNeeded(k, n);
    if (totLevels == 0) {
      final int bbItems = computeBaseBufferItems(k, n);
      return Math.max(2 * MIN_K, ceilingPowerOf2(bbItems));
    }
    return (2 + totLevels) * k;
  }

  /**
   * Used by Classic Quantiles.
   * Computes the number of valid levels above the base buffer
   * @param bitPattern the bit pattern
   * @return the number of valid levels above the base buffer
   */
  static int computeValidLevels(final long bitPattern) {
    return Long.bitCount(bitPattern);
  }

  /**
   * Used by Classic Quantiles.
   * Computes the total number of logarithmic levels above the base buffer given the bitPattern.
   * @param bitPattern the given bit pattern
   * @return the total number of logarithmic levels above the base buffer
   */
  static int computeTotalLevels(final long bitPattern) {
    return hiBitPos(bitPattern) + 1;
  }

  /**
   * Used by Classic Quantiles.
   * Computes the total number of logarithmic levels above the base buffer given k and n.
   * This is equivalent to max(floor(lg(n/k), 0).
   * Returns zero if n is less than 2 * k.
   * @param k the configured size of the sketch
   * @param n the total number presented to the sketch.
   * @return the total number of levels needed.
   */
  static int computeNumLevelsNeeded(final int k, final long n) {
    return 1 + hiBitPos(n / (2L * k));
  }

  /**
   * Used by Classic Quantiles.
   * Computes the number of base buffer items given k, n
   * @param k the configured size of the sketch
   * @param n the total number presented to the sketch
   * @return the number of base buffer items
   */
  static int computeBaseBufferItems(final int k, final long n) {
    return (int) (n % (2L * k));
  }

  /**
   * Used by Classic Quantiles.
   * Computes the levels bit pattern given k, n.
   * This is computed as <i>n / (2*k)</i>.
   * @param k the configured size of the sketch
   * @param n the total number presented to the sketch.
   * @return the levels bit pattern
   */
  static long computeBitPattern(final int k, final long n) {
    return n / (2L * k);
  }

  /**
   * Used by Classic Quantiles.
   * Zero-based position of the highest one-bit of the given long.
   * Returns minus one if num is zero.
   * @param num the given long
   * @return Zero-based position of the highest one-bit of the given long
   */
  static int hiBitPos(final long num) {
    return 63 - Long.numberOfLeadingZeros(num);
  }

  /**
   * Used by Classic Quantiles.
   * Returns the zero-based bit position of the lowest zero bit of <i>bits</i> starting at
   * <i>startingBit</i>. If input is all ones, this returns 64.
   * @param bits the input bits as a long
   * @param startingBit the zero-based starting bit position. Only the low 6 bits are used.
   * @return the zero-based bit position of the lowest zero bit starting at <i>startingBit</i>.
   */
  static int lowestZeroBitStartingAt(final long bits, final int startingBit) {
    int pos = startingBit & 0X3F;
    long myBits = bits >>> pos;

    while ((myBits & 1L) != 0) {
      myBits = myBits >>> 1;
      pos++;
    }
    return pos;
  }

}
