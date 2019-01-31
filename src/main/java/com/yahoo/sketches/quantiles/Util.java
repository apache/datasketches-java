/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.isPowerOf2;
import static com.yahoo.sketches.quantiles.DoublesSketch.MAX_K;
import static com.yahoo.sketches.quantiles.DoublesSketch.MIN_K;
import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.Math.round;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Utility class for quantiles sketches.
 *
 * <p>This class contains a highly specialized sort called blockyTandemMergeSort().
 * It also contains methods that are used while building histograms and other common
 * functions.</p>
 *
 * @author Lee Rhodes
 */
final class Util {

  private Util() {}

  /**
   * The java line separator character as a String.
   */
  static final String LS = System.getProperty("line.separator");

  /**
   * The tab character
   */
  static final char TAB = '\t';

  /**
   * Computes the raw delta area between two quantile sketches for the
   * {@link #kolmogorovSmirnovTest(DoubleSketch, DoubleSketch, double)
   * Kolmogorov-Smirnov Test}
   * method.
   * @param sketch1 Input DoubleSketch 1
   * @param sketch2 Input DoubleSketch 2
   * @return the raw delta area between two quantile sketches
   */
  public static double computeKSDelta(final DoublesSketch sketch1,
      final DoublesSketch sketch2) {
    final DoublesAuxiliary p = new DoublesAuxiliary(sketch1);
    final DoublesAuxiliary q = new DoublesAuxiliary(sketch2);

    final double[] pSamplesArr = p.auxSamplesArr_;
    final double[] qSamplesArr = q.auxSamplesArr_;
    final long[] pCumWtsArr = p.auxCumWtsArr_;
    final long[] qCumWtsArr = q.auxCumWtsArr_;
    final int pSamplesArrLen = pSamplesArr.length;
    final int qSamplesArrLen = qSamplesArr.length;

    final double n1 = sketch1.getN();
    final double n2 = sketch2.getN();

    //Compute D from the two distributions
    double deltaArea = 0.0;
    int i = getNextIndex(pSamplesArr, -1);
    int j = getNextIndex(qSamplesArr, -1);

    // We're done if either array reaches the end
    while ((i < pSamplesArrLen) && (j < qSamplesArrLen)) {
      final double pSample = pSamplesArr[i];
      final double qSample = qSamplesArr[j];
      final long pWt = pCumWtsArr[i];
      final long qWt = qCumWtsArr[j];
      final double pNormWt = pWt / n1;
      final double qNormWt = qWt / n2;
      final double pMinusQ = Math.abs(pNormWt - qNormWt);
      final double curD = deltaArea;
      deltaArea = Math.max(curD, pMinusQ);

      //Increment i or j or both
      if (pSample == qSample) {
        i = getNextIndex(pSamplesArr, i);
        j = getNextIndex(qSamplesArr, j);
      } else if (pSample < qSample) {
        i = getNextIndex(pSamplesArr, i);
      } else {
        j = getNextIndex(qSamplesArr, j);
      }
    }

    //This is D, the delta difference in area of the two distributions
    deltaArea = Math.max(deltaArea, Math.abs((pCumWtsArr[i] / n1) - (qCumWtsArr[j] / n2)));
    return deltaArea;
  }

  /**
   * Computes the adjusted delta area threshold for the
   * {@link #kolmogorovSmirnovTest(DoubleSketch, DoubleSketch, double) Kolmogorov-Smirnov Test}
   * method.
   * This adjusts the computed threshold by the error epsilons of the two given sketches.
   * See <a href="https://en.wikipedia.org/wiki/Kolmogorov-Smirnov_test">Kolmogorov–Smirnov Test</a>
   * @param sketch1 Input DoubleSketch 1
   * @param sketch2 Input DoubleSketch 2
   * @param tgtPvalue Target p-value. Typically .001 to .1, e.g., .05.
   * @return the adjusted threshold to be compared with the raw delta area.
   */
  public static double computeKSThreshold(final DoublesSketch sketch1,
                                           final DoublesSketch sketch2,
                                           final double tgtPvalue) {
    final double r1 = sketch1.getRetainedItems();
    final double r2 = sketch2.getRetainedItems();
    final double alpha = tgtPvalue;
    final double alphaFactor = Math.sqrt(-0.5 * Math.log(0.5 * alpha));
    final double deltaAreaThreshold = alphaFactor * Math.sqrt((r1 + r2) / (r1 * r2));
    final double eps1 = Util.getNormalizedRankError(sketch1.getK(), false);
    final double eps2 = Util.getNormalizedRankError(sketch2.getK(), false);

    final double adjDeltaAreaThreshold = deltaAreaThreshold + eps1 + eps2;
    return adjDeltaAreaThreshold;
  }

  /**
   * Performs the Kolmogorov-Smirnov Test between two quantiles sketches.
   * Note: if the given sketches have insufficient data or if the sketch sizes are too small,
   * this will return false.
   * @param sketch1 Input DoubleSketch 1
   * @param sketch2 Input DoubleSketch 2
   * @param tgtPvalue Target p-value. Typically .001 to .1, e.g., .05.
   * @return Boolean indicating whether we can reject the null hypothesis (that the sketches
   * reflect the same underlying distribution) using the provided tgtPValue.
   */
  public static boolean kolmogorovSmirnovTest(final DoublesSketch sketch1,
      final DoublesSketch sketch2, final double tgtPvalue) {
    final double delta = computeKSDelta(sketch1, sketch2);
    final double thresh = computeKSThreshold(sketch1, sketch2, tgtPvalue);
    return delta > thresh;
  }

  private static final int getNextIndex(final double[] samplesArr, final int stIdx) {
    int idx = stIdx + 1;
    final int samplesArrLen = samplesArr.length;

    if (idx >= samplesArrLen) { return samplesArrLen; }

    // if we have a sequence of equal values, use the last one of the sequence
    final double val = samplesArr[idx];
    int nxtIdx = idx + 1;
    while ((nxtIdx < samplesArrLen) && (samplesArr[nxtIdx] == val)) {
      idx = nxtIdx;
      ++nxtIdx;
    }
    return idx;
  }

  /**
   * Gets the normalized rank error given k and pmf for the Quantiles DoubleSketch and ItemsSketch.
   * @param k the configuation parameter
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
   * Gets the approximate value of <em>k</em> to use given epsilon, the normalized rank error
   * for the Quantiles DoubleSketch and ItemsSketch.
   * @param epsilon the normalized rank error between zero and one.
   * @param pmf if true, this function returns the value of <em>k</em> assuming the input epsilon
   * is the desired "double-sided" epsilon for the getPMF() function. Otherwise, this function
   * returns the value of <em>k</em> assuming the input epsilon is the desired "single-sided"
   * epsilon for all the other queries.
   * @return the value of <i>k</i> given a value of epsilon.
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
   * Checks the validity of the given value k
   * @param k must be greater than 1 and less than 65536 and a power of 2.
   */
  static void checkK(final int k) {
    if ((k < MIN_K) || (k > MAX_K) || !isPowerOf2(k)) {
      throw new SketchesArgumentException(
          "K must be >= " + MIN_K + " and <= " + MAX_K + " and a power of 2: " + k);
    }
  }

  /**
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
   * Checks the consistency of the flag bits and the state of preambleLong and the memory
   * capacity and returns the empty state.
   * @param preambleLongs the size of preamble in longs
   * @param flags the flags field
   * @param memCapBytes the memory capacity
   * @return the value of the empty state
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
   * Checks the sequential validity of the given array of double values.
   * They must be unique, monotonically increasing and not NaN.
   * @param values the given array of double values
   */
  static final void checkSplitPointsOrder(final double[] values) {
    if (values == null) {
      throw new SketchesArgumentException("Values cannot be null.");
    }
    final int lenM1 = values.length - 1;
    for (int j = 0; j < lenM1; j++) {
      if (values[j] < values[j + 1]) { continue; }
      throw new SketchesArgumentException(
          "Values must be unique, monotonically increasing and not NaN.");
    }
  }

  /**
   * Checks that the given fractional rank: <i>0 &le; frank &le; 1.0</i>.
   * @param frank the given fractional rank.
   */
  static final void checkFractionalRankBounds(final double frank) {
    if ((frank < 0.0) || (frank > 1.0)) {
      throw new SketchesArgumentException(
        "Fractional rank must be >= 0 and <= 1.0: " + frank);
    }
  }

  /**
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
      return Math.max(2 * DoublesSketch.MIN_K, ceilingPowerOf2(bbItems));
    }
    return (2 + totLevels) * k;
  }

  /**
   * Computes the number of valid levels above the base buffer
   * @param bitPattern the bit pattern
   * @return the number of valid levels above the base buffer
   */
  static int computeValidLevels(final long bitPattern) {
    return Long.bitCount(bitPattern);
  }

  /**
   * Computes the total number of logarithmic levels above the base buffer given the bitPattern.
   * @param bitPattern the given bit pattern
   * @return the total number of logarithmic levels above the base buffer
   */
  static int computeTotalLevels(final long bitPattern) {
    return hiBitPos(bitPattern) + 1;
  }

  /**
   * Computes the total number of logarithmic levels above the base buffer given k and n.
   * This is equivalent to max(floor(lg(n/k), 0).
   * Returns zero if n is less than 2 * k.
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch.
   * @return the total number of levels needed.
   */
  static int computeNumLevelsNeeded(final int k, final long n) {
    return 1 + hiBitPos(n / (2L * k));
  }

  /**
   * Computes the number of base buffer items given k, n
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch
   * @return the number of base buffer items
   */
  static int computeBaseBufferItems(final int k, final long n) {
    return (int) (n % (2L * k));
  }

  /**
   * Computes the levels bit pattern given k, n.
   * This is computed as <i>n / (2*k)</i>.
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch.
   * @return the levels bit pattern
   */
  static long computeBitPattern(final int k, final long n) {
    return n / (2L * k);
  }

  /**
   * Returns the log_base2 of x
   * @param x the given x
   * @return the log_base2 of x
   */
  static double lg(final double x) {
    return ( Math.log(x) / Math.log(2.0) );
  }

  /**
   * Zero-based position of the highest one-bit of the given long.
   * Returns minus one if num is zero.
   * @param num the given long
   * @return Zero-based position of the highest one-bit of the given long
   */
  static int hiBitPos(final long num) {
    return 63 - Long.numberOfLeadingZeros(num);
  }

  /**
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
