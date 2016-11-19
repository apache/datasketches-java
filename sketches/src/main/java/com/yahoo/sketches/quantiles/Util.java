/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.isPowerOf2;
import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;

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

  static final int MIN_BASE_BUF_SIZE = 4;

  /**
   * The java line separator character as a String.
   */
  public static final String LS = System.getProperty("line.separator");

  /**
   * The tab character
   */
  public static final char TAB = '\t';

  /**
   * Checks the validity of the given value k
   * @param k must be greater than 0 and less than 65536.
   */
  static void checkK(final int k) {
    if ((k < 1) || (k >= (1 << 16)) || !isPowerOf2(k)) {
      throw new SketchesArgumentException("K must be > 0 and < 65536");
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
    final int minPre = Family.QUANTILES.getMinPreLongs();
    final int maxPre = Family.QUANTILES.getMaxPreLongs();
    final boolean valid = ((preambleLongs == minPre) && empty) || ((preambleLongs == maxPre) && !empty);
    if (!valid) {
      throw new SketchesArgumentException(
          "Possible corruption: PreambleLongs inconsistent with empty state: " + preambleLongs);
    }
    checkFlags(flags);
    if (!empty && (memCapBytes < (preambleLongs << 3))) {
      throw new SketchesArgumentException(
          "Possible corruption: Insufficient capacity for preamble: " + memCapBytes);
    }
    return empty;
  }

  /**
   * Checks just the flags field of the preamble
   * @param flags the flags field
   */
  static void checkFlags(final int flags) {  //only used by checkPreLongsFlagsCap and test
    final int allowedFlags =
        READ_ONLY_FLAG_MASK | EMPTY_FLAG_MASK | COMPACT_FLAG_MASK | ORDERED_FLAG_MASK;
    final int flagsMask = ~allowedFlags;
    if ((flags & flagsMask) > 0) {
      throw new SketchesArgumentException(
         "Possible corruption: Invalid flags field: " + Integer.toBinaryString(flags));
    }
  }

  /**
   * Checks the sequential validity of the given array of fractions.
   * They must be unique, monotonically increasing and not NaN, not &lt; 0 and not &gt; 1.0.
   * @param fractions array
   */
  static final void validateFractions(final double[] fractions) {
    if (fractions == null) {
      throw new SketchesArgumentException("Fractions cannot be null.");
    }
    final int len = fractions.length;
    if (len == 0) { return; }
    final double flo = fractions[0];
    final double fhi = fractions[fractions.length - 1];
    if ((flo < 0.0) || (fhi > 1.0)) {
      throw new SketchesArgumentException(
          "A fraction cannot be less than zero or greater than 1.0");
    }
    Util.validateValues(fractions);
    return;
  }

  /**
   * Checks the sequential validity of the given array of double values.
   * They must be unique, monotonically increasing and not NaN.
   * @param values the given array of double values
   */
  static final void validateValues(final double[] values) {
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
   * Returns the number of retained items in the sketch given k and n.
   * @param k the given configured k of the sketch
   * @param n the current number of items seen by the sketch
   * @return the number of retained items in the sketch given k and n.
   */
  static int computeRetainedItems(final int k, final long n) {
    final int bbCnt = computeBaseBufferItems(k, n);
    final long bitPattern = computeBitPattern(k, n);
    final int validLevels = Long.bitCount(bitPattern);
    return bbCnt + validLevels * k;
  }

  /**
   * Returns the current item capacity of the non-compact, expanded combined data buffer
   * given <i>k</i> and <i>n</i>.  If total levels = 0, this returns the ceiling power of 2
   * size for the base buffer or the MIN_BASE_BUF_SIZE, whichever is larger.
   *
   * @param k sketch parameter. This determines the accuracy of the sketch and the
   * size of the updatable data structure, which is a function of <i>k</i> and <i>n</i>.
   *
   * @param n The number of items in the input stream
   * @return the current item capacity of the combined data buffer
   */
  static int computeExpandedCombinedBufferItemCapacity(final int k, final long n) {
    final int totLevels = computeNumLevelsNeeded(k, n);
    int ret;
    if (totLevels > 0) {
      ret = (2 + totLevels) * k;
    } else { //compute the partial the base buffer when totLevels = 0
      final int bbItems = computeBaseBufferItems(k, n);
      ret = Math.max(MIN_BASE_BUF_SIZE, ceilingPowerOf2(bbItems));
    }
    return ret;
  }

  /**
   * Computes the number of valid levels above the base buffer
   * @param bitPattern the bit pattern for valid log levels
   * @return the number of valid levels above the base buffer
   */
  static int computeValidLevels(final long bitPattern) {
    return Long.bitCount(bitPattern);
  }

  /**
   * Computes the number of logarithmic levels needed given k and n.
   * This is equivalent to max(floor(lg(n/k), 0).
   * Returns zero if n is less than 2 * k.
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch.
   * @return the number of levels needed.
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
   * Zero based position of the highest one-bit of the given long.
   * Returns minus one if num is zero.
   * @param num the given long
   * @return Zero based position of the highest one-bit of the given long
   */
  static int hiBitPos(final long num) {
    return 63 - Long.numberOfLeadingZeros(num);
  }

  /**
   * Returns the zero-based bit position of the lowest zero bit starting at bit startingPos.
   * @param numIn the input bits as a long
   * @param startingPos the zero-based starting bit position
   * @return the zero-based bit position of the lowest zero bit starting at bit startingPos.
   */
  static int positionOfLowestZeroBitStartingAt(final long numIn, final int startingPos) {
    long num = numIn >>> startingPos;
    int pos = 0;
    while ((num & 1L) != 0) {
      num = num >>> 1;
      pos++;
    }
    return (pos + startingPos);
  }

  /**
   * Computes epsilon from K. The following table are examples.
   * <code>
   *           eps      eps from inverted
   *     K   empirical  adjusted formula
   *  -------------------------------------
   *    16   0.121094   0.121454102233560
   *    32   0.063477   0.063586601346532
   *    64   0.033081   0.033169048393679
   *   128   0.017120   0.017248096847308
   *   256   0.008804   0.008944835012965
   *   512   0.004509   0.004627803568920
   *  1024   0.002303   0.002389303789572
   *
   *  these could be used in a unit test
   *  2   0.821714930853465
   *  16   0.12145410223356
   *  1024   0.00238930378957284
   *  1073741824   3.42875166500824e-09
   * </code>
   */
  static class EpsilonFromK {
    /**
     *  Used while crunching down the empirical results. If this value is changed the adjustKForEps
     *  value will be incorrect and must also be recomputed. Don't touch this!
     */
    private static final double deltaForEps = 0.01;

    /**
     *  A heuristic fudge factor that causes the inverted formula to better match the empirical.
     *  The value of 4/3 is directly associated with the deltaForEps value of 0.01.
     *  Don't touch this!
     */
    private static final double adjustKForEps = 4.0 / 3.0;  // fudge factor

    /**
     *  Ridiculously fine tolerance given the fudge factor; 1e-3 would probably suffice
     */
    private static final double bracketedBinarySearchForEpsTol = 1e-15;

    /**
     * From extensive empirical testing we recommend most users use this method for deriving
     * epsilon. This uses a fudge factor of 4/3 times the theoretical calculation of epsilon.
     * @param k the given k that must be greater than one.
     * @return the resulting epsilon
     */
    static double getAdjustedEpsilon(final int k) { //used by HeapQS, so far
      if (k == 1) { return 1.0; }
      return getTheoreticalEpsilon(k, adjustKForEps);
    }

    /**
     * Finds the epsilon given K and a fudge factor.
     * See Cormode's Mergeable Summaries paper, Journal version, Theorem 3.6.
     * This has a good fit between values of k between 16 and 1024.
     * Beyond that has not been empirically tested.
     * @param k The given value of k
     * @param ff The given fudge factor. No fudge factor = 1.0.
     * @return the resulting epsilon
     */
    //used only by getAdjustedEpsilon()
    private static double getTheoreticalEpsilon(final int k, final double ff) {
      if (k < 2) {
        throw new SketchesArgumentException("K must be greater than one.");
      }
      // don't need to check in the other direction because an int is very small
      final double kf = k * ff;
      assert kf >= 2.15; // ensures that the bracketing succeeds
      assert kf < 1e12;  // ditto, but could actually be bigger
      final double lo = 1e-16;
      final double hi = 1.0 - 1e-16;
      assert epsForKPredicate(lo, kf);
      assert !epsForKPredicate(hi, kf);
      return bracketedBinarySearchForEps(kf, lo, hi);
    }

    private static double kOfEpsFormula(final double eps) {
      return (1.0 / eps) * (Math.sqrt(Math.log(1.0 / (eps * deltaForEps))));
    }

    private static boolean epsForKPredicate(final double eps, final double kf) {
      return kOfEpsFormula(eps) >= kf;
    }

    private static double bracketedBinarySearchForEps(final double kf, final double lo, final double hi) {
      assert lo < hi;
      assert epsForKPredicate(lo, kf);
      assert !epsForKPredicate(hi, kf);
      if ((hi - lo) / lo < bracketedBinarySearchForEpsTol) {
        return lo;
      }
      final double mid = (lo + hi) / 2.0;
      assert mid > lo;
      assert mid < hi;
      if (epsForKPredicate(mid, kf)) {
        return bracketedBinarySearchForEps(kf, mid, hi);
      }
      else {
        return bracketedBinarySearchForEps(kf, lo, mid);
      }
    }
  } //End of EpsilonFromK

  //static void println(String s) { System.out.println(s); }
  //static void print(String s) { System.out.print(s); }
}
