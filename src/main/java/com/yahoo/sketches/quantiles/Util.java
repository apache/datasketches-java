/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.quantiles.PreambleUtil.BIG_ENDIAN_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.SER_VER;

import com.yahoo.sketches.Family;

/**
 * Utility class for quantiles sketches.
 * 
 * <p>This class contains a highly specialized sort called blockyTandemMergeSort().
 * It also contains methods that are used while building histograms and other common
 * functions.</p>
 */
final class Util {

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
   * @param k must be greater than or equal to 2 and less than 65536.
   */
  static void checkK(int k) {
    if ((k < 1) || (k > ((1 << 16)-1))) {
      throw new IllegalArgumentException("K must be >= 1 and < 65536");
    }
  }

  /**
   * Check the validity of the given serialization version
   * @param serVer the given serialization version
   */
  static void checkSerVer(int serVer) {
    if (serVer != SER_VER) {
      throw new IllegalArgumentException(
          "Possible corruption: Invalid Serialization Version: "+serVer);
    }
  }

  /**
   * Checks the validity of the given family ID
   * @param familyID the given family ID
   */
  static void checkFamilyID(int familyID) {
    Family family = Family.idToFamily(familyID);
    if (!family.equals(Family.QUANTILES)) {
      throw new IllegalArgumentException(
          "Possible corruption: Invalid Family: " + family.toString());
    }
  }

  /**
   * Checks the validity of the memory buffer allocation and the memory capacity assuming
   * n and k.
   * @param k the given value of k
   * @param n the given value of n
   * @param memBufAlloc the memory buffer allocation
   * @param memCapBytes the memory capacity
   */
  static void checkBufAllocAndCap(int k, long n, int memBufAlloc, long memCapBytes) {
    int computedBufAlloc = bufferElementCapacity(k, n);
    if (memBufAlloc != computedBufAlloc) {
      throw new IllegalArgumentException("Possible corruption: Invalid Buffer Allocated Count: "
          + memBufAlloc +" != " +computedBufAlloc);
    }
    int maxPre = Family.QUANTILES.getMaxPreLongs();
    int reqBufBytes = (maxPre + memBufAlloc) << 3;
    if (memCapBytes < reqBufBytes) {
      throw new IllegalArgumentException("Possible corruption: Memory capacity too small: "+ 
          memCapBytes + " < "+ reqBufBytes);
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
  static boolean checkPreLongsFlagsCap(int preambleLongs, int flags, long memCapBytes) {
    boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    int minPre = Family.QUANTILES.getMinPreLongs();
    int maxPre = Family.QUANTILES.getMaxPreLongs();
    boolean valid = ((preambleLongs == minPre) && empty) || ((preambleLongs == maxPre) && !empty);
    if (!valid) {
      throw new IllegalArgumentException(
          "Possible corruption: PreambleLongs inconsistent with empty state: " +preambleLongs);
    }
    checkFlags(flags);
    if (!empty && (memCapBytes < (maxPre<<3))) {
      throw new IllegalArgumentException(
          "Possible corruption: Insufficient capacity for preamble: " +memCapBytes);
    }
    return empty;
  }

  /**
   * Checks just the flags field of the preamble
   * @param flags the flags field
   */ //only used by checkPreLongsFlagsCap and test
  static void checkFlags(int flags) {
    int flagsMask = 
        ORDERED_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | BIG_ENDIAN_FLAG_MASK;
    if ((flags & flagsMask) > 0) {
      throw new IllegalArgumentException(
         "Possible corruption: Input srcMem cannot be: big-endian, compact, ordered, or read-only");
    }
  }

  /**
   * Checks the sequential validity of the given array of fractions. 
   * They must be unique, monotonically increasing and not NaN, not &lt; 0 and not &gt; 1.0.
   * @param fractions array
   */
  static final void validateFractions(double[] fractions) {
    if (fractions == null) {
      throw new IllegalArgumentException("Fractions array may not be null.");
    }
    if (fractions.length == 0) return;
    double flo = fractions[0];
    double fhi = fractions[fractions.length - 1];
    if ((flo < 0.0) || (fhi > 1.0)) {
      throw new IllegalArgumentException("A fraction cannot be less than zero or greater than 1.0");
    }
    DoublesUtil.validateValues(fractions);
  }

  /**
   * Returns the current element capacity of the combined data buffer given <i>k</i> and <i>n</i>.
   * 
   * @param k sketch parameter. This determines the accuracy of the sketch and the 
   * size of the updatable data structure, which is a function of k.
   * 
   * @param n The number of elements in the input stream
   * @return the current element capacity of the combined data buffer
   */
  static int bufferElementCapacity(int k, long n) {
    int maxLevels = computeNumLevelsNeeded(k, n);
    if (maxLevels > 0) return (2+maxLevels) * k;
    assert n < 2*k;
    int m = Math.min(MIN_BASE_BUF_SIZE,2*k);
    if (n <= m) return m;
    int q = intDivideRoundUp(n, m);
    assert q >= 1;
    int q2 = ceilingPowerOf2(q);
    int x = m*q2;
    return Math.min(x, 2*k);
  }

  private static int intDivideRoundUp(long n, int m) {
    int q = (int)n/m;
    if (q*m == n) return q;
    else return q+1;
  }
  
  /**
   * Computes the number of valid levels above the base buffer
   * @param bitPattern the bit pattern for valid log levels
   * @return the number of valid levels above the base buffer
   */
  static int numValidLevels(long bitPattern) {
    return Long.bitCount(bitPattern);
  }

  /**
   * Computes the base buffer count given k, n
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch
   * @return the base buffer count
   */
  static int computeBaseBufferCount(int k, long n) {
    return (int) (n % (2L * k));
  }

  /**
   * Computes the levels bit pattern given k, n.
   * This is computed as <i>n / (2*k)</i>.
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch.
   * @return the levels bit pattern
   */
  static long computeBitPattern(int k, long n) {
    return n / (2L * k);
  }

  static double lg(double x) {
    return ( Math.log(x) / Math.log(2.0) );
  }
  
  /**
   * Computes the number of logarithmic levels needed given k and n.
   * This is equivalent to max(floor(lg(n/k), 0).
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch.
   * @return the number of levels needed.
   */
  static int computeNumLevelsNeeded(int k, long n) {
    return 1 + hiBitPos(n / (2L * k));
  }
  
  /**
   * Zero based position of the highest one-bit of the given long
   * @param num the given long
   * @return Zero based position of the highest one-bit of the given long
   */
  static int hiBitPos(long num) {
    return 63 - Long.numberOfLeadingZeros(num);
  }

  static int positionOfLowestZeroBitStartingAt(long numIn, int startingPos) {
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
     *  The value of 4/3 is directly associated with the deltaForEps value of 0.01. Don't touch this!
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
     */ //used by HeapQS, so far
    static double getAdjustedEpsilon(int k) {
      if (k == 1) return 1.0;
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
     */ //used only by getAdjustedEpsilon()
    private static double getTheoreticalEpsilon(int k, double ff) {
      if (k < 2) throw new IllegalArgumentException("K must be greater than one.");
      // don't need to check in the other direction because an int is very small
      double kf = k*ff;
      assert kf >= 2.15; // ensures that the bracketing succeeds
      assert kf < 1e12;  // ditto, but could actually be bigger
      double lo = 1e-16;
      double hi = 1.0 - 1e-16;
      assert epsForKPredicate(lo, kf);
      assert !epsForKPredicate(hi, kf);
      return bracketedBinarySearchForEps(kf, lo, hi);
    }

    private static double kOfEpsFormula(double eps) {
      return (1.0 / eps) * (Math.sqrt(Math.log(1.0 / (eps * deltaForEps))));
    }

    private static boolean epsForKPredicate(double eps, double kf) {
      return kOfEpsFormula(eps) >= kf;
    }

    private static double bracketedBinarySearchForEps(double kf, double lo, double hi) {
      assert lo < hi;
      assert epsForKPredicate(lo, kf);
      assert !epsForKPredicate(hi, kf);
      if ((hi - lo) / lo < bracketedBinarySearchForEpsTol) {
        return lo;
      }
      double mid = (lo + hi) / 2.0;
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

}
