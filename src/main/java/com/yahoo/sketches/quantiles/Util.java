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
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;

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
   * Computes the Komologorov-Smirnov Statistic between two quantiles sketches.
   * @param sketch1 Input DoubleSketch 1
   * @param sketch2 Input DoubleSketch 2
   * @param tgtConf Target confidence threshold
   * @return Boolean indicating whether the two sketches were likely generated from the same
   * underlying distribution.
   */
  public static boolean computeKSStatistic(final DoublesSketch sketch1,
                                           final DoublesSketch sketch2,
                                           final double tgtConf) {
    final DoublesAuxiliary p = sketch1.constructAuxiliary();
    final DoublesAuxiliary q = sketch2.constructAuxiliary();

    final double[] pSamplesArr = p.auxSamplesArr_;
    final double[] qSamplesArr = q.auxSamplesArr_;
    final long[] pCumWtsArr = p.auxCumWtsArr_;
    final long[] qCumWtsArr = q.auxCumWtsArr_;
    final int pSamplesArrLen = pSamplesArr.length;
    final int qSamplesArrLen = qSamplesArr.length;

    final double n1 = sketch1.getN();
    final double n2 = sketch2.getN();
    final double confScale = Math.sqrt(-0.5 * Math.log(0.5 * tgtConf));

    // reject null hypothesis at tgtConf if D_{KS} > thresh
    final double thresh = confScale * Math.sqrt((n1 + n2) / (n1 * n2));
    double D = 0.0;
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
      final double curD = D;
      D = Math.max(curD, pMinusQ);

      System.out.printf("p[%d]: (%f, %f)\t", i, pSample, pNormWt);
      System.out.printf("q[%d]: (%f, %f)\n", j, qSample, qNormWt);
      System.out.printf("\tpCumWt = %d \tqCumWt = %d\n", pWt, qWt);
      System.out.printf("\tD = max(D, pNormWt - qNormWt) = max(%f, %f) = %f\n",
          curD, pMinusQ, D);

      //Increment i or j or both
      if (pSample == qSample) {
        System.out.println("\tIncrement both\n");
        i = getNextIndex(pSamplesArr, i);
        j = getNextIndex(qSamplesArr, j);
      } else if ((pSample < qSample) && (i < pSamplesArrLen)) {
        System.out.println("\tIncrement p\n");
        i = getNextIndex(pSamplesArr, i);
      } else {
        System.out.println("\tIncrement q\n");
        j = getNextIndex(qSamplesArr, j);
      }
    }

    // One final comparison, with one of the two values at 1.0.
    // Subsequent values for the smaller CDF will be strictly larger, so the difference for any
    // later tests cannot be greater.
    System.out.printf("Final D = max(%f, %f)\n", D,
            Math.abs((pCumWtsArr[i] / n1) - (qCumWtsArr[j] / n2)));
    D = Math.max(D, Math.abs((pCumWtsArr[i] / n1) - (qCumWtsArr[j] / n2)));

    /*
    System.out.println("N: " + p.auxN_);
    for (int i = 0; i < p.auxSamplesArr_.length; ++i) {
      System.out.printf("%5d:\t%d\t%f\t%f\n", i, p.auxCumWtsArr_[i], p.auxSamplesArr_[i],
              p.auxCumWtsArr_[i] / (double) p.auxN_);
    }
    */
    final double eps1 = Util.EpsilonFromK.getAdjustedEpsilon(sketch1.getK());
    final double eps2 = Util.EpsilonFromK.getAdjustedEpsilon(sketch2.getK());
    final double adjustedD = D - eps1 - eps2;

    System.out.printf("D: %f\te1: %f\te2: %f\ttotal: %f \tthresh: %f \tresult: %s\n",
            D, eps1, eps2, adjustedD, thresh, adjustedD > thresh);

    return adjustedD > thresh;
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
   * Checks the validity of the given value k
   * @param k must be greater than 1 and less than 65536.
   */
  static void checkK(final int k) {
    if ((k < DoublesSketch.MIN_K) || (k >= (1 << 16)) || !isPowerOf2(k)) {
      throw new SketchesArgumentException("K must be > 1 and < 65536 and Power of 2: " + k);
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
      if (((hi - lo) / lo) < bracketedBinarySearchForEpsTol) {
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

}
