/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.Fm85Util.maxLgK;
import static com.yahoo.sketches.cpc.Fm85Util.minLgK;
import static com.yahoo.sketches.cpc.IconPolynomialCoefficients.iconPolynomialCoefficents;
import static com.yahoo.sketches.cpc.IconPolynomialCoefficients.iconPolynomialNumCoefficients;

import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesStateException;

/**
 * The ICON estimator for FM85 sketches is defined by the arXiv paper.
 *
 * <p>The current file provides exact and approximate implementations of this estimator.
 *
 * <p>The exact version works for any value of K, but is quite slow.
 *
 * <p>The much faster approximate version works for K values that are powers of two
 * ranging from 2^4 to 2^32.
 *
 * <p>At a high-level, this approximation can be described as using an
 * exponential approximation when C > K * (5.6 or 5.7), while smaller
 * values of C are handled by a degree-19 polynomial approximation of
 * a pre-conditioned version of the true ICON mapping from C to N_hat.
 *
 * <p>This file also provides a validation procedure that compares its approximate
 * and exact implementations of the FM85 ICON estimator.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class IconEstimator {

  static double evaluatePolynomial(final double[] coefficients, final int start, final int num,
      final double x) {
    final int end = (start + num) - 1;
    double total = coefficients[end];
    for (int j = end - 1; j >= start; j--) {
      total *= x;
      total += coefficients[j];
    }
    return total;
  }

  static double iconExponentialApproximation(final double k, final double c) {
    return (0.7940236163830469 * k * Math.pow(2.0, c / k));
  }

  static double getIconEstimate(final int lgK, final long c) {
    assert lgK >= minLgK;
    assert lgK <= maxLgK;
    if (c < 2L) { return ((c == 0L) ? 0.0 : 1.0); }
    final int k = 1 << lgK;
    final double doubleK = k;
    final double doubleC = c;
    // Differing thresholds ensure that the approximated estimator is monotonically increasing.
    final double thresholdFactor = ((lgK < 14) ? 5.7 : 5.6);
    if (doubleC > (thresholdFactor * doubleK)) {
      return (iconExponentialApproximation(doubleK, doubleC));
    }
    final double factor = evaluatePolynomial(iconPolynomialCoefficents,
        iconPolynomialNumCoefficients * (lgK - minLgK),
        iconPolynomialNumCoefficients,
        // The somewhat arbitrary constant 2.0 is baked into the table iconPolynomialCoefficents[].
        doubleC / (2.0 * doubleK));
    final double ratio = doubleC / doubleK;
    // The somewhat arbitrary constant 66.774757 is baked into the table iconPolynomialCoefficents[].
    final double term = 1.0 + ((ratio * ratio * ratio) / 66.774757);
    final double result = doubleC * factor * term;
    return (result >= doubleC) ? result : doubleC;
  }

  //SLOW EXACT VERSION  //TODO REMOVE

  /**
   * Important note: do not change anything in the following function.
   * It has been carefully designed and tested for numerical accuracy.
   * In particular, the use of log1p and expm1 is critically important.
   */
  static double qnj(final double kf, final double nf, final int col) {
    final double tmp1 = -1.0 / (kf * (Math.pow(2.0, col)));
    final double tmp2 = Math.log1p(tmp1);
    return (-1.0 * (Math.expm1(nf * tmp2)));
  }

  static double exactCofN(final double kf, final double nf) {
    double total = 0.0;
    for (int col = 128; col >= 1; col--) {
      total += qnj(kf, nf, col);
    }
    return kf * total;
  }

  static final double iconInversionTolerance = 1.0e-15;

  static double exactIconEstimatorBinarySearch(final double kf, final double targetC, final double nLoIn,
      final double nHiIn) {
    int depth = 0;
    double nLo = nLoIn;
    double nHi = nHiIn;

    while (true) { // manual tail recursion optimization
      if (depth > 100) {
        throw new SketchesStateException("Excessive recursion in binary search\n");
      }
      assert nHi > nLo;
      final double nMid = nLo + (0.5 * (nHi - nLo));
      assert ((nMid > nLo) && (nMid < nHi));
      if (((nHi - nLo) / nMid) < iconInversionTolerance) {
        return (nMid);
      }
      final double midC = exactCofN(kf, nMid);
      if (midC == targetC) {
        return (nMid);
      }
      if (midC < targetC) {
        nLo = nMid;
        depth++;
        continue;
      }
      if (midC  > targetC) {
        nHi = nMid;
        depth++;
        continue;
      }
      throw new SketchesStateException("bad value in binary search\n");
    }
  }

  static double exactIconEstimatorBracketHi(final double kf, final double targetC, final double nLo) {
    int depth = 0;
    double curN = 2.0 * nLo;
    double curC = exactCofN(kf, curN);
    while (curC <= targetC) {
      if (depth > 100) {
        throw new SketchesStateException("Excessive looping in exactIconEstimatorBracketHi\n");
      }
      depth++;
      curN *= 2.0;
      curC = exactCofN(kf, curN);
    }
    assert (curC > targetC);
    return (curN);
  }

  static double exactIconEstimator(final int lgK, final long c) {
    final double targetC = c;
    if ((c == 0L) || (c == 1L)) {
      return targetC;
    }
    final double kf = 1L << lgK;
    final double nLo = targetC;
    assert exactCofN(kf, nLo) < targetC; // bracket lo
    final double nHi = exactIconEstimatorBracketHi(kf, targetC, nLo); // bracket hi
    return exactIconEstimatorBinarySearch(kf, targetC, nLo, nHi);
  }

  /*
    while  (exactCofN(kf, nHi) <= targetC) {nHi *= 2.0;} // bracket
  */

  //TESTING DRIVER
  @Test
  public static void testDriver() {

    //    if (argv.length != 1) {
    //      throw new SketchesArgumentException("Usage: testDriver(lg_k)");
    //    }
    final int lgK = 12; //Integer.parseInt(argv[0]);
    final long k = (1L << lgK);
    long c = 1;
    while (c < (k * 64)) { // was k * 15
      final double exact  = exactIconEstimator(lgK, c);
      final double approx = getIconEstimate(lgK, c);
      final double relDiff = (approx - exact) / exact;
      final String out = String.format("%d\t%.19g\t%.19g\t%.19g", c, relDiff, exact, approx);
      System.out.println(out);
      final long a = c + 1;
      final long b = (1001 * c) / 1000;
      c = ((a > b) ? a : b);
    }
  }
}
