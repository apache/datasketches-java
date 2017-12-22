/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

/**
 * Confidence intervals for binomial proportions.
 *
 * <p>This class computes an approximation to the Clopper-Pearson confidence interval
 * for a binomial proportion. Exact Clopper-Pearson intervals are strictly
 * conservative, but these approximations are not.</p>
 *
 * <p>The main inputs are numbers <i>n</i> and <i>k</i>, which are not the same as other things
 * that are called <i>n</i> and <i>k</i> in our sketching library. There is also a third
 * parameter, numStdDev, that specifies the desired confidence level.</p>
 * <ul>
 * <li><i>n</i> is the number of independent randomized trials. It is given and therefore known.
 * </li>
 * <li><i>p</i> is the probability of a trial being a success. It is unknown.</li>
 * <li><i>k</i> is the number of trials (out of <i>n</i>) that turn out to be successes. It is
 * a random variable governed by a binomial distribution. After any given
 * batch of <i>n</i> independent trials, the random variable <i>k</i> has a specific
 * value which is observed and is therefore known.</li>
 * <li><i>pHat</i> = <i>k</i> / <i>n</i> is an unbiased estimate of the unknown success
 * probability <i>p</i>.</li>
 * </ul>
 *
 * <p>Alternatively, consider a coin with unknown heads probability <i>p</i>. Where
 * <i>n</i> is the number of independent flips of that coin, and <i>k</i> is the number
 * of times that the coin comes up heads during a given batch of <i>n</i> flips.
 * This class computes a frequentist confidence interval [lowerBoundOnP, upperBoundOnP] for the
 * unknown <i>p</i>.</p>
 *
 * <p>Conceptually, the desired confidence level is specified by a tail probability delta.</p>
 *
 * <p>Ideally, over a large ensemble of independent batches of trials,
 * the fraction of batches in which the true <i>p</i> lies below lowerBoundOnP would be at most
 * delta, and the fraction of batches in which the true <i>p</i> lies above upperBoundOnP
 * would also be at most delta.
 *
 * <p>Setting aside the philosophical difficulties attaching to that statement, it isn't quite
 * true because we are approximating the Clopper-Pearson interval.</p>
 *
 * <p>Finally, we point out that in this class's interface, the confidence parameter delta is
 * not specified directly, but rather through a "number of standard deviations" numStdDev.
 * The library effectively converts that to a delta via delta = normalCDF (-1.0 * numStdDev).</p>
 *
 * <p>It is perhaps worth emphasizing that the library is NOT merely adding and subtracting
 * numStdDev standard deviations to the estimate. It is doing something better, that to some
 * extent accounts for the fact that the binomial distribution has a non-gaussian shape.</p>
 *
 * <p>In particular, it is using an approximation to the inverse of the incomplete beta function
 * that appears as formula 26.5.22 on page 945 of the "Handbook of Mathematical Functions"
 * by Abramowitz and Stegun.</p>
 *
 * @author Kevin Lang
 */
public final class BoundsOnBinomialProportions { // confidence intervals for binomial proportions

  private BoundsOnBinomialProportions() {}

  /**
   * Computes lower bound of approximate Clopper-Pearson confidence interval for a binomial
   * proportion.
   *
   * <p>Implementation Notes:<br>
   * The approximateLowerBoundOnP is defined with respect to the right tail of the binomial
   * distribution.</p>
   * <ul>
   * <li>We want to solve for the <i>p</i> for which sum<sub><i>j,k,n</i></sub>bino(<i>j;n,p</i>)
   * = delta.</li>
   * <li>We now restate that in terms of the left tail.</li>
   * <li>We want to solve for the p for which sum<sub><i>j,0,(k-1)</i></sub>bino(<i>j;n,p</i>)
   * = 1 - delta.</li>
   * <li>Define <i>x</i> = 1-<i>p</i>.</li>
   * <li>We want to solve for the <i>x</i> for which I<sub><i>x(n-k+1,k)</i></sub> = 1 - delta.</li>
   * <li>We specify 1-delta via numStdDevs through the right tail of the standard normal
   * distribution.</li>
   * <li>Smaller values of numStdDevs correspond to bigger values of 1-delta and hence to smaller
   * values of delta. In fact, usefully small values of delta correspond to negative values of
   * numStdDevs.</li>
   * <li>return <i>p</i> = 1-<i>x</i>.</li>
   * </ul>
   *
   * @param n is the number of trials. Must be non-negative.
   * @param k is the number of successes. Must be non-negative, and cannot exceed n.
   * @param numStdDevs the number of standard deviations defining the confidence interval
   * @return the lower bound of the approximate Clopper-Pearson confidence interval for the
   * unknown success probability.
   */
  public static double approximateLowerBoundOnP(final long n, final long k, final double numStdDevs) {
    checkInputs(n, k);
    if (n == 0) { return 0.0; } // the coin was never flipped, so we know nothing
    else if (k == 0) { return 0.0; }
    else if (k == 1) { return (exactLowerBoundOnPForKequalsOne(n, deltaOfNumStdevs(numStdDevs))); }
    else if (k == n) { return (exactLowerBoundOnPForKequalsN(n, deltaOfNumStdevs(numStdDevs))); }
    else {
      final double x = abramowitzStegunFormula26p5p22((n - k) + 1, k, (-1.0 * numStdDevs));
      return (1.0 - x); // which is p
    }
  }

  /**
   * Computes upper bound of approximate Clopper-Pearson confidence interval for a binomial
   * proportion.
   *
   * <p>Implementation Notes:<br>
   * The approximateUpperBoundOnP is defined with respect to the left tail of the binomial
   * distribution.</p>
   * <ul>
   * <li>We want to solve for the <i>p</i> for which sum<sub><i>j,0,k</i></sub>bino(<i>j;n,p</i>)
   * = delta.</li>
   * <li>Define <i>x</i> = 1-<i>p</i>.</li>
   * <li>We want to solve for the <i>x</i> for which I<sub><i>x(n-k,k+1)</i></sub> = delta.</li>
   * <li>We specify delta via numStdDevs through the right tail of the standard normal
   * distribution.</li>
   * <li>Bigger values of numStdDevs correspond to smaller values of delta.</li>
   * <li>return <i>p</i> = 1-<i>x</i>.</li>
   * </ul>
   * @param n is the number of trials. Must be non-negative.
   * @param k is the number of successes. Must be non-negative, and cannot exceed <i>n</i>.
   * @param numStdDevs the number of standard deviations defining the confidence interval
   * @return the upper bound of the approximate Clopper-Pearson confidence interval for the
   * unknown success probability.
   */
  public static double approximateUpperBoundOnP(final long n, final long k, final double numStdDevs) {
    checkInputs(n, k);
    if (n == 0) { return 1.0; } // the coin was never flipped, so we know nothing
    else if (k == n) { return 1.0; }
    else if (k == (n - 1)) {
      return (exactUpperBoundOnPForKequalsNminusOne(n, deltaOfNumStdevs(numStdDevs)));
    }
    else if (k == 0) {
      return (exactUpperBoundOnPForKequalsZero(n, deltaOfNumStdevs(numStdDevs)));
    }
    else {
      final double x = abramowitzStegunFormula26p5p22(n - k, k + 1, numStdDevs);
      return (1.0 - x); // which is p
    }
  }

  /**
   * Computes an estimate of an unknown binomial proportion.
   * @param n is the number of trials. Must be non-negative.
   * @param k is the number of successes. Must be non-negative, and cannot exceed n.
   * @return the estimate of the unknown binomial proportion.
   */
  public static double estimateUnknownP(final long n, final long k) {
    checkInputs(n, k);
    if (n == 0) { return 0.5; } // the coin was never flipped, so we know nothing
    else { return ((double) k / (double) n); }
  }

  private static void checkInputs(final long n, final long k) {
    if (n < 0) { throw new SketchesArgumentException("N must be non-negative"); }
    if (k < 0) { throw new SketchesArgumentException("K must be non-negative"); }
    if (k > n) { throw new SketchesArgumentException("K cannot exceed N"); }
  }

  /**
   * Computes an approximation to the erf() function.
   * @param x is the input to the erf function
   * @return returns erf(x), accurate to roughly 7 decimal digits.
   */
  public static double erf(final double x) {
    if (x < 0.0) { return (-1.0 * (erf_of_nonneg(-1.0 * x))); }
    else { return (erf_of_nonneg(x)); }
  }

  /**
   * Computes an approximation to normalCDF(x).
   * @param x is the input to the normalCDF function
   * @return returns the approximation to normalCDF(x).
   */
  public static double normalCDF(final double x) {
    return (0.5 * (1.0 + (erf(x / (Math.sqrt(2.0))))));
  }

  //@formatter:off
  // Abramowitz and Stegun formula 7.1.28, p. 88; Claims accuracy of about 7 decimal digits */
  private static double erf_of_nonneg(final double x) {
    // The constants that appear below, formatted for easy checking against the book.
    //    a1 = 0.07052 30784
    //    a3 = 0.00927 05272
    //    a5 = 0.00027 65672
    //    a2 = 0.04228 20123
    //    a4 = 0.00015 20143
    //    a6 = 0.00004 30638
    final double a1 = 0.0705230784;
    final double a3 = 0.0092705272;
    final double a5 = 0.0002765672;
    final double a2 = 0.0422820123;
    final double a4 = 0.0001520143;
    final double a6 = 0.0000430638;
    final double x2 = x * x; // x squared, x cubed, etc.
    final double x3 = x2 * x;
    final double x4 = x2 * x2;
    final double x5 = x2 * x3;
    final double x6 = x3 * x3;
    final double sum = ( 1.0
                 + (a1 * x)
                 + (a2 * x2)
                 + (a3 * x3)
                 + (a4 * x4)
                 + (a5 * x5)
                 + (a6 * x6) );
    final double sum2 = sum * sum; // raise the sum to the 16th power
    final double sum4 = sum2 * sum2;
    final double sum8 = sum4 * sum4;
    final double sum16 = sum8 * sum8;
    return (1.0 - (1.0 / sum16));
  }

  private static double deltaOfNumStdevs(final double kappa) {
    return (normalCDF(-1.0 * kappa));
  }

  //@formatter:on
  // Formula 26.5.22 on page 945 of Abramowitz & Stegun, which is an approximation
  // of the inverse of the incomplete beta function I_x(a,b) = delta
  // viewed as a scalar function of x.
  // In other words, we specify delta, and it gives us x (with a and b held constant).
  // However, delta is specified in an indirect way through yp which
  // is the number of stdDevs that leaves delta probability in the right
  // tail of a standard gaussian distribution.

  // We point out that the variable names correspond to those in the book,
  // and it is worth keeping it that way so that it will always be easy to verify
  // that the formula was typed in correctly.

  private static double abramowitzStegunFormula26p5p22(final double a, final double b,
      final double yp) {
    final double b2m1 = (2.0 * b) - 1.0;
    final double a2m1 = (2.0 * a) - 1.0;
    final double lambda = ((yp * yp) - 3.0) / 6.0;
    final double htmp = (1.0 / a2m1) + (1.0 / b2m1);
    final double h = 2.0 / htmp;
    final double term1 = (yp * (Math.sqrt(h + lambda))) / h;
    final double term2 = (1.0 / b2m1) - (1.0 / a2m1);
    final double term3 = (lambda + (5.0 / 6.0)) - (2.0 / (3.0 * h));
    final double w = term1 - (term2 * term3);
    final double xp = a / (a + (b * (Math.exp(2.0 * w))));
    return xp;
  }

  // Formulas for some special cases.

  private static double exactUpperBoundOnPForKequalsZero(final double n, final double delta) {
    return (1.0 - Math.pow(delta, (1.0 / n)));
  }

  private static double exactLowerBoundOnPForKequalsN(final double n, final double delta) {
    return (Math.pow(delta, (1.0 / n)));
  }

  private static double exactLowerBoundOnPForKequalsOne(final double n, final double delta) {
    return (1.0 - Math.pow((1.0 - delta), (1.0 / n)));
  }

  private static double exactUpperBoundOnPForKequalsNminusOne(final double n, final double delta) {
    return (Math.pow((1.0 - delta), (1.0 / n)));
  }

}
