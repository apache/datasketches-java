/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import static com.yahoo.sketches.BoundsOnBinomialProportions.approximateLowerBoundOnP;
import static com.yahoo.sketches.BoundsOnBinomialProportions.approximateUpperBoundOnP;

/**
 * This class is used to compute the bounds on the estimate of the ratio <i>|B| / |A|</i>, where:
 * <ul>
 * <li><i>|A|</i> is the unknown size of a set <i>A</i> of unique identifiers.</li>
 * <li><i>|B|</i> is the unknown size of a subset <i>B</i> of <i>A</i>.</li>
 * <li><i>a</i> = <i>|S<sub>A</sub>|</i> is the observed size of a sample of <i>A</i>
 * that was obtained by Bernoulli sampling with a known inclusion probability <i>f</i>.</li>
 * <li><i>b</i> = <i>|S<sub>A</sub> &cap; B|</i> is the observed size of a subset
 * of <i>S<sub>A</sub></i>.</li>
 * </ul>
 *
 * @author Kevin Lang
 */
public final class BoundsOnRatiosInSampledSets {
  private static final double NUM_STD_DEVS = 2.0; //made a constant to simplify interface.

  private BoundsOnRatiosInSampledSets() {}

  /**
   * Return the approximate lower bound based on a 95% confidence interval
   * @param a See class javadoc
   * @param b See class javadoc
   * @param f the inclusion probability used to produce the set with size <i>a</i> and should
   * generally be less than 0.5. Above this value, the results not be reliable.
   * When <i>f</i> = 1.0 this returns the estimate.
   * @return the approximate upper bound
   */
  public static double getLowerBoundForBoverA(final long a, final long b, final double f) {
    checkInputs(a, b, f);
    if (a == 0) { return 0.0; }
    if (f == 1.0) { return (double) b / a; }
    return approximateLowerBoundOnP(a, b, NUM_STD_DEVS * hackyAdjuster(f));
  }

  /**
   * Return the approximate upper bound based on a 95% confidence interval
   * @param a See class javadoc
   * @param b See class javadoc
   * @param f the inclusion probability used to produce the set with size <i>a</i>.
   * @return the approximate lower bound
   */
  public static double getUpperBoundForBoverA(final long a, final long b, final double f) {
    checkInputs(a, b, f);
    if (a == 0) { return 1.0; }
    if (f == 1.0) { return (double) b / a; }
    return approximateUpperBoundOnP(a, b, NUM_STD_DEVS * hackyAdjuster(f));
  }

  /**
   * Return the estimate of b over a
   * @param a See class javadoc
   * @param b See class javadoc
   * @return the estimate of b over a
   */
  public static double getEstimateOfBoverA(final long a, final long b) {
    checkInputs(a, b, 0.3);
    if (a == 0) { return 0.5; }
    return (double) b / a;
  }

  /**
   * This hackyAdjuster is tightly coupled with the width of the confidence interval normally
   * specified with number of standard deviations. To simplify this interface the number of
   * standard deviations has been fixed to 2.0, which corresponds to a confidence interval of
   * 95%.
   * @param f the inclusion probability used to produce the set with size <i>a</i>.
   * @return the hacky Adjuster
   */
  private static double hackyAdjuster(final double f) {
    final double tmp = Math.sqrt(1.0 - f);
    return (f <= 0.5) ? tmp : tmp + (0.01 * (f - 0.5));
  }

  static void checkInputs(final long a, final long b, final double f) {
    if ( ( (a - b) | (a) | (b) ) < 0) {  //if any group goes negative
      throw new SketchesArgumentException(
          "a must be >= b and neither a nor b can be < 0: a = " + a + ", b = " + b);
    }
    if ((f > 1.0) || (f <= 0.0)) {
      throw new SketchesArgumentException("Required: ((f <= 1.0) && (f > 0.0)): " + f);
    }
  }

  /**
   * Return the estimate of A. See class javadoc.
   * @param a See class javadoc
   * @param f the inclusion probability used to produce the set with size <i>a</i>.
   * @return the approximate lower bound
   */
  public static double getEstimateOfA(final long a, final double f) {
    checkInputs(a, 1, f);
    return a / f;
  }

  /**
   * Return the estimate of B. See class javadoc.
   * @param b See class javadoc
   * @param f the inclusion probability used to produce the set with size <i>a</i>.
   * @return the approximate lower bound
   */
  public static double getEstimateOfB(final long b, final double f) {
    checkInputs(b + 1, b, f);
    return b / f;
  }
}
