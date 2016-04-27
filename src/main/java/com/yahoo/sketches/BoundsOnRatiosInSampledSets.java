/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import static com.yahoo.sketches.BoundsOnBinomialProportions.approximateLowerBoundOnP;
import static com.yahoo.sketches.BoundsOnBinomialProportions.approximateUpperBoundOnP;
//import static com.yahoo.sketches.BinomialBoundsN.getLowerBound;
//import static com.yahoo.sketches.BinomialBoundsN.getUpperBound;
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
public class BoundsOnRatiosInSampledSets {
  
  /**
   * Return the approximate lower bound.
   * @param a See class javadoc
   * @param b See class javadoc
   * @param numStdDevs "number of standard deviations" that specifies the confidence interval
   * @param f the inclusion probability used to produce the set with size <i>a</i> and should
   * generally be less than 0.5. Above this value, the results not be reliable. 
   * When <i>f</i> = 1.0 this returns the estimate.
   * @return the approximate upper bound
   */
  public static double getLowerBoundForBoverA(long a, long b, double numStdDevs, double f) {
    checkInputs(a, b, numStdDevs, f);
    if ((f == 1.0) || (numStdDevs == 0)) return (double) b / a; 
    return approximateLowerBoundOnP(a, b, numStdDevs * Math.sqrt(1.0 - f));
  }
  
  /**
   * Return the approximate upper bound.
   * @param a See class javadoc
   * @param b See class javadoc
   * @param numStdDevs "number of standard deviations" that specifies the confidence interval
   * @param f the inclusion probability used to produce the set with size <i>a</i>.
   * @return the approximate lower bound
   */
  public static double getUpperBoundForBoverA(long a, long b, double numStdDevs, double f) {
    checkInputs(a, b, numStdDevs, f);
    if ((f == 1.0) || (numStdDevs == 0)) return (double) b / a; 
    return approximateUpperBoundOnP(a, b, numStdDevs * Math.sqrt(1.0 - f));
  }
  
  public static double getEstimateOfBoverA(long a, long b) {
    checkInputs(a, b, 1.0, 0.3);
    return (double) b / a; 
  }
  
  static void checkInputs(long a, long b, double numStdDevs, double f) {
    if ( ( (a - b) | (a -1) | (b-1) ) < 0) {  //if any group goes negative
      throw new IllegalArgumentException("a must be >= b and neither a nor b can be < 0");
    }
    if ((f != 1.0) && ((f > .5001) || (f <= 0.0))) {
      throw new IllegalArgumentException("Required: ((f > 0.0) && (f <= 0.5)) || (f == 1.0)");
    }
    if ( (numStdDevs < 0) || (numStdDevs > 3.1)) {
      throw new IllegalArgumentException("numStdDevs must be >= 0 and < 3");
    }
  }
  
  /**
   * Return the estimate of A. See class javadoc.
   * @param a See class javadoc
   * @param f the inclusion probability used to produce the set with size <i>a</i>.
   * @return the approximate lower bound
   */
  public static double getEstimateOfA(long a, double f) {
    checkInputs(a, 1, 1.0, f);
    return a / f;
  }
  
  /**
   * Return the estimate of B. See class javadoc.
   * @param b See class javadoc
   * @param f the inclusion probability used to produce the set with size <i>a</i>.
   * @return the approximate lower bound
   */
  public static double getEstimateOfB(long b, double f) {
    checkInputs(b+1, b, 1.0, f);
    return b / f;
  }
}
