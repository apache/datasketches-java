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

package org.apache.datasketches;

import static org.apache.datasketches.BoundsOnBinomialProportions.approximateLowerBoundOnP;
import static org.apache.datasketches.BoundsOnBinomialProportions.approximateUpperBoundOnP;

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
   * @param f the inclusion probability used to produce the set with size <i>b</i>.
   * @return the approximate lower bound
   */
  public static double getEstimateOfB(final long b, final double f) {
    checkInputs(b + 1, b, f);
    return b / f;
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

}
