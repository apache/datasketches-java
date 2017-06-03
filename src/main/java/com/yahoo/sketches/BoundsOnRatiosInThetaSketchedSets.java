/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.sketches.theta.Sketch;

/**
 * This class is used to compute the bounds on the estimate of the ratio <i>B / A</i>, where:
 * <ul>
 * <li><i>A</i> is a Theta Sketch of population <i>PopA</i>.</li>
 * <li><i>B</i> is a Theta Sketch of population <i>PopB</i> that is a subset of <i>A</i>,
 * obtained by an intersection of <i>A</i> with some other Theta Sketch <i>C</i>,
 * which acts like a predicate or selection clause.</li>
 * <li>The estimate of the ratio <i>PopB/PopA</i> is
 * BoundsOnRatiosInThetaSketchedSets.getEstimateOfBoverA(<i>A, B</i>).</li>
 * <li>The Upper Bound estimate on the ratio PopB/PopA is
 * BoundsOnRatiosInThetaSketchedSets.getUpperBoundForBoverA(<i>A, B</i>).</li>
 * <li>The Lower Bound estimate on the ratio PopB/PopA is
 * BoundsOnRatiosInThetaSketchedSets.getLowerBoundForBoverA(<i>A, B</i>).</li>
 * </ul>
 * Note: The theta of <i>A</i> cannot be greater than the theta of <i>B</i>.
 * If <i>B</i> is formed as an intersection of <i>A</i> and some other set <i>C</i>,
 * then the theta of <i>B</i> is guaranteed to be less than or equal to the theta of <i>B</i>.
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 */
public final class BoundsOnRatiosInThetaSketchedSets {

  private BoundsOnRatiosInThetaSketchedSets() {}

  /**
   * Gets the approximate lower bound for B over A based on a 95% confidence interval
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the approximate lower bound for B over A
   */
  public static double getLowerBoundForBoverA(final Sketch sketchA, final Sketch sketchB) {
    final double thetaA = sketchA.getTheta();
    final double thetaB = sketchB.getTheta();
    checkThetas(thetaA, thetaB);

    final int countB = sketchB.getRetainedEntries(true);
    final int countA = (thetaB == thetaA) ? sketchA.getRetainedEntries(true)
        : sketchA.getCountLessThanTheta(thetaB);

    if (countA <= 0) { return 0; }

    return BoundsOnRatiosInSampledSets.getLowerBoundForBoverA(countA, countB, thetaB);
  }

  /**
   * Gets the approximate upper bound for B over A based on a 95% confidence interval
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the approximate upper bound for B over A
   */
  public static double getUpperBoundForBoverA(final Sketch sketchA, final Sketch sketchB) {
    final double thetaA = sketchA.getTheta();
    final double thetaB = sketchB.getTheta();
    checkThetas(thetaA, thetaB);

    final int countB = sketchB.getRetainedEntries(true);
    final int countA = (thetaB == thetaA) ? sketchA.getRetainedEntries(true)
        : sketchA.getCountLessThanTheta(thetaB);

    if (countA <= 0) { return 1.0; }

    return BoundsOnRatiosInSampledSets.getUpperBoundForBoverA(countA, countB, thetaB);
  }

  /**
   * Gets the estimate for B over A
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the estimate for B over A
   */
  public static double getEstimateOfBoverA(final Sketch sketchA, final Sketch sketchB) {
    final double thetaA = sketchA.getTheta();
    final double thetaB = sketchB.getTheta();
    checkThetas(thetaA, thetaB);

    final int countB = sketchB.getRetainedEntries(true);
    final int countA = (thetaB == thetaA) ? sketchA.getRetainedEntries(true)
        : sketchA.getCountLessThanTheta(thetaB);

    if (countA <= 0) { return 0.5; }

    return (double) countB / (double) countA;
  }

  static void checkThetas(final double thetaA, final double thetaB) {
    if (thetaB > thetaA) {
      throw new SketchesArgumentException("ThetaB cannot be > ThetaA.");
    }
  }
}
