/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.sketches.theta.Sketch;

/**
 * This class is used to compute the bounds on the estimate of the ratio <i>B / A</i>, where:
 * <ul>
 * <li><i>A</i> is a Theta Sketch of unique identifiers.</li>
 * <li><i>B</i> is a Theta Sketch of a subset <i>B</i> of <i>A</i> obtained by an intersection
 * with a Theta Sketch <i>C</i>.</li>
 * </ul>
 * The theta of A cannot be greater than the theta of B.
 * 
 * @author Kevin Lang
 */
public final class BoundsOnRatiosInThetaSketchedSets {
  
  private BoundsOnRatiosInThetaSketchedSets() {}
  
  /**
   * Gets the approximate lower bound for B over A based on a 95% confidence interval
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the approximate lower bound for B over A
   */
  public static double getLowerBoundForBoverA(Sketch sketchA, Sketch sketchB) {
    double thetaA = sketchA.getTheta();
    double thetaB = sketchB.getTheta();
    checkThetas(thetaA, thetaB);
    
    int countB = sketchB.getRetainedEntries(true);
    int countA = (thetaB == thetaA)? sketchA.getRetainedEntries(true) 
        : sketchA.getCountLessThanTheta(thetaB);
    
    if (countA <= 0) return 0;
    
    return BoundsOnRatiosInSampledSets.getLowerBoundForBoverA(countA, countB, thetaB);
  }
  
  /**
   * Gets the approximate upper bound for B over A based on a 95% confidence interval
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the approximate upper bound for B over A
   */
  public static double getUpperBoundForBoverA(Sketch sketchA, Sketch sketchB) {
    double thetaA = sketchA.getTheta();
    double thetaB = sketchB.getTheta();
    checkThetas(thetaA, thetaB);

    int countB = sketchB.getRetainedEntries(true);
    int countA = (thetaB == thetaA)? sketchA.getRetainedEntries(true) 
        : sketchA.getCountLessThanTheta(thetaB);
    
    if (countA <= 0) return 1.0;
    
    return BoundsOnRatiosInSampledSets.getUpperBoundForBoverA(countA, countB, thetaB);
  }
  
  /**
   * Gets the estimate for B over A
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the estimate for B over A
   */
  public static double getEstimateOfBoverA(Sketch sketchA, Sketch sketchB) {
    double thetaA = sketchA.getTheta();
    double thetaB = sketchB.getTheta();
    checkThetas(thetaA, thetaB);

    int countB = sketchB.getRetainedEntries(true);
    int countA = (thetaB == thetaA)? sketchA.getRetainedEntries(true) 
        : sketchA.getCountLessThanTheta(thetaB);
    
    if (countA <= 0) return 0.5;
    
    return (double) countB / (double) countA;
  }
  
  static void checkThetas(double thetaA, double thetaB) {
    if (thetaB > thetaA) {
      throw new SketchesArgumentException("ThetaB cannot be > ThetaA.");
    }
  }
}
