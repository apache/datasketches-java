/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.BoundsOnRatiosInThetaSketchedSets.getEstimateOfBoverA;
import static com.yahoo.sketches.BoundsOnRatiosInThetaSketchedSets.getLowerBoundForBoverA;
import static com.yahoo.sketches.BoundsOnRatiosInThetaSketchedSets.getUpperBoundForBoverA;

/**
 * Jaccard similarity of two Theta Sketches.
 *
 * @author Lee Rhodes
 */
public final class JaccardSimilarity {

  /**
   * Computes the Jaccard similarity ratio with upper and lower bounds. The Jaccard similarity ratio
   * <i>J(A,B) = (A ^ B)/(A U B)</i> is used to measure how similar the two sketches are to each
   * other. If J = 1.0, the sketches are considered equal. If J = 0, the two sketches are
   * distinct from each other. A Jaccard of .95 means the overlap between the two
   * populations is 95% of the union of the two populations.
   *
   * @param sketchA given sketch A
   * @param sketchB given sketch B
   * @param minK The minimum value of <i>k</i> or <i>NominalEntries</i> used to create the two
   * input sketches.
   * @return a double array {LowerBound, Estimate, UpperBound} of the Jaccard ratio.
   * The Upper and Lower bounds are for a confidence interval of 95.4% or +/- 2 standard deviations.
   */
  public static double[] jaccard(final Sketch sketchA, final Sketch sketchB, final int minK) {
    final double[] ret = {0.0, 0.0, 0.0}; // LB, Estimate, UB

    //Corner case checks
    if ((sketchA == null) || (sketchB == null)) { return ret; } //or throw?
    if (sketchA.isEmpty() && sketchB.isEmpty())  {
      ret[0] = ret[1] = ret[2] = 1.0;
      return ret;
    }
    if (sketchA.isEmpty() || sketchB.isEmpty())  { return ret; }

    //Create the Union
    final Union union = SetOperation.builder().buildUnion(minK);
    union.update(sketchA);
    union.update(sketchB);
    final Sketch unionAB = union.getResult(true, null);

    //Create the Intersection
    final Intersection inter = SetOperation.builder().buildIntersection();
    inter.update(sketchA);
    inter.update(sketchB);
    inter.update(unionAB); //ensures that intersection is a subset of the union
    final Sketch interABU = inter.getResult(true, null);

    ret[0] = getLowerBoundForBoverA(unionAB, interABU);
    ret[1] = getEstimateOfBoverA(unionAB, interABU);
    ret[2] = getUpperBoundForBoverA(unionAB, interABU);
    return ret;
  }

  /**
   * Tests similarity of a measured Sketch against an expected Sketch.
   * Computes the lower bound of the Jaccard ratio <i>J<sub>LB</sub></i> of the measured and
   * expected sketches.
   * if <i>J<sub>LB</sub> >= threshold</i>, then the sketches are considered to be
   * similar with a confidence of 97.7%.
   *
   * @param measured the sketch to be tested
   * @param expected the reference sketch that is considered to be correct.
   * @param minK the minimum value of <i>k</i> used for the given sketches.
   * @param threshold a real value between zero and one.
   * @return if true, the similarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static boolean similarityTest(final Sketch measured, final Sketch expected,
      final int minK, final double threshold) {
      //index 0: the lower bound
      //index 1: the mean estimate
      //index 2: the upper bound
      final double jRatio = jaccard(measured, expected, minK)[0]; //choosing the lower bound
    return jRatio >= threshold;
  }

  /**
   * Tests dissimilarity of a measured Sketch against an expected Sketch.
   * Computes the upper bound of the Jaccard ratio <i>J<sub>LB</sub></i> of the measured and
   * expected sketches.
   * if <i>J<sub>UB</sub> >= threshold</i>, then the sketches are considered to be
   * dissimilar with a confidence of 97.7%.
   *
   * @param measured the sketch to be tested
   * @param expected the reference sketch that is considered to be correct.
   * @param minK the minimum value of <i>k</i> used for the given sketches.
   * @param threshold a real value between zero and one.
   * @return if true, the dissimilarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static boolean dissimilarityTest(final Sketch measured, final Sketch expected,
      final int minK, final double threshold) {
      //index 0: the lower bound
      //index 1: the mean estimate
      //index 2: the upper bound
      final double jRatio = jaccard(measured, expected, minK)[2]; //choosing the upper bound
    return jRatio <= threshold;
  }

}
