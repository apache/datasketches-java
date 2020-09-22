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

package org.apache.datasketches.theta;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.BoundsOnRatiosInThetaSketchedSets.getEstimateOfBoverA;
import static org.apache.datasketches.BoundsOnRatiosInThetaSketchedSets.getLowerBoundForBoverA;
import static org.apache.datasketches.BoundsOnRatiosInThetaSketchedSets.getUpperBoundForBoverA;
import static org.apache.datasketches.Util.MAX_LG_NOM_LONGS;
import static org.apache.datasketches.Util.MIN_LG_NOM_LONGS;
import static org.apache.datasketches.Util.ceilingPowerOf2;

/**
 * Jaccard similarity of two Theta Sketches.
 *
 * @author Lee Rhodes
 */
public final class JaccardSimilarity {
  private static final double[] ZEROS = {0.0, 0.0, 0.0}; // LB, Estimate, UB
  private static final double[] ONES = {1.0, 1.0, 1.0};

  /**
   * Computes the Jaccard similarity index with upper and lower bounds. The Jaccard similarity index
   * <i>J(A,B) = (A ^ B)/(A U B)</i> is used to measure how similar the two sketches are to each
   * other. If J = 1.0, the sketches are considered equal. If J = 0, the two sketches are
   * distinct from each other. A Jaccard of .95 means the overlap between the two
   * populations is 95% of the union of the two populations.
   *
   * <p>Note: For very large pairs of sketches, where the configured nominal entries of the sketches
   * are 2^25 or 2^26, this method may produce unpredictable results.
   *
   * @param sketchA given sketch A
   * @param sketchB given sketch B
   * @return a double array {LowerBound, Estimate, UpperBound} of the Jaccard index.
   * The Upper and Lower bounds are for a confidence interval of 95.4% or +/- 2 standard deviations.
   */
  public static double[] jaccard(final Sketch sketchA, final Sketch sketchB) {
    //Corner case checks
    if ((sketchA == null) || (sketchB == null)) { return ZEROS.clone(); }
    if (sketchA == sketchB) { return ONES.clone(); }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return ONES.clone(); }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return ZEROS.clone(); }

    final int countA = sketchA.getRetainedEntries(true);
    final int countB = sketchB.getRetainedEntries(true);

    //Create the Union
    final int minK = 1 << MIN_LG_NOM_LONGS;
    final int maxK = 1 << MAX_LG_NOM_LONGS;
    final int newK = max(min(ceilingPowerOf2(countA + countB), maxK), minK);
    final Union union =
        SetOperation.builder().setNominalEntries(newK).buildUnion();
    union.update(sketchA);
    union.update(sketchB);
    final Sketch unionAB = union.getResult(false, null);
    final long thetaLongUAB = unionAB.getThetaLong();
    final long thetaLongA = sketchA.getThetaLong();
    final long thetaLongB = sketchB.getThetaLong();
    final int countUAB = unionAB.getRetainedEntries(true);

    //Check for identical data
    if ((countUAB == countA) && (countUAB == countB)
        && (thetaLongUAB == thetaLongA) && (thetaLongUAB == thetaLongB)) {
      return ONES.clone();
    }

    //Create the Intersection
    final Intersection inter = SetOperation.builder().buildIntersection();
    inter.intersect(sketchA);
    inter.intersect(sketchB);
    inter.intersect(unionAB); //ensures that intersection is a subset of the union
    final Sketch interABU = inter.getResult(false, null);

    final double lb = getLowerBoundForBoverA(unionAB, interABU);
    final double est = getEstimateOfBoverA(unionAB, interABU);
    final double ub = getUpperBoundForBoverA(unionAB, interABU);
    return new double[] {lb, est, ub};
  }

  /**
   * Returns true if the two given sketches have exactly the same hash values and the same
   * theta values. Thus, they are equivalent.
   * @param sketchA the given sketch A
   * @param sketchB the given sketch B
   * @return true if the two given sketches have exactly the same hash values and the same
   * theta values.
   */
  public static boolean exactlyEqual(final Sketch sketchA, final Sketch sketchB) {
    //Corner case checks
    if ((sketchA == null) || (sketchB == null)) { return false; }
    if (sketchA == sketchB) { return true; }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return true; }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return false; }

    final int countA = sketchA.getRetainedEntries(true);
    final int countB = sketchB.getRetainedEntries(true);

    //Create the Union
    final Union union =
        SetOperation.builder().setNominalEntries(ceilingPowerOf2(countA + countB)).buildUnion();
    union.update(sketchA);
    union.update(sketchB);
    final Sketch unionAB = union.getResult();
    final long thetaLongUAB = unionAB.getThetaLong();
    final long thetaLongA = sketchA.getThetaLong();
    final long thetaLongB = sketchB.getThetaLong();
    final int countUAB = unionAB.getRetainedEntries(true);

    //Check for identical counts and thetas
    if ((countUAB == countA) && (countUAB == countB)
        && (thetaLongUAB == thetaLongA) && (thetaLongUAB == thetaLongB)) {
      return true;
    }
    return false;
  }

  /**
   * Tests similarity of a measured Sketch against an expected Sketch.
   * Computes the lower bound of the Jaccard index <i>J<sub>LB</sub></i> of the measured and
   * expected sketches.
   * if <i>J<sub>LB</sub> &ge; threshold</i>, then the sketches are considered to be
   * similar with a confidence of 97.7%.
   *
   * @param measured the sketch to be tested
   * @param expected the reference sketch that is considered to be correct.
   * @param threshold a real value between zero and one.
   * @return if true, the similarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static boolean similarityTest(final Sketch measured, final Sketch expected,
      final double threshold) {
      //index 0: the lower bound
      //index 1: the mean estimate
      //index 2: the upper bound
      final double jRatioLB = jaccard(measured, expected)[0]; //choosing the lower bound
    return jRatioLB >= threshold;
  }

  /**
   * Tests dissimilarity of a measured Sketch against an expected Sketch.
   * Computes the upper bound of the Jaccard index <i>J<sub>UB</sub></i> of the measured and
   * expected sketches.
   * if <i>J<sub>UB</sub> &le; threshold</i>, then the sketches are considered to be
   * dissimilar with a confidence of 97.7%.
   *
   * @param measured the sketch to be tested
   * @param expected the reference sketch that is considered to be correct.
   * @param threshold a real value between zero and one.
   * @return if true, the dissimilarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static boolean dissimilarityTest(final Sketch measured, final Sketch expected,
      final double threshold) {
      //index 0: the lower bound
      //index 1: the mean estimate
      //index 2: the upper bound
      final double jRatioUB = jaccard(measured, expected)[2]; //choosing the upper bound
    return jRatioUB <= threshold;
  }

}
