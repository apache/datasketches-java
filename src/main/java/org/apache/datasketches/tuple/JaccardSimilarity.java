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

package org.apache.datasketches.tuple;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.BoundsOnRatiosInTupleSketchedSets.getEstimateOfBoverA;
import static org.apache.datasketches.BoundsOnRatiosInTupleSketchedSets.getLowerBoundForBoverA;
import static org.apache.datasketches.BoundsOnRatiosInTupleSketchedSets.getUpperBoundForBoverA;
import static org.apache.datasketches.Util.MAX_LG_NOM_LONGS;
import static org.apache.datasketches.Util.MIN_LG_NOM_LONGS;
import static org.apache.datasketches.Util.ceilingPowerOf2;

import org.apache.datasketches.SketchesArgumentException;

/**
 * Jaccard similarity of two Tuple Sketches, or alternatively, of a Tuple and Theta Sketch.
 *
 * <p>Note: only retained hash values are compared, and the Tuple summary values are not accounted for in the
 * similarity measure.</p>
 *
 * @author Lee Rhodes
 * @author David Cromberge
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
   * @param sketchA The first argument, a Tuple sketch with summary type <i>S</i>
   * @param sketchB The second argument, a Tuple sketch with summary type <i>S</i>
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param <S> Summary
   * @return a double array {LowerBound, Estimate, UpperBound} of the Jaccard index.
   * The Upper and Lower bounds are for a confidence interval of 95.4% or +/- 2 standard deviations.
   */
  public static <S extends Summary> double[] jaccard(
      final Sketch<S> sketchA,
      final Sketch<S> sketchB,
      final SummarySetOperations<S> summarySetOps) {
    //Corner case checks
    if (sketchA == null || sketchB == null) { return ZEROS.clone(); }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return ONES.clone(); }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return ZEROS.clone(); }

    final int countA = sketchA.getRetainedEntries();
    final int countB = sketchB.getRetainedEntries();

    //Create the Union
    final int minK = 1 << MIN_LG_NOM_LONGS;
    final int maxK = 1 << MAX_LG_NOM_LONGS;
    final int newK = max(min(ceilingPowerOf2(countA + countB), maxK), minK);
    final Union<S> union = new Union<>(newK, summarySetOps);
    union.union(sketchA);
    union.union(sketchB);

    final Sketch<S> unionAB = union.getResult();
    final long thetaLongUAB = unionAB.getThetaLong();
    final long thetaLongA = sketchA.getThetaLong();
    final long thetaLongB = sketchB.getThetaLong();
    final int countUAB = unionAB.getRetainedEntries();

    //Check for identical data
    if (countUAB == countA && countUAB == countB
            && thetaLongUAB == thetaLongA && thetaLongUAB == thetaLongB) {
      return ONES.clone();
    }

    //Create the Intersection
    final Intersection<S> inter = new Intersection<>(summarySetOps);
    inter.intersect(sketchA);
    inter.intersect(sketchB);
    inter.intersect(unionAB); //ensures that intersection is a subset of the union
    final Sketch<S> interABU = inter.getResult();

    final double lb = getLowerBoundForBoverA(unionAB, interABU);
    final double est = getEstimateOfBoverA(unionAB, interABU);
    final double ub = getUpperBoundForBoverA(unionAB, interABU);
    return new double[] {lb, est, ub};
  }

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
   * @param sketchA The first argument, a Tuple sketch with summary type <i>S</i>
   * @param sketchB The second argument, a Theta sketch
   * @param summary the given proxy summary for the theta sketch, which doesn't have one.
   * This may not be null.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param <S> Summary
   * @return a double array {LowerBound, Estimate, UpperBound} of the Jaccard index.
   * The Upper and Lower bounds are for a confidence interval of 95.4% or +/- 2 standard deviations.
   */
  public static <S extends Summary> double[] jaccard(
      final Sketch<S> sketchA,
      final org.apache.datasketches.theta.Sketch sketchB,
      final S summary, final SummarySetOperations<S> summarySetOps) {
    // Null case checks
    if (summary == null) {
      throw new SketchesArgumentException("Summary cannot be null."); }

    //Corner case checks
    if (sketchA == null || sketchB == null) { return ZEROS.clone(); }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return ONES.clone(); }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return ZEROS.clone(); }

    final int countA = sketchA.getRetainedEntries();
    final int countB = sketchB.getRetainedEntries(true);

    //Create the Union
    final int minK = 1 << MIN_LG_NOM_LONGS;
    final int maxK = 1 << MAX_LG_NOM_LONGS;
    final int newK = max(min(ceilingPowerOf2(countA + countB), maxK), minK);
    final Union<S> union = new Union<>(newK, summarySetOps);
    union.union(sketchA);
    union.union(sketchB, summary);

    final Sketch<S> unionAB = union.getResult();
    final long thetaLongUAB = unionAB.getThetaLong();
    final long thetaLongA = sketchA.getThetaLong();
    final long thetaLongB = sketchB.getThetaLong();
    final int countUAB = unionAB.getRetainedEntries();

    //Check for identical data
    if (countUAB == countA && countUAB == countB
            && thetaLongUAB == thetaLongA && thetaLongUAB == thetaLongB) {
      return ONES.clone();
    }

    //Create the Intersection
    final Intersection<S> inter = new Intersection<>(summarySetOps);
    inter.intersect(sketchA);
    inter.intersect(sketchB, summary);
    inter.intersect(unionAB); //ensures that intersection is a subset of the union
    final Sketch<S> interABU = inter.getResult();

    final double lb = getLowerBoundForBoverA(unionAB, interABU);
    final double est = getEstimateOfBoverA(unionAB, interABU);
    final double ub = getUpperBoundForBoverA(unionAB, interABU);
    return new double[] {lb, est, ub};
  }

  /**
   * Returns true if the two given sketches have exactly the same hash values and the same
   * theta values. Thus, they are equivalent.
   * @param sketchA The first argument, a Tuple sketch with summary type <i>S</i>
   * @param sketchB The second argument, a Tuple sketch with summary type <i>S</i>
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param <S> Summary
   * @return true if the two given sketches have exactly the same hash values and the same
   * theta values.
   */
  public static <S extends Summary> boolean exactlyEqual(
      final Sketch<S> sketchA,
      final Sketch<S> sketchB,
      final SummarySetOperations<S> summarySetOps) {
    //Corner case checks
    if (sketchA == null || sketchB == null) { return false; }
    if (sketchA == sketchB) { return true; }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return true; }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return false; }

    final int countA = sketchA.getRetainedEntries();
    final int countB = sketchB.getRetainedEntries();

    //Create the Union
    final Union<S> union = new Union<>(ceilingPowerOf2(countA + countB), summarySetOps);
    union.union(sketchA);
    union.union(sketchB);
    final Sketch<S> unionAB = union.getResult();
    final long thetaLongUAB = unionAB.getThetaLong();
    final long thetaLongA = sketchA.getThetaLong();
    final long thetaLongB = sketchB.getThetaLong();
    final int countUAB = unionAB.getRetainedEntries();

    //Check for identical counts and thetas
    if (countUAB == countA && countUAB == countB
            && thetaLongUAB == thetaLongA && thetaLongUAB == thetaLongB) {
      return true;
    }
    return false;
  }

  /**
   * Returns true if the two given sketches have exactly the same hash values and the same
   * theta values. Thus, they are equivalent.
   * @param sketchA The first argument, a Tuple sketch with summary type <i>S</i>
   * @param sketchB The second argument, a Theta sketch
   * @param summary the given proxy summary for the theta sketch, which doesn't have one.
   * This may not be null.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param <S> Summary
   * @return true if the two given sketches have exactly the same hash values and the same
   * theta values.
   */
  public static <S extends Summary> boolean exactlyEqual(
      final Sketch<S> sketchA,
      final org.apache.datasketches.theta.Sketch sketchB,
      final S summary, final SummarySetOperations<S> summarySetOps) {
    // Null case checks
    if (summary == null) {
      throw new SketchesArgumentException("Summary cannot be null."); }

    //Corner case checks
    if (sketchA == null || sketchB == null) { return false; }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return true; }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return false; }

    final int countA = sketchA.getRetainedEntries();
    final int countB = sketchB.getRetainedEntries(true);

    //Create the Union
    final Union<S> union = new Union<>(ceilingPowerOf2(countA + countB), summarySetOps);
    union.union(sketchA);
    union.union(sketchB, summary);
    final Sketch<S> unionAB = union.getResult();
    final long thetaLongUAB = unionAB.getThetaLong();
    final long thetaLongA = sketchA.getThetaLong();
    final long thetaLongB = sketchB.getThetaLong();
    final int countUAB = unionAB.getRetainedEntries();

    //Check for identical counts and thetas
    if (countUAB == countA && countUAB == countB
        && thetaLongUAB == thetaLongA && thetaLongUAB == thetaLongB) {
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
   * @param measured a Tuple sketch with summary type <i>S</i> to be tested
   * @param expected the reference Tuple sketch with summary type <i>S</i> that is considered to be correct.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param threshold a real value between zero and one.
   * @param <S> Summary
   * @return if true, the similarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static <S extends Summary> boolean similarityTest(
      final Sketch<S> measured, final Sketch<S> expected,
      final SummarySetOperations<S> summarySetOps,
      final double threshold) {
    //index 0: the lower bound
    //index 1: the mean estimate
    //index 2: the upper bound
    final double jRatioLB = jaccard(measured, expected, summarySetOps)[0]; //choosing the lower bound
    return jRatioLB >= threshold;
  }

  /**
   * Tests similarity of a measured Sketch against an expected Sketch.
   * Computes the lower bound of the Jaccard index <i>J<sub>LB</sub></i> of the measured and
   * expected sketches.
   * if <i>J<sub>LB</sub> &ge; threshold</i>, then the sketches are considered to be
   * similar with a confidence of 97.7%.
   *
   * @param measured a Tuple sketch with summary type <i>S</i> to be tested
   * @param expected the reference Theta sketch that is considered to be correct.
   * @param summary the given proxy summary for the theta sketch, which doesn't have one.
   * This may not be null.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param threshold a real value between zero and one.
   * @param <S> Summary
   * @return if true, the similarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static <S extends Summary> boolean similarityTest(
      final Sketch<S> measured, final org.apache.datasketches.theta.Sketch expected,
      final S summary, final SummarySetOperations<S> summarySetOps,
      final double threshold) {
    //index 0: the lower bound
    //index 1: the mean estimate
    //index 2: the upper bound
    final double jRatioLB = jaccard(measured, expected, summary, summarySetOps)[0]; //choosing the lower bound
    return jRatioLB >= threshold;
  }

  /**
   * Tests dissimilarity of a measured Sketch against an expected Sketch.
   * Computes the upper bound of the Jaccard index <i>J<sub>UB</sub></i> of the measured and
   * expected sketches.
   * if <i>J<sub>UB</sub> &le; threshold</i>, then the sketches are considered to be
   * dissimilar with a confidence of 97.7%.
   *
   * @param measured a Tuple sketch with summary type <i>S</i> to be tested
   * @param expected the reference Theta sketch that is considered to be correct.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param threshold a real value between zero and one.
   * @param <S> Summary
   * @return if true, the dissimilarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static <S extends Summary> boolean dissimilarityTest(
      final Sketch<S> measured, final Sketch<S> expected,
      final SummarySetOperations<S> summarySetOps,
      final double threshold) {
    //index 0: the lower bound
    //index 1: the mean estimate
    //index 2: the upper bound
    final double jRatioUB = jaccard(measured, expected, summarySetOps)[2]; //choosing the upper bound
    return jRatioUB <= threshold;
  }

  /**
   * Tests dissimilarity of a measured Sketch against an expected Sketch.
   * Computes the upper bound of the Jaccard index <i>J<sub>UB</sub></i> of the measured and
   * expected sketches.
   * if <i>J<sub>UB</sub> &le; threshold</i>, then the sketches are considered to be
   * dissimilar with a confidence of 97.7%.
   *
   * @param measured a Tuple sketch with summary type <i>S</i> to be tested
   * @param expected the reference Theta sketch that is considered to be correct.
   * @param summary the given proxy summary for the theta sketch, which doesn't have one.
   * This may not be null.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param threshold a real value between zero and one.
   * @param <S> Summary
   * @return if true, the dissimilarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static <S extends Summary> boolean dissimilarityTest(
      final Sketch<S> measured, final org.apache.datasketches.theta.Sketch expected,
      final S summary, final SummarySetOperations<S> summarySetOps,
      final double threshold) {
    //index 0: the lower bound
    //index 1: the mean estimate
    //index 2: the upper bound
    final double jRatioUB = jaccard(measured, expected, summary, summarySetOps)[2]; //choosing the upper bound
    return jRatioUB <= threshold;
  }

}
