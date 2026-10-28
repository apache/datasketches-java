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
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.thetacommon.BoundsOnRatiosInTupleSketchedSets.getEstimateOfBoverA;
import static org.apache.datasketches.thetacommon.BoundsOnRatiosInTupleSketchedSets.getLowerBoundForBoverA;
import static org.apache.datasketches.thetacommon.BoundsOnRatiosInTupleSketchedSets.getUpperBoundForBoverA;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * Jaccard similarity of two TupleSketches, or alternatively, of a TupleSketch and a ThetaSketch.
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
   * No argument constructor.
   */
  public JaccardSimilarity() { }

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
   * @param sketchA The first argument, a TupleSketch with summary type <i>S</i>
   * @param sketchB The second argument, a TupleSketch with summary type <i>S</i>
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param <S> Summary
   * @return a double array {LowerBound, Estimate, UpperBound} of the Jaccard index.
   * The Upper and Lower bounds are for a confidence interval of 95.4% or +/- 2 standard deviations.
   */
  public static <S extends Summary> double[] jaccard(
      final TupleSketch<S> sketchA,
      final TupleSketch<S> sketchB,
      final SummarySetOperations<S> summarySetOps) {
    //Corner case checks
    if (sketchA == null || sketchB == null) { return ZEROS.clone(); }
    if (sketchA == sketchB) { return ONES.clone(); }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return ONES.clone(); }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return ZEROS.clone(); }

    final TupleSketch<S> unionAB = computeUnion(sketchA, sketchB, summarySetOps);
    if (identicalSets(sketchA.getRetainedEntries(), sketchA.getThetaLong(),
        sketchB.getRetainedEntries(), sketchB.getThetaLong(), unionAB)) {
      return ONES.clone();
    }

    //Create the TupleIntersection
    final TupleIntersection<S> inter = new TupleIntersection<>(summarySetOps);
    inter.intersect(sketchA);
    inter.intersect(sketchB);
    inter.intersect(unionAB); //ensures that intersection is a subset of the union
    final TupleSketch<S> interABU = inter.getResult();

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
   * @param sketchA The first argument, a TupleSketch with summary type <i>S</i>
   * @param sketchB The second argument, a ThetaSketch
   * @param summary the given proxy summary for the ThetaSketch, which doesn't have one.
   * This may not be null.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param <S> Summary
   * @return a double array {LowerBound, Estimate, UpperBound} of the Jaccard index.
   * The Upper and Lower bounds are for a confidence interval of 95.4% or +/- 2 standard deviations.
   */
  public static <S extends Summary> double[] jaccard(
      final TupleSketch<S> sketchA,
      final ThetaSketch sketchB,
      final S summary, final SummarySetOperations<S> summarySetOps) {
    // Null case checks
    if (summary == null) {
      throw new SketchesArgumentException("Summary cannot be null."); }

    //Corner case checks
    if (sketchA == null || sketchB == null) { return ZEROS.clone(); }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return ONES.clone(); }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return ZEROS.clone(); }

    final TupleSketch<S> unionAB = computeUnion(sketchA, sketchB, summary, summarySetOps);
    if (identicalSets(sketchA.getRetainedEntries(), sketchA.getThetaLong(),
        sketchB.getRetainedEntries(true), sketchB.getThetaLong(), unionAB)) {
      return ONES.clone();
    }

    //Create the Tuple/Theta Intersection
    final TupleIntersection<S> inter = new TupleIntersection<>(summarySetOps);
    inter.intersect(sketchA);
    inter.intersect(sketchB, summary);
    inter.intersect(unionAB); //ensures that intersection is a subset of the union
    final TupleSketch<S> interABU = inter.getResult();

    final double lb = getLowerBoundForBoverA(unionAB, interABU);
    final double est = getEstimateOfBoverA(unionAB, interABU);
    final double ub = getUpperBoundForBoverA(unionAB, interABU);
    return new double[] {lb, est, ub};
  }

  /**
   * Returns true if the two given sketches have exactly the same hash values and the same
   * theta values. Thus, they are equivalent.
   * @param sketchA The first argument, a TupleSketch with summary type <i>S</i>
   * @param sketchB The second argument, a TupleSketch with summary type <i>S</i>
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param <S> Summary
   * @return true if the two given sketches have exactly the same hash values and the same
   * theta values.
   */
  public static <S extends Summary> boolean exactlyEqual(
      final TupleSketch<S> sketchA,
      final TupleSketch<S> sketchB,
      final SummarySetOperations<S> summarySetOps) {
    //Corner case checks
    if (sketchA == null || sketchB == null) { return false; }
    if (sketchA == sketchB) { return true; }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return true; }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return false; }

    final TupleSketch<S> unionAB = computeUnion(sketchA, sketchB, summarySetOps);
    return identicalSets(sketchA.getRetainedEntries(), sketchA.getThetaLong(),
        sketchB.getRetainedEntries(), sketchB.getThetaLong(), unionAB);
  }

  /**
   * Returns true if the two given sketches have exactly the same hash values and the same
   * theta values. Thus, they are equivalent.
   * @param sketchA The first argument, a TupleSketch with summary type <i>S</i>
   * @param sketchB The second argument, a ThetaSketch
   * @param summary the given proxy summary for the ThetaSketch, which doesn't have one.
   * This may not be null.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param <S> Summary
   * @return true if the two given sketches have exactly the same hash values and the same
   * theta values.
   */
  public static <S extends Summary> boolean exactlyEqual(
      final TupleSketch<S> sketchA,
      final ThetaSketch sketchB,
      final S summary, final SummarySetOperations<S> summarySetOps) {
    // Null case checks
    if (summary == null) {
      throw new SketchesArgumentException("Summary cannot be null."); }

    //Corner case checks
    if (sketchA == null || sketchB == null) { return false; }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return true; }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return false; }

    final TupleSketch<S> unionAB = computeUnion(sketchA, sketchB, summary, summarySetOps);
    return identicalSets(sketchA.getRetainedEntries(), sketchA.getThetaLong(),
        sketchB.getRetainedEntries(true), sketchB.getThetaLong(), unionAB);
  }

  /**
   * Tests similarity of a measured TupleSketch against an expected TupleSketch.
   * Computes the lower bound of the Jaccard index <i>J<sub>LB</sub></i> of the measured and
   * expected sketches.
   * if <i>J<sub>LB</sub> &ge; threshold</i>, then the sketches are considered to be
   * similar with a confidence of 97.7%.
   *
   * @param measured a TupleSketch with summary type <i>S</i> to be tested
   * @param expected the reference TupleSketch with summary type <i>S</i> that is considered to be correct.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param threshold a real value between zero and one.
   * @param <S> Summary
   * @return if true, the similarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static <S extends Summary> boolean similarityTest(
      final TupleSketch<S> measured, final TupleSketch<S> expected,
      final SummarySetOperations<S> summarySetOps,
      final double threshold) {
    //index 0: the lower bound
    //index 1: the mean estimate
    //index 2: the upper bound
    final double jRatioLB = jaccard(measured, expected, summarySetOps)[0]; //choosing the lower bound
    return jRatioLB >= threshold;
  }

  /**
   * Tests similarity of a measured TupleSketch against an expected ThetaSketch.
   * Computes the lower bound of the Jaccard index <i>J<sub>LB</sub></i> of the measured and
   * expected sketches.
   * if <i>J<sub>LB</sub> &ge; threshold</i>, then the sketches are considered to be
   * similar with a confidence of 97.7%.
   *
   * @param measured a TupleSketch with summary type <i>S</i> to be tested
   * @param expected the reference ThetaSketch that is considered to be correct.
   * @param summary the given proxy summary for the ThetaSketch, which doesn't have one.
   * This may not be null.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param threshold a real value between zero and one.
   * @param <S> Summary
   * @return if true, the similarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static <S extends Summary> boolean similarityTest(
      final TupleSketch<S> measured, final ThetaSketch expected,
      final S summary, final SummarySetOperations<S> summarySetOps,
      final double threshold) {
    //index 0: the lower bound
    //index 1: the mean estimate
    //index 2: the upper bound
    final double jRatioLB = jaccard(measured, expected, summary, summarySetOps)[0]; //choosing the lower bound
    return jRatioLB >= threshold;
  }

  /**
   * Tests dissimilarity of a measured TupleSketch against an expected TupleSketch.
   * Computes the upper bound of the Jaccard index <i>J<sub>UB</sub></i> of the measured and
   * expected sketches.
   * if <i>J<sub>UB</sub> &le; threshold</i>, then the sketches are considered to be
   * dissimilar with a confidence of 97.7%.
   *
   * @param measured a TupleSketch with summary type <i>S</i> to be tested
   * @param expected the reference TupleSketch that is considered to be correct.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param threshold a real value between zero and one.
   * @param <S> Summary
   * @return if true, the dissimilarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static <S extends Summary> boolean dissimilarityTest(
      final TupleSketch<S> measured, final TupleSketch<S> expected,
      final SummarySetOperations<S> summarySetOps,
      final double threshold) {
    //index 0: the lower bound
    //index 1: the mean estimate
    //index 2: the upper bound
    final double jRatioUB = jaccard(measured, expected, summarySetOps)[2]; //choosing the upper bound
    return jRatioUB <= threshold;
  }

  /**
   * Tests dissimilarity of a measured TupleSketch against an expected ThetaSketch.
   * Computes the upper bound of the Jaccard index <i>J<sub>UB</sub></i> of the measured and
   * expected sketches.
   * if <i>J<sub>UB</sub> &le; threshold</i>, then the sketches are considered to be
   * dissimilar with a confidence of 97.7%.
   *
   * @param measured a TupleSketch with summary type <i>S</i> to be tested
   * @param expected the reference ThetaSketch that is considered to be correct.
   * @param summary the given proxy summary for the ThetaSketch, which doesn't have one.
   * This may not be null.
   * @param summarySetOps instance of SummarySetOperations used to unify or intersect summaries.
   * @param threshold a real value between zero and one.
   * @param <S> Summary
   * @return if true, the dissimilarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static <S extends Summary> boolean dissimilarityTest(
      final TupleSketch<S> measured, final ThetaSketch expected,
      final S summary, final SummarySetOperations<S> summarySetOps,
      final double threshold) {
    //index 0: the lower bound
    //index 1: the mean estimate
    //index 2: the upper bound
    final double jRatioUB = jaccard(measured, expected, summary, summarySetOps)[2]; //choosing the upper bound
    return jRatioUB <= threshold;
  }

  /**
   * Computes the union of the two given TupleSketches. See {@link #newUnion(int, SummarySetOperations)}.
   * @param sketchA the given TupleSketch A
   * @param sketchB the given TupleSketch B
   * @param summarySetOps instance of SummarySetOperations used to unify summaries.
   * @param <S> Summary
   * @return the unordered union of sketch A and sketch B
   */
  private static <S extends Summary> TupleSketch<S> computeUnion(
      final TupleSketch<S> sketchA,
      final TupleSketch<S> sketchB,
      final SummarySetOperations<S> summarySetOps) {
    final TupleUnion<S> union =
        newUnion(sketchA.getRetainedEntries() + sketchB.getRetainedEntries(), summarySetOps);
    union.union(sketchA);
    union.union(sketchB);
    return union.getResult();
  }

  /**
   * Computes the union of the given TupleSketch and ThetaSketch. See {@link #newUnion(int, SummarySetOperations)}.
   * @param sketchA the given TupleSketch A
   * @param sketchB the given ThetaSketch B
   * @param summary the given proxy summary for the ThetaSketch.
   * @param summarySetOps instance of SummarySetOperations used to unify summaries.
   * @param <S> Summary
   * @return the unordered union of sketch A and sketch B
   */
  private static <S extends Summary> TupleSketch<S> computeUnion(
      final TupleSketch<S> sketchA,
      final ThetaSketch sketchB,
      final S summary, final SummarySetOperations<S> summarySetOps) {
    final TupleUnion<S> union =
        newUnion(sketchA.getRetainedEntries() + sketchB.getRetainedEntries(true), summarySetOps);
    union.union(sketchA);
    union.union(sketchB, summary);
    return union.getResult();
  }

  /**
   * Creates a TupleUnion sized to hold the given total number of retained entries, clamped to the
   * legal range of nominal entries.
   * @param totalCount the sum of the retained entries of the two sketches
   * @param summarySetOps instance of SummarySetOperations used to unify summaries.
   * @param <S> Summary
   * @return a new TupleUnion
   */
  private static <S extends Summary> TupleUnion<S> newUnion(
      final int totalCount, final SummarySetOperations<S> summarySetOps) {
    final int minK = 1 << ThetaUtil.MIN_LG_NOM_LONGS;
    final int maxK = 1 << ThetaUtil.MAX_LG_NOM_LONGS;
    final int newK = max(min(ceilingPowerOf2(totalCount), maxK), minK);
    return new TupleUnion<>(newK, summarySetOps);
  }

  /**
   * Returns true if the two given sketches and their union all have the same number of retained
   * entries and the same theta, which means the two sketches hold identical sets.
   * @param countA the retained entries of sketch A
   * @param thetaLongA the theta of sketch A
   * @param countB the retained entries of sketch B
   * @param thetaLongB the theta of sketch B
   * @param unionAB the union of sketch A and sketch B
   * @return true if the two given sketches hold identical sets
   */
  private static boolean identicalSets(final int countA, final long thetaLongA,
      final int countB, final long thetaLongB, final TupleSketch<?> unionAB) {
    final int countUAB = unionAB.getRetainedEntries();
    final long thetaLongUAB = unionAB.getThetaLong();
    return countUAB == countA && countUAB == countB
        && thetaLongUAB == thetaLongA && thetaLongUAB == thetaLongB;
  }

}
