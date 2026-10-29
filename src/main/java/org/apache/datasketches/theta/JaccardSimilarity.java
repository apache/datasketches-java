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
import static org.apache.datasketches.common.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.thetacommon.BoundsOnRatiosInThetaSketchedSets.getEstimateOfBoverA;
import static org.apache.datasketches.thetacommon.BoundsOnRatiosInThetaSketchedSets.getLowerBoundForBoverA;
import static org.apache.datasketches.thetacommon.BoundsOnRatiosInThetaSketchedSets.getUpperBoundForBoverA;

import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * Jaccard similarity of two ThetaSketches.
 *
 * @author Lee Rhodes
 */
public final class JaccardSimilarity {
  private static final double[] ZEROS = {0.0, 0.0, 0.0}; // LB, Estimate, UB
  private static final double[] ONES = {1.0, 1.0, 1.0};

  private JaccardSimilarity() { }

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
   * <p>Both sketches must have been created with the default update seed.</p>
   *
   * @param sketchA given sketch A
   * @param sketchB given sketch B
   * @return a double array {LowerBound, Estimate, UpperBound} of the Jaccard index.
   * The Upper and Lower bounds are for a confidence interval of 95.4% or +/- 2 standard deviations.
   */
  public static double[] jaccard(final ThetaSketch sketchA, final ThetaSketch sketchB) {
    return jaccard(sketchA, sketchB, DEFAULT_UPDATE_SEED);
  }

  /**
   * Computes the Jaccard similarity index with upper and lower bounds, for two sketches created
   * with the given update seed. See {@link #jaccard(ThetaSketch, ThetaSketch)}.
   *
   * @param sketchA given sketch A
   * @param sketchB given sketch B
   * @param seed the update seed that both sketches were created with.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return a double array {LowerBound, Estimate, UpperBound} of the Jaccard index.
   * The Upper and Lower bounds are for a confidence interval of 95.4% or +/- 2 standard deviations.
   */
  public static double[] jaccard(final ThetaSketch sketchA, final ThetaSketch sketchB,
      final long seed) {
    //Corner case checks
    if (sketchA == null || sketchB == null) { return ZEROS.clone(); }
    if (sketchA == sketchB) { return ONES.clone(); }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return ONES.clone(); }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return ZEROS.clone(); }

    final ThetaSketch unionAB = computeUnion(sketchA, sketchB, seed);
    if (identicalSets(sketchA, sketchB, unionAB)) { return ONES.clone(); }

    //Create the ThetaIntersection
    final ThetaIntersection inter = ThetaSetOperation.builder().setSeed(seed).buildIntersection();
    inter.intersect(sketchA);
    inter.intersect(sketchB);
    inter.intersect(unionAB); //ensures that intersection is a subset of the union
    final ThetaSketch interABU = inter.getResult(false, null);

    final double lb = getLowerBoundForBoverA(unionAB, interABU);
    final double est = getEstimateOfBoverA(unionAB, interABU);
    final double ub = getUpperBoundForBoverA(unionAB, interABU);
    return new double[] {lb, est, ub};
  }

  /**
   * Returns true if the two given sketches have exactly the same hash values and the same
   * theta values. Thus, they are equivalent.
   *
   * <p>Note: For very large pairs of sketches, where the configured nominal entries of the sketches
   * are 2^26, this method may return false even if the two sketches are equal.
   *
   * <p>Both sketches must have been created with the default update seed.</p>
   *
   * @param sketchA the given sketch A
   * @param sketchB the given sketch B
   * @return true if the two given sketches have exactly the same hash values and the same
   * theta values.
   */
  public static boolean exactlyEqual(final ThetaSketch sketchA, final ThetaSketch sketchB) {
    return exactlyEqual(sketchA, sketchB, DEFAULT_UPDATE_SEED);
  }

  /**
   * Returns true if the two given sketches, created with the given update seed, have exactly the
   * same hash values and the same theta values. See {@link #exactlyEqual(ThetaSketch, ThetaSketch)}.
   *
   * @param sketchA the given sketch A
   * @param sketchB the given sketch B
   * @param seed the update seed that both sketches were created with.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return true if the two given sketches have exactly the same hash values and the same
   * theta values.
   */
  public static boolean exactlyEqual(final ThetaSketch sketchA, final ThetaSketch sketchB,
      final long seed) {
    //Corner case checks
    if (sketchA == null || sketchB == null) { return false; }
    if (sketchA == sketchB) { return true; }
    if (sketchA.isEmpty() && sketchB.isEmpty()) { return true; }
    if (sketchA.isEmpty() || sketchB.isEmpty()) { return false; }

    final ThetaSketch unionAB = computeUnion(sketchA, sketchB, seed);
    return identicalSets(sketchA, sketchB, unionAB);
  }

  /**
   * Tests similarity of a measured ThetaSketch against an expected ThetaSketch.
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
  public static boolean similarityTest(final ThetaSketch measured, final ThetaSketch expected,
      final double threshold) {
    return similarityTest(measured, expected, threshold, DEFAULT_UPDATE_SEED);
  }

  /**
   * Tests similarity of a measured ThetaSketch against an expected ThetaSketch, both created
   * with the given update seed. See {@link #similarityTest(ThetaSketch, ThetaSketch, double)}.
   *
   * @param measured the sketch to be tested
   * @param expected the reference sketch that is considered to be correct.
   * @param threshold a real value between zero and one.
   * @param seed the update seed that both sketches were created with.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return if true, the similarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static boolean similarityTest(final ThetaSketch measured, final ThetaSketch expected,
      final double threshold, final long seed) {
      //index 0: the lower bound
      //index 1: the mean estimate
      //index 2: the upper bound
      final double jRatioLB = jaccard(measured, expected, seed)[0]; //choosing the lower bound
    return jRatioLB >= threshold;
  }

  /**
   * Tests dissimilarity of a measured ThetaSketch against an expected ThetaSketch.
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
  public static boolean dissimilarityTest(final ThetaSketch measured, final ThetaSketch expected,
      final double threshold) {
    return dissimilarityTest(measured, expected, threshold, DEFAULT_UPDATE_SEED);
  }

  /**
   * Tests dissimilarity of a measured ThetaSketch against an expected ThetaSketch, both created
   * with the given update seed. See {@link #dissimilarityTest(ThetaSketch, ThetaSketch, double)}.
   *
   * @param measured the sketch to be tested
   * @param expected the reference sketch that is considered to be correct.
   * @param threshold a real value between zero and one.
   * @param seed the update seed that both sketches were created with.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return if true, the dissimilarity of the two sketches is greater than the given threshold
   * with at least 97.7% confidence.
   */
  public static boolean dissimilarityTest(final ThetaSketch measured, final ThetaSketch expected,
      final double threshold, final long seed) {
      //index 0: the lower bound
      //index 1: the mean estimate
      //index 2: the upper bound
      final double jRatioUB = jaccard(measured, expected, seed)[2]; //choosing the upper bound
    return jRatioUB <= threshold;
  }

  /**
   * Computes the union of the two given sketches. The union is sized to hold the retained entries
   * of both sketches, clamped to the legal range of nominal entries. The result is unordered,
   * because only its retained entries and theta are used.
   * @param sketchA the given sketch A
   * @param sketchB the given sketch B
   * @param seed the update seed that both sketches were created with
   * @return the unordered union of sketch A and sketch B
   */
  private static ThetaSketch computeUnion(final ThetaSketch sketchA, final ThetaSketch sketchB,
      final long seed) {
    final int countA = sketchA.getRetainedEntries(true);
    final int countB = sketchB.getRetainedEntries(true);
    final int minK = 1 << ThetaUtil.MIN_LG_NOM_LONGS;
    final int maxK = 1 << ThetaUtil.MAX_LG_NOM_LONGS;
    final int newK = max(min(ceilingPowerOf2(countA + countB), maxK), minK);
    final ThetaUnion union =
        ThetaSetOperation.builder().setNominalEntries(newK).setSeed(seed).buildUnion();
    union.union(sketchA);
    union.union(sketchB);
    return union.getResult(false, null);
  }

  /**
   * Returns true if the two given sketches and their union all have the same number of retained
   * entries and the same theta, which means the two sketches hold identical sets.
   * @param sketchA the given sketch A
   * @param sketchB the given sketch B
   * @param unionAB the union of sketch A and sketch B
   * @return true if the two given sketches hold identical sets
   */
  private static boolean identicalSets(final ThetaSketch sketchA, final ThetaSketch sketchB,
      final ThetaSketch unionAB) {
    final int countUAB = unionAB.getRetainedEntries(true);
    final long thetaLongUAB = unionAB.getThetaLong();
    return countUAB == sketchA.getRetainedEntries(true)
        && countUAB == sketchB.getRetainedEntries(true)
        && thetaLongUAB == sketchA.getThetaLong()
        && thetaLongUAB == sketchB.getThetaLong();
  }

}
