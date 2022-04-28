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

import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.adouble.DoubleSummarySetOperations;
import org.testng.annotations.Test;

import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import static org.apache.datasketches.tuple.JaccardSimilarity.dissimilarityTest;
import static org.apache.datasketches.tuple.JaccardSimilarity.exactlyEqual;
import static org.apache.datasketches.tuple.JaccardSimilarity.jaccard;
import static org.apache.datasketches.tuple.JaccardSimilarity.similarityTest;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Lee Rhodes
 * @author David Cromberge
 */
public class JaccardSimilarityTest {
  private final DoubleSummary.Mode umode = DoubleSummary.Mode.Sum;
  private final DoubleSummarySetOperations dsso = new DoubleSummarySetOperations();
  private final DoubleSummaryFactory factory = new DoubleSummaryFactory(umode);
  private final UpdateSketchBuilder thetaBldr = UpdateSketch.builder();
  private final UpdatableSketchBuilder<Double, DoubleSummary> tupleBldr = new UpdatableSketchBuilder<>(factory);
  private final Double constSummary = 1.0;

  @Test
  public void checkNullsEmpties1() { // tuple, tuple
    int minK = 1 << 12;
    double threshold = 0.95;
    println("Check nulls & empties, minK: " + minK + "\t Th: " + threshold);
    //check both null
    double[] jResults = jaccard(null, null, dsso);
    boolean state = jResults[1] > threshold;
    println("null \t null:\t" + state + "\t" + jaccardString(jResults));
    assertFalse(state);

    state = exactlyEqual(null, null, dsso);
    assertFalse(state);

    final UpdatableSketch<Double, DoubleSummary> measured = tupleBldr.setNominalEntries(minK).build();
    final UpdatableSketch<Double, DoubleSummary> expected = tupleBldr.setNominalEntries(minK).build();

    //check both empty
    jResults = jaccard(measured, expected, dsso);
    state = jResults[1] > threshold;
    println("empty\tempty:\t" + state + "\t" + jaccardString(jResults));
    assertTrue(state);

    state = exactlyEqual(measured, expected, dsso);
    assertTrue(state);

    state = exactlyEqual(measured, measured, dsso);
    assertTrue(state);

    //adjust one
    expected.update(1, constSummary);
    jResults = jaccard(measured, expected, dsso);
    state = jResults[1] > threshold;
    println("empty\t    1:\t" + state + "\t" + jaccardString(jResults));
    assertFalse(state);

    state = exactlyEqual(measured, expected, dsso);
    assertFalse(state);

    println("");
  }

  @Test
  public void checkNullsEmpties2() { // tuple, theta
    int minK = 1 << 12;
    double threshold = 0.95;
    println("Check nulls & empties, minK: " + minK + "\t Th: " + threshold);
    //check both null
    double[] jResults = jaccard(null, null, factory.newSummary(), dsso);
    boolean state = jResults[1] > threshold;
    println("null \t null:\t" + state + "\t" + jaccardString(jResults));
    assertFalse(state);

    state = exactlyEqual(null, null, factory.newSummary(), dsso);
    assertFalse(state);

    final UpdatableSketch<Double, DoubleSummary> measured = tupleBldr.setNominalEntries(minK).build();
    final UpdateSketch expected = thetaBldr.setNominalEntries(minK).build();

    //check both empty
    jResults = jaccard(measured, expected, factory.newSummary(), dsso);
    state = jResults[1] > threshold;
    println("empty\tempty:\t" + state + "\t" + jaccardString(jResults));
    assertTrue(state);

    state = exactlyEqual(measured, expected, factory.newSummary(), dsso);
    assertTrue(state);

    state = exactlyEqual(measured, measured, dsso);
    assertTrue(state);

    //adjust one
    expected.update(1);
    jResults = jaccard(measured, expected, factory.newSummary(), dsso);
    state = jResults[1] > threshold;
    println("empty\t    1:\t" + state + "\t" + jaccardString(jResults));
    assertFalse(state);

    state = exactlyEqual(measured, expected, factory.newSummary(), dsso);
    assertFalse(state);

    println("");
  }

  @Test
  public void checkExactMode1() { // tuple, tuple
    int k = 1 << 12;
    int u = k;
    double threshold = 0.9999;
    println("Exact Mode, minK: " + k + "\t Th: " + threshold);

    final UpdatableSketch<Double, DoubleSummary> measured = tupleBldr.setNominalEntries(k).build();
    final UpdatableSketch<Double, DoubleSummary> expected = tupleBldr.setNominalEntries(k).build();

    for (int i = 0; i < (u-1); i++) { //one short
      measured.update(i, constSummary);
      expected.update(i, constSummary);
    }

    double[] jResults = jaccard(measured, expected, dsso);
    boolean state = jResults[1] > threshold;
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);

    state = exactlyEqual(measured, expected, dsso);
    assertTrue(state);

    measured.update(u-1, constSummary); //now exactly k entries
    expected.update(u, constSummary);   //now exactly k entries but differs by one
    jResults = jaccard(measured, expected, dsso);
    state = jResults[1] > threshold;
    println(state + "\t" + jaccardString(jResults));
    assertFalse(state);

    state = exactlyEqual(measured, expected, dsso);
    assertFalse(state);

    println("");
  }

  @Test
  public void checkExactMode2() { // tuple, theta
    int k = 1 << 12;
    int u = k;
    double threshold = 0.9999;
    println("Exact Mode, minK: " + k + "\t Th: " + threshold);

    final UpdatableSketch<Double, DoubleSummary> measured = tupleBldr.setNominalEntries(k).build();
    final UpdateSketch expected = thetaBldr.setNominalEntries(k).build();

    for (int i = 0; i < (u-1); i++) { //one short
      measured.update(i, constSummary);
      expected.update(i);
    }

    double[] jResults = jaccard(measured, expected, factory.newSummary(), dsso);
    boolean state = jResults[1] > threshold;
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);

    state = exactlyEqual(measured, expected, factory.newSummary(), dsso);
    assertTrue(state);

    measured.update(u-1, constSummary); //now exactly k entries
    expected.update(u);   //now exactly k entries but differs by one
    jResults = jaccard(measured, expected, factory.newSummary(), dsso);
    state = jResults[1] > threshold;
    println(state + "\t" + jaccardString(jResults));
    assertFalse(state);

    state = exactlyEqual(measured, expected, factory.newSummary(), dsso);
    assertFalse(state);

    println("");
  }

  @Test
  public void checkEstMode1() { // tuple, tuple
    int k = 1 << 12;
    int u = 1 << 20;
    double threshold = 0.9999;
    println("Estimation Mode, minK: " + k + "\t Th: " + threshold);

    final UpdatableSketch<Double, DoubleSummary> measured = tupleBldr.setNominalEntries(k).build();
    final UpdatableSketch<Double, DoubleSummary> expected = tupleBldr.setNominalEntries(k).build();

    for (int i = 0; i < u; i++) {
      measured.update(i, constSummary);
      expected.update(i, constSummary);
    }

    double[] jResults = jaccard(measured, expected, dsso);
    boolean state = jResults[1] > threshold;
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);

    state = exactlyEqual(measured, expected, dsso);
    assertTrue(state);

    for (int i = u; i < (u + 50); i++) { //empirically determined
      measured.update(i, constSummary);
    }

    jResults = jaccard(measured, expected, dsso);
    state = jResults[1] >= threshold;
    println(state + "\t" + jaccardString(jResults));
    assertFalse(state);

    state = exactlyEqual(measured, expected, dsso);
    assertFalse(state);

    println("");
  }

  @Test
  public void checkEstMode2() { // tuple, theta
    int k = 1 << 12;
    int u = 1 << 20;
    double threshold = 0.9999;
    println("Estimation Mode, minK: " + k + "\t Th: " + threshold);

    final UpdatableSketch<Double, DoubleSummary> measured = tupleBldr.setNominalEntries(k).build();
    final UpdateSketch expected = thetaBldr.setNominalEntries(k).build();

    for (int i = 0; i < u; i++) {
      measured.update(i, constSummary);
      expected.update(i);
    }

    double[] jResults = jaccard(measured, expected, factory.newSummary(), dsso);
    boolean state = jResults[1] > threshold;
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);

    state = exactlyEqual(measured, expected, factory.newSummary(), dsso);
    assertTrue(state);

    for (int i = u; i < (u + 50); i++) { //empirically determined
      measured.update(i, constSummary);
    }

    jResults = jaccard(measured, expected, factory.newSummary(), dsso);
    state = jResults[1] >= threshold;
    println(state + "\t" + jaccardString(jResults));
    assertFalse(state);

    state = exactlyEqual(measured, expected, factory.newSummary(), dsso);
    assertFalse(state);

    println("");
  }

  /**
   * Enable printing on this test and you will see that the distribution is pretty tight,
   * about +/- 0.7%, which is pretty good since the accuracy of the underlying sketch is about
   * +/- 1.56%.
   */
  @Test
  public void checkSimilarity1() { // tuple, tuple
    int minK = 1 << 12;
    int u1 = 1 << 20;
    int u2 = (int) (u1 * 0.95);
    double threshold = 0.943;
    println("Estimation Mode, minK: " + minK + "\t Th: " + threshold);

    final UpdatableSketch<Double, DoubleSummary> measured = tupleBldr.setNominalEntries(minK).build();
    final UpdatableSketch<Double, DoubleSummary> expected = tupleBldr.setNominalEntries(minK).build();

    for (int i = 0; i < u1; i++) {
      expected.update(i, constSummary);
    }

    for (int i = 0; i < u2; i++) {
      measured.update(i, constSummary);
    }

    double[] jResults = jaccard(measured, expected, dsso);
    boolean state = similarityTest(measured, expected, dsso, threshold);
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);
    //check identity case
    state = similarityTest(measured, measured, dsso, threshold);
    assertTrue(state);
  }

  /**
   * Enable printing on this test and you will see that the distribution is pretty tight,
   * about +/- 0.7%, which is pretty good since the accuracy of the underlying sketch is about
   * +/- 1.56%.
   */
  @Test
  public void checkSimilarity2() { // tuple, theta
    int minK = 1 << 12;
    int u1 = 1 << 20;
    int u2 = (int) (u1 * 0.95);
    double threshold = 0.943;
    println("Estimation Mode, minK: " + minK + "\t Th: " + threshold);

    final UpdatableSketch<Double, DoubleSummary> measured = tupleBldr.setNominalEntries(minK).build();
    final UpdateSketch expected = thetaBldr.setNominalEntries(minK).build();

    for (int i = 0; i < u1; i++) {
      expected.update(i);
    }

    for (int i = 0; i < u2; i++) {
      measured.update(i, constSummary);
    }

    double[] jResults = jaccard(measured, expected, factory.newSummary(), dsso);
    boolean state = similarityTest(measured, expected, factory.newSummary(), dsso, threshold);
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);
    //check identity case
    state = similarityTest(measured, measured, dsso, threshold);
    assertTrue(state);
  }

  /**
   * Enable printing on this test and you will see that the distribution is much looser,
   * about +/- 14%.  This is due to the fact that intersections loose accuracy as the ratio of
   * intersection to the union becomes a small number.
   */
  @Test
  public void checkDissimilarity1() { // tuple, tuple
    int minK = 1 << 12;
    int u1 = 1 << 20;
    int u2 = (int) (u1 * 0.05);
    double threshold = 0.061;
    println("Estimation Mode, minK: " + minK + "\t Th: " + threshold);

    final UpdatableSketch<Double, DoubleSummary> measured = tupleBldr.setNominalEntries(minK).setNominalEntries(minK).build();
    final UpdatableSketch<Double, DoubleSummary> expected = tupleBldr.setNominalEntries(minK).setNominalEntries(minK).build();

    for (int i = 0; i < u1; i++) {
      expected.update(i, constSummary);
    }

    for (int i = 0; i < u2; i++) {
      measured.update(i, constSummary);
    }

    double[] jResults = jaccard(measured, expected, dsso);
    boolean state = dissimilarityTest(measured, expected, dsso, threshold);
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);
  }

  /**
   * Enable printing on this test and you will see that the distribution is much looser,
   * about +/- 14%.  This is due to the fact that intersections loose accuracy as the ratio of
   * intersection to the union becomes a small number.
   */
  @Test
  public void checkDissimilarity2() { // tuple, theta
    int minK = 1 << 12;
    int u1 = 1 << 20;
    int u2 = (int) (u1 * 0.05);
    double threshold = 0.061;
    println("Estimation Mode, minK: " + minK + "\t Th: " + threshold);

    final UpdatableSketch<Double, DoubleSummary> measured = tupleBldr.setNominalEntries(minK).setNominalEntries(minK).build();
    final UpdateSketch expected = thetaBldr.setNominalEntries(minK).build();

    for (int i = 0; i < u1; i++) {
      expected.update(i);
    }

    for (int i = 0; i < u2; i++) {
      measured.update(i, constSummary);
    }

    double[] jResults = jaccard(measured, expected, factory.newSummary(), dsso);
    boolean state = dissimilarityTest(measured, expected, factory.newSummary(), dsso, threshold);
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);
  }

  private static String jaccardString(double[] jResults) {
    double lb = jResults[0];
    double est = jResults[1];
    double ub = jResults[2];
    return lb + "\t" + est + "\t" + ub + "\t" + ((lb/est) - 1.0) + "\t" + ((ub/est) - 1.0);
  }

  @Test
  public void checkMinK1() { // tuple, tuple
    final UpdatableSketch<Double, DoubleSummary> skA = tupleBldr.build(); //4096
    final UpdatableSketch<Double, DoubleSummary> skB = tupleBldr.build(); //4096
    skA.update(1, constSummary);
    skB.update(1, constSummary);
    double[] result = jaccard(skA, skB, dsso);
    println(result[0] + ", " + result[1] + ", " + result[2]);
    for (int i = 1; i < 4096; i++) {
      skA.update(i, constSummary);
      skB.update(i, constSummary);
    }
    result = jaccard(skA, skB, dsso);
    println(result[0] + ", " + result[1] + ", " + result[2]);
  }

  @Test
  public void checkMinK2() { // tuple, theta
    final UpdatableSketch<Double, DoubleSummary> skA = tupleBldr.build(); //4096
    final UpdateSketch skB = UpdateSketch.builder().build(); //4096
    skA.update(1, constSummary);
    skB.update(1);
    double[] result = jaccard(skA, skB, factory.newSummary(), dsso);
    println(result[0] + ", " + result[1] + ", " + result[2]);
    for (int i = 1; i < 4096; i++) {
      skA.update(i, constSummary);
      skB.update(i);
    }
    result = jaccard(skA, skB, factory.newSummary(), dsso);
    println(result[0] + ", " + result[1] + ", " + result[2]);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
