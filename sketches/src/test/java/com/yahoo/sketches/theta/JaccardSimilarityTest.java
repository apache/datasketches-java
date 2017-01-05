/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class JaccardSimilarityTest {

  @Test
  public void checkNullsEmptiesJaccard() {
    int minK = 1 << 12;
    double threshold = 0.95;
    println("Check nulls & empties, minK: " + minK + "\t Th: " + threshold);
    //check both null
    double[] jResults = JaccardSimilarity.jaccard(null, null);
    boolean state = jResults[1] > threshold;
    println("null \t null:\t" + state + "\t" + jaccardString(jResults));
    assertFalse(state);

    UpdateSketch measured = UpdateSketch.builder().build(minK);
    UpdateSketch expected = UpdateSketch.builder().build(minK);

    //check both empty
    jResults = JaccardSimilarity.jaccard(measured, expected);
    state = jResults[1] > threshold;
    println("empty\tempty:\t" + state + "\t" + jaccardString(jResults));
    assertTrue(state);

    //adjust one
    expected.update(1);
    jResults = JaccardSimilarity.jaccard(measured, expected);
    state = jResults[1] > threshold;
    println("empty\t    1:\t" + state + "\t" + jaccardString(jResults));
    assertFalse(state);
    println("");
  }

  @Test
  public void checkJaccardExactMode() {
    int k = 1 << 12;
    int u = k;
    double threshold = 0.9999;
    println("Exact Mode, minK: " + k + "\t Th: " + threshold);

    UpdateSketch measured = UpdateSketch.builder().build(k);
    UpdateSketch expected = UpdateSketch.builder().build(k);

    for (int i = 0; i < u-1; i++) { //one short
      measured.update(i);
      expected.update(i);
    }

    double[] jResults = JaccardSimilarity.jaccard(measured, expected);
    boolean state = jResults[1] > threshold;
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);

    measured.update(u-1); //now exactly k entries
    expected.update(u);   //now exactly k entries but differs by one
    jResults = JaccardSimilarity.jaccard(measured, expected);
    state = jResults[1] > threshold;
    println(state + "\t" + jaccardString(jResults));
    assertFalse(state);
    println("");
  }

  @Test
  public void checkJaccardEstMode() {
    int k = 1 << 12;
    int u = 1 << 20;
    double threshold = 0.9999;
    println("Estimation Mode, minK: " + k + "\t Th: " + threshold);

    UpdateSketch measured = UpdateSketch.builder().build(k);
    UpdateSketch expected = UpdateSketch.builder().build(k);

    for (int i = 0; i < u; i++) {
      measured.update(i);
      expected.update(i);
    }

    double[] jResults = JaccardSimilarity.jaccard(measured, expected);
    boolean state = jResults[1] > threshold;
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);

    for (int i = u; i < u + 50; i++) { //empirically determined
      measured.update(i);
    }

    jResults = JaccardSimilarity.jaccard(measured, expected);
    state = jResults[1] >= threshold;
    println(state + "\t" + jaccardString(jResults));
    assertFalse(state);
    println("");
  }

  /**
   * Enable printing on this test and you will see that the distribution is pretty tight,
   * about +/- 0.7%, which is pretty good since the accuracy of the underlying sketch is about
   * +/- 1.56%.
   */
  @Test
  public void checkSimilarity() {
    int minK = 1 << 12;
    int u1 = 1 << 20;
    int u2 = (int) (u1 * 0.95);
    double threshold = 0.943;
    println("Estimation Mode, minK: " + minK + "\t Th: " + threshold);

    UpdateSketch expected = UpdateSketch.builder().build(minK);
    UpdateSketch measured = UpdateSketch.builder().build(minK);

    for (int i = 0; i < u1; i++) {
      expected.update(i);
    }

    for (int i = 0; i < u2; i++) {
      measured.update(i);
    }

    double[] jResults = JaccardSimilarity.jaccard(measured, expected);
    boolean state = JaccardSimilarity.similarityTest(measured, expected, threshold);
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);
    //check identity case
    state = JaccardSimilarity.similarityTest(measured, measured, threshold);
    assertTrue(state);
  }

  /**
   * Enable printing on this test and you will see that the distribution is much looser,
   * about +/- 14%.  This is due to the fact that intersections loose accuracy as the ratio of
   * intersection to the union becomes a small number.
   */
  @Test
  public void checkDissimilarity() {
    int minK = 1 << 12;
    int u1 = 1 << 20;
    int u2 = (int) (u1 * 0.05);
    double threshold = 0.061;
    println("Estimation Mode, minK: " + minK + "\t Th: " + threshold);

    UpdateSketch expected = UpdateSketch.builder().build(minK);
    UpdateSketch measured = UpdateSketch.builder().build(minK);

    for (int i = 0; i < u1; i++) {
      expected.update(i);
    }

    for (int i = 0; i < u2; i++) {
      measured.update(i);
    }

    double[] jResults = JaccardSimilarity.jaccard(measured, expected);
    boolean state = JaccardSimilarity.dissimilarityTest(measured, expected, threshold);
    println(state + "\t" + jaccardString(jResults));
    //assertTrue(state);
  }

  private static String jaccardString(double[] jResults) {
    double lb = jResults[0];
    double est = jResults[1];
    double ub = jResults[2];
    return lb + "\t" + est + "\t" + ub + "\t" + (lb/est - 1.0) + "\t" + (ub/est - 1.0);
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
