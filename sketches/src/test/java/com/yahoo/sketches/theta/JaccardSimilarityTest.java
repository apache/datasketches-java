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
    double thresh = 0.95;
    println("Check nulls & empties, minK: " + minK + "\t Th: " + thresh);
    //check both null
    double[] jResults = JaccardSimilarity.jaccard(null, null, minK);
    boolean state = jResults[1] > thresh;
    println("null \t null:\t" + state + "\t" + jaccardString(jResults));
    assertFalse(state);

    UpdateSketch measured = UpdateSketch.builder().build(minK);
    UpdateSketch expected = UpdateSketch.builder().build(minK);

    //check both empty
    jResults = JaccardSimilarity.jaccard(measured, expected, minK);
    state = jResults[1] > thresh;
    println("empty\tempty:\t" + state + "\t" + jaccardString(jResults));
    assertTrue(state);

    //adjust one
    expected.update(1);
    jResults = JaccardSimilarity.jaccard(measured, expected, minK);
    state = jResults[1] > thresh;
    println("empty\t    1:\t" + state + "\t" + jaccardString(jResults));
    assertFalse(state);
    println("");
  }

  @Test
  public void checkJaccardExactMode() {
    int minK = 1 << 12;
    int u = minK;
    double thresh = 0.9999;
    println("Exact Mode, minK: " + minK + "\t Th: " + thresh);

    UpdateSketch measured = UpdateSketch.builder().build(minK);
    UpdateSketch expected = UpdateSketch.builder().build(minK);

    for (int i = 0; i < u-1; i++) {
      measured.update(i);
      expected.update(i);
    }

    double[] jResults = JaccardSimilarity.jaccard(measured, expected, minK);
    boolean state = jResults[1] > thresh;
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);

    measured.update(u-1);
    expected.update(u);
    jResults = JaccardSimilarity.jaccard(measured, expected, minK);
    state = jResults[1] > thresh;
    println(state + "\t" + jaccardString(jResults));
    assertFalse(state);
    println("");
  }

  @Test
  public void checkJaccardEstMode() {
    int minK = 1 << 12;
    int u = 1 << 20;
    double thresh = 0.9999;
    println("Estimation Mode, minK: " + minK + "\t Th: " + thresh);

    UpdateSketch measured = UpdateSketch.builder().build(minK);
    UpdateSketch expected = UpdateSketch.builder().build(minK);

    for (int i = 0; i < u; i++) {
      measured.update(i);
      expected.update(i);
    }

    double[] jResults = JaccardSimilarity.jaccard(measured, expected, minK);
    boolean state = jResults[1] > thresh;
    println(state + "\t" + jaccardString(jResults));
    assertTrue(state);

    for (int i = u; i < u + 50; i++) { measured.update(i); }

    jResults = JaccardSimilarity.jaccard(measured, expected, minK);
    state = jResults[1] > thresh;
    println(state + "\t" + jaccardString(jResults));
    assertFalse(state);
    println("");
  }

  private static String jaccardString(double[] jResults) {
    return jResults[0] + "\t" + jResults[1] + "\t" + jResults[2];
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
