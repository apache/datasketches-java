/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.kll;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class DeprecatedAndMiscTest {

  @Test
  public void checkDeprecatedRankError() {
    KllFloatsSketch sketch = new KllFloatsSketch();
    int k = KllFloatsSketch.DEFAULT_K;
    double eps1 = sketch.getNormalizedRankError();
    double eps2 = KllFloatsSketch.getNormalizedRankError(k);
    double expectEps = KllFloatsSketch.getNormalizedRankError(k, true);
    assertEquals(eps1, expectEps);
    assertEquals(eps2, expectEps);
  }

  @Test
  public void checkGetKFromEps() {
    int k = KllFloatsSketch.DEFAULT_K;
    double eps = KllFloatsSketch.getNormalizedRankError(k, false);
    double epsPmf = KllFloatsSketch.getNormalizedRankError(k, true);
    int kEps = KllFloatsSketch.getKFromEpsilon(eps, false);
    int kEpsPmf = KllFloatsSketch.getKFromEpsilon(epsPmf, true);
    assertEquals(kEps, k);
    assertEquals(kEpsPmf, k);
  }

  @Test //requires visual check
  public void checkNumRetainedAboveLevelZero() {
    KllFloatsSketch sketch = new KllFloatsSketch(20);
    for (int i = 0; i < 10; i++) { sketch.update(i + 1); }
    String s1 = sketch.toString(true, true);
    println(s1);
    KllFloatsSketch sketch2 = new KllFloatsSketch(20);
    for (int i = 0; i < 400; i++) {
      sketch2.update(i + 1);
    }
    sketch2.merge(sketch);
    String s2 = sketch2.toString(true, true);
    println(s2);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    System.out.println(s); //disable here
  }

}
