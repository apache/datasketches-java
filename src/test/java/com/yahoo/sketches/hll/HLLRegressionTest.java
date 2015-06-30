/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HLLRegression.ARRLEN;
import static com.yahoo.sketches.hll.HLLRegression.HLL_LG_HIBINS;
import static com.yahoo.sketches.hll.HLLRegression.HLL_LG_LOBINS;
import static com.yahoo.sketches.hll.HLLRegression.getRegressionArrays;
import static com.yahoo.sketches.hll.HLLRegression.regress;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class HLLRegressionTest {

  private static final double[][] rArr = getRegressionArrays();
  private static final double errorBound = .05;

  /**
   * Test below range for all ranges
   */
  @Test
  public void regressTest1() {
    for (int i = HLL_LG_LOBINS; i <= HLL_LG_HIBINS; i++ ) {
      double r = regress(i, 0.0);
      Assert.assertEquals(r, 0.0);
    }
  }

  /**
   * Test above range for all ranges
   */
  @Test
  public void regressTest2() {
    for (int i = HLL_LG_LOBINS, j = 0; i <= HLL_LG_HIBINS; i++ , j++ ) {
      double hllEst = rArr[j][ARRLEN - 1];
      double r = regress(i, hllEst);
      Assert.assertEquals(r, hllEst);
    }
  }

  /**
   * Test all vertices
   */
  @Test
  public void regressTest3() {
    for (int i = HLL_LG_LOBINS, j = 0; i <= HLL_LG_HIBINS; i++ , j++ ) {
      for (int k = 0; k < ARRLEN; k++ ) {
        double yin = rArr[j][k];
        double r = regress(i, yin);
        double expect = (1 << i) * Math.pow(2.0, k / 16.0);
        String s = yin + ", " + r + ", " + expect + ", " + ((r / expect) - 1.0);
        //println(s);
        Assert.assertTrue(testBounds(r, expect), s);
      }
      //println("\n");
    }
  }

  private static boolean testBounds(double result, double expected) {
    return Math.abs((result / expected) - 1.0) <= errorBound;
  }

}