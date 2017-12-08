/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.HLL_HIP_RSE_FACTOR;
import static com.yahoo.sketches.hll.HllUtil.HLL_NON_HIP_RSE_FACTOR;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class RelativeErrorTablesTest {

  //@Test
  public void checkHipBounds() {
    //int lgK = 4;
    double u = 1.0;
    boolean oooFlag = false;
    int numStdDev = 1;
    for (int lgK = 4; lgK <= 21; lgK++) {
      double ub = hllUB(oooFlag, lgK, numStdDev, u);
      double re = (ub/u) - 1.0;
      println("LgK: " + lgK + ", re: "+ (re * 100));
    }

  }

  static double hllUB(boolean oooFlag, int lgK, int numStdDev, double est) {
    int configK = 1 << lgK;
    if (lgK > 12) {
      if (oooFlag) {
        final double hllNonHipEps =
            (numStdDev * HLL_NON_HIP_RSE_FACTOR) / Math.sqrt(configK);
        return est / (1.0 - hllNonHipEps);
      }
      final double hllHipEps = (numStdDev * HLL_HIP_RSE_FACTOR) / Math.sqrt(configK);
      return est / (1.0 - hllHipEps);
    }
    //lgConfigK <= 12
    final double re = RelativeErrorTables.getRelErr(true, oooFlag, lgK, numStdDev);
    return est / (1.0 + re);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    System.out.println(s); //disable here
  }

}
