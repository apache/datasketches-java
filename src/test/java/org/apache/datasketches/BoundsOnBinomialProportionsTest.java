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

package org.apache.datasketches;

import static org.apache.datasketches.BoundsOnBinomialProportions.approximateLowerBoundOnP;
import static org.apache.datasketches.BoundsOnBinomialProportions.approximateUpperBoundOnP;
import static org.apache.datasketches.BoundsOnBinomialProportions.erf;
import static org.apache.datasketches.BoundsOnBinomialProportions.estimateUnknownP;
import static org.apache.datasketches.BoundsOnBinomialProportions.normalCDF;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

/**
 * @author Kevin Lang
 */
@SuppressWarnings("javadoc")
public class BoundsOnBinomialProportionsTest {

  @Test
  public static void tinyLBTest() {
    //these answers were computed using a different programming language and therefore might not
    // match exactly.
    final double[] answers = {0.0, 0.004592032688529923, 0.04725537386564205,
        0.1396230607626959, 0.2735831034867167, 0.4692424353373485};
    final double kappa = 2.0;
    assertTrue( 0.0 == approximateLowerBoundOnP(0, 0, kappa) );
    final long n = 5;
    for (long k = 0; k <= n; k++) {
      final double lb = approximateLowerBoundOnP(n, k, kappa);
      final double est = estimateUnknownP(n, k);
      assertTrue(lb <= est);
      assertTrue(Math.abs(lb - answers[(int) k]) < 1e-14);
      //      System.out.printf ("LB\t%d\t%d\t%.1f\t%.16g%n", n, k, kappa, lb);
    }
  }

  @Test
  public static void tinyUBTest() {
    //these answers were computed using a different programming language and therefore might not
    // match exactly.
    final double[] answers = {0.5307575646626514, 0.7264168965132833, 0.860376939237304,
        0.952744626134358, 0.9954079673114701, 1.0};
    final double kappa = 2.0;
    assertTrue(1.0 == approximateUpperBoundOnP(0, 0, kappa));
    final long n = 5;
    for (long k = 0; k <= n; k++) {
      final double ub = approximateUpperBoundOnP(n, k, kappa);
      final double est = estimateUnknownP(n, k);
      assertTrue(ub >= est);
      assertTrue(Math.abs(ub - answers[(int) k]) < 1e-14);
      //      System.out.printf ("UB\t%d\t%d\t%.1f\t%.16g%n", n, k, kappa, ub);
    }
  }

  // This is for Kevin's use only, and will not be one of the unit tests.
  public static void lotsOfSpewage(final long maxN) {
    for (long n = 0; n <= maxN; n++) {
      for (long k = 0; k <= n; k++) {
        for (double kappa = 0.5; kappa < 5.0; kappa += 0.5) {
          final double lb = approximateLowerBoundOnP(n, k, kappa);
          final double ub = approximateUpperBoundOnP(n, k, kappa);
          final double est = estimateUnknownP(n, k);
          assertTrue(lb <= est);
          assertTrue(ub >= est);
          final String slb = String.format("LB\t%d\t%d\t%.1f\t%.16g%n", n, k, kappa, lb);
          final String sub = String.format("UB\t%d\t%d\t%.1f\t%.16g%n", n, k, kappa, ub);
          println(slb);
          println(sub);
        }
      }
    }
  }

  // This is for Kevin's use only, and will not be one of the unit tests.
  public static void printSomeNormalCDF() {
    final double[] someX = {-10.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 10.0};
    for (int i = 0; i < 11; i++) {
      final String s = String.format("normalCDF(%.1f) = %.12f%n", someX[i], normalCDF(someX[i]));
      println(s);
    }
  }

  // This is for Kevin's use only, and will not be one of the unit tests.
  //  public static void main (String[] args) {
  //    tinyLBTest ();
  //    tinyUBTest ();
  //    assertTrue (args.length == 1);
  //    long maxN = Long.parseLong(args[0]);
  //    lotsOfSpewage (maxN);
  //  }

  @Test
  public static void checkNumStdDevZero() {
    final double lb = BoundsOnBinomialProportions.approximateLowerBoundOnP( 1000, 100, 0.0);
    final double ub = BoundsOnBinomialProportions.approximateUpperBoundOnP( 1000, 100, 0.0);
    println("LB: " + lb);
    println("UB: " + ub);
  }

  @Test
  public static void checkInputs() {
    try {
      estimateUnknownP(-1, 50);
      fail("Should have thrown SketchesArgumentException.");
    }
    catch (final SketchesArgumentException e) {
      //expected
    }
    try {
      estimateUnknownP(500, -50);
      fail("Should have thrown SketchesArgumentException.");
    }
    catch (final SketchesArgumentException e) {
      //expected
    }
    try {
      estimateUnknownP(500, 5000);
      fail("Should have thrown SketchesArgumentException.");
    }
    catch (final SketchesArgumentException e) {
      //expected
    }
    assertEquals(estimateUnknownP(0, 0), 0.5, 0.0);
  }

  @Test
  public static void checkErf() {
    assertTrue(erf(-2.0) < 0.99);
    assertTrue(erf(2.0)  > 0.99);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }


}
