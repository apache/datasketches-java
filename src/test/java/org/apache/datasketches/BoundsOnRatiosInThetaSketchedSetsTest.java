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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class BoundsOnRatiosInThetaSketchedSetsTest {

  @Test
  public void checkNormalReturns() {
    final UpdateSketch skA = Sketches.updateSketchBuilder().build(); //4K
    final UpdateSketch skC = Sketches.updateSketchBuilder().build();
    final int uA = 10000;
    final int uC = 100000;
    for (int i = 0; i < uA; i++) { skA.update(i); }
    for (int i = 0; i < uC; i++) { skC.update(i + (uA / 2)); }
    final Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.intersect(skA);
    inter.intersect(skC);
    final CompactSketch skB = inter.getResult();

    double est = BoundsOnRatiosInThetaSketchedSets.getEstimateOfBoverA(skA, skB);
    double lb = BoundsOnRatiosInThetaSketchedSets.getLowerBoundForBoverA(skA, skB);
    double ub = BoundsOnRatiosInThetaSketchedSets.getUpperBoundForBoverA(skA, skB);
    assertTrue(ub > est);
    assertTrue(est > lb);
    assertEquals(est, 0.5, .03);
    println("ub : " + ub);
    println("est: " + est);
    println("lb : " + lb);
    skA.reset(); //skA is now empty
    est = BoundsOnRatiosInThetaSketchedSets.getEstimateOfBoverA(skA, skB);
    lb = BoundsOnRatiosInThetaSketchedSets.getLowerBoundForBoverA(skA, skB);
    ub = BoundsOnRatiosInThetaSketchedSets.getUpperBoundForBoverA(skA, skB);
    println("ub : " + ub);
    println("est: " + est);
    println("lb : " + lb);
    skC.reset(); //Now both are empty
    est = BoundsOnRatiosInThetaSketchedSets.getEstimateOfBoverA(skA, skC);
    lb = BoundsOnRatiosInThetaSketchedSets.getLowerBoundForBoverA(skA, skC);
    ub = BoundsOnRatiosInThetaSketchedSets.getUpperBoundForBoverA(skA, skC);
    println("ub : " + ub);
    println("est: " + est);
    println("lb : " + lb);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkAbnormalReturns() {
    final UpdateSketch skA = Sketches.updateSketchBuilder().build(); //4K
    final UpdateSketch skC = Sketches.updateSketchBuilder().build();
    final int uA = 100000;
    final int uC = 10000;
    for (int i = 0; i < uA; i++) { skA.update(i); }
    for (int i = 0; i < uC; i++) { skC.update(i + (uA / 2)); }
    BoundsOnRatiosInThetaSketchedSets.getEstimateOfBoverA(skA, skC);
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
