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

import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.adouble.DoubleSummarySetOperations;
import org.apache.datasketches.tuple.Intersection;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Lee Rhodes
 * @author David Cromberge
 */
public class BoundsOnRatiosInTupleSketchedSetsTest {

  private final DoubleSummary.Mode umode = DoubleSummary.Mode.Sum;
  private final DoubleSummarySetOperations dsso = new DoubleSummarySetOperations();
  private final DoubleSummaryFactory factory = new DoubleSummaryFactory(umode);
  private final UpdateSketchBuilder thetaBldr = UpdateSketch.builder();
  private final UpdatableSketchBuilder<Double, DoubleSummary> tupleBldr = new UpdatableSketchBuilder<>(factory);
  private final Double constSummary = 1.0;

  @Test
  public void checkNormalReturns1() { // tuple, tuple
    final UpdatableSketch<Double, DoubleSummary> skA = tupleBldr.build(); //4K
    final UpdatableSketch<Double, DoubleSummary> skC = tupleBldr.build();
    final int uA = 10000;
    final int uC = 100000;
    for (int i = 0; i < uA; i++) { skA.update(i, constSummary); }
    for (int i = 0; i < uC; i++) { skC.update(i + (uA / 2), constSummary); }
    final Intersection<DoubleSummary> inter = new Intersection<>(dsso);
    inter.intersect(skA);
    inter.intersect(skC);
    final Sketch<DoubleSummary> skB = inter.getResult();

    double est = BoundsOnRatiosInTupleSketchedSets.getEstimateOfBoverA(skA, skB);
    double lb = BoundsOnRatiosInTupleSketchedSets.getLowerBoundForBoverA(skA, skB);
    double ub = BoundsOnRatiosInTupleSketchedSets.getUpperBoundForBoverA(skA, skB);
    assertTrue(ub > est);
    assertTrue(est > lb);
    assertEquals(est, 0.5, .03);
    println("ub : " + ub);
    println("est: " + est);
    println("lb : " + lb);
    skA.reset(); //skA is now empty
    est = BoundsOnRatiosInTupleSketchedSets.getEstimateOfBoverA(skA, skB);
    lb = BoundsOnRatiosInTupleSketchedSets.getLowerBoundForBoverA(skA, skB);
    ub = BoundsOnRatiosInTupleSketchedSets.getUpperBoundForBoverA(skA, skB);
    println("ub : " + ub);
    println("est: " + est);
    println("lb : " + lb);
    skC.reset(); //Now both are empty
    est = BoundsOnRatiosInTupleSketchedSets.getEstimateOfBoverA(skA, skC);
    lb = BoundsOnRatiosInTupleSketchedSets.getLowerBoundForBoverA(skA, skC);
    ub = BoundsOnRatiosInTupleSketchedSets.getUpperBoundForBoverA(skA, skC);
    println("ub : " + ub);
    println("est: " + est);
    println("lb : " + lb);
  }

  @Test
  public void checkNormalReturns2() { // tuple, theta
    final UpdatableSketch<Double, DoubleSummary> skA = tupleBldr.build(); //4K
    final UpdateSketch skC = thetaBldr.build();
    final int uA = 10000;
    final int uC = 100000;
    for (int i = 0; i < uA; i++) { skA.update(i, constSummary); }
    for (int i = 0; i < uC; i++) { skC.update(i + (uA / 2)); }
    final Intersection<DoubleSummary> inter = new Intersection<>(dsso);
    inter.intersect(skA);
    inter.intersect(skC, factory.newSummary());
    final Sketch<DoubleSummary> skB = inter.getResult();

    double est = BoundsOnRatiosInTupleSketchedSets.getEstimateOfBoverA(skA, skB);
    double lb = BoundsOnRatiosInTupleSketchedSets.getLowerBoundForBoverA(skA, skB);
    double ub = BoundsOnRatiosInTupleSketchedSets.getUpperBoundForBoverA(skA, skB);
    assertTrue(ub > est);
    assertTrue(est > lb);
    assertEquals(est, 0.5, .03);
    println("ub : " + ub);
    println("est: " + est);
    println("lb : " + lb);
    skA.reset(); //skA is now empty
    est = BoundsOnRatiosInTupleSketchedSets.getEstimateOfBoverA(skA, skB);
    lb = BoundsOnRatiosInTupleSketchedSets.getLowerBoundForBoverA(skA, skB);
    ub = BoundsOnRatiosInTupleSketchedSets.getUpperBoundForBoverA(skA, skB);
    println("ub : " + ub);
    println("est: " + est);
    println("lb : " + lb);
    skC.reset(); //Now both are empty
    est = BoundsOnRatiosInTupleSketchedSets.getEstimateOfBoverA(skA, skC);
    lb = BoundsOnRatiosInTupleSketchedSets.getLowerBoundForBoverA(skA, skC);
    ub = BoundsOnRatiosInTupleSketchedSets.getUpperBoundForBoverA(skA, skC);
    println("ub : " + ub);
    println("est: " + est);
    println("lb : " + lb);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkAbnormalReturns1() { // tuple, tuple
    final UpdatableSketch<Double, DoubleSummary> skA = tupleBldr.build(); //4K
    final UpdatableSketch<Double, DoubleSummary> skC = tupleBldr.build();
    final int uA = 100000;
    final int uC = 10000;
    for (int i = 0; i < uA; i++) { skA.update(i, constSummary); }
    for (int i = 0; i < uC; i++) { skC.update(i + (uA / 2), constSummary); }
    BoundsOnRatiosInTupleSketchedSets.getEstimateOfBoverA(skA, skC);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkAbnormalReturns2() { // tuple, theta
    final UpdatableSketch<Double, DoubleSummary> skA = tupleBldr.build(); //4K
    final UpdateSketch skC = thetaBldr.build();
    final int uA = 100000;
    final int uC = 10000;
    for (int i = 0; i < uA; i++) { skA.update(i, constSummary); }
    for (int i = 0; i < uC; i++) { skC.update(i + (uA / 2)); }
    BoundsOnRatiosInTupleSketchedSets.getEstimateOfBoverA(skA, skC);
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
