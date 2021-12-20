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

package org.apache.datasketches.tuple.aninteger;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.tuple.AnotB;
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.Intersection;
import org.apache.datasketches.tuple.Union;
import org.testng.annotations.Test;

public class CornerCaseTupleSetOperationsTest {

  /* Hash Values
   * 9223372036854775807  Theta = 1.0
   *
   * 6730918654704304314  hash(3L)[0] >>> 1    GT_MIDP
   * 4611686018427387904  Theta for p = 0.5f = MIDP
   * 2206043092153046979  hash(2L)[0] >>> 1    LT_MIDP_V
   * 1498732507761423037  hash(5L)[0] >>> 1    LTLT_MIDP_V
   *
   * 1206007004353599230  hash(6L)[0] >>> 1    GT_LOWP_V
   *  922337217429372928  Theta for p = 0.1f = LOWP
   *  593872385995628096  hash(4L)[0] >>> 1    LT_LOWP_V
   *  405753591161026837  hash(1L)[0] >>> 1    LTLT_LOWP_V
   */

  private static final long GT_MIDP_V   = 3L;
  private static final float MIDP_FLT   = 0.5f;

  private static final long GT_LOWP_V   = 6L;
  private static final float LOWP_FLT   = 0.1f;
  private static final long LT_LOWP_V   = 4L;


  private IntegerSummary.Mode mode = IntegerSummary.Mode.Min;
  private IntegerSummary integerSummary = new IntegerSummary(mode);
  private IntegerSummarySetOperations setOperations = new IntegerSummarySetOperations(mode, mode);

  private enum SkType {
    EMPTY,      // { 1.0,  0, T} Bin: 101  Oct: 05
    EXACT,      // { 1.0, >0, F} Bin: 110  Oct: 06, specify only value
    ESTIMATION, // {<1.0, >0, F} Bin: 010  Oct: 02, specify only value
    DEGENERATE  // {<1.0,  0, F} Bin: 000  Oct: 0, specify p, value
  }

  //=================================

  @Test
  public void emptyEmpty() {
    IntegerSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = 1.0;
    final int expectedUnionCount = 0;
    final boolean expectedUnionEmpty = true;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void emptyExact() {
    IntegerSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT, 0, GT_MIDP_V);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = 1.0;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void EmptyDegenerate() {
    IntegerSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 0;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void emptyEstimation() {
    IntegerSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  //=================================

  @Test
  public void exactEmpty() {
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 1;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = 1.0;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void exactExact() {
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT, 0, GT_MIDP_V);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 1;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = 1.0;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void exactDegenerate() {
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V); //entries = 0
    UpdateSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 1;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void exactEstimation() {
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 1;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  //=================================

  @Test
  public void estimationEmpty() {
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 1;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void estimationExact() {
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT, 0, LT_LOWP_V);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 1;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void estimationDegenerate() {
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION, MIDP_FLT, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 1;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void estimationEstimation() {
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION, MIDP_FLT, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 1;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  //=================================

  @Test
  public void degenerateEmpty() {
    IntegerSketch tupleA = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 0;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void degenerateExact() {
    IntegerSketch tupleA = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT, 0, LT_LOWP_V);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void degenerateDegenerate() {
    IntegerSketch tupleA = getTupleSketch(SkType.DEGENERATE, MIDP_FLT, GT_MIDP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 0;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void degenerateEstimation() {
    IntegerSketch tupleA = getTupleSketch(SkType.DEGENERATE, MIDP_FLT, GT_MIDP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(tupleA, tupleB, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  //=================================
  //=================================

  private void checks(
      IntegerSketch tupleA,
      IntegerSketch tupleB,
      UpdateSketch  thetaB,
      double expectedIntersectTheta,
      int expectedIntersectCount,
      boolean expectedIntersectEmpty,
      double expectedAnotbTheta,
      int expectedAnotbCount,
      boolean expectedAnotbEmpty,
      double expectedUnionTheta,
      int expectedUnionCount,
      boolean expectedUnionEmpty) {
    CompactSketch<IntegerSummary> csk;
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    AnotB<IntegerSummary> anotb = new AnotB<>();
    Union<IntegerSummary> union = new Union<>(16, setOperations);

    //Intersection Stateless Tuple, Tuple Updatable
    csk = inter.intersect(tupleA, tupleB);
    checkResult("Intersect Stateless Tuple, Tuple", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);
    //Intersection Stateless Tuple, Tuple Compact
    csk = inter.intersect(tupleA.compact(), tupleB.compact());
    checkResult("Intersect Stateless Tuple, Tuple", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);
    //Intersection Stateless Tuple, Theta Updatable
    csk = inter.intersect(tupleA, thetaB, integerSummary); //Tuple, Theta
    checkResult("Intersect Stateless Tuple, Theta", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);
  //Intersection Stateless Tuple, Theta Compact
    csk = inter.intersect(tupleA.compact(), thetaB.compact(), integerSummary);
    checkResult("Intersect Stateless Tuple, Theta", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);

    //AnotB Stateless Tuple, Tuple Updatable
    csk = AnotB.aNotB(tupleA, tupleB);
    checkResult("AnotB Stateless Tuple, Tuple", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //AnotB Stateless Tuple, Tuple Compact
    csk = AnotB.aNotB(tupleA.compact(), tupleB.compact());
    checkResult("AnotB Stateless Tuple, Tuple", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //AnotB Stateless Tuple, Theta Updatable
    csk = AnotB.aNotB(tupleA, thetaB);
    checkResult("AnotB Stateless Tuple, Theta", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //AnotB Stateless Tuple, Theta Compact
    csk = AnotB.aNotB(tupleA.compact(), thetaB.compact());
    checkResult("AnotB Stateless Tuple, Theta", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);

    //AnotB Stateful Tuple, Tuple Updatable
    anotb.setA(tupleA);
    anotb.notB(tupleB);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk,  expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //AnotB Stateful Tuple, Tuple Compact
    anotb.setA(tupleA.compact());
    anotb.notB(tupleB.compact());
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk,  expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //AnotB Stateful Tuple, Theta Updatable
    anotb.setA(tupleA);
    anotb.notB(thetaB);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Theta", csk,  expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //AnotB Stateful Tuple, Theta Compact
    anotb.setA(tupleA.compact());
    anotb.notB(thetaB.compact());
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Theta", csk,  expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);

    //Union Stateless Tuple, Tuple Updatable
    csk = union.union(tupleA, tupleB);
    checkResult("Union Stateless Tuple, Tuple", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //Union Stateless Tuple, Tuple Compact
    csk = union.union(tupleA.compact(), tupleB.compact());
    checkResult("Union Stateless Tuple, Tuple", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //Union Stateless Tuple, Theta Updatable
    csk = union.union(tupleA, thetaB, integerSummary);
    checkResult("Union Stateless Tuple, Theta", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //Union Stateless Tuple, Theta Compact
    csk = union.union(tupleA.compact(), thetaB.compact(), integerSummary);
    checkResult("Union Stateless Tuple, Theta", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);

    //Union Stateful Tuple, Tuple Updatable
    union.union(tupleA);
    union.union(tupleB);
    csk = union.getResult(true);
    checkResult("Union Stateful Tuple, Tuple", csk,  expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //AnotB Stateful Tuple, Tuple Compact
    union.union(tupleA.compact());
    union.union(tupleB.compact());
    csk = union.getResult(true);
    checkResult("Union Stateful Tuple, Tuple", csk,  expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //AnotB Stateful Tuple, Theta Updatable
    union.union(tupleA);
    union.union(thetaB, integerSummary);
    csk = union.getResult(true);
    checkResult("Union Stateful Tuple, Theta", csk,  expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //AnotB Stateful Tuple, Theta Compact
    union.union(tupleA.compact());
    union.union(thetaB.compact(), integerSummary);
    csk = union.getResult(true);
    checkResult("Union Stateful Tuple, Theta", csk,  expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);

  }

  private static void checkResult(
      String comment,
      CompactSketch<IntegerSummary> csk,
      double expectedTheta,
      int expectedEntries,
      boolean expectedEmpty) {
    double actualTheta = csk.getTheta();
    int actualEntries = csk.getRetainedEntries();
    boolean actualEmpty = csk.isEmpty();

    boolean thetaOk = actualTheta == expectedTheta;
    boolean entriesOk = actualEntries == expectedEntries;
    boolean emptyOk = actualEmpty == expectedEmpty;
    if (!thetaOk || !entriesOk || !emptyOk) {
      StringBuilder sb = new StringBuilder();
      sb.append(comment + ": ");
      if (!thetaOk)   { sb.append("Theta: expected " + expectedTheta + ", got " + actualTheta + "; "); }
      if (!entriesOk) { sb.append("Entries: expected " + expectedEntries + ", got " + actualEntries + "; "); }
      if (!emptyOk)   { sb.append("Empty: expected " + expectedEmpty + ", got " + actualEmpty + "."); }
      throw new IllegalArgumentException(sb.toString());
    }
  }

  private static IntegerSketch getTupleSketch(
      SkType skType,
      float p,
      long updateKey) {

    IntegerSketch sk;
    switch(skType) {
      case EMPTY: { // { 1.0,  0, T} p and value are not used
        sk = new IntegerSketch(4, 2, 1.0f, IntegerSummary.Mode.Min);
        break;
      }
      case EXACT: { // { 1.0, >0, F} p is not used
        sk = new IntegerSketch(4, 2, 1.0f, IntegerSummary.Mode.Min);
        sk.update(updateKey, 1);
        break;
      }
      case ESTIMATION: { // {<1.0, >0, F}
        checkValidUpdate(p, updateKey);
        sk = new IntegerSketch(4, 2, p, IntegerSummary.Mode.Min);
        sk.update(updateKey, 1);
        break;
      }
      case DEGENERATE: { // {<1.0,  0, F}
        checkInvalidUpdate(p, updateKey);
        sk = new IntegerSketch(4, 2, p, IntegerSummary.Mode.Min);
        sk.update(updateKey, 1); // > theta
        break;
      }

      default: { return null; } // should not happen
    }
    return sk;
  }

  //NOTE: p and value arguments are used for every case
  private static UpdateSketch getThetaSketch(
      SkType skType,
      float p,
      long updateKey) {
    UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setLogNominalEntries(4);
    bldr.setResizeFactor(ResizeFactor.X4);

    UpdateSketch sk;
    switch(skType) {
      case EMPTY: { // { 1.0,  0, T} p and value are not used
        sk = bldr.build();
        break;
      }
      case EXACT: { // { 1.0, >0, F} p is not used
        sk = bldr.build();
        sk.update(updateKey);
        break;
      }
      case ESTIMATION: { // {<1.0, >0, F}
        checkValidUpdate(p, updateKey);
        bldr.setP(p);
        sk = bldr.build();
        sk.update(updateKey);
        break;
      }
      case DEGENERATE: { // {<1.0,  0, F}
        checkInvalidUpdate(p, updateKey);
        bldr.setP(p);
        sk = bldr.build();
        sk.update(updateKey);
        break;
      }

      default: { return null; } // should not happen
    }
    return sk;
  }

  private static void checkValidUpdate(float p, long updateKey) {
    assertTrue( getLongHash(updateKey) < (long) (p * Long.MAX_VALUE));
  }

  private static void checkInvalidUpdate(float p, long updateKey) {
    assertTrue( getLongHash(updateKey) > (long) (p * Long.MAX_VALUE));
  }

  static long getLongHash(long v) {
    return (hash(v, DEFAULT_UPDATE_SEED)[0]) >>> 1;
  }
}
