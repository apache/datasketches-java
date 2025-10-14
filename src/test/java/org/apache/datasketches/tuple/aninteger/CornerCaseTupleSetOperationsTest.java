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

import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketchBuilder;
import org.apache.datasketches.tuple.CompactTupleSketch;
import org.apache.datasketches.tuple.TupleAnotB;
import org.apache.datasketches.tuple.TupleIntersection;
import org.apache.datasketches.tuple.TupleUnion;
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.EXACT, 0, GT_MIDP_V);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.EXACT, 0, GT_MIDP_V);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V); //entries = 0
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.EXACT, 0, LT_LOWP_V);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.ESTIMATION, MIDP_FLT, LT_LOWP_V);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.ESTIMATION, MIDP_FLT, LT_LOWP_V);
    IntegerTupleSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V); //entries = 0
    IntegerTupleSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V); //entries = 0
    IntegerTupleSketch tupleB = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.EXACT, 0, LT_LOWP_V);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.DEGENERATE, MIDP_FLT, GT_MIDP_V); //entries = 0
    IntegerTupleSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_V);
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
    IntegerTupleSketch tupleA = getTupleSketch(SkType.DEGENERATE, MIDP_FLT, GT_MIDP_V); //entries = 0
    IntegerTupleSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
    UpdatableThetaSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_V);
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
      IntegerTupleSketch tupleA,
      IntegerTupleSketch tupleB,
      UpdatableThetaSketch  thetaB,
      double expectedIntersectTheta,
      int expectedIntersectCount,
      boolean expectedIntersectEmpty,
      double expectedAnotbTheta,
      int expectedAnotbCount,
      boolean expectedAnotbEmpty,
      double expectedUnionTheta,
      int expectedUnionCount,
      boolean expectedUnionEmpty) {
    CompactTupleSketch<IntegerSummary> csk;
    TupleIntersection<IntegerSummary> inter = new TupleIntersection<>(setOperations);
    TupleAnotB<IntegerSummary> anotb = new TupleAnotB<>();
    TupleUnion<IntegerSummary> union = new TupleUnion<>(16, setOperations);

    //TupleIntersection Stateless TupleSketch, TupleSketch Updatable
    csk = inter.intersect(tupleA, tupleB);
    checkResult("Intersect Stateless TupleSketch, TupleSketch", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);
    //TupleIntersection Stateless TupleSketch, TupleSketch Compact
    csk = inter.intersect(tupleA.compact(), tupleB.compact());
    checkResult("Intersect Stateless TupleSketch, TupleSketch", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);
    //TupleIntersection Stateless TupleSketch, ThetaSketch Updatable
    csk = inter.intersect(tupleA, thetaB, integerSummary); //TupleSketch, ThetaSketch
    checkResult("Intersect Stateless TupleSketch, ThetaSketch", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);
  //TupleIntersection Stateless TupleSketch, ThetaSketch Compact
    csk = inter.intersect(tupleA.compact(), thetaB.compact(), integerSummary);
    checkResult("Intersect Stateless TupleSketch, ThetaSketch", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);

    //TupleAnotB Stateless TupleSketch, TupleSketch Updatable
    csk = TupleAnotB.aNotB(tupleA, tupleB);
    checkResult("TupleAnotB Stateless TupleSketch, TupleSketch", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //TupleAnotB Stateless TupleSketch, TupleSketch Compact
    csk = TupleAnotB.aNotB(tupleA.compact(), tupleB.compact());
    checkResult("TupleAnotB Stateless TupleSketch, TupleSketch", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //TupleAnotB Stateless TupleSketch, ThetaSketch Updatable
    csk = TupleAnotB.aNotB(tupleA, thetaB);
    checkResult("TupleAnotB Stateless TupleSketch, ThetaSketch", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //TupleAnotB Stateless TupleSketch, ThetaSketch Compact
    csk = TupleAnotB.aNotB(tupleA.compact(), thetaB.compact());
    checkResult("TupleAnotB Stateless TupleSketch, ThetaSketch", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);

    //TupleAnotB Stateful TupleSketch, TupleSketch Updatable
    anotb.setA(tupleA);
    anotb.notB(tupleB);
    csk = anotb.getResult(true);
    checkResult("TupleAnotB Stateful TupleSketch, TupleSketch", csk,  expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //TupleAnotB Stateful TupleSketch, TupleSketch Compact
    anotb.setA(tupleA.compact());
    anotb.notB(tupleB.compact());
    csk = anotb.getResult(true);
    checkResult("TupleAnotB Stateful TupleSketch, TupleSketch", csk,  expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //TupleAnotB Stateful TupleSketch, ThetaSketch Updatable
    anotb.setA(tupleA);
    anotb.notB(thetaB);
    csk = anotb.getResult(true);
    checkResult("TupleAnotB Stateful TupleSketch, ThetaSketch", csk,  expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //TupleAnotB Stateful TupleSketch, ThetaSketch Compact
    anotb.setA(tupleA.compact());
    anotb.notB(thetaB.compact());
    csk = anotb.getResult(true);
    checkResult("TupleAnotB Stateful TupleSketch, ThetaSketch", csk,  expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);

    //TupleUnion Stateless TupleSketch, TupleSketch Updatable
    csk = union.union(tupleA, tupleB);
    checkResult("TupleUnion Stateless TupleSketch, TupleSketch", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //TupleUnion Stateless TupleSketch, TupleSketch Compact
    csk = union.union(tupleA.compact(), tupleB.compact());
    checkResult("TupleUnion Stateless TupleSketch, TupleSketch", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //TupleUnion Stateless TupleSketch, ThetaSketch Updatable
    csk = union.union(tupleA, thetaB, integerSummary);
    checkResult("TupleUnion Stateless TupleSketch, ThetaSketch", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //TupleUnion Stateless TupleSketch, ThetaSketch Compact
    csk = union.union(tupleA.compact(), thetaB.compact(), integerSummary);
    checkResult("TupleUnion Stateless TupleSketch, ThetaSketch", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);

    //TupleUnion Stateful TupleSketch, TupleSketch Updatable
    union.union(tupleA);
    union.union(tupleB);
    csk = union.getResult(true);
    checkResult("TupleUnion Stateful TupleSketch, TupleSketch", csk,  expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //TupleAnotB Stateful TupleSketch, TupleSketch Compact
    union.union(tupleA.compact());
    union.union(tupleB.compact());
    csk = union.getResult(true);
    checkResult("TupleUnion Stateful TupleSketch, TupleSketch", csk,  expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //TupleAnotB Stateful TupleSketch, ThetaSketch Updatable
    union.union(tupleA);
    union.union(thetaB, integerSummary);
    csk = union.getResult(true);
    checkResult("TupleUnion Stateful TupleSketch, ThetaSketch", csk,  expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //TupleAnotB Stateful TupleSketch, ThetaSketch Compact
    union.union(tupleA.compact());
    union.union(thetaB.compact(), integerSummary);
    csk = union.getResult(true);
    checkResult("TupleUnion Stateful TupleSketch, ThetaSketch", csk,  expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);

  }

  private static void checkResult(
      String comment,
      CompactTupleSketch<IntegerSummary> csk,
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

  private static IntegerTupleSketch getTupleSketch(
      SkType skType,
      float p,
      long updateKey) {

    IntegerTupleSketch sk;
    switch(skType) {
      case EMPTY: { // { 1.0,  0, T} p and value are not used
        sk = new IntegerTupleSketch(4, 2, 1.0f, IntegerSummary.Mode.Min);
        break;
      }
      case EXACT: { // { 1.0, >0, F} p is not used
        sk = new IntegerTupleSketch(4, 2, 1.0f, IntegerSummary.Mode.Min);
        sk.update(updateKey, 1);
        break;
      }
      case ESTIMATION: { // {<1.0, >0, F}
        checkValidUpdate(p, updateKey);
        sk = new IntegerTupleSketch(4, 2, p, IntegerSummary.Mode.Min);
        sk.update(updateKey, 1);
        break;
      }
      case DEGENERATE: { // {<1.0,  0, F}
        checkInvalidUpdate(p, updateKey);
        sk = new IntegerTupleSketch(4, 2, p, IntegerSummary.Mode.Min);
        sk.update(updateKey, 1); // > theta
        break;
      }

      default: { return null; } // should not happen
    }
    return sk;
  }

  //NOTE: p and value arguments are used for every case
  private static UpdatableThetaSketch getThetaSketch(
      SkType skType,
      float p,
      long updateKey) {
    UpdatableThetaSketchBuilder bldr = new UpdatableThetaSketchBuilder();
    bldr.setLogNominalEntries(4);
    bldr.setResizeFactor(ResizeFactor.X4);

    UpdatableThetaSketch sk;
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
    return (hash(v, Util.DEFAULT_UPDATE_SEED)[0]) >>> 1;
  }
}
