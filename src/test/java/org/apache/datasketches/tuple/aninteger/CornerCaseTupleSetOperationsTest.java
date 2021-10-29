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

import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.tuple.AnotB;
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.Intersection;
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
  private static final float MIDP       = 0.5f;
  private static final long LT_MIDP_V   = 2L;

  private static final long GT_LOWP_V   = 6L;
  private static final float LOWP       = 0.1f;
  private static final long LT_LOWP_V   = 4L;

  private static final double MIDP_THETA = MIDP;
  private static final double LOWP_THETA = LOWP;

  private IntegerSummary.Mode mode = IntegerSummary.Mode.Min;
  private IntegerSummary intSum = new IntegerSummary(mode);
  private IntegerSummarySetOperations setOperations = new IntegerSummarySetOperations(mode, mode);

  private enum SkType {
    NEW,         //{ 1.0,  0, T} Bin: 101  Oct: 05
    EXACT,       //{ 1.0, >0, F} Bin: 111  Oct: 07, specify only value
    ESTIMATION,  //{<1.0, >0, F} Bin: 010  Oct: 02, specify only value
    NEW_DEGEN,   //{<1.0,  0, T} Bin: 001  Oct: 01, specify only p
    RESULT_DEGEN //{<1.0,  0, F} Bin: 000  Oct: 0, specify p, value
  }

  //NOTE: 0 values in getTupleSketch or getThetaSketch are not used.

  private void checks(
      IntegerSketch tupleA,
      IntegerSketch tupleB,
      UpdateSketch  thetaB,
      double resultInterTheta,
      int resultInterCount,
      boolean resultInterEmpty,
      double resultAnotbTheta,
      int resultAnotbCount,
      boolean resultAnotbEmpty) {
    CompactSketch<IntegerSummary> csk;

    //Intersection
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);

    csk = inter.intersect(tupleA, tupleB);
    checkResult("Intersect Stateless Tuple, Tuple", csk, resultInterTheta, resultInterCount, resultInterEmpty);
    csk = inter.intersect(tupleA.compact(), tupleB.compact());
    checkResult("Intersect Stateless Tuple, Tuple", csk, resultInterTheta, resultInterCount, resultInterEmpty);

    csk = inter.intersect(tupleA, thetaB, intSum);
    checkResult("Intersect Stateless Tuple, Theta", csk, resultInterTheta, resultInterCount, resultInterEmpty);
    csk = inter.intersect(tupleA.compact(), thetaB.compact(), intSum);
    checkResult("Intersect Stateless Tuple, Theta", csk, resultInterTheta, resultInterCount, resultInterEmpty);


    //AnotB
    csk = AnotB.aNotB(tupleA, tupleB);
    checkResult("AnotB Stateless Tuple, Tuple", csk, resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
    csk = AnotB.aNotB(tupleA.compact(), tupleB.compact());
    checkResult("AnotB Stateless Tuple, Tuple", csk, resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);

    csk = AnotB.aNotB(tupleA, thetaB);
    checkResult("AnotB Stateless Tuple, Theta", csk, resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
    csk = AnotB.aNotB(tupleA.compact(), thetaB.compact());
    checkResult("AnotB Stateless Tuple, Theta", csk, resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);

    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(tupleA);
    anotb.notB(tupleB);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk,  resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);

    anotb.setA(tupleA.compact());
    anotb.notB(tupleB.compact());
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk,  resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);

    anotb.reset();
    anotb.setA(tupleA);
    anotb.notB(thetaB);
    checkResult("AnotB Stateful Tuple, Theta", csk,  resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);

    anotb.reset();
    anotb.setA(tupleA.compact());
    anotb.notB(thetaB.compact());
    checkResult("AnotB Stateful Tuple, Theta", csk,  resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }


  @Test
  public void newNew() {
    IntegerSketch tupleA = getTupleSketch(SkType.NEW,    0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.NEW,    0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.NEW,    0, 0);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = true;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void newExact() {
    IntegerSketch tupleA = getTupleSketch(SkType.NEW,    0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT,  0, GT_MIDP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT,  0, GT_MIDP_V);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = true;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void newNewDegen() {
    IntegerSketch tupleA = getTupleSketch(SkType.NEW,       0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.NEW_DEGEN, LOWP, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.NEW_DEGEN, LOWP, 0);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = true;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void newResultDegen() {
    IntegerSketch tupleA = getTupleSketch(SkType.NEW,          0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = true;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void newNewEstimation() {
    IntegerSketch tupleA = getTupleSketch(SkType.NEW,        0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = true;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  /*********************/

  @Test
  public void exactNew() {
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT,  0, GT_MIDP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.NEW,    0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.NEW,    0, 0);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 1;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void exactExact() {
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT,  0, GT_MIDP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT,  0, GT_MIDP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT,  0, GT_MIDP_V);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 1;
    final boolean resultInterEmpty = false;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = true;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void exactNewDegen() {
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT,     0, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.NEW_DEGEN, LOWP, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.NEW_DEGEN, LOWP, 0);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 1;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void exactResultDegen() {
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT,        0, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V); //entries = 0
    UpdateSketch thetaB =  getThetaSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);
    final double resultInterTheta = LOWP_THETA;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = false;
    final double resultAnotbTheta = LOWP_THETA;
    final int resultAnotbCount = 1;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void exactEstimation() {
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT,      0, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    final double resultInterTheta = LOWP_THETA;
    final int resultInterCount = 1;
    final boolean resultInterEmpty = false;
    final double resultAnotbTheta = LOWP_THETA;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  /*********************/

  @Test
  public void estimationNew() {
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.NEW,        0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.NEW,        0, 0);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = LOWP_THETA;
    final int resultAnotbCount = 1;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void estimationExact() {
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT,      0, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT,      0, LT_LOWP_V);
    final double resultInterTheta = LOWP_THETA;
    final int resultInterCount = 1;
    final boolean resultInterEmpty = false;
    final double resultAnotbTheta = LOWP_THETA;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void estimationNewDegen() {
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION,  MIDP, LT_MIDP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.NEW_DEGEN,   LOWP, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.NEW_DEGEN,   LOWP, 0);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = MIDP_THETA;
    final int resultAnotbCount = 1;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void estimationResultDegen() {
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION,   MIDP, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);
    final double resultInterTheta = LOWP_THETA;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = false;
    final double resultAnotbTheta = LOWP_THETA;
    final int resultAnotbCount = 1;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void estimationEstimation() {
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION,  MIDP, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.ESTIMATION,  LOWP, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.ESTIMATION,  LOWP, LT_LOWP_V);
    final double resultInterTheta = LOWP_THETA;
    final int resultInterCount = 1;
    final boolean resultInterEmpty = false;
    final double resultAnotbTheta = LOWP_THETA;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  /*********************/

  @Test
  public void newDegenNew() {
    IntegerSketch tupleA = getTupleSketch(SkType.NEW_DEGEN,   LOWP, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.NEW,         0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.NEW,         0, 0);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = true;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void newDegenExact() {
    IntegerSketch tupleA = getTupleSketch(SkType.NEW_DEGEN, LOWP,0);
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT,      0, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT,      0, LT_LOWP_V);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = true;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void newDegenNewDegen() {
    IntegerSketch tupleA = getTupleSketch(SkType.NEW_DEGEN, MIDP, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.NEW_DEGEN, LOWP, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.NEW_DEGEN, LOWP, 0);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = true;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void newDegenResultDegen() {
    IntegerSketch tupleA = getTupleSketch(SkType.NEW_DEGEN,    MIDP, 0);
    IntegerSketch tupleB =  getTupleSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);
    UpdateSketch thetaB =   getThetaSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = true;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void newDegenEstimation() {
    IntegerSketch tupleA = getTupleSketch(SkType.NEW_DEGEN,  MIDP, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = 1.0;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = true;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  /*********************/

  @Test
  public void resultDegenNew() {
    IntegerSketch tupleA = getTupleSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.NEW,           0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.NEW,           0, 0);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = LOWP_THETA;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void resultDegenExact() {
    IntegerSketch tupleA = getTupleSketch(SkType.RESULT_DEGEN,  LOWP, GT_LOWP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT,         0, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT,         0, LT_LOWP_V);
    final double resultInterTheta = LOWP_THETA;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = false;
    final double resultAnotbTheta = LOWP_THETA;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void resultDegenNewDegen() {
    IntegerSketch tupleA = getTupleSketch(SkType.RESULT_DEGEN, MIDP, GT_MIDP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.NEW_DEGEN,    LOWP, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.NEW_DEGEN,    LOWP, 0);
    final double resultInterTheta = 1.0;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = true;
    final double resultAnotbTheta = MIDP_THETA;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void resultDegenResultDegen() {
    IntegerSketch tupleA = getTupleSketch(SkType.RESULT_DEGEN, MIDP, GT_MIDP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);
    final double resultInterTheta = LOWP_THETA;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = false;
    final double resultAnotbTheta = LOWP_THETA;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  @Test
  public void resultDegenEstimation() {
    IntegerSketch tupleA = getTupleSketch(SkType.RESULT_DEGEN, MIDP, GT_MIDP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.ESTIMATION,   LOWP, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.ESTIMATION,   LOWP, LT_LOWP_V);
    final double resultInterTheta = LOWP_THETA;
    final int resultInterCount = 0;
    final boolean resultInterEmpty = false;
    final double resultAnotbTheta = LOWP_THETA;
    final int resultAnotbCount = 0;
    final boolean resultAnotbEmpty = false;

    checks(tupleA, tupleB, thetaB, resultInterTheta, resultInterCount, resultInterEmpty,
        resultAnotbTheta, resultAnotbCount, resultAnotbEmpty);
  }

  //=================================

  private static void checkResult(String comment, CompactSketch<IntegerSummary> sk,
      double theta, int entries, boolean empty) {
    double skTheta = sk.getTheta();
    int skEntries = sk.getRetainedEntries();
    boolean skEmpty = sk.isEmpty();

    boolean thetaOk = skTheta == theta;
    boolean entriesOk = skEntries == entries;
    boolean emptyOk = skEmpty == empty;
    if (!thetaOk || !entriesOk || !emptyOk) {
      StringBuilder sb = new StringBuilder();
      sb.append(comment + ": ");
      if (!thetaOk)   { sb.append("Got: " + skTheta + ", Expected: " + theta + "; "); }
      if (!entriesOk) { sb.append("Got: " + skEntries + ", Expected: " + entries + "; "); }
      if (!emptyOk)   { sb.append("Got: " + skEmpty + ", Expected: " + empty + "."); }
      throw new IllegalArgumentException(sb.toString());
    }
  }

  private static IntegerSketch getTupleSketch(SkType skType, float p, long value) {

    IntegerSketch sk;
    switch(skType) {
      case NEW: {      //{ 1.0,  0, T} Bin: 101  Oct: 05
        sk = new IntegerSketch(4, 2, 1.0f, IntegerSummary.Mode.Min);
        break;
      }
      case EXACT: {     //{ 1.0, >0, F} Bin: 111  Oct: 07
        sk = new IntegerSketch(4, 2, 1.0f, IntegerSummary.Mode.Min);
        sk.update(value, 1);
        break;
      }
      case ESTIMATION: {   //{<1.0, >0, F} Bin: 010  Oct: 02
        sk = new IntegerSketch(4, 2, p, IntegerSummary.Mode.Min);
        sk.update(value, 1);
        break;
      }
      case NEW_DEGEN: {    //{<1.0,  0, T} Bin: 001  Oct: 01
        sk =  new IntegerSketch(4, 2, p, IntegerSummary.Mode.Min);
        break;
      }
      case RESULT_DEGEN: { //{<1.0,  0, F} Bin: 000  Oct: 0
        sk = new IntegerSketch(4, 2, p, IntegerSummary.Mode.Min);
        sk.update(value, 1); // > theta
        break;
      }

      default: { return null; } //should not happen
    }
    return sk;
  }

  private static UpdateSketch getThetaSketch(SkType skType, float p, long value) {
    UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setLogNominalEntries(4);
    bldr.setResizeFactor(ResizeFactor.X4);

    UpdateSketch sk;
    switch(skType) {
      case NEW: { //{ 1.0,  0, T} Bin: 101  Oct: 05
        sk = bldr.build();
        break;
      }
      case EXACT: { //{ 1.0, >0, F} Bin: 111  Oct: 07
        sk = bldr.build();
        sk.update(value);
        break;
      }
      case ESTIMATION: {   //{<1.0, >0, F} Bin: 010  Oct: 02
        bldr.setP(p);
        sk = bldr.build();
        sk.update(value);
        break;
      }
      case NEW_DEGEN: {    //{<1.0,  0, T} Bin: 001  Oct: 01
        bldr.setP(p);
        sk = bldr.build();
        break;
      }
      case RESULT_DEGEN: { //{<1.0,  0, F} Bin: 000  Oct: 0
        bldr.setP(p);
        sk = bldr.build();
        sk.update(value); // > theta
        break;
      }

      default: { return null; } //should not happen
    }
    return sk;
  }

}
