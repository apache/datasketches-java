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

  private static final long GT_LOWP_V   = 6L;
  private static final float LOWP       = 0.1f;
  private static final long LT_LOWP_V   = 4L;

  private static final double LOWP_THETA = LOWP;

  private IntegerSummary.Mode mode = IntegerSummary.Mode.Min;
  private IntegerSummary intSum = new IntegerSummary(mode);
  private IntegerSummarySetOperations setOperations = new IntegerSummarySetOperations(mode, mode);

  private enum SkType {
    EMPTY,      // { 1.0,  0, T} Bin: 101  Oct: 05
    EXACT,      // { 1.0, >0, F} Bin: 110  Oct: 06, specify only value
    ESTIMATION, // {<1.0, >0, F} Bin: 010  Oct: 02, specify only value
    DEGENERATE  // {<1.0,  0, F} Bin: 000  Oct: 0, specify p, value
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

    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);

    csk = inter.intersect(tupleA, tupleB);
    checkResult("Intersect Stateless Tuple, Tuple", csk, resultInterTheta, resultInterCount, resultInterEmpty);
    csk = inter.intersect(tupleA.compact(), tupleB.compact());
    checkResult("Intersect Stateless Tuple, Tuple", csk, resultInterTheta, resultInterCount, resultInterEmpty);

    csk = inter.intersect(tupleA, thetaB, intSum);
    checkResult("Intersect Stateless Tuple, Theta", csk, resultInterTheta, resultInterCount, resultInterEmpty);
    csk = inter.intersect(tupleA.compact(), thetaB.compact(), intSum);
    checkResult("Intersect Stateless Tuple, Theta", csk, resultInterTheta, resultInterCount, resultInterEmpty);

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
  public void emptyEmpty() {
    IntegerSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
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
  public void emptyExact() {
    IntegerSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT, 0, GT_MIDP_V);
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
  public void EmptyDegenerate() {
    IntegerSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
    IntegerSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V);
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
  public void emptyEstimation() {
    IntegerSketch tupleA = getTupleSketch(SkType.EMPTY, 0, 0);
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
  public void exactEmpty() {
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
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
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT, 0, GT_MIDP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT, 0, GT_MIDP_V);
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
  public void exactDegenerate() {
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V); //entries = 0
    UpdateSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V);
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
    IntegerSketch tupleA = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
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
  public void estimationEmpty() {
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
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
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT, 0, LT_LOWP_V);
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
  public void estimationDegenerate() {
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION, MIDP, LT_LOWP_V);
    IntegerSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V);
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
    IntegerSketch tupleA = getTupleSketch(SkType.ESTIMATION, MIDP, LT_LOWP_V);
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
  public void degenerateEmpty() {
    IntegerSketch tupleA = getTupleSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB =  getThetaSketch(SkType.EMPTY, 0, 0);
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
  public void degenerateExact() {
    IntegerSketch tupleA = getTupleSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.EXACT, 0, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.EXACT, 0, LT_LOWP_V);
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
  public void degenerateDegenerate() {
    IntegerSketch tupleA = getTupleSketch(SkType.DEGENERATE, MIDP, GT_MIDP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V);
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
  public void degenerateEstimation() {
    IntegerSketch tupleA = getTupleSketch(SkType.DEGENERATE, MIDP, GT_MIDP_V); //entries = 0
    IntegerSketch tupleB = getTupleSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    UpdateSketch thetaB =  getThetaSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
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
      if (!thetaOk)   { sb.append("theta: expected " + theta + ", got " + skTheta + "; "); }
      if (!entriesOk) { sb.append("entries: expected " + entries + ", got " + skEntries + "; "); }
      if (!emptyOk)   { sb.append("empty: expected " + empty + ", got " + skEmpty + "."); }
      throw new IllegalArgumentException(sb.toString());
    }
  }

  private static IntegerSketch getTupleSketch(SkType skType, float p, long value) {

    IntegerSketch sk;
    switch(skType) {
      case EMPTY: { // { 1.0,  0, T}
        sk = new IntegerSketch(4, 2, 1.0f, IntegerSummary.Mode.Min);
        break;
      }
      case EXACT: { // { 1.0, >0, F}
        sk = new IntegerSketch(4, 2, 1.0f, IntegerSummary.Mode.Min);
        sk.update(value, 1);
        break;
      }
      case ESTIMATION: { // {<1.0, >0, F}
        sk = new IntegerSketch(4, 2, p, IntegerSummary.Mode.Min);
        sk.update(value, 1);
        break;
      }
      case DEGENERATE: { // {<1.0,  0, F}
        sk = new IntegerSketch(4, 2, p, IntegerSummary.Mode.Min);
        sk.update(value, 1); // > theta
        break;
      }

      default: { return null; } // should not happen
    }
    return sk;
  }

  private static UpdateSketch getThetaSketch(SkType skType, float p, long value) {
    UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setLogNominalEntries(4);
    bldr.setResizeFactor(ResizeFactor.X4);

    UpdateSketch sk;
    switch(skType) {
      case EMPTY: { // { 1.0,  0, T}
        sk = bldr.build();
        break;
      }
      case EXACT: { // { 1.0, >0, F}
        sk = bldr.build();
        sk.update(value);
        break;
      }
      case ESTIMATION: { // {<1.0, >0, F}
        bldr.setP(p);
        sk = bldr.build();
        sk.update(value);
        break;
      }
      case DEGENERATE: { // {<1.0,  0, F}
        bldr.setP(p);
        sk = bldr.build();
        sk.update(value); // > theta
        break;
      }

      default: { return null; } // should not happen
    }
    return sk;
  }

}
