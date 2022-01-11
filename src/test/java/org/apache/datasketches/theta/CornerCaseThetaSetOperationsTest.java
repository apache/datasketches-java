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

package org.apache.datasketches.theta;

import org.testng.annotations.Test;
//import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
//import static org.apache.datasketches.hash.MurmurHash3.hash;

public class CornerCaseThetaSetOperationsTest {

  /* Hash Values
   * 9223372036854775807  Theta = 1.0
   *
   * 6730918654704304314  hash(3L)[0] >>> 1    GT_MIDP
   * 4611686018427387904  Theta for p = 0.5f = MIDP
   *
   * 1206007004353599230  hash(6L)[0] >>> 1    GT_LOWP_V
   *  922337217429372928  Theta for p = 0.1f = LOWP
   *  593872385995628096  hash(4L)[0] >>> 1    LT_LOWP_V
   */

  private static final long GT_MIDP_V   = 3L;
  private static final float MIDP       = 0.5f;

  private static final long GT_LOWP_V   = 6L;
  private static final float LOWP       = 0.1f;
  private static final long LT_LOWP_V   = 4L;

  private static final double LOWP_THETA = LOWP;

  private enum SkType {
    EMPTY,      // { 1.0,  0, T} Bin: 101  Oct: 05
    EXACT,      // { 1.0, >0, F} Bin: 110  Oct: 06, specify only value
    ESTIMATION, // {<1.0, >0, F} Bin: 010  Oct: 02, specify only value
    DEGENERATE  // {<1.0,  0, F} Bin: 000  Oct: 0, specify p, value
  }

  //=================================

  @Test
  public void emptyEmpty() {
    UpdateSketch thetaA = getSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB = getSketch(SkType.EMPTY, 0, 0);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = 1.0;
    final int expectedUnionCount = 0;
    final boolean expectedUnionEmpty = true;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void emptyExact() {
    UpdateSketch thetaA = getSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB = getSketch(SkType.EXACT, 0, GT_MIDP_V);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = 1.0;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void emptyDegenerate() {
    UpdateSketch thetaA = getSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB = getSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 0;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void emptyEstimation() {
    UpdateSketch thetaA = getSketch(SkType.EMPTY, 0, 0);
    UpdateSketch thetaB = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  //=================================

  @Test
  public void exactEmpty() {
    UpdateSketch thetaA = getSketch(SkType.EXACT, 0, GT_MIDP_V);
    UpdateSketch thetaB = getSketch(SkType.EMPTY, 0, 0);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 1;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = 1.0;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void exactExact() {
    UpdateSketch thetaA = getSketch(SkType.EXACT, 0, GT_MIDP_V);
    UpdateSketch thetaB = getSketch(SkType.EXACT, 0, GT_MIDP_V);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 1;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = 1.0;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void exactDegenerate() {
    UpdateSketch thetaA = getSketch(SkType.EXACT, 0, LT_LOWP_V);
    UpdateSketch thetaB = getSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V); //entries = 0
    final double expectedIntersectTheta = LOWP_THETA;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_THETA;
    final int expectedAnotbCount = 1;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void exactEstimation() {
    UpdateSketch thetaA = getSketch(SkType.EXACT, 0, LT_LOWP_V);
    UpdateSketch thetaB = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    final double expectedIntersectTheta = LOWP_THETA;
    final int expectedIntersectCount = 1;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_THETA;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  //=================================

  @Test
  public void estimationEmpty() {
    UpdateSketch thetaA = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    UpdateSketch thetaB = getSketch(SkType.EMPTY, 0, 0);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = LOWP_THETA;
    final int expectedAnotbCount = 1;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void estimationExact() {
    UpdateSketch thetaA = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    UpdateSketch thetaB = getSketch(SkType.EXACT, 0, LT_LOWP_V);
    final double expectedIntersectTheta = LOWP_THETA;
    final int expectedIntersectCount = 1;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_THETA;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void estimationDegenerate() {
    UpdateSketch thetaA = getSketch(SkType.ESTIMATION, MIDP, LT_LOWP_V);
    UpdateSketch thetaB = getSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V);
    final double expectedIntersectTheta = LOWP_THETA;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_THETA;
    final int expectedAnotbCount = 1;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void estimationEstimation() {
    UpdateSketch thetaA = getSketch(SkType.ESTIMATION, MIDP, LT_LOWP_V);
    UpdateSketch thetaB = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    final double expectedIntersectTheta = LOWP_THETA;
    final int expectedIntersectCount = 1;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_THETA;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  //=================================

  @Test
  public void degenerateEmpty() {
    UpdateSketch thetaA = getSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V); //entries = 0
    UpdateSketch thetaB = getSketch(SkType.EMPTY, 0, 0);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = LOWP_THETA;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 0;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void degenerateExact() {
    UpdateSketch thetaA = getSketch(SkType.DEGENERATE,  LOWP, GT_LOWP_V); //entries = 0
    UpdateSketch thetaB = getSketch(SkType.EXACT, 0, LT_LOWP_V);
    final double expectedIntersectTheta = LOWP_THETA;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_THETA;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void degenerateDegenerate() {
    UpdateSketch thetaA = getSketch(SkType.DEGENERATE, MIDP, GT_MIDP_V); //entries = 0
    UpdateSketch thetaB = getSketch(SkType.DEGENERATE, LOWP, GT_LOWP_V);
    final double expectedIntersectTheta = LOWP_THETA;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_THETA;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 0;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void degenerateEstimation() {
    UpdateSketch thetaA = getSketch(SkType.DEGENERATE, MIDP, GT_MIDP_V); //entries = 0
    UpdateSketch thetaB = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    final double expectedIntersectTheta = LOWP_THETA;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_THETA;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  //=================================
  //=================================

  private static void checks(
      UpdateSketch thetaA,
      UpdateSketch thetaB,
      double expectedIntersectTheta,
      int expectedIntersectCount,
      boolean expectedIntersectEmpty,
      double expectedAnotbTheta,
      int expectedAnotbCount,
      boolean expectedAnotbEmpty,
      double expectedUnionTheta,
      int expectedUnionCount,
      boolean expectedUnionEmpty) {
    CompactSketch csk;
    Intersection inter = SetOperation.builder().buildIntersection();
    AnotB anotb = SetOperation.builder().buildANotB();
    Union union = new SetOperationBuilder().buildUnion();

    //Intersection Stateless Theta, Theta Updatable
    csk = inter.intersect(thetaA, thetaB);
    checkResult("Intersect Stateless Theta, Theta", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);
    //Intersection Stateless Theta, Theta Compact
    csk = inter.intersect(thetaA.compact(), thetaB.compact());
    checkResult("Intersect Stateless Theta, Theta", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);

    //AnotB Stateless Theta, Theta Updatable
    csk = anotb.aNotB(thetaA, thetaB);
    checkResult("AnotB Stateless Theta, Theta", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //AnotB Stateless Theta, Theta Compact
    csk = anotb.aNotB(thetaA.compact(), thetaB.compact());
    checkResult("AnotB Stateless Theta, Theta", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);

    //AnotB Stateful Theta, Theta Updatable
    anotb.setA(thetaA);
    anotb.notB(thetaB);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Theta, Theta", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //AnotB Stateful Theta, Theta Compact
    anotb.setA(thetaA.compact());
    anotb.notB(thetaB.compact());
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Theta, Theta", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);

    //Union Stateful Theta, Theta Updatable
    union.union(thetaA);
    union.union(thetaB);
    csk = union.getResult();
    union.reset();
    checkResult("Union Stateless Theta, Theta", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //Union Stateful Theta, Theta Compact
    union.union(thetaA.compact());
    union.union(thetaB.compact());
    csk = union.getResult();
    union.reset();
    checkResult("Union Stateless Theta, Theta", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);

  }

  private static void checkResult(
      String comment,
      CompactSketch csk,
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

  private static UpdateSketch getSketch(SkType skType, float p, long value) {
    UpdateSketchBuilder bldr = UpdateSketch.builder();
    bldr.setLogNominalEntries(4);
    UpdateSketch sk;
    switch(skType) {
      case EMPTY: { // { 1.0,  0, T} p and value are not used
        sk = bldr.build();
        break;
      }
      case EXACT: { // { 1.0, >0, F} p is not used
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
        sk.update(value);
        break;
      }

      default: { return null; } // should not happen
    }
    return sk;
  }

//  private static void println(Object o) {
//    System.out.println(o.toString());
//  }
//
//  @Test
//  public void printHash() {
//    long seed = DEFAULT_UPDATE_SEED;
//    long v = 6;
//    long hash = (hash(v, seed)[0]) >>> 1;
//    println(v + ", " + hash);
//  }
//
//  @Test
//  public void printPAsLong() {
//    float p = 0.5f;
//    println("p = " + p + ", " + (long)(Long.MAX_VALUE * p));
//  }

}
