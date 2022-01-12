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

package org.apache.datasketches.tuple.arrayofdoubles;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import static org.apache.datasketches.Util.zeroPad;

public class CornerCaseArrayOfDoublesSetOperationsTest {
  //Stateful Intersection with intersect(sketch A, combiner), followed by getResult()
  //Essentially Stateless AnotB with update(Sketch A, Sketch B), followed by getResult()
  //Stateful Union with union(Sketch A), followed by getResult()

  /* Hashes and Hash Equivalents
   *               Top8bits  Hex               Decimal
   * MAX:           01111111, 7fffffffffffffff, 9223372036854775807
   * GT_MIDP:       01011101, 5d6906dac1b340ba, 6730918654704304314  3L
   * MIDP_THETALONG:01000000, 4000000000000000, 4611686018427387904
   * GT_LOWP:       00010000, 10bc98fb132116fe, 1206007004353599230  6L
   * LOWP_THETALONG:00010000, 1000000000000000, 1152921504606846976
   * LT_LOWP:       00001000,  83ddbc9e12ede40,  593872385995628096  4L
   */


  private static final float MIDP_FLT = 0.5f;
  private static final float LOWP_FLT = 0.125f;
  private static final long GT_MIDP_KEY = 3L;
  private static final long GT_LOWP_KEY = 6L;
  private static final long LT_LOWP_KEY = 4L;

  private static final long MAX_LONG = Long.MAX_VALUE;

  private static final long HASH_GT_MIDP = getLongHash(GT_MIDP_KEY);
  private static final long MIDP_THETALONG = (long)(MAX_LONG * MIDP_FLT);

  private static final long HASH_GT_LOWP = getLongHash(GT_LOWP_KEY);
  private static final long LOWP_THETALONG = (long)(MAX_LONG * LOWP_FLT);
  private static final long HASH_LT_LOWP = getLongHash(LT_LOWP_KEY);

  private static final String LS = System.getProperty("line.separator");

  private enum SkType {
    EMPTY,      // { 1.0,  0, T} Bin: 101  Oct: 05
    EXACT,      // { 1.0, >0, F} Bin: 110  Oct: 06, specify only value
    ESTIMATION, // {<1.0, >0, F} Bin: 010  Oct: 02, specify only value
    DEGENERATE  // {<1.0,  0, F} Bin: 000  Oct: 0, specify p, value
  }

  private static class MinCombiner implements ArrayOfDoublesCombiner {
    MinCombiner() {}

    @Override
    public double[] combine(double[] a, double[] b) {
      return new double[] { Math.min(a[0], b[0]) };
    }
  }

  private static MinCombiner minCombiner = new MinCombiner();

  //=================================f

  @Test
  public void emptyEmpty() {
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.EMPTY, 0, 0);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.EMPTY, 0, 0);
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
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.EMPTY, 0, 0);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.EXACT, 0, GT_MIDP_KEY);
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
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.EMPTY, 0, 0);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_KEY);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 0;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void emptyEstimation() {
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.EMPTY, 0, 0);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_KEY);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = 1.0;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = true;
    final double expectedUnionTheta = LOWP_FLT;
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
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.EXACT, 0, GT_MIDP_KEY);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.EMPTY, 0, 0);
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
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.EXACT, 0, GT_MIDP_KEY);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.EXACT, 0, GT_MIDP_KEY);
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
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.EXACT, 0, LT_LOWP_KEY);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_KEY); //entries = 0
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 1;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void exactEstimation() {
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.EXACT, 0, LT_LOWP_KEY);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_KEY);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 1;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
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
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_KEY);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.EMPTY, 0, 0);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 1;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void estimationExact() {
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_KEY);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.EXACT, 0, LT_LOWP_KEY);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 1;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void estimationDegenerate() {
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.ESTIMATION, MIDP_FLT, LT_LOWP_KEY);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_KEY);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 1;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void estimationEstimation() {
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.ESTIMATION, MIDP_FLT, LT_LOWP_KEY);
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_KEY);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 1;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
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
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_KEY); //entries = 0
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.EMPTY, 0, 0);
    final double expectedIntersectTheta = 1.0;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = true;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 0;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void degenerateExact() {
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.DEGENERATE,  LOWP_FLT, GT_LOWP_KEY); //entries = 0
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.EXACT, 0, LT_LOWP_KEY);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 1;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void degenerateDegenerate() {
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.DEGENERATE, MIDP_FLT, GT_MIDP_KEY); //entries = 0
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.DEGENERATE, LOWP_FLT, GT_LOWP_KEY);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
    final int expectedUnionCount = 0;
    final boolean expectedUnionEmpty = false;

    checks(thetaA, thetaB,
        expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
        expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
        expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  @Test
  public void degenerateEstimation() {
    ArrayOfDoublesUpdatableSketch thetaA = getSketch(SkType.DEGENERATE, MIDP_FLT, GT_MIDP_KEY); //entries = 0
    ArrayOfDoublesUpdatableSketch thetaB = getSketch(SkType.ESTIMATION, LOWP_FLT, LT_LOWP_KEY);
    final double expectedIntersectTheta = LOWP_FLT;
    final int expectedIntersectCount = 0;
    final boolean expectedIntersectEmpty = false;
    final double expectedAnotbTheta = LOWP_FLT;
    final int expectedAnotbCount = 0;
    final boolean expectedAnotbEmpty = false;
    final double expectedUnionTheta = LOWP_FLT;
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
      ArrayOfDoublesUpdatableSketch tupleA,
      ArrayOfDoublesUpdatableSketch tupleB,
      double expectedIntersectTheta,
      int expectedIntersectCount,
      boolean expectedIntersectEmpty,
      double expectedAnotbTheta,
      int expectedAnotbCount,
      boolean expectedAnotbEmpty,
      double expectedUnionTheta,
      int expectedUnionCount,
      boolean expectedUnionEmpty) {
    ArrayOfDoublesCompactSketch csk;
    ArrayOfDoublesIntersection inter = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    ArrayOfDoublesAnotB anotb = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion();

    //Intersection Tuple, Tuple Updatable Stateful
    inter.intersect(tupleA, minCombiner);
    inter.intersect(tupleB, minCombiner);
    csk = inter.getResult();
    inter.reset();
    checkResult("Intersect Stateless Theta, Theta", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);
    //Intersection Tuple, Tuple Compact Stateful
    inter.intersect(tupleA.compact(), minCombiner);
    inter.intersect(tupleB.compact(), minCombiner);
    csk = inter.getResult();
    inter.reset();
    checkResult("Intersect Stateless Theta, Theta", csk, expectedIntersectTheta, expectedIntersectCount,
        expectedIntersectEmpty);

    //AnotB Stateless Tuple, Tuple Updatable
    anotb.update(tupleA, tupleB);
    csk = anotb.getResult();
    checkResult("AnotB Stateless Theta, Theta", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
    //AnotB Stateless Tuple, Tuple Compact
    anotb.update(tupleA, tupleB);
    csk = anotb.getResult();
    checkResult("AnotB Stateless Theta, Theta", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);

    //Union Stateful Tuple, Tuple Updatable
    union.union(tupleA);
    union.union(tupleB);
    csk = union.getResult();
    union.reset();
    checkResult("Union Stateless Theta, Theta", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
    //Union Stateful Tuple, Tuple Compact
    union.union(tupleA.compact());
    union.union(tupleB.compact());
    csk = union.getResult();
    union.reset();
    checkResult("Union Stateless Theta, Theta", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }

  private static void checkResult(
      String comment,
      ArrayOfDoublesCompactSketch csk,
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

  private static ArrayOfDoublesUpdatableSketch getSketch(
      SkType skType,
      float p,
      long updateKey) {

    ArrayOfDoublesUpdatableSketchBuilder bldr = new ArrayOfDoublesUpdatableSketchBuilder();
    bldr.setNominalEntries(16);
    //Assume defaults: 1 double value, resize factor, seed
    double[] summaryVal = {1.0};

    ArrayOfDoublesUpdatableSketch sk;
    switch(skType) {
      case EMPTY: { // { 1.0,  0, T} p and value are not used
        sk = bldr.build();
        break;
      }
      case EXACT: { // { 1.0, >0, F} p is not used
        sk = bldr.build();
        sk.update(updateKey, summaryVal);
        break;
      }
      case ESTIMATION: { // {<1.0, >0, F}
        checkValidUpdate(p, updateKey);
        bldr.setSamplingProbability(p);
        sk = bldr.build();
        sk.update(updateKey, summaryVal);
        break;
      }
      case DEGENERATE: { // {<1.0,  0, F}
        checkInvalidUpdate(p, updateKey);
        bldr.setSamplingProbability(p);
        sk = bldr.build();
        sk.update(updateKey, summaryVal); // > theta
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

  //*******************************************
  //Helper functions for setting the hash values

  //@Test
  public void printTable() {
    println("              Top8bits  Hex               Decimal");
    printf("MAX:           %8s, %16x, %19d" + LS, getTop8(MAX_LONG), MAX_LONG, MAX_LONG);
    printf("GT_MIDP:       %8s, %16x, %19d" + LS, getTop8(HASH_GT_MIDP), HASH_GT_MIDP, HASH_GT_MIDP);
    printf("MIDP_THETALONG:%8s, %16x, %19d" + LS, getTop8(MIDP_THETALONG), MIDP_THETALONG, MIDP_THETALONG);
    printf("GT_LOWP:       %8s, %16x, %19d" + LS, getTop8(HASH_GT_LOWP), HASH_GT_LOWP, HASH_GT_LOWP);
    printf("LOWP_THETALONG:%8s, %16x, %19d" + LS, getTop8(LOWP_THETALONG), LOWP_THETALONG, LOWP_THETALONG);
    printf("LT_LOWP:       %8s, %16x, %19d" + LS, getTop8(HASH_LT_LOWP), HASH_LT_LOWP, HASH_LT_LOWP);
    println(LS +"Doubles");

    println(LS + "Longs");
    for (long v = 1L; v < 10; v++) {
      long hash = (hash(v, DEFAULT_UPDATE_SEED)[0]) >>> 1;
      printLong(v, hash);
    }
  }

  static long getLongHash(long v) {
    return (hash(v, DEFAULT_UPDATE_SEED)[0]) >>> 1;
  }

  static void printLong(long v, long hash) {
    System.out.printf("     %8d, %8s, %16x, %19d" + LS,v, getTop8(hash), hash, hash);
  }

  static String getTop8(final long v) {
    int i = (int) (v >>> 56);
    String s = Integer.toBinaryString(i);
    return zeroPad(s, 8);
  }

  private static void println(Object o) {
    System.out.println(o.toString());
  }

  private static void printf(String fmt, Object ...args) {
    System.out.printf(fmt, args);
  }
}

