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
  //private static final long

  private static final long GT_MIDP_V   = 3L;
  private static final float MIDP       = 0.5f;
  private static final long LT_MIDP_V   = 2L;
  //private static final long LTLT_MIDP_V = 5L;

  private static final long GT_LOWP_V   = 6L;
  private static final float LOWP       = 0.1f;
  private static final long LT_LOWP_V   = 4L;
  //private static final long VALUE_1 = 1L;


  private static final double MIDP_THETA = MIDP;
  private static final double LOWP_THETA = LOWP;

  IntegerSummarySetOperations setOperations =
      new IntegerSummarySetOperations(IntegerSummary.Mode.Min, IntegerSummary.Mode.Min);
  Intersection<IntegerSummary> intersection = new Intersection<>(setOperations);

  enum SkType {
    NEW,         //{ 1.0,  0, T} Bin: 101  Oct: 05
    EXACT,       //{ 1.0, >0, F} Bin: 111  Oct: 07, specify only value
    ESTIMATION,  //{<1.0, >0, F} Bin: 010  Oct: 02, specify only value
    NEW_DEGEN,   //{<1.0,  0, T} Bin: 001  Oct: 01, specify only p
    RESULT_DEGEN //{<1.0,  0, F} Bin: 000  Oct: 0, specify p, value
  }

  //NOTE: 0 values in getSketch are not used.

  @Test
  public void newNew() {
    IntegerSketch ska = getSketch(SkType.NEW,    0, 0);
    IntegerSketch skb = getSketch(SkType.NEW,    0, 0);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 0, true);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk, 1.0, 0, true);
  }

  @Test
  public void newExact() {
    IntegerSketch ska = getSketch(SkType.NEW,    0, 0);
    IntegerSketch skb = getSketch(SkType.EXACT,  0, GT_MIDP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 0, true);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk, 1.0, 0, true);
  }

  @Test
  public void newNewDegen() {
    IntegerSketch ska = getSketch(SkType.NEW,       0, 0);
    IntegerSketch skb = getSketch(SkType.NEW_DEGEN, LOWP, 0);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 0, true);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk, 1.0, 0, true);
  }

  @Test
  public void newResultDegen() {
    IntegerSketch ska = getSketch(SkType.NEW,          0, 0);
    IntegerSketch skb = getSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 0, true);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk, 1.0, 0, true);
  }

  @Test
  public void newNewEstimation() {
    IntegerSketch ska = getSketch(SkType.NEW,        0, 0);
    IntegerSketch skb = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 0, true);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk, 1.0, 0, true);
  }

  /*********************/

  @Test
  public void exactNew() {
    IntegerSketch ska = getSketch(SkType.EXACT,  0, GT_MIDP_V);
    IntegerSketch skb = getSketch(SkType.NEW,    0, 0);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 1, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 1, false);
  }

  @Test
  public void exactExact() {
    IntegerSketch ska = getSketch(SkType.EXACT,  0, GT_MIDP_V);
    IntegerSketch skb = getSketch(SkType.EXACT,  0, GT_MIDP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 1, false);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 0, true);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk, 1.0, 0, true);
  }

  @Test
  public void exactNewDegen() {
    IntegerSketch ska = getSketch(SkType.EXACT,     0, LT_LOWP_V);
    IntegerSketch skb = getSketch(SkType.NEW_DEGEN, LOWP, 0);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 1, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 1, false);
  }

  @Test
  public void exactResultDegen() { //AnotB: 1.0 != 0.10000000149011612;
    IntegerSketch ska = getSketch(SkType.EXACT,        0, LT_LOWP_V);
    IntegerSketch skb = getSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V); //entries = 0

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 0, false);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 1, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 1, false);
  }

  @Test
  public void exactEstimation() {
    IntegerSketch ska = getSketch(SkType.EXACT,      0, LT_LOWP_V);
    IntegerSketch skb = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 1, false);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);
  }

  /*********************/

  @Test
  public void estimationNew() {
    IntegerSketch ska = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    IntegerSketch skb = getSketch(SkType.NEW,        0, 0);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 1, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 1, false);
  }

  @Test
  public void estimationExact() {
    IntegerSketch ska = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    IntegerSketch skb = getSketch(SkType.EXACT,      0, LT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 1, false);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);
  }

  @Test
  public void estimationNewDegen() {
    IntegerSketch ska = getSketch(SkType.ESTIMATION,  MIDP, LT_MIDP_V);
    IntegerSketch skb = getSketch(SkType.NEW_DEGEN,   LOWP, 0);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, MIDP_THETA, 1, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, MIDP_THETA, 1, false);
  }

  @Test
  public void estimationResultDegen() {
    IntegerSketch ska = getSketch(SkType.ESTIMATION,   MIDP, LT_LOWP_V);
    IntegerSketch skb = getSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 0, false);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 1, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 1, false);
  }

  @Test
  public void estimationEstimation() {
    IntegerSketch ska = getSketch(SkType.ESTIMATION,  MIDP, LT_LOWP_V);
    IntegerSketch skb = getSketch(SkType.ESTIMATION,  LOWP, LT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 1, false);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);
  }

  /*********************/

  @Test
  public void newDegenNew() {//AnotB: 0.10000000149011612 != 1.0;
    IntegerSketch ska = getSketch(SkType.NEW_DEGEN,  LOWP, 0);
    IntegerSketch skb = getSketch(SkType.NEW,         0, 0);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 0, true);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk, 1.0, 0, true);
  }

  @Test
  public void newDegenExact() { //AnotB: 0.10000000149011612 != 1.0;
    IntegerSketch ska = getSketch(SkType.NEW_DEGEN, LOWP,0);
    IntegerSketch skb = getSketch(SkType.EXACT,      0, LT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 0, true);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk, 1.0, 0, true);
  }

  @Test
  public void newDegenNewDegen() { //AnotB: 0.10000000149011612 != 1.0;
    IntegerSketch ska = getSketch(SkType.NEW_DEGEN, MIDP, 0);
    IntegerSketch skb = getSketch(SkType.NEW_DEGEN, LOWP, 0);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 0, true);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk, 1.0, 0, true);
  }

  @Test
  public void newDegenResultDegen() { //AnotB: 0.10000000149011612 != 1.0;
    IntegerSketch ska = getSketch(SkType.NEW_DEGEN,    MIDP, 0);
    IntegerSketch skb =  getSketch(SkType.RESULT_DEGEN, LOWP,GT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 0, true);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk, 1.0, 0, true);
  }

  @Test
  public void newDegenEstimation() { //AnotB: 0.10000000149011612 != 1.0;
    IntegerSketch ska = getSketch(SkType.NEW_DEGEN,  MIDP, 0);
    IntegerSketch skb =getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, 1.0, 0, true);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful Tuple, Tuple", csk, 1.0, 0, true);
  }

  /*********************/

  @Test
  public void resultDegenNew() {
    IntegerSketch ska = getSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V); //entries = 0
    IntegerSketch skb = getSketch(SkType.NEW,           0, 0);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);
  }

  @Test
  public void resultDegenExact() {
    IntegerSketch ska = getSketch(SkType.RESULT_DEGEN,  LOWP, GT_LOWP_V); //entries = 0
    IntegerSketch skb = getSketch(SkType.EXACT,         0, LT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 0, false);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);
  }

  @Test
  public void resultDegenNewDegen() {
    IntegerSketch ska = getSketch(SkType.RESULT_DEGEN, MIDP, GT_MIDP_V); //entries = 0
    IntegerSketch skb = getSketch(SkType.NEW_DEGEN,    LOWP, 0);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, MIDP_THETA, 0, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, MIDP_THETA, 0, false);
  }

  @Test
  public void resultDegenResultDegen() {
    IntegerSketch ska = getSketch(SkType.RESULT_DEGEN, MIDP, GT_MIDP_V); //entries = 0
    IntegerSketch skb = getSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 0, false);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);
  }

  @Test
  public void resultDegenEstimation() {
    IntegerSketch ska = getSketch(SkType.RESULT_DEGEN, MIDP, GT_MIDP_V); //entries = 0
    IntegerSketch skb = getSketch(SkType.ESTIMATION,   LOWP, LT_LOWP_V);

    //Stateless Tuple, Tuple
    Intersection<IntegerSummary> inter = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 0, false);

    csk = AnotB.aNotB(ska, skb);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);

    //Stateful Tuple, Tuple
    AnotB<IntegerSummary> anotb = new AnotB<>();
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateless Tuple, Tuple", csk, LOWP_THETA, 0, false);
  }

  //=================================

  private static void checkResult(String comment, CompactSketch<IntegerSummary> sk, double theta, int entries, boolean empty) {
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

  private static IntegerSketch getSketch(SkType skType, float p, long value) {

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

}
