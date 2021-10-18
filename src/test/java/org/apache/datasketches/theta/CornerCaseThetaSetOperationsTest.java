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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.hash.MurmurHash3.hash;

import org.testng.annotations.Test;

public class CornerCaseThetaSetOperationsTest {

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


  enum SkType {
    NEW,          //{ 1.0,  0, T} Bin: 101  Oct: 05
    EXACT,        //{ 1.0, >0, F} Bin: 111  Oct: 07, specify only value
    ESTIMATION,   //{<1.0, >0, F} Bin: 010  Oct: 02, specify only value
    NEW_DEGEN,    //{<1.0,  0, T} Bin: 001  Oct: 01, specify only p
    RESULT_DEGEN  //{<1.0,  0, F} Bin: 000  Oct: 0, specify p, value
  }

  //NOTE: 0 values in getSketch are not used.

  @Test
  public void newNew() {
    UpdateSketch ska = getSketch(SkType.NEW, 0, 0);
    UpdateSketch skb = getSketch(SkType.NEW, 0, 0);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 0, true);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 0, true);

  }

  @Test
  public void newExact() {
    UpdateSketch ska = getSketch(SkType.NEW,    0, 0);
    UpdateSketch skb = getSketch(SkType.EXACT,  0, GT_MIDP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 0, true);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 0, true);
  }

  @Test
  public void newNewDegen() {
    UpdateSketch ska = getSketch(SkType.NEW,       0, 0);
    UpdateSketch skb = getSketch(SkType.NEW_DEGEN, LOWP, 0);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 0, true);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 0, true);
  }

  @Test
  public void newResultDegen() {
    UpdateSketch ska = getSketch(SkType.NEW,          0, 0);
    UpdateSketch skb = getSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 0, true);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 0, true);
  }

  @Test
  public void newNewEstimation() {
    UpdateSketch ska = getSketch(SkType.NEW,        0, 0);
    UpdateSketch skb = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 0, true);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 0, true);
  }

  /*********************/

  @Test
  public void exactNew() {
    UpdateSketch ska = getSketch(SkType.EXACT,  0, GT_MIDP_V);
    UpdateSketch skb = getSketch(SkType.NEW,    0, 0);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 1, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 1, false);
  }

  @Test
  public void exactExact() {
    UpdateSketch ska = getSketch(SkType.EXACT,  0, GT_MIDP_V);
    UpdateSketch skb = getSketch(SkType.EXACT,  0, GT_MIDP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 1, false);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 0, true);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 0, true);
  }

  @Test
  public void exactNewDegen() {
    UpdateSketch ska = getSketch(SkType.EXACT,     0, LT_LOWP_V);
    UpdateSketch skb = getSketch(SkType.NEW_DEGEN, LOWP, 0);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 1, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 1, false);
  }

  @Test
  public void exactResultDegen() {
    UpdateSketch ska = getSketch(SkType.EXACT,        0, LT_LOWP_V);
    UpdateSketch skb = getSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V); //entries = 0

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 0, false);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, LOWP_THETA, 1, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, LOWP_THETA, 1, false);
  }

  @Test
  public void exactEstimation() {
    UpdateSketch ska = getSketch(SkType.EXACT,      0, LT_LOWP_V);
    UpdateSketch skb = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 1, false);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, LOWP_THETA, 0, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, LOWP_THETA, 0, false);
  }

  /*********************/

  @Test
  public void estimationNew() {
    UpdateSketch ska = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    UpdateSketch skb = getSketch(SkType.NEW,        0, 0);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, LOWP_THETA, 1, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, LOWP_THETA, 1, false);
  }

  @Test
  public void estimationExact() {
    UpdateSketch ska = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);
    UpdateSketch skb = getSketch(SkType.EXACT,      0, LT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 1, false);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, LOWP_THETA, 0, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, LOWP_THETA, 0, false);
  }

  @Test
  public void estimationNewDegen() {
    UpdateSketch ska = getSketch(SkType.ESTIMATION,  MIDP, LT_MIDP_V);
    UpdateSketch skb = getSketch(SkType.NEW_DEGEN,   LOWP, 0);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, MIDP_THETA, 1, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, MIDP_THETA, 1, false);
  }

  @Test
  public void estimationResultDegen() {
    UpdateSketch ska = getSketch(SkType.ESTIMATION,   MIDP, LT_LOWP_V);
    UpdateSketch skb = getSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 0, false);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, LOWP_THETA, 1, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, LOWP_THETA, 1, false);
  }

  @Test
  public void estimationEstimation() {
    UpdateSketch ska = getSketch(SkType.ESTIMATION,  MIDP, LT_LOWP_V);
    UpdateSketch skb = getSketch(SkType.ESTIMATION,  LOWP, LT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 1, false);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, LOWP_THETA, 0, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, LOWP_THETA, 0, false);
  }

  /*********************/

  @Test
  public void newDegenNew() {
    UpdateSketch ska = getSketch(SkType.NEW_DEGEN,  LOWP, 0);
    UpdateSketch skb = getSketch(SkType.NEW,         0, 0);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 0, true);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 0, true);
  }

  @Test
  public void newDegenExact() {
    UpdateSketch ska = getSketch(SkType.NEW_DEGEN, LOWP,0);
    UpdateSketch skb = getSketch(SkType.EXACT,      0, LT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 0, true);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 0, true);
  }

  @Test
  public void newDegenNewDegen() {
    UpdateSketch ska = getSketch(SkType.NEW_DEGEN, MIDP, 0);
    UpdateSketch skb = getSketch(SkType.NEW_DEGEN, LOWP, 0);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 0, true);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 0, true);
  }

  @Test
  public void newDegenResultDegen() {
    UpdateSketch ska = getSketch(SkType.NEW_DEGEN,    MIDP, 0);
    UpdateSketch skb = getSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 0, true);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 0, true);
  }

  @Test
  public void newDegenEstimation() {
    UpdateSketch ska = getSketch(SkType.NEW_DEGEN,  MIDP, 0);
    UpdateSketch skb = getSketch(SkType.ESTIMATION, LOWP, LT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, 1.0, 0, true);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, 1.0, 0, true);
  }

  /*********************/

  @Test
  public void resultDegenNew() {
    UpdateSketch ska = getSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V); //entries = 0
    UpdateSketch skb = getSketch(SkType.NEW,           0, 0);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, LOWP_THETA, 0, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, LOWP_THETA, 0, false);
  }

  @Test
  public void resultDegenExact() {
    UpdateSketch ska = getSketch(SkType.RESULT_DEGEN,  LOWP, GT_LOWP_V); //entries = 0
    UpdateSketch skb = getSketch(SkType.EXACT,         0, LT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 0, false);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, LOWP_THETA, 0, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, LOWP_THETA, 0, false);
  }

  @Test
  public void resultDegenNewDegen() {
    UpdateSketch ska = getSketch(SkType.RESULT_DEGEN, MIDP, GT_MIDP_V); //entries = 0
    UpdateSketch skb = getSketch(SkType.NEW_DEGEN,    LOWP, 0);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, 1.0, 0, true);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, MIDP_THETA, 0, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, MIDP_THETA, 0, false);
  }

  @Test
  public void resultDegenResultDegen() {
    UpdateSketch ska = getSketch(SkType.RESULT_DEGEN, MIDP, GT_MIDP_V); //entries = 0
    UpdateSketch skb = getSketch(SkType.RESULT_DEGEN, LOWP, GT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 0, false);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, LOWP_THETA, 0, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, LOWP_THETA, 0, false);
  }

  @Test
  public void resultDegenEstimation() {
    UpdateSketch ska = getSketch(SkType.RESULT_DEGEN, MIDP, GT_MIDP_V); //entries = 0
    UpdateSketch skb = getSketch(SkType.ESTIMATION,   LOWP, LT_LOWP_V);

    //Stateless
    Intersection inter = SetOperation.builder().buildIntersection();
    CompactSketch csk = inter.intersect(ska, skb);
    checkResult("Intersect", csk, LOWP_THETA, 0, false);

    AnotB anotb = SetOperation.builder().buildANotB();
    csk = anotb.aNotB(ska, skb);
    checkResult("AnotB Stateless", csk, LOWP_THETA, 0, false);

    //Stateful AnotB
    anotb.setA(ska);
    anotb.notB(skb);
    csk = anotb.getResult(true);
    checkResult("AnotB Stateful", csk, LOWP_THETA, 0, false);
  }

  //=================================

  private static void checkResult(String comment, CompactSketch sk, double theta, int entries, boolean empty) {
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

  private static UpdateSketch getSketch(SkType skType, float p, long value) {
    UpdateSketchBuilder bldr = UpdateSketch.builder();
    bldr.setLogNominalEntries(4);
    UpdateSketch sk;
    switch(skType) {
      case NEW: {      //{ 1.0,  0, T} Bin: 101  Oct: 05
        sk = bldr.build();
        break;
      }
      case EXACT: {     //{ 1.0, >0, F} Bin: 111  Oct: 07
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
        sk.update(value);
        break;
      }

      default: { return null; } //should not happen
    }
    return sk;
  }

  private static void println(Object o) {
    System.out.println(o.toString());
  }

  //@Test
  public void printHash() {
    long seed = DEFAULT_UPDATE_SEED;
    long v = 6;
    long hash = (hash(v, seed)[0]) >>> 1;
    println(v + ", " + hash);
  }

  //@Test
  public void printPAsLong() {
    float p = 0.5f;
    println("p = " + p + ", " + (long)(Long.MAX_VALUE * p));
  }

}
