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

package org.apache.datasketches.req;

import static org.apache.datasketches.Criteria.GE;
import static org.apache.datasketches.Criteria.GT;
import static org.apache.datasketches.Criteria.LE;
import static org.apache.datasketches.Criteria.LT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.Criteria;
import org.apache.datasketches.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings({"javadoc", "unused"})
public class ReqSketchOtherTest {
  final ReqSketchTest reqSketchTest = new ReqSketchTest();
  static Criteria critLT = LT;
  static Criteria critLE = LE;
  static Criteria critGT = GT;
  static Criteria critGE = GE;

  @Test
  public void checkConstructors() {
    ReqSketch sk = ReqSketch.builder().build();
    assertEquals(sk.getK(), 12);
  }

  @Test
  public void checkCopyConstructors() {
    Criteria criterion = LE;
    ReqSketch sk = reqSketchTest.loadSketch( 6,   1, 50,  true,  true,  criterion, 0);
    long n = sk.getN();
    float min = sk.getMinValue();
    float max = sk.getMaxValue();
    ReqSketch sk2 = new ReqSketch(sk);
    assertEquals(sk2.getMinValue(), min);
    assertEquals(sk2.getMaxValue(), max);
  }

  @Test
  public void checkEmptyPMF_CDF() {
    ReqSketch sk = ReqSketch.builder().build();
    float[] sp = new float[] {0, 1};
    assertEquals(sk.getCDF(sp), new double[0]);
    assertEquals(sk.getPMF(sp), new double[0]);
    sk.update(1);
    try {sk.getCDF(new float[] { Float.NaN }); fail(); } catch (SketchesArgumentException e) {}
  }

  @Test
  public void checkQuantilesExceedLimits() {
    Criteria criterion = LE;
    ReqSketch sk = reqSketchTest.loadSketch( 6,   1, 200,  true,  true,  criterion, 0);
    try { sk.getQuantile(2.0f); fail(); } catch (SketchesArgumentException e) {}
    try { sk.getQuantile(-2.0f); fail(); } catch (SketchesArgumentException e) {}
  }

  @Test
  public void checkEstimationMode() {
    boolean up = true;
    boolean hra = true;
    Criteria criterion = LE;
    ReqSketch sk = reqSketchTest.loadSketch( 20,   1, 119,  up,  hra,  criterion, 0);
    assertEquals(sk.isEstimationMode(), false);
    double lb = sk.getRankLowerBound(1.0, 1);
    double ub = sk.getRankUpperBound(1.0, 1);
    assertEquals(lb, 1.0);
    assertEquals(ub, 1.0);
    int maxNomSize = sk.getMaxNomSize();
    assertEquals(maxNomSize, 120);
    sk.update(120);
    assertEquals(sk.isEstimationMode(), true);
    //  lb = sk.getRankLowerBound(0, 1);
    //  ub = sk.getRankUpperBound(1.0, 1);
    //  assertEquals(lb, 0.0);
    //  assertEquals(ub, 2.0);
    maxNomSize = sk.getMaxNomSize();
    assertEquals(maxNomSize, 240);
    float v = sk.getQuantile(1.0);
    assertEquals(v, 120.0f);
    ReqAuxiliary aux = sk.getAux();
    assertNotNull(aux);
    assertTrue(sk.getRSE(sk.getK(), .5, false, 120) > 0);
    assertTrue(sk.getSerializationBytes() > 0);
  }

  @Test
  public void checkNaNUpdate() {
    Criteria criterion = LE;
    ReqSketch sk = ReqSketch.builder().build();
    sk.update(Float.NaN);
    assertTrue(sk.isEmpty());
  }

  @Test
  public void checkNonFiniteGetRank() {
    ReqSketch sk = ReqSketch.builder().build();
    sk.update(1);
    try { sk.getRank(Float.POSITIVE_INFINITY); fail(); } catch (AssertionError e) {}
  }

  @Test
  public void checkFlags() {
    ReqSketch sk = ReqSketch.builder().build();
    sk.setCriterion(Criteria.LE);
    assertEquals(sk.isLessThanOrEqual(), true);
    assertEquals(sk.getCriterion(), Criteria.LE);
    assertEquals(sk.isCompatible(), true);
    sk.setCriterion(Criteria.LT);
    sk.setCompatible(false);
    assertEquals(sk.isLessThanOrEqual(), false);
    assertEquals(sk.getCriterion(), Criteria.LT);
    assertEquals(sk.isCompatible(), false);
  }

  @Test
  public void checkEmpty() {
    ReqSketch sk = ReqSketch.builder().build();
    try { sk.getQuantile(0.5); fail(); } catch (SketchesArgumentException e) { }
  }

  @Test
  public void moreMergeTests() {
    ReqSketch sk1 = ReqSketch.builder().build();
    ReqSketch sk2 = ReqSketch.builder().build();
    for (int i = 5; i < 10; i++) {sk1.update(i); }
    sk1.merge(sk2); //does nothing
    for (int i = 1; i <= 15; i++) {sk2.update(i); }
    sk1.merge(sk2);
    assertEquals(sk1.getN(), 20);
    for (int i = 16; i <= 300; i++) { sk2.update(i); }
    sk1.merge(sk2);
  }

  @Test
  public void checkCompatibleGT() {
    ReqSketchBuilder bldr = ReqSketch.builder();
    bldr.setK(6).setCompatible(true).setHighRankAccuracy(false);
    ReqSketch sk = bldr.build();
    sk.setCriterion(GT);
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    float v = sk.getQuantile(1.0);
    assertEquals(v, 100f);
  }

  @Test
  public void checkComplementCriteria() {
    int k = 24;
    boolean up = false;
    boolean hra = true;
    int min = 1;
    int max = 100;
    ReqSketch sk = reqSketchTest.loadSketch( k,   min, max,  up,  hra, LE, 0);
    sk.setCompatible(false);

    for (float v = 0.5f; v <= max + 0.5f; v += 0.5f) {
      //float v = 0.5f;
      double rle = sk.setCriterion(LE).getRank(v);
      sk.setLessThanOrEqual(true);
      float qrle = sk.getQuantile(rle);
      double rgt = sk.setCriterion(GT).getRank(v);
      float qrgt = sk.getQuantile(rgt);
      myAssertEquals(rle, rgt);
      myAssertEquals(qrle, qrgt);
      sk.setCriterion(LT);
      double rlt = sk.setCriterion(LT).getRank(v);
      float qrlt = sk.getQuantile(rlt);
      double rge = sk.setCriterion(GE).getRank(v);
      float qrge = sk.getQuantile(rge);
      myAssertEquals(rlt, rge);
      myAssertEquals(qrlt, qrge);
    }

    float v1 = sk.getQuantile(1.0);
    assertEquals(v1, (float)max);

    sk.reset();
    assertTrue(sk.isEmpty());
    assertEquals(sk.getK(), k);
    assertTrue(sk.getHighRankAccuracy());
    assertFalse(sk.isLessThanOrEqual());
    assertFalse(sk.isCompatible());
  }

  @Test
  public void checkGetRankUBLB() {
    checkGetRank(true, LT);
    checkGetRank(false, LE);
    checkGetRank(true, GE);
    checkGetRank(false, GT);
  }

  private void checkGetRank(boolean hra, Criteria criterion) {
    int k = 12;
    boolean up = true;
    int min = 1;
    int max = 1000;
    int skDebug = 0;
    ReqSketch sk = reqSketchTest.loadSketch(k, min, max, up, hra, criterion, skDebug);
    double rLB = sk.getRankLowerBound(0.5, 1);
    assertTrue(rLB > 0);
    if (hra) { rLB = sk.getRankLowerBound(995.0/1000, 1); }
    else { rLB = sk.getRankLowerBound(5.0/1000, 1); }
    assertTrue(rLB > 0);
    double rUB = sk.getRankUpperBound(0.5, 1);
    assertTrue(rUB > 0);
    if (hra) { rUB = sk.getRankUpperBound(995.0/1000, 1); }
    else { rUB = sk.getRankUpperBound(5.0/1000, 1); }
    assertTrue(rUB > 0);
    double[] ranks = sk.getRanks(new float[] {5f, 100f});

  }

  private static void myAssertEquals(double v1, double v2) {
    if (Double.isNaN(v1) && Double.isNaN(v2)) { assert true; }
    else if (v1 == v2) { assert true; }
    else { assert false; }
  }

  private static void myAssertEquals(float v1, float v2) {
    if (Float.isNaN(v1) && Float.isNaN(v2)) { assert true; }
    else if (v1 == v2) { assert true; }
    else { assert false; }
  }

}
