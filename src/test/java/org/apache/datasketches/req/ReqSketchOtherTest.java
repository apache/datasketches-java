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
import static org.testng.Assert.assertNull;
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
    final ReqSketch sk = ReqSketch.builder().build();
    assertEquals(sk.getK(), 12);
  }

  @Test
  public void checkCopyConstructors() {
    final Criteria criterion = LE;
    final ReqSketch sk = reqSketchTest.loadSketch( 6,   1, 50,  true,  true,  criterion, 0);
    final long n = sk.getN();
    final float min = sk.getMinValue();
    final float max = sk.getMaxValue();
    final ReqSketch sk2 = new ReqSketch(sk);
    assertEquals(sk2.getMinValue(), min);
    assertEquals(sk2.getMaxValue(), max);
  }

  @Test
  public void checkNonFinitePMF_CDF() {
    final ReqSketch sk = ReqSketch.builder().build();
    sk.update(1);
    try { //splitpoint values must be finite
      sk.getCDF(new float[] { Float.NaN });
      fail();
    }
    catch (final SketchesArgumentException e) {}
  }

  @Test
  public void checkQuantilesExceedLimits() {
    final Criteria criterion = LE;
    final ReqSketch sk = reqSketchTest.loadSketch( 6,   1, 200,  true,  true,  criterion, 0);
    try { sk.getQuantile(2.0f); fail(); } catch (final SketchesArgumentException e) {}
    try { sk.getQuantile(-2.0f); fail(); } catch (final SketchesArgumentException e) {}
  }

  @Test
  public void checkEstimationMode() {
    final boolean up = true;
    final boolean hra = true;
    final Criteria criterion = LE;
    final ReqSketch sk = reqSketchTest.loadSketch( 20,   1, 119,  up,  hra,  criterion, 0);
    assertEquals(sk.isEstimationMode(), false);
    final double lb = sk.getRankLowerBound(1.0, 1);
    final double ub = sk.getRankUpperBound(1.0, 1);
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
    final float v = sk.getQuantile(1.0);
    assertEquals(v, 120.0f);
    final ReqAuxiliary aux = sk.getAux();
    assertNotNull(aux);
    assertTrue(sk.getRSE(sk.getK(), .5, false, 120) > 0);
    assertTrue(sk.getSerializationBytes() > 0);
  }

  @Test
  public void checkNaNUpdate() {
    final Criteria criterion = LE;
    final ReqSketch sk = ReqSketch.builder().build();
    sk.update(Float.NaN);
    assertTrue(sk.isEmpty());
  }

  @Test
  public void checkNonFiniteGetRank() {
    final ReqSketch sk = ReqSketch.builder().build();
    sk.update(1);
    try { sk.getRank(Float.POSITIVE_INFINITY); fail(); } catch (final AssertionError e) {}
  }

  @Test
  public void checkFlags() {
    final ReqSketch sk = ReqSketch.builder().build();
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
  public void moreMergeTests() {
    final ReqSketch sk1 = ReqSketch.builder().build();
    final ReqSketch sk2 = ReqSketch.builder().build();
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
    final ReqSketchBuilder bldr = ReqSketch.builder();
    bldr.setK(6).setCompatible(true).setHighRankAccuracy(false);
    final ReqSketch sk = bldr.build();
    sk.setCriterion(GT);
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    final float v = sk.getQuantile(1.0);
    assertEquals(v, 100f);
  }

  @Test
  public void checkComplementCriteria() {
    final int k = 24;
    final boolean up = false;
    final boolean hra = true;
    final int min = 1;
    final int max = 100;
    final ReqSketch sk = reqSketchTest.loadSketch( k, min, max,  up,  hra, LE, 0);
    sk.setCompatible(false);

    for (float v = 0.5f; v <= max + 0.5f; v += 0.5f) {
      //float v = 0.5f;
      final double rle = sk.setCriterion(LE).getRank(v);
      sk.setLessThanOrEqual(true);
      final float qrle = sk.getQuantile(rle);
      final double rgt = sk.setCriterion(GT).getRank(v);
      final float qrgt = sk.getQuantile(rgt);
      myAssertEquals(rle, rgt);
      myAssertEquals(qrle, qrgt);
      sk.setCriterion(LT);
      final double rlt = sk.setCriterion(LT).getRank(v);
      final float qrlt = sk.getQuantile(rlt);
      final double rge = sk.setCriterion(GE).getRank(v);
      final float qrge = sk.getQuantile(rge);
      myAssertEquals(rlt, rge);
      myAssertEquals(qrlt, qrge);
    }

    final float v1 = sk.getQuantile(1.0);
    assertEquals(v1, (float)max);

    sk.reset();
    assertTrue(sk.isEmpty());
    assertEquals(sk.getK(), k);
    assertTrue(sk.getHighRankAccuracy());
    assertFalse(sk.isLessThanOrEqual());
    assertFalse(sk.isCompatible());
  }

  @Test
  public void simpleTest() {
    ReqSketch sk;
    final ReqSketchBuilder bldr = ReqSketch.builder();
    bldr.setK(50).setHighRankAccuracy(false);
    bldr.setCompatible(false);
    bldr.setReqDebug(null);
    sk = bldr.build();
    final float[] vArr = { 5, 5, 5, 6, 6, 6, 7, 8, 8, 8 };
    for (int i = 0; i < vArr.length; i++) { sk.update(vArr[i]); }
    sk.setCriterion(Criteria.LT);
    final double[] rArrLT = {0.0, 0.0, 0.0, 0.3, 0.3, 0.3, 0.6, 0.7, 0.7, 0.7};
    for (int i = 0; i < vArr.length; i++) {
      assertEquals(sk.getRank(vArr[i]), rArrLT[i]);
      //System.out.println("v:" + vArr[i] + " r:" + sk.getRank(vArr[i]));
    }
    sk.setCriterion(Criteria.LE);
    final double[] rArrLE = {0.3, 0.3, 0.3, 0.6, 0.6, 0.6, 0.7, 1.0, 1.0, 1.0};
    for (int i = 0; i < vArr.length; i++) {
      assertEquals(sk.getRank(vArr[i]), rArrLE[i]);
      //System.out.println("v:" + vArr[i] + " r:" + sk.getRank(vArr[i]));
    }
  }

  @Test
  public void checkGetRankUBLB() {
    checkGetRank(true, LT);
    checkGetRank(false, LE);
    checkGetRank(true, GE);
    checkGetRank(false, GT);
  }

  @Test
  public void checkEmpty() {
    final ReqSketchBuilder bldr = new ReqSketchBuilder();
    bldr.setCompatible(false);
    bldr.setLessThanOrEqual(false);
    final ReqSketch sk = new ReqSketchBuilder().build();
    assertEquals(sk.getRank(1f), Double.NaN);
    assertNull(sk.getRanks(new float[] { 1f }));
    assertEquals(sk.getQuantile(0.5), Float.NaN);
    assertNull(sk.getQuantiles(new double[] {0.5}));
    assertNull(sk.getPMF(new float[] { 1f }));
    assertNull(sk.getCDF(new float[] { 1f }));
    assertTrue(sk.getRSE(50, 0.5, true, 0) > 0);
    assertTrue(sk.getRankUpperBound(0.5, 1) > 0);
  }

  private void checkGetRank(final boolean hra, final Criteria criterion) {
    final int k = 12;
    final boolean up = true;
    final int min = 1;
    final int max = 1000;
    final int skDebug = 0;
    final ReqSketch sk = reqSketchTest.loadSketch(k, min, max, up, hra, criterion, skDebug);
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
    final double[] ranks = sk.getRanks(new float[] {5f, 100f});
  }

  private static void myAssertEquals(final double v1, final double v2) {
    if (Double.isNaN(v1) && Double.isNaN(v2)) { assert true; }
    else if (v1 == v2) { assert true; }
    else { assert false; }
  }

  private static void myAssertEquals(final float v1, final float v2) {
    if (Float.isNaN(v1) && Float.isNaN(v2)) { assert true; }
    else if (v1 == v2) { assert true; }
    else { assert false; }
  }

}
