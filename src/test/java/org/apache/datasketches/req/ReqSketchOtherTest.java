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

import static org.apache.datasketches.req.Criteria.GE;
import static org.apache.datasketches.req.Criteria.GT;
import static org.apache.datasketches.req.Criteria.LE;
import static org.apache.datasketches.req.Criteria.LT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

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
    ReqSketch sk = new ReqSketch();
    assertEquals(sk.getK(), 50);
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
    ReqSketch sk = new ReqSketch();
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
    //    double lb = sk.getRankLowerBound(1.0, 1);
    //    double ub = sk.getRankUpperBound(1.0, 1);
    //    assertEquals(lb, 1.0);
    //    assertEquals(ub, 1.0);
    int maxNomSize = sk.getMaxNomSize();
    assertEquals(maxNomSize, 120);
    sk.update(120);
    assertEquals(sk.isEstimationMode(), true);
    //    lb = sk.getRankLowerBound(0, 1);
    //    ub = sk.getRankUpperBound(1.0, 1);
    //    assertEquals(lb, 0.0);
    //    assertEquals(ub, 2.0);
    maxNomSize = sk.getMaxNomSize();
    assertEquals(maxNomSize, 240);
    float v = sk.getQuantile(1.0);
    assertEquals(v, 120.0f);
    ReqAuxiliary aux = sk.getAux();
    assertNotNull(aux);
  }

  @Test
  public void checkNonFiniteUpdate() {
    Criteria criterion = LE;
    ReqSketch sk = reqSketchTest.loadSketch( 6,   1, 35,  true,  false,  criterion, 0);
    try { sk.update(Float.POSITIVE_INFINITY); fail(); } catch (SketchesArgumentException e) {}
  }

  @Test
  public void checkNonFinateGetRank() {
    ReqSketch sk = new ReqSketch();
    sk.update(1);
    try { sk.getRank(Float.POSITIVE_INFINITY); fail(); } catch (AssertionError e) {}
  }

  @Test
  public void checkFlags() {
    ReqSketch sk = new ReqSketch();
    sk.setCriterion(Criteria.LE);
    assertEquals(sk.getCriterion(), Criteria.LE);
    sk.setCriterion(Criteria.LT);
    assertEquals(sk.getCriterion(), Criteria.LT);
  }




}
