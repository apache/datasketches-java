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

import static org.apache.datasketches.Util.evenlySpacedFloats;
import static org.apache.datasketches.req.ReqHelper.LS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.SketchesArgumentException;
//import static org.apache.datasketches.req.FloatBuffer.TAB;
import org.apache.datasketches.req.ReqAuxiliary.Row;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings({"javadoc", "unused"})
public class ReqSketchTest {
  private int k = 6;
  private int min = 1;
  private int max = 200;
  private boolean up = true;
  private boolean hra = true;
  private boolean lteq = true;
  private ReqSketch sk;

  //To control debug printing:
  private int skDebug = 2; // sketch debug printing
  private int iDebug = 2; // debug printing for individual tests below

  @Test
  public void checkConstructors() {
    ReqSketch sk = new ReqSketch();
    assertEquals(sk.getK(), 50);
  }

  @Test
  public void checkCopyConstructors() {
    sk = loadSketch( 6,   1, 50,  true,  true,  true, 0);
    long n = sk.getN();
    float min = sk.getMin();
    float max = sk.getMax();
    ReqSketch sk2 = new ReqSketch(sk);
    assertEquals(sk2.getMin(), min);
    assertEquals(sk2.getMax(), max);
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
    sk = loadSketch( 6,   1, 200,  true,  true,  true, 0);
    try { sk.getQuantile(2.0f); fail(); } catch (SketchesArgumentException e) {}
    try { sk.getQuantile(-2.0f); fail(); } catch (SketchesArgumentException e) {}
  }

  @Test
  public void checkEstimationMode() {
    sk = loadSketch( 6,   1, 35,  true,  false,  false, 0);
    assertEquals(sk.isEstimationMode(), false);
    sk.update(36);
    assertEquals(sk.isEstimationMode(), true);
  }

  @Test
  public void checkNonFiniteUpdate() {
    sk = loadSketch( 6,   1, 35,  true,  false,  false, 0);
    try { sk.update(Float.POSITIVE_INFINITY); fail(); } catch (SketchesArgumentException e) {}
  }

  @Test
  public void checkNonFinateGetRank() {
    ReqSketch sk = new ReqSketch();
    sk.update(1);
    try { sk.getRank(Float.POSITIVE_INFINITY); fail(); } catch (AssertionError e) {}
  }


  @Test
  public void bigTest() {
    //               k, min, max,    up,   hra,  lteq, skDebug
    sk = loadSketch( 6,   1, 200,  true,  true,  true, skDebug);
    bigTestImpl(sk);
    printBoundary(skDebug);
    sk = loadSketch( 6,   1, 200, false, false,  true, skDebug);
    bigTestImpl(sk);
    printBoundary(skDebug);
    sk = loadSketch( 6,   1, 200, false,  true, false, skDebug);
    bigTestImpl(sk);
    printBoundary(skDebug);
    sk = loadSketch( 6,   1, 200,  true, false, true, skDebug);
    bigTestImpl(sk);
    printBoundary(skDebug);
  }

  public void bigTestImpl(ReqSketch sk) {
    if (iDebug > 0) {
      println(LS + "*************************");
      println("k=" + k + " min=" + min + " max=" + max
          + " up=" + up + " hra=" + hra + " lteq=" + lteq + LS);
    }
    checkToString(iDebug);
    checkAux(iDebug);
    //checkSingleRank(iDebug, 20f);
    checkGetRank(iDebug);
    checkGetRanks(iDebug);
    checkGetQuantiles(iDebug);
    checkGetCDF(iDebug);
    checkGetPMF(iDebug);
    checkIterator(iDebug);
    checkMerge(iDebug);
  }

  private static void printBoundary(int iDebug) {
    if (iDebug > 0) {
      println("===========================================================");
    }
  }

  //  left here for debugging
  //  private void checkSingleRank(int iDebug, float v) {
  //    if (iDebug > 0) { println("GetSingleRank Test:"); }
  //    String dfmt = "%10.2f%10.6f" + LS;
  //    String sfmt = "%10s%10s" + LS;
  //    if (iDebug > 0) { printf(sfmt, "Value", "Rank"); }
  //    double nRank = sk.getRank(v);
  //    if (iDebug > 0) { printf(dfmt, v, nRank); }
  //    if (iDebug > 0) { println(""); }
  //  }

  private void checkToString(int iDebug) {
    if (iDebug > 0) {
      println(sk.toString());
    }
    if (iDebug > 1) {
      println(sk.viewCompactorDetail("%4.0f"));
    }
  }

  private void checkGetRank(int iDebug) {
    if (iDebug > 0) { println("GetRank Test:"); }
    float[] spArr = evenlySpacedFloats(0, max, 11);
    String dfmt = "%10.2f%10.6f" + LS;
    String sfmt = "%10s%10s" + LS;
    if (iDebug > 0) { printf(sfmt, "Value", "Rank"); }
    double slope = (max - min) + min;
    float va = 0;
    double ranka = 0;
    for (int i = 0; i < spArr.length; i++) {
      float v = spArr[i];
      double rank = sk.getRank(v);
      if (iDebug > 0) { printf(dfmt, v, rank); }
      if (i == 0) {
        va = v;
        ranka = rank;
      } else {
        assertTrue(v >= va);
        assertTrue(rank >= ranka);
        va = v;
        ranka = rank;
      }
    }
    if (iDebug > 0) { println(""); }
  }

  private void checkGetRanks(int iDebug) {
    if (iDebug > 0) { println("GetRanks Test:"); }
    float[] sp = evenlySpacedFloats(0, max, 11);
    String dfmt = "%10.2f%10.6f" + LS;
    String sfmt = "%10s%10s" + LS;
    if (iDebug > 0) { printf(sfmt, "Value", "Rank"); }
    double[] nRanks = sk.getRanks(sp);
    for (int i = 0; i < nRanks.length; i++) {
      if (iDebug > 0) { printf(dfmt, sp[i], nRanks[i]); }
    }
    if (iDebug > 0) { println(""); }
  }

  private void checkAux(int iDebug) {
    ReqAuxiliary aux = new ReqAuxiliary(sk);
    if (iDebug > 0) { println(aux.toString(3,12)); }

    final int totalCount = sk.getRetainedEntries();
    float item = 0;
    double normRank = 0;
    for (int i = 0; i < totalCount; i++) {
      Row row = aux.getRow(i);
      if (i == 0) {
        item = row.item;
        normRank = row.normRank;
      } else {
        assertTrue(row.item >= item);
        assertTrue(row.normRank >= normRank);
        item = row.item;
        normRank = row.normRank;
      }
    }
  }

  private void checkGetQuantiles(int iDebug) {
    if (iDebug > 0) { println("GetQuantiles() Test"); }
    double[] rArr = {0, .1F, .2F, .3F, .4F, .5F, .6F, .7F, .8F, .9F, 1.0F};
    float[] qOut = sk.getQuantiles(rArr);
    if (iDebug > 0) {
      for (int i = 0; i < qOut.length; i++) {
        String r = String.format("%6.3f", rArr[i]);
        println("nRank: " + r + ", q: " + qOut[i]);
      }
    }
    if (iDebug > 0) { println(""); }
  }

  private void checkGetCDF(int iDebug) {
    if (iDebug > 0) { println("GetCDF() Test"); }
    float[] spArr = { 20, 40, 60, 80, 100, 120, 140, 160, 180 };
    double[] cdf = sk.getCDF(spArr);
    if (iDebug > 0) {
      for (int i = 0; i < cdf.length; i++) {
        float sp = (i == spArr.length) ? sk.getMax() : spArr[i];
        println("SP: " +sp + ", Den: " + cdf[i]);
      }
    }
    if (iDebug > 0) { println(""); }
  }

  private void checkGetPMF(int iDebug) {
    if (iDebug > 0) { println("GetPMF() Test"); }
    float[] spArr = { 20, 40, 60, 80, 100, 120, 140, 160, 180 };
    double[] pmf = sk.getPMF(spArr);
    if (iDebug > 0) {
      for (int i = 0; i < pmf.length; i++) {
        float sp = (i == spArr.length) ? sk.getMax() : spArr[i];
        println("SP: " +sp + ", Mass: " + pmf[i]);
      }
    }
    if (iDebug > 0) { println(""); }
  }

  private void checkIterator(int iDebug) {
    if (iDebug > 0) { println("iterator() Test"); }
    ReqIterator itr = sk.iterator();
    while (itr.next()) {
      float v = itr.getValue();
      long wt = itr.getWeight();
      int cnt = itr.getCount();
      if (iDebug > 0) { println("count=" + cnt +" v=" + v + " wt=" +wt); }
    }
    if (iDebug > 0) { println(""); }
  }

  private void checkMerge(int iDebug) {
    if (iDebug > 0) { println("Merge Test"); }
    ReqSketch sk2 = new ReqSketch(sk);
    sk.merge(sk2);
    assertEquals(sk.getN(), 400);
    if (iDebug > 1) {
      println(sk.viewCompactorDetail("%4.0f"));
    }
  }


  private ReqSketch loadSketch(int k, int min, int max, boolean up, boolean hra,
      boolean lteq, int skDebug) {
    this.k = k;
    this.min = min;
    this.max = max;
    this.up = up;
    this.hra = hra;
    this.lteq = lteq;

    ReqSketch sk = new ReqSketch(k, hra, lteq, skDebug);
    if (up) {
      for (int i = min; i <= max; i++) {
        sk.update(i);
      }
    } else { //down
      for (int i = max + 1; i-- > min; ) {
        sk.update(i);
      }
    }
    return sk;
  }

  static final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
  }

  static final void print(final Object o) { System.out.print(o.toString()); }

  static final void println(final Object o) { System.out.println(o.toString()); }

}
