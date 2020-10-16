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

import static org.apache.datasketches.Criteria.LE;
import static org.apache.datasketches.Criteria.LT;
import static org.apache.datasketches.Util.evenlySpacedFloats;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.Criteria;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
//import static org.apache.datasketches.req.FloatBuffer.TAB;
import org.apache.datasketches.req.ReqAuxiliary.Row;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings({"javadoc", "unused"})
public class ReqSketchTest {
  private static final String LS = System.getProperty("line.separator");
  //To control debug printing:
  private int skDebug = 0; // sketch debug printing: 0 = none, 1 = summary, 2 = extensive detail
  private int iDebug = 0; // debug printing for individual tests below, same scale as above

  @Test
  public void bigTest() {
    //          k, min, max,    up,   hra,   lteq, skDebug
    bigTestImpl(6, 1,   200,  true,  true,   LE, skDebug);
    bigTestImpl(6, 1,   200, false,  false,  LE, skDebug);
    bigTestImpl(6, 1,   200, false,   true,  LT, skDebug);
    bigTestImpl(6, 1,   200,  true,  false,  LE, skDebug);
  }

  public void bigTestImpl(int k, int min, int max, boolean up, boolean hra,
      Criteria criterion, int skDebug) {
    if (iDebug > 0) {
      println(LS + "*************************");
      println("k=" + k + " min=" + min + " max=" + max
          + " up=" + up + " hra=" + hra + " criterion=" + criterion + LS);
    }
    ReqSketch sk = loadSketch(k, min, max, up, hra, criterion, skDebug);
    checkToString(sk, iDebug);
    checkAux(sk, iDebug);
    checkGetRank(sk, min, max, iDebug);
    checkGetRanks(sk, max, iDebug);
    checkGetQuantiles(sk, iDebug);
    checkGetCDF(sk, iDebug);
    checkGetPMF(sk, iDebug);
    checkIterator(sk, iDebug);
    checkMerge(sk, iDebug);
    printBoundary(skDebug);
  }

  //Common loadSketch
  public ReqSketch loadSketch(int k, int min, int max, boolean up, boolean hra,
      Criteria criterion, int skDebug) {
    ReqSketch sk = new ReqSketch(k, hra);
    sk.setReqDebug(new ReqDebugImpl(skDebug, "%5.0f"));
    sk.setCriterion(criterion);
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

  private static void printBoundary(int iDebug) {
    if (iDebug > 0) {
      println("===========================================================");
    }
  }

  private static void checkToString(ReqSketch sk, int iDebug) {
    boolean summary = iDebug == 1;
    boolean allData = iDebug == 2;
    String brief = sk.toString();
    String all = sk.viewCompactorDetail("%4.0f", true);
    if (summary) {
      println(brief);
      println(sk.viewCompactorDetail("%4.0f", false));
    }
    if (allData) {
      println(brief);
      println(all);
    }
  }

  private static void checkGetRank(ReqSketch sk, int min, int max, int iDebug) {
    if (iDebug > 0) { println("GetRank Test:"); }
    float[] spArr = evenlySpacedFloats(0, max, 11);
    String dfmt = "%10.2f%10.6f" + LS;
    String sfmt = "%10s%10s" + LS;
    if (iDebug > 0) { printf(sfmt, "Value", "Rank"); }
    double slope = max - min + min;
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

  private static void checkGetRanks(ReqSketch sk, int max, int iDebug) {
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

  private static void checkAux(ReqSketch sk, int iDebug) {
    ReqAuxiliary aux = new ReqAuxiliary(sk);
    if (iDebug > 0) { println(aux.toString(3,12)); }

    final int totalCount = sk.getRetainedItems();
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

  private static void checkGetQuantiles(ReqSketch sk, int iDebug) {
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

  private static void checkGetCDF(ReqSketch sk, int iDebug) {
    if (iDebug > 0) { println("GetCDF() Test"); }
    float[] spArr = { 20, 40, 60, 80, 100, 120, 140, 160, 180 };
    double[] cdf = sk.getCDF(spArr);
    if (iDebug > 0) {
      for (int i = 0; i < cdf.length; i++) {
        float sp = i == spArr.length ? sk.getMaxValue() : spArr[i];
        println("SP: " +sp + ", Den: " + cdf[i]);
      }
    }
    if (iDebug > 0) { println(""); }
  }

  private static void checkGetPMF(ReqSketch sk, int iDebug) {
    if (iDebug > 0) { println("GetPMF() Test"); }
    float[] spArr = { 20, 40, 60, 80, 100, 120, 140, 160, 180 };
    double[] pmf = sk.getPMF(spArr);
    if (iDebug > 0) {
      for (int i = 0; i < pmf.length; i++) {
        float sp = i == spArr.length ? sk.getMaxValue() : spArr[i];
        println("SP: " +sp + ", Mass: " + pmf[i]);
      }
    }
    if (iDebug > 0) { println(""); }
  }

  private static void checkIterator(ReqSketch sk, int iDebug) {
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

  private static void checkMerge(ReqSketch sk, int iDebug) {
    boolean allData = iDebug > 1;
    boolean summary = iDebug > 0;
    if (summary) {
      println("Merge Test");
      println("Before Merge:");
      outputCompactorDetail(sk, "%5.0f", allData, "Host Sketch:");
    }
    ReqSketch sk2 = new ReqSketch(sk); //copy ctr
    if (summary) {
      outputCompactorDetail(sk2, "%5.0f", allData, "Incoming Sketch:");
      println("Merge Process:");
    }
    sk.merge(sk2);
    assertEquals(sk.getN(), 400);
  }

  @Test
  public void checkValidateSplits() {
    float[] arr = {1,2,3,4,5};
    ReqSketch.validateSplits(arr);
    try {
      float[] arr1 = {1,2,4,3,5};
      ReqSketch.validateSplits(arr1);
      fail();
    }
    catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkSerDe() {
    int k = 12;
    int exact = 2 * 3 * k - 1;
    checkSerDeImpl(12, false, exact);
    checkSerDeImpl(12, true, exact);
    checkSerDeImpl(12, false, 2 * exact); //more than one compactor
    checkSerDeImpl(12, true, 2 * exact);
  }

  private static void checkSerDeImpl(int k, boolean hra, int count) {
    ReqSketch sk1 = new ReqSketch(k, hra);
    for (int i = 1; i <= count; i++) {
      sk1.update(i);
    }
    byte[] sk1Arr = sk1.toByteArray();
    Memory mem = Memory.wrap(sk1Arr);
    ReqSketch sk2 = ReqSketch.heapify(mem);
  }

  private static void outputCompactorDetail(ReqSketch sk, String fmt, boolean allData, String text) {
    println(text);
    println(sk.viewCompactorDetail(fmt, allData));
  }

  private static final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
  }

  private static final void print(final Object o) {
    System.out.print(o.toString());
  }

  private static final void println(final Object o) {
    System.out.println(o.toString());
  }

}
