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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
//import static org.apache.datasketches.req.FloatBuffer.TAB;
import org.apache.datasketches.req.ReqAuxiliary.Row;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class ReqSketchTest {
  private static final String LS = System.getProperty("line.separator");

  //To control debug printing:
  private final static int skDebug = 0; // sketch debug printing: 0 = none, 1 = summary, 2 = extensive detail
  private final static int iDebug = 0;  // debug printing for individual tests below, same scale as above

  @Test
  public void bigTest() {
    //          k, min, max,    up,   hra,   lteq, skDebug
    bigTestImpl(6, 1,   200,  true,  true,   true, skDebug);
    bigTestImpl(6, 1,   200, false,  false,  true, skDebug);
    bigTestImpl(6, 1,   200, false,   true,  false, skDebug);
    bigTestImpl(6, 1,   200,  true,  false,  true, skDebug);
  }

  public void bigTestImpl(final int k, final int min, final int max, final boolean up, final boolean hra,
      final boolean ltEq, final int skDebug) {
    if (iDebug > 0) {
      println(LS + "*************************");
      println("k=" + k + " min=" + min + " max=" + max
          + " up=" + up + " hra=" + hra + " criterion=" + ltEq + LS);
    }
    final ReqSketch sk = loadSketch(k, min, max, up, hra, ltEq, skDebug);
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
    sk.reset();
  }

  //Common loadSketch
  public ReqSketch loadSketch(final int k, final int min, final int max, final boolean up,
      final boolean hra, final boolean ltEq, final int skDebug) {
    final ReqSketchBuilder bldr = ReqSketch.builder();
    bldr.setReqDebug(new ReqDebugImpl(skDebug, "%5.0f"));
    bldr.setK(k);
    bldr.setHighRankAccuracy(hra);
    final ReqSketch sk = bldr.build();
    sk.setLessThanOrEqual(ltEq);
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

  private static void printBoundary(final int iDebug) {
    if (iDebug > 0) {
      println("===========================================================");
    }
  }

  private static void checkToString(final ReqSketch sk, final int iDebug) {
    final boolean summary = iDebug == 1;
    final boolean allData = iDebug == 2;
    final String brief = sk.toString();
    final String all = sk.viewCompactorDetail("%4.0f", true);
    if (summary) {
      println(brief);
      println(sk.viewCompactorDetail("%4.0f", false));
    }
    if (allData) {
      println(brief);
      println(all);
    }
  }

  private static void checkGetRank(final ReqSketch sk, final int min, final int max, final int iDebug) {
    if (iDebug > 0) { println("GetRank Test:"); }
    final float[] spArr = evenlySpacedFloats(0, max, 11);
    final String dfmt = "%10.2f%10.6f" + LS;
    final String sfmt = "%10s%10s" + LS;
    if (iDebug > 0) { printf(sfmt, "Value", "Rank"); }
    final double slope = max - min + min;
    float va = 0;
    double ranka = 0;
    for (int i = 0; i < spArr.length; i++) {
      final float v = spArr[i];
      final double rank = sk.getRank(v);
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

  private static void checkGetRanks(final ReqSketch sk, final int max, final int iDebug) {
    if (iDebug > 0) { println("GetRanks Test:"); }
    final float[] sp = evenlySpacedFloats(0, max, 11);
    final String dfmt = "%10.2f%10.6f" + LS;
    final String sfmt = "%10s%10s" + LS;
    if (iDebug > 0) { printf(sfmt, "Value", "Rank"); }
    final double[] nRanks = sk.getRanks(sp);
    for (int i = 0; i < nRanks.length; i++) {
      if (iDebug > 0) { printf(dfmt, sp[i], nRanks[i]); }
    }
    if (iDebug > 0) { println(""); }
  }

  private static void checkAux(final ReqSketch sk, final int iDebug) {
    final ReqAuxiliary aux = new ReqAuxiliary(sk);
    if (iDebug > 0) { println(aux.toString(3,12)); }

    final int totalCount = sk.getRetainedItems();
    float item = 0;
    long wt = 0;
    for (int i = 0; i < totalCount; i++) {
      final Row row = aux.getRow(i);
      if (i == 0) {
        item = row.item;
        wt = row.weight;
      } else {
        assertTrue(row.item >= item);
        assertTrue(row.weight >= wt);
        item = row.item;
        wt = row.weight;
      }
    }
  }

  private static void checkGetQuantiles(final ReqSketch sk, final int iDebug) {
    if (iDebug > 0) { println("GetQuantiles() Test"); }
    final double[] rArr = {0, .1F, .2F, .3F, .4F, .5F, .6F, .7F, .8F, .9F, 1.0F};
    final float[] qOut = sk.getQuantiles(rArr);
    if (iDebug > 0) {
      for (int i = 0; i < qOut.length; i++) {
        final String r = String.format("%6.3f", rArr[i]);
        println("nRank: " + r + ", q: " + qOut[i]);
      }
    }
    if (iDebug > 0) { println(""); }
  }

  private static void checkGetCDF(final ReqSketch sk, final int iDebug) {
    if (iDebug > 0) { println("GetCDF() Test"); }
    final float[] spArr = { 20, 40, 60, 80, 100, 120, 140, 160, 180 };
    final double[] cdf = sk.getCDF(spArr);
    if (iDebug > 0) {
      for (int i = 0; i < cdf.length; i++) {
        final float sp = i == spArr.length ? sk.getMaxValue() : spArr[i];
        println("SP: " +sp + ", Den: " + cdf[i]);
      }
    }
    if (iDebug > 0) { println(""); }
  }

  private static void checkGetPMF(final ReqSketch sk, final int iDebug) {
    if (iDebug > 0) { println("GetPMF() Test"); }
    final float[] spArr = { 20, 40, 60, 80, 100, 120, 140, 160, 180 };
    final double[] pmf = sk.getPMF(spArr);
    if (iDebug > 0) {
      for (int i = 0; i < pmf.length; i++) {
        final float sp = i == spArr.length ? sk.getMaxValue() : spArr[i];
        println("SP: " +sp + ", Mass: " + pmf[i]);
      }
    }
    if (iDebug > 0) { println(""); }
  }

  private static void checkIterator(final ReqSketch sk, final int iDebug) {
    if (iDebug > 0) { println("iterator() Test"); }
    final ReqIterator itr = sk.iterator();
    while (itr.next()) {
      final float v = itr.getValue();
      final long wt = itr.getWeight();
      final int cnt = itr.getCount();
      if (iDebug > 0) { println("count=" + cnt +" v=" + v + " wt=" +wt); }
    }
    if (iDebug > 0) { println(""); }
  }

  private static void checkMerge(final ReqSketch sk, final int iDebug) {
    final boolean allData = iDebug > 1;
    final boolean summary = iDebug > 0;
    if (summary) {
      println("Merge Test");
      println("Before Merge:");
      outputCompactorDetail(sk, "%5.0f", allData, "Host Sketch:");
    }
    final ReqSketch sk2 = new ReqSketch(sk); //copy ctr
    if (summary) {
      outputCompactorDetail(sk2, "%5.0f", allData, "Incoming Sketch:");
      println("Merge Process:");
    }
    sk.merge(sk2);
    assertEquals(sk.getN(), 400);
  }

  @Test
  public void merge() {
    final ReqSketch s1 = ReqSketch.builder().setK(12).build();
    for (int i = 0; i < 40; i++) {
      s1.update(i);
    }
    final ReqSketch s2 = ReqSketch.builder().setK(12).build();
    for (int i = 0; i < 40; i++) {
      s2.update(i);
    }
    final ReqSketch s3 = ReqSketch.builder().setK(12).build();
    for (int i = 0; i < 40; i++) {
      s3.update(i);
    }
    final ReqSketch s = ReqSketch.builder().setK(12).build();
    s.merge(s1);
    s.merge(s2);
    s.merge(s3);
  }

  @Test
  public void checkValidateSplits() {
    final float[] arr = {1,2,3,4,5};
    ReqSketch.validateSplits(arr);
    try {
      final float[] arr1 = {1,2,4,3,5};
      ReqSketch.validateSplits(arr1);
      fail();
    }
    catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkSerDe() {
    final int k = 12;
    final int exact = 2 * 3 * k - 1;
    checkSerDeImpl(12, false, 0);
    checkSerDeImpl(12, true, 0);
    checkSerDeImpl(12, false, 4);
    checkSerDeImpl(12, true, 4);
    checkSerDeImpl(12, false, exact);
    checkSerDeImpl(12, true, exact);
    checkSerDeImpl(12, false, 2 * exact); //more than one compactor
    checkSerDeImpl(12, true, 2 * exact);
  }

  private static void checkSerDeImpl(final int k, final boolean hra, final int count) {
    final ReqSketch sk1 = ReqSketch.builder().setK(k).setHighRankAccuracy(hra).build();
    for (int i = 1; i <= count; i++) {
      sk1.update(i);
    }
    final byte[] sk1Arr = sk1.toByteArray();
    final Memory mem = Memory.wrap(sk1Arr);
    final ReqSketch sk2 = ReqSketch.heapify(mem);
    assertEquals(sk2.getRetainedItems(), sk1.getRetainedItems());
    assertEquals(sk2.getMinValue(), sk1.getMinValue());
    assertEquals(sk2.getMaxValue(), sk1.getMaxValue());
    assertEquals(sk2.getN(), sk1.getN());
    assertEquals(sk2.getHighRankAccuracy(),sk1.getHighRankAccuracy());
    assertEquals(sk2.getK(), sk1.getK());
    assertEquals(sk2.getMaxNomSize(), sk1.getMaxNomSize());
    assertEquals(sk2.getLtEq(), sk1.getLtEq());
    assertEquals(sk2.getNumLevels(), sk1.getNumLevels());
    assertEquals(sk2.getSerializationBytes(), sk1.getSerializationBytes());
  }

  @Test
  public void checkK() {
    try {
      final ReqSketch sk1 = ReqSketch.builder().setK(1).build();
      fail();
    } catch (final SketchesArgumentException e) {}
  }

  @Test
  public void checkAuxDeDup() {
    final ReqSketchBuilder bldr = ReqSketch.builder();
    bldr.setK(8);
    bldr.setLessThanOrEqual(false);
    bldr.setHighRankAccuracy(true);
    final ReqSketch sk1 = ReqSketch.builder().setK(8).build();
    final float[] arr = {1, 2, 2, 3, 3, 4, 4};
    for (final float i : arr) {sk1.update(i); }
    float q = sk1.getQuantile(.5);
    assertEquals(q, 3.0f);
    q = sk1.getQuantile(1.0);
    assertEquals(q, 4.0f);
  }

  private static void outputCompactorDetail(final ReqSketch sk, final String fmt, final boolean allData, final String text) {
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
