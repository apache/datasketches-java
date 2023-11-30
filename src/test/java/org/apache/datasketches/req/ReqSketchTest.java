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

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.evenlySpacedDoubles;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantilescommon.FloatsSortedView;
import org.apache.datasketches.quantilescommon.FloatsSortedViewIterator;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesFloatsSketchIterator;
import org.apache.datasketches.quantilescommon.QuantilesUtil;
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
  public void bigTest() { //ALL IN EXACT MODE
    //          k, min, max,    up,   hra,        crit, skDebug
    bigTestImpl(20, 1,   100,  true,  true,   INCLUSIVE, skDebug);
    bigTestImpl(20, 1,   100, false,  false,  INCLUSIVE, skDebug);
    bigTestImpl(20, 1,   100, false,   true,  EXCLUSIVE, skDebug);
    bigTestImpl(20, 1,   100,  true,  false,  INCLUSIVE, skDebug);
  }

  public void bigTestImpl(final int k, final int min, final int max, final boolean up, final boolean hra,
      final QuantileSearchCriteria crit, final int skDebug) {
    if (iDebug > 0) {
      println(LS + "*************************");
      println("k=" + k + " min=" + min + " max=" + max
          + " up=" + up + " hra=" + hra + " criterion=" + crit + LS);
    }
    final ReqSketch sk = loadSketch(k, min, max, up, hra, skDebug);
    final FloatsSortedView sv = sk.getSortedView();
    checkToString(sk, iDebug);
    checkSortedView(sk, iDebug);
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
      final boolean hra, final int skDebug) {
    final ReqSketchBuilder bldr = ReqSketch.builder();
    bldr.setReqDebug(new ReqDebugImplTest(skDebug, "%5.0f"));
    bldr.setK(k);
    bldr.setHighRankAccuracy(hra);
    final ReqSketch sk = bldr.build();
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
    if (iDebug > 0) { println("GetRank Test: INCLUSIVE"); }
    final float[] spArr = QuantilesUtil.evenlySpacedFloats(0, max, 11);
    final double[] trueRanks = evenlySpacedDoubles(0, 1.0, 11);
    final String dfmt = "%10.2f%10.6f" + LS;
    final String sfmt = "%10s%10s" + LS;
    if (iDebug > 0) { printf(sfmt, "Value", "Rank"); }
    float va = 0;
    double ranka = 0;
    for (int i = 0; i < spArr.length; i++) {
      final float v = spArr[i];
      final double trueRank = trueRanks[i];
      final double rank = sk.getRank(v);
      if (iDebug > 0) { printf(dfmt, v, rank); }
      assertEquals(rank, trueRank, .01);

    }
    if (iDebug > 0) { println(""); }
  }

  private static void checkGetRanks(final ReqSketch sk, final int max, final int iDebug) {
    if (iDebug > 0) { println("GetRanks Test:"); }
    final float[] sp = QuantilesUtil.evenlySpacedFloats(0, max, 11);
    final String dfmt = "%10.2f%10.6f" + LS;
    final String sfmt = "%10s%10s" + LS;
    if (iDebug > 0) { printf(sfmt, "Value", "Rank"); }
    final double[] nRanks = sk.getRanks(sp);
    for (int i = 0; i < nRanks.length; i++) {
      if (iDebug > 0) { printf(dfmt, sp[i], nRanks[i]); }
    }
    if (iDebug > 0) { println(""); }
  }

  private static void checkSortedView(final ReqSketch sk, final int iDebug) {
    final ReqSketchSortedView sv = new ReqSketchSortedView(sk);
    final FloatsSortedViewIterator itr = sv.iterator();
    final int retainedCount = sk.getNumRetained();
    final long totalN = sk.getN();
    int count = 0;
    long cumWt = 0;
    while (itr.next()) {
      cumWt = itr.getNaturalRank(INCLUSIVE);
      count++;
    }
    assertEquals(cumWt, totalN);
    assertEquals(count, retainedCount);
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
    final float[] spArr = { 20, 40, 60, 80 };
    final double[] cdf = sk.getCDF(spArr);
    if (iDebug > 0) {
      for (int i = 0; i < cdf.length; i++) {
        final float sp = i == spArr.length ? sk.getMaxItem() : spArr[i];
        println("SP: " +sp + ", Den: " + cdf[i]);
      }
    }
    if (iDebug > 0) { println(""); }
  }

  private static void checkGetPMF(final ReqSketch sk, final int iDebug) {
    if (iDebug > 0) { println("GetPMF() Test"); }
    final float[] spArr = { 20, 40, 60, 80 };
    final double[] pmf = sk.getPMF(spArr);
    if (iDebug > 0) {
      for (int i = 0; i < pmf.length; i++) {
        final float sp = i == spArr.length ? sk.getMaxItem() : spArr[i];
        println("SP: " +sp + ", Mass: " + pmf[i]);
      }
    }
    if (iDebug > 0) { println(""); }
  }

  private static void checkIterator(final ReqSketch sk, final int iDebug) {
    if (iDebug > 0) { println("Sketch iterator() Test"); }
    final QuantilesFloatsSketchIterator itr = sk.iterator();
    while (itr.next()) {
      final float v = itr.getQuantile();
      final long wt = itr.getWeight();
      if (iDebug > 0) { println(" v=" + v + " wt=" +wt); }
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
    assertEquals(sk.getN(), 200);
  }

  //specific tests

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
    assertEquals(sk2.getNumRetained(), sk1.getNumRetained());
    assertEquals(sk1.isEmpty(), sk2.isEmpty());
    if (sk2.isEmpty()) {
      try { sk2.getMinItem(); fail(); } catch (IllegalArgumentException e) {}
      try { sk2.getMaxItem(); fail(); } catch (IllegalArgumentException e) {}
    } else {
      assertEquals(sk2.getMinItem(), sk1.getMinItem());
      assertEquals(sk2.getMaxItem(), sk1.getMaxItem());
    }
    assertEquals(sk2.getN(), sk1.getN());
    assertEquals(sk2.getHighRankAccuracyMode(),sk1.getHighRankAccuracyMode());
    assertEquals(sk2.getK(), sk1.getK());
    assertEquals(sk2.getMaxNomSize(), sk1.getMaxNomSize());
    assertEquals(sk2.getNumLevels(), sk1.getNumLevels());
    assertEquals(sk2.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
  }

  @Test
  public void checkK() {
    try {
      final ReqSketch sk1 = ReqSketch.builder().setK(1).build();
      fail();
    } catch (final SketchesArgumentException e) {}
  }

  @Test
  public void tenValues() {
    final ReqSketch sketch = ReqSketch.builder().build();
    for (int i = 1; i <= 10; i++) { sketch.update(i); }
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 10);
    assertEquals(sketch.getNumRetained(), 10);
    for (int i = 1; i <= 10; i++) {
      assertEquals(sketch.getRank(i), (i) / 10.0);
      assertEquals(sketch.getRank(i, EXCLUSIVE), (i - 1) / 10.0);
      assertEquals(sketch.getRank(i, INCLUSIVE), (i) / 10.0);
    }
    // inclusive = false
    assertEquals(sketch.getQuantile(0, EXCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.1, EXCLUSIVE), 2);
    assertEquals(sketch.getQuantile(0.2, EXCLUSIVE), 3);
    assertEquals(sketch.getQuantile(0.3, EXCLUSIVE), 4);
    assertEquals(sketch.getQuantile(0.4, EXCLUSIVE), 5);
    assertEquals(sketch.getQuantile(0.5, EXCLUSIVE), 6);
    assertEquals(sketch.getQuantile(0.6, EXCLUSIVE), 7);
    assertEquals(sketch.getQuantile(0.7, EXCLUSIVE), 8);
    assertEquals(sketch.getQuantile(0.8, EXCLUSIVE), 9);
    assertEquals(sketch.getQuantile(0.9, EXCLUSIVE), 10);
    assertEquals(sketch.getQuantile(1, EXCLUSIVE), 10.0);
    // inclusive = true
    assertEquals(sketch.getQuantile(0, INCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.1, INCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.2, INCLUSIVE), 2);
    assertEquals(sketch.getQuantile(0.3, INCLUSIVE), 3);
    assertEquals(sketch.getQuantile(0.4, INCLUSIVE), 4);
    assertEquals(sketch.getQuantile(0.5, INCLUSIVE), 5);
    assertEquals(sketch.getQuantile(0.6, INCLUSIVE), 6);
    assertEquals(sketch.getQuantile(0.7, INCLUSIVE), 7);
    assertEquals(sketch.getQuantile(0.8, INCLUSIVE), 8);
    assertEquals(sketch.getQuantile(0.9, INCLUSIVE), 9);
    assertEquals(sketch.getQuantile(1, INCLUSIVE), 10);

    // getQuantile() and getQuantiles() equivalence
    {
      // inclusive = false (default)
      final float[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1});
      for (int i = 0; i <= 10; i++) {
        assertEquals(sketch.getQuantile(i / 10.0), quantiles[i]);
      }
    }
    {
      // inclusive = true
      final float[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1}, INCLUSIVE);
      for (int i = 0; i <= 10; i++) {
        assertEquals(sketch.getQuantile(i / 10.0, INCLUSIVE), quantiles[i]);
      }
    }
  }

  private static void outputCompactorDetail(final ReqSketch sk, final String fmt, final boolean allData,
      final String text) {
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
