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

import static org.apache.datasketches.QuantilesHelper.getEvenlySpacedRanks;
import static org.apache.datasketches.req.FloatBuffer.LS;
import static org.testng.Assert.assertTrue;

//import static org.apache.datasketches.req.FloatBuffer.TAB;
import org.apache.datasketches.req.ReqAuxiliary.Row;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings({"javadoc", "unused"})
public class ReqSketchTest {

  @Test
  public void test1() {
    int min = 1;
    int max = 200;
    boolean up = true;
    int k = 6;
    boolean lteq = false;
    boolean debug = false;
    ReqSketch sk = loadSketch(min, max, up, k, lteq, debug);

    checkToString(sk, debug);
    checkGetRank(sk, min, max, debug);
    checkAux(sk, debug);
    checkGetQuantiles(sk, debug);
    checkGetCDF(sk, debug);
    checkGetPMF(sk, debug);
  }

  private static void checkToString(ReqSketch sk, boolean debug) {
    if (debug) { println("ToString() Test"); }
    String str = sk.toString("%4.0f", true);
    if (debug) { print(str); } //print dataDetail
  }

  private static void checkGetRank(ReqSketch sk, int min, int max, boolean debug) {
    if (debug) { println(LS + "GetRank Test:"); }
    double[] nranks = getEvenlySpacedRanks(11);
    String dfmt = "%10.2f%10.6f" + LS;
    String sfmt = "%10s%10s" + LS;
    if (debug) { printf(sfmt, "Value", "Rank"); }
    float slope = (max - min) + min;
    float va = 0;
    float ranka = 0;
    for (int i = 0; i < nranks.length; i++) {
      float nrank = (float) nranks[i];
      float v = slope * nrank;
      float rank = sk.getRank(v);
      if (debug) { printf(dfmt, v, rank); }
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
  }

  private static void checkAux(ReqSketch sk, boolean debug) {
    if (debug) { println(LS + "Aux Test"); }
    ReqAuxiliary aux = new ReqAuxiliary(sk);
    String dfmt = "%12.1f%12d%12.5f" + LS;
    String sfmt = "%12s%12s%12s" + LS;
    if (debug) { printf(sfmt, "Item", "Weight", "NormRank"); }
    final int totalCount = sk.getRetainedEntries();
    float item = 0;
    float normRank = 0;
    for (int i = 0; i < totalCount; i++) {
      Row row = aux.getRow(i);
      if (debug) { printf(dfmt, row.item, row.weight, row.normRank); }
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

  private static void checkGetQuantiles(ReqSketch sk, boolean debug) {
    if (debug) { println(LS + "GetQuantiles() Test"); }
    float[] rArr = {0, .1F, .2F, .3F, .4F, .5F, .6F, .7F, .8F, .9F, 1.0F};
    float[] qOut = sk.getQuantiles(rArr);
    if (debug) {
      for (int i = 0; i < qOut.length; i++) {
        println("nRank: " + rArr[i] + ", q: " + qOut[i]);
      }
    }
  }

  private static void checkGetCDF(ReqSketch sk, boolean debug) {
    if (debug) { println(LS + "GetCDF() Test"); }
    float[] spArr = { 20, 40, 60, 80, 100, 120, 140, 160, 180 };
    double[] cdf = sk.getCDF(spArr);
    if (debug) {
      for (int i = 0; i < cdf.length; i++) {
        float sp = (i == spArr.length) ? sk.getMax() : spArr[i];
        println("SP: " +sp + ", Den: " + cdf[i]);
      }
    }
  }

  private static void checkGetPMF(ReqSketch sk, boolean debug) {
    if (debug) { println(LS + "GetPMF() Test"); }
    float[] spArr = { 20, 40, 60, 80, 100, 120, 140, 160, 180 };
    double[] pmf = sk.getPMF(spArr);
    if (debug) {
      for (int i = 0; i < pmf.length; i++) {
        float sp = (i == spArr.length) ? sk.getMax() : spArr[i];
        println("SP: " +sp + ", Mass: " + pmf[i]);
      }
    }
  }

  private static ReqSketch loadSketch(int min, int max, boolean up, int k, boolean lteq, boolean debug) {
    ReqSketch sk = new ReqSketch(k, lteq, debug);
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


  private static void printRank(ReqSketch sk, float v) {
    double r = sk.getRank(v);
    String rstr = String.format("%.2f", r);
    String vstr = String.format("%.2f", v);
    println("Value: " + vstr + ", Rank: " + rstr);
  }


  static final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
  }

  static final void print(final Object o) { System.out.print(o.toString()); }

  static final void println(final Object o) { System.out.println(o.toString()); }


}
