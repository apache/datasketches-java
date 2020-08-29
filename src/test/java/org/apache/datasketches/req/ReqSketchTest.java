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
    ReqSketch sk = loadSketch(min, max, true, 6, true); //up, debug

    checkToString(sk);
    checkGetRank(sk, min, max);
    checkAux(sk);
    checkGetQuantiles(sk);
  }

  private static void checkToString(ReqSketch sk) {
    println("ToString() Test");
    print(sk.toString("%4.0f", true)); //print dataDetail
  }

  private static void checkGetRank(ReqSketch sk, int min, int max) {
    println(LS + "GetRank Test:");
    double[] ranks = getEvenlySpacedRanks(11);
    String dfmt = "%10.2f%10.6f" + LS;
    String sfmt = "%10s%10s";
    printf(sfmt, "Value", "Rank");
    for (int i = 0; i < ranks.length; i++) {
      float rank = (float) ranks[i];
      float scaledV = (rank * (max - min)) + min;
      double r = sk.getRank(scaledV);
      printf(dfmt, scaledV, r);
    }
  }

  private static void checkAux(ReqSketch sk) {
    println(LS + "Aux Test");
    ReqAuxiliary aux = new ReqAuxiliary(sk);
    aux.buildAuxTable(sk);
    String dfmt = "%12.1f%12d%12.5f" + LS;
    String sfmt = "%12s%12s%12s" + LS;
    printf(sfmt, "Item", "Weight", "NormRank");
    final int totalCount = sk.getRetainedEntries();
    for (int i = 0; i < totalCount; i++) {
      Row row = aux.getRow(i);
      printf(dfmt, row.item, row.weight, row.normRank);
    }
  }

  private static void checkGetQuantiles(ReqSketch sk) {
    println(LS + "GetQuantiles() Test");
    float[] rArr = {0, .1F, .2F, .3F, .4F, .5F, .6F, .7F, .8F, .9F, 1.0F};
    float[] qOut = sk.getQuantiles(rArr);
    for (int i = 0; i < qOut.length; i++) {
      println("nRank: " + rArr[i] + ", q: " + qOut[i]);
    }
  }


  private static ReqSketch loadSketch(int min, int max, boolean up, int k, boolean debug) {
    ReqSketch sk = new ReqSketch(k, debug);
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
