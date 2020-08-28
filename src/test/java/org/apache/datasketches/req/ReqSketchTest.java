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

import org.apache.datasketches.req.ReqAuxiliary.Row;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class ReqSketchTest {


  @Test
  public void test1() {
    ReqSketch sk = new ReqSketch(6, true); //w debug
    int max = 200;
    int min = 1;
    boolean up = false;

    if (up) {
      for (int i = min; i <= max; i++) {
        sk.update(i);
      }
    } else { //down
      for (int i = max + 1; i-- > min; ) {
        sk.update(i);
      }
    }
    print(sk.toString(0));

    double[] ranks = getEvenlySpacedRanks(11);
    println("Ranks Test:");
    for (int i = 0; i < ranks.length; i++) {
      printRank(sk, ((float)ranks[i] * (max - min)) + min);
    }

    ReqAuxiliary aux = new ReqAuxiliary(sk);
    aux.buildAuxTable(sk);
    String fmt = "%12.1f%12d%12.5f\n";
    final int totalCount = sk.getRetainedEntries();
    for (int i = 0; i < totalCount; i++) {
      Row row = aux.getRow(i);
      printf(fmt, row.item, row.weight, row.normRank);
    }

    float[] rArr = {0, .1F, .2F, .3F, .4F, .5F, .6F, .7F, .8F, .9F, 1.0F};
    float[] qOut = sk.getQuantiles(rArr);
    for (int i = 0; i < qOut.length; i++) {
      println("nRank: " + rArr[i] + ", q: " + qOut[i]);
    }

  }

  private static void printRank(ReqSketch sk, float v) {
    double r = sk.getRank(v);
    String rstr = String.format("%.2f", r);
    String vstr = String.format("%.2f", v);
    println("Value: " + vstr + ", Rank: " + rstr);
  }

  @Test
  public void strTest() {
    StringBuilder sb = new StringBuilder();
    float[] arr = {1, 2, 3};
    String fmt = " %.0f";
    for (int i = 0; i < arr.length; i++) {
      String str = String.format(fmt, arr[i]);
      sb.append(str);
    }
    println(sb.toString());
  }


  static final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
  }

  static final void print(final Object o) { System.out.print(o.toString()); }

  static final void println(final Object o) { System.out.println(o.toString()); }


}
