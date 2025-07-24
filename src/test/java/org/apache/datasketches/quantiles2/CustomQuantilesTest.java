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

package org.apache.datasketches.quantiles2;

import static org.apache.datasketches.quantilescommon.LinearRanksAndQuantiles.getTrueDoubleQuantile;
import static org.apache.datasketches.quantilescommon.LinearRanksAndQuantiles.getTrueDoubleRank;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.getNaturalRank;
import static org.testng.Assert.assertEquals;

import org.apache.datasketches.quantilescommon.DoublesSketchSortedView;
import org.testng.annotations.Test;

public class CustomQuantilesTest {

  /**
   * Currently, this test only exercises the classic DoublesSketch, but all the quantiles
   * sketches use the same code for getQuantile() and getRank() anyway.
   * This same pattern is also part of the CrossCheckQuantilesTest.
   * This structure of this test allows more detailed analysis for troubleshooting.
   */
  @Test
  public void checkQuantilesV400() {
    println("org.apache.datasketches.quantiles.CustomQuantilesTest:");
    println("Classic DoubleSketch, Version 4.0.0, k=4, N=12");
    println("");
    //The following for loop creates the following pattern for the sorted view:
    // Quantiles: {10,10,20,20,30,30,40,40}
    // Weights  : { 2, 1, 2, 1, 2, 1, 2, 1}
    //This is easy to create from the classic quantiles sketch directly, but for the other
    //quantiles sketches it is easier to create by loading the sorted view directly via
    //a package-private constructor.
    int k = 4;
    UpdateDoublesSketch sk = DoublesSketch.builder().setK(k).build();
    for (int i = 1; i <= 3; i++) {
      for (int q = 10; q <= k * 10; q += 10) {
        sk.update(q);
      }
    }
    long N = sk.getN();
    DoublesSketchSortedView sv = sk.getSortedView();
    double[] quantilesArr = sv.getQuantiles();
    long[] cumWtsArr = sv.getCumulativeWeights();
    int lenQ = quantilesArr.length;
    println("Sorted View:");
    printf("%13s %13s %13s\n", "QuantilesArr", "CumWtsArr", "NormRanks");
    double normRank;
    for (int i = 0; i < lenQ; i++) {
      normRank = (double)cumWtsArr[i] / N;
      printf("%12.1f%12d%12.4f\n", quantilesArr[i], cumWtsArr[i], normRank);
    }
    println("");

    println("GetRanks, EXCLUSIVE:");
    println("  R of the largest Q at the highest index that is < q. If q <= smallest Q => 0");
    printf("%12s %12s\n", "Quantiles", "NormRanks");
    for (int q = 0; q <= (k * 10) + 5; q += 5) { //create a range of quantiles for input
      double normRankEst = sk.getRank(q, EXCLUSIVE);
      double normRankTrue = getTrueDoubleRank(cumWtsArr, quantilesArr, q, EXCLUSIVE);
      assertEquals(normRankEst, normRankTrue);
      printf("%12.1f %12.3f", (double)q, normRankEst);
      if (normRankEst != normRankTrue) { println("  " + normRankEst + " != " + normRankTrue); } else { println(""); }
    }
    println("");

    println("GetQuantiles, EXCLUSIVE (round down)");
    println("  Q of the smallest rank > r. If r = 1.0 => null or NaN");
    printf("%22s %22s %22s %13s\n", "NormRanksIn", "RawNaturalRank", "TrimmedNatRank", "QuantilesEst");
    long limit = 4 * N;
    double inc = 1.0 / limit;
    for (long j = 0; j <= limit; j++) {
      double normRankIn = (j * inc);
      double qEst = sk.getQuantile(normRankIn, EXCLUSIVE);
      double qTrue = getTrueDoubleQuantile(cumWtsArr, quantilesArr, normRankIn, EXCLUSIVE);
      assertEquals(qEst, qTrue);
      double rawNatRank = normRankIn * N;
      double trimNatRank = getNaturalRank(normRankIn, N, EXCLUSIVE);
      printf("%22.18f %22.18f %22.18f %13.1f", normRankIn, rawNatRank, trimNatRank, qEst);
      if (qEst != qTrue) { println("  " + qEst + " != " +qTrue); } else { println(""); }
    }
    println("");

    println("GetRanks, INCLUSIVE:");
    println("  R of the largest Q at the highest index that is <= q. If q < smallest Q => 0");
    printf("%12s %12s\n", "Quantiles", "NormRanks");
    for (int q = 0; q <= (k * 10) + 5; q += 5) {
      double nr = sk.getRank(q, INCLUSIVE);
      double nrTrue = getTrueDoubleRank(cumWtsArr, quantilesArr, q, INCLUSIVE);
      assertEquals(nr, nrTrue);
      printf("%12.1f %12.3f", (double)q, nr);
      if (nr != nrTrue) { println("  " + nr + " != " +nrTrue); } else { println(""); }
    }
    println("");

    println("GetQuantiles, INCLUSIVE (round up)");
    println("  Q of the smallest rank >= r.");
    printf("%22s %22s %22s %13s\n", "NormRanksIn", "RawNaturalRank", "TrimmedNatRank", "QuantilesEst");

    inc = 1.0 / limit;
    for (long j = 0; j <= limit; j++) {
      double normRankIn = (j * inc);
      double qEst = sk.getQuantile(normRankIn, INCLUSIVE);
      double qTrue = getTrueDoubleQuantile(cumWtsArr, quantilesArr, normRankIn, INCLUSIVE);
      assertEquals(qEst, qTrue);
      double rawNatRank = normRankIn * N;
      double trimNatRank = getNaturalRank(normRankIn, N, INCLUSIVE);
      printf("%22.18f %22.18f %22.18f %13.1f", normRankIn, rawNatRank, trimNatRank, qEst);
      if (qEst != qTrue) { println("  " + qEst + " != " +qTrue); } else { println(""); }
    }
    println("");
  }

  private final static boolean enablePrinting = false;

  /**
   * @param o the Object to print
   */
  static final void print(final Object o) {
    if (enablePrinting) { System.out.print(o.toString()); }
  }

  /**
   * @param o the Object to println
   */
  static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

  /**
   * @param format the format
   * @param args the args
   */
  static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }
}

