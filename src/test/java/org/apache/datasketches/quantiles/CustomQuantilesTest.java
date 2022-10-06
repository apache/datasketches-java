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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.quantilescommon.LinearRanksAndQuantiles.*;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class CustomQuantilesTest {

  @Test
  /**
   * Currently, this test only exercises the classic DoublesSketch, but all the quantiles
   * sketches use the same code for getQuantile() and getRank() anyway.
   * This same pattern is also part of the CrossCheckQuantilesTest.
   * This structure of this test allows more detailed analysis for troubleshooting.
   */
  public void checkQuantilesV400() {
    println("Classic DoubleSketch, Version 4.0.0, k=4");
    println("");
    //The following for loop creates the following pattern for the sorted view:
    // Quantiles: {10,10,20,20,30,30,40,40}
    // Weights  : { 2, 1, 2, 1, 2, 1, 2, 1}
    //This is easy to create from the classic quantiles sketch directly, but for the other
    //quantiles sketches it would be easier to create by loading the sorted view directly via
    //a package-private constructor.
    int k = 4;
    UpdateDoublesSketch sk = DoublesSketch.builder().setK(k).build();
    for (int i = 1; i <= 3; i++) {
      for (int q = 10; q <= k * 10; q += 10) {
        sk.update(q);
      }
    }
    long N = sk.getN();
    DoublesSketchSortedView sv = new DoublesSketchSortedView(sk);
    double[] quantilesArr = sv.getQuantiles();
    long[] cumWtsArr = sv.getCumulativeWeights();
    int lenQ = quantilesArr.length;
    println("Sorted View:");
    printf("%12s%12s%12s\n", "Quantiles", "ICumWts", "IRanks");
    double normRank;
    for (int i = 0; i < lenQ; i++) {
      normRank = (double)cumWtsArr[i] / N;
      printf("%12.1f%12d%12.4f\n", quantilesArr[i], cumWtsArr[i], normRank);
    }
    println("");

    println("GetRanks, EXCLUSIVE:");
    println("  R of the largest Q at the highest index that is < q. If q <= smallest Q => 0");
    printf("%12s%12s\n", "Quantiles", "Ranks");
    for (int q = 0; q <= (k * 10) + 5; q += 5) {
      double nr = sk.getRank(q, EXCLUSIVE);
      double nrTrue = getTrueDoubleRank(cumWtsArr, quantilesArr, q, EXCLUSIVE);
      assertEquals(nr, nrTrue);
      printf("%12.1f%12.3f\n", (double)q, nr);
    }
    println("");

    println("GetQuantiles, EXCLUSIVE (round down)");
    println("  Q of the smallest rank > r. If r = 1.0 => null or NaN");
    printf("%12s%12s%12s\n", "Ranks",  "Quantiles", "CompRank");
    double inc = 1.0 / (2 * N);
    for (int j = 0; j <= (2 * N); j++) {
      double nr = (j * inc);
      double q = sk.getQuantile(nr, EXCLUSIVE);
      double qTrue = getTrueDoubleQuantile(cumWtsArr, quantilesArr, nr, EXCLUSIVE);
      assertEquals(q, qTrue);
      double nrN = Math.floor(nr * N);
      printf("%12.4f%12.1f%12.1f\n", nr, q, nrN);
    }
    println("");

    println("GetRanks, INCLUSIVE:");
    println("  R of the largest Q at the highest index that is <= q. If q < smallest Q => 0");
    printf("%12s%12s\n", "Quantiles", "Ranks");
    for (int q = 0; q <= (k * 10) + 5; q += 5) {
      double nr = sk.getRank(q, INCLUSIVE);
      double nrTrue = getTrueDoubleRank(cumWtsArr, quantilesArr, q, INCLUSIVE);
      assertEquals(nr, nrTrue);
      printf("%12.1f%12.3f\n", (double)q, nr);
    }
    println("");

    println("GetQuantiles, INCLUSIVE (round up)");
    println("  Q of the smallest rank >= r.");
    printf("%12s%12s%12s\n", "Ranks", "Quantiles", "CompRank");
    inc = 1.0 / (2 * N);
    for (int j = 0; j <= (2 * N); j++) {
      double nr = (j * inc);
      double q = sk.getQuantile(nr, INCLUSIVE);
      double qTrue = getTrueDoubleQuantile(cumWtsArr, quantilesArr, nr, INCLUSIVE);
      assertEquals(q, qTrue);
      double nrN = Math.ceil(nr * N);
      printf("%12.4f%12.1f%12.1f\n", nr, q, nrN);
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

