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

import static org.testng.Assert.assertTrue;

import org.apache.datasketches.req.ReqAuxiliary.Row;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */

public class ReqAuxiliaryTest {

  @Test
  public void checkMergeSortIn() {
    checkMergeSortInImpl(true);
    checkMergeSortInImpl(false);
  }

  private static void checkMergeSortInImpl(final boolean hra) {
    final FloatBuffer buf1 = new FloatBuffer(25, 0, hra);
    for (int i = 1; i < 12; i += 2) { buf1.append(i); } //6 odd items
    final FloatBuffer buf2 = new FloatBuffer(25, 0, hra);
    for (int i = 2; i <= 12; i += 2) { buf2.append(i); } //6 even items
    final long N = 18;

    final float[] items = new float[25];
    final long[] weights = new long[25];

    final ReqAuxiliary aux = new ReqAuxiliary(items, weights, hra, N);
    aux.mergeSortIn(buf1, 1, 0);
    aux.mergeSortIn(buf2, 2, 6); //at weight of 2
    println(aux.toString(3, 12));
    Row row = aux.getRow(0);
    for (int i = 1; i < 12; i++) {
      final Row rowi = aux.getRow(i);
      assertTrue(rowi.item >= row.item);
      row = rowi;
    }
  }

  @Test
  public void checkAuxVsSketch() {
    int k = 4;
    boolean hra = false;
    boolean inclusive;
    boolean useSketch;
    int numV = 3;
    int dup = 2;
    inclusive = false;
    useSketch = true;
    checkAux(k, hra, inclusive, useSketch, numV, dup);
    println("-------------------");
    inclusive = false;
    useSketch = false;
    checkAux(k, hra, inclusive, useSketch, numV, dup);
    println("###################");
    inclusive = true;
    useSketch = true;
    checkAux(k, hra, inclusive, useSketch, numV, dup);
    println("-------------------");
    inclusive = true;
    useSketch = false;
    checkAux(k, hra, inclusive, useSketch, numV, dup);
    println("###################");
  }


  private void checkAux(final int k, final boolean hra, final boolean inclusive,
      final boolean useSketch, final int numV, final int dup) {
    println("CHECK AUX");
    println("k: " + k + ", hra: " + hra + ", inclusive: " + inclusive + ", useSketch: " + useSketch);
    ReqSketchBuilder bldr = ReqSketch.builder();
    bldr.setK(4).setHighRankAccuracy(hra).setLessThanOrEqual(inclusive);
    ReqSketch sk = bldr.build();
    int n = numV * dup; //num items
    println("numV: " + numV + ", dup: " + dup);

    float[] arr = new float[n];

    int h = 0;
    for (int i = 0; i < numV; i++) {
      float flt = (i + 1) * 10;
      for (int j = 1; j <= dup; j++) { arr[h++] = flt; }
    }
    println("");
    println("Sketch Input:");
    printf("%12s%12s%12s\n", "Q", "NatRank", "NormRank");
    for (int i = 0; i < n; i++) {
      printf("%12.1f%12d%12.3f\n", arr[i], i + 1, (i + 1.0)/n);
      sk.update(arr[i]);
    }

    println("");

    //Aux Detail
    ReqAuxiliary aux = new ReqAuxiliary(sk);
    println(aux.toString(0, 10));

    println("getQuantile(NormRank):"); //LT
//    println("Convert NormRank to NatRank.");
//    println("Search Aux CumWt array:");
//    println("  if (natRank <= minCumWt) return minItem.");
//    println("  find pair of cw where: cwBelow <= NatRank < cwAbove.");
//    println("Return Item of cwAbove.");
    printf("%12s%12s%12s\n", "NormRank", "NatRank", "Item-Q");
    int m = 2 * n;
    for (int i = 0; i <= m; i++) {
      double fract = (double) i / m;
      float q = useSketch
          ? sk.getQuantile(fract, inclusive)
          : aux.getQuantile(fract, inclusive); //until aux iterator is created
      printf("%12.3f%12.3f%12.1f\n", fract, fract * n, q);
    }

    println("");
    println("getRank(Q):"); //LT
//    println("Search Aux Item array:");
//    println("  if (Q < minValue) return 0.0");
//    println("  if (Q > maxValue) return 1.0");
//    println("  find pair of items where: itemBelow < Q <= itemAbove,");
//    println("Convert CumWt of itemBelow to NormRank,");
//    println("Return NormRank.");
    printf("%12s%12s\n", "Q", "NormRank");
    float q = 5.0F;
    for (int i = 1; i <= numV * 2 + 1; i++) {
      double r = useSketch
          ? sk.getRank(q, inclusive)
          : aux.getRank(q,  inclusive); //until aux iterator is created
      printf("%12.1f%12.3f\n", q, r);
      q += 5.0F;
    }
  }

  private final static boolean enablePrinting = true;

  /**
   * @param format the format
   * @param args the args
   */
  static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
