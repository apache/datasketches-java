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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.InequalitySearch;
import org.apache.datasketches.req.ReqSketchSortedView.Row;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */

public class ReqSketchSortedViewTest {

  /**
   * just tests the mergeSortIn. It does NOT test anything else.
   */
  @Test
  public void checkMergeSortIn() {
    checkMergeSortInImpl(true);
    checkMergeSortInImpl(false);
  }

  private static void checkMergeSortInImpl(final boolean hra) {
    final FloatBuffer buf1 = new FloatBuffer(25, 0, hra);
    for (int i = 1; i < 12; i += 2) { buf1.append(i); } //6 odd values
    final FloatBuffer buf2 = new FloatBuffer(25, 0, hra);
    for (int i = 2; i <= 12; i += 2) { buf2.append(i); } //6 even values
    final long N = 18;

    final float[] values = new float[25];
    final long[] valueWeights = new long[25]; //not used

    final ReqSketchSortedView rssv = new ReqSketchSortedView(values, valueWeights, hra, N);
    rssv.mergeSortIn(buf1, 1, 0);
    rssv.mergeSortIn(buf2, 2, 6); //at weight of 2
    println(rssv.toString(3, 12));
    Row row = rssv.getRow(0);
    for (int i = 1; i < 12; i++) {
      final Row rowi = rssv.getRow(i);
      assertTrue(rowi.value >= row.value);
      row = rowi;
    }
  }

  enum TestQ {REQ, REQSV, REQ_NO_DEDUP }

  @SuppressWarnings("unused")
  //@Test
  public void checkRssvVsSketch() {
    int k = 4;
    boolean hra = false;
    boolean inclusive;
    checkQAndR(k, hra, inclusive = false, TestQ.REQ); //must do this first
    println("\n-------------------\n");
    checkQAndR(k, hra, inclusive = true, TestQ.REQ);
    println("\n###################\n");
    checkQAndR(k, hra, inclusive = false, TestQ.REQSV);
    println("\n-------------------\n");
    checkQAndR(k, hra, inclusive = true, TestQ.REQSV);
    println("\n###################\n");
    checkQAndR(k, hra, inclusive = false, TestQ.REQ_NO_DEDUP);
    println("\n-------------------\n");
    checkQAndR(k, hra, inclusive = true, TestQ.REQ_NO_DEDUP);
    println("\n###################\n");
  }

  double[] testRankResults_NI = null;
  float[] testQuantileResults_NI = null;
  double[] testRankResults_I = null;
  float[] testQuantileResults_I = null;

  private void checkQAndR(final int k, final boolean hra, final boolean inclusive, final TestQ testQ) {
    println("");
    println("CHECK SketchSortedView");
    println("  k: " + k + ", hra: " + hra + ", inclusive: " + inclusive + ", TestQ: " + testQ.toString());

    ReqSketchBuilder bldr = ReqSketch.builder();
    bldr.setK(4).setHighRankAccuracy(hra).setLessThanOrEqual(inclusive);
    ReqSketch reqSk = bldr.build();

    //Example Sketch Input, always use sequential multiples of 10
    float[] baseVals = {10,20,30,40,50};
    int[] baseDups   = { 1, 4, 6, 2, 1};

    int N = 0;
    for (int i : baseDups) { N += i; } //compute N
    int numV = baseVals.length;        //num of distinct input values

//    int gps = 0;
//    for (int bd = 0; bd < baseDups.length; bd++) {
//      gps += Math.ceil(baseDups[bd] / 2.0);
//    }

    //Create Sketch input values
    float[] skValues = new float[N];
    int n = 0;
    for (int bv = 0; bv < baseVals.length; bv++) {
      float bvf = baseVals[bv];
      for (int i = 0; i < baseDups[bv]; i++) {
        skValues[n++] = bvf;
      }
    }

    //Create testValues for getRank
    int numTV = 2 * numV + 1;
    float[] testValues = new float[numTV];
    for (int i = 0; i < numTV; i++) {
      testValues[i] = 5F * (i + 1);
    }

    //Create testRanks for getQuantile()
    int numTR = 2 * N + 1;
    double[] testRanks = new double[numTR];
    testRanks[0] = 0;
    for (int i = 1; i < numTR; i++) {
      testRanks[i] = (double) i / (numTR - 1);
    }

    if (testRankResults_NI == null) {
      testRankResults_NI = new double[numTV];
      testRankResults_I  = new double[numTV];
      testQuantileResults_NI = new float[numTR];
      testQuantileResults_I  = new float[numTR];
    }

    //Create simulated input for RAW tests
    float[] rawVals =  {10,20,20,30,30, 30, 40, 50};
    long[] rawCumWts = { 1, 3, 5, 7, 9, 11, 13, 14};

    println("");
    println("Example Sketch Input with illustrated weights and ranks:");
    println("  Sketch only keeps individual value weights per level");
    println("  Cumulative Weights are computed in RSSV.");
    println("  Normalized Ranks are computed on the fly.");
    println("");
    printf("%16s%16s%16s\n", "Value", "CumWeight", "NormalizedRank");
    //LOAD THE SKETCHES and PRINT
    for (int i = 0; i < N; i++) {
      printf("%16.1f%16d%16.3f\n", skValues[i], i + 1, (i + 1.0)/N);
      reqSk.update(skValues[i]);
    }
    println("");

    //REQ SORTED VIEW DATA:
    ReqSketchSortedView rssv = new ReqSketchSortedView(reqSk);
    println(rssv.toString(1, 16));

    /**************************************/

    println("GetQuantile(NormalizedRank):");
    println("  CumWeight is for illustration");
    println("  Convert NormalizedRank to CumWeight (CW).");
    println("  Search RSSV CumWeights[] array:");
    println("    Non Inclusive (uses GT): arr[A] <= CW <  arr[B], return B");
    println("    Inclusive     (uses GE): arr[A] <  CW <= arr[B], return B");
    println("  Return Values[B]");
    println("");
    printf("%16s%16s%16s%16s\n", "NormalizedRank", "CumWeight", "Quantile", "REQ_Quantile");
    for (int i = 0; i < numTR; i++) {
      double testRank = testRanks[i];
      float q; //result
      switch (testQ) {
        case REQ: {
          q = reqSk.getQuantile(testRank, inclusive);
          if (inclusive) { testQuantileResults_I[i] = q; }
          else { testQuantileResults_NI[i] = q; }
          break;
        }
        case REQSV: {
          q = rssv.getQuantile(testRank, inclusive);
          if (inclusive) { assertEquals(q, testQuantileResults_I[i]); }
          else { assertEquals(q, testQuantileResults_NI[i]); };
          break;
        }
        case REQ_NO_DEDUP: {
          q = getQuantile(rawCumWts, rawVals, testRank, inclusive);
          if (inclusive) { assertEquals(q, testQuantileResults_I[i]); }
          else { assertEquals(q, testQuantileResults_NI[i]); };
          break;
        }
        default: q = 0; break;
      }
      if (inclusive) {
        printf("%16.3f%16.3f%16.1f%16.1f\n", testRank, testRank * N, q, testQuantileResults_I[i]);
      } else {
        printf("%16.3f%16.3f%16.1f%16.1f\n", testRank, testRank * N, q, testQuantileResults_NI[i]);
      }
    }

    println("");
    println("GetRank(Value):");
    println("  Search RSSV Values[] array:");
    println("    Non Inclusive (uses LT): arr[A] <  V <= arr[B], return A");
    println("    Inclusive     (uses LE): arr[A] <= V <  arr[B], return A");
    println("  Convert CumWeights[A] to NormRank,");
    println("  Return NormRank");
    printf("%16s%16s%16s\n", "ValueIn", "NormalizedRank", "REQ-NormRank");

    double r; //result
    for (int i = 0; i < numTV; i++) {
      float testValue = testValues[i];
      switch (testQ) {
        case REQ: {
          r = reqSk.getRank(testValue, inclusive);
          if (inclusive) { testRankResults_I[i] = r; }
          else { testRankResults_NI[i] = r; }
          break;
        }
        case REQSV: {
          r = rssv.getRank(testValue,  inclusive);
          if (inclusive) { assertEquals(r, testRankResults_I[i]); }
          else { assertEquals(r, testRankResults_NI[i]); };
          break;
        }
        case REQ_NO_DEDUP: {
          r = getRank(rawCumWts, rawVals, testValue, inclusive);
          if (inclusive) { assertEquals(r, testRankResults_I[i]); }
          else { assertEquals(r, testRankResults_NI[i]); };
          break;
        }
        default: r = 0; break;
      }
      if (inclusive) {
        printf("%16.1f%16.3f%16.3f\n", testValue, r, testRankResults_I[i]);
      } else {
        printf("%16.1f%16.3f%16.3f\n", testValue, r, testRankResults_NI[i]);
      }
    }
  }

  /**
   * Gets the quantile based on the given normalized rank,
   * which must be in the range [0.0, 1.0], inclusive.
   * @param cumWeights the given cumulative weights
   * @param values the given values
   * @param normRank the given normalized rank
   * @param inclusive determines the search criterion used.
   * @return the quantile
   */
  public float getQuantile(final long[] cumWeights, final float[] values, final double normRank, final boolean inclusive) {
    final int len = cumWeights.length;
    final long N = cumWeights[len -1];
    final long rank = (int)(normRank * N);
    final InequalitySearch crit = inclusive ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, rank, crit);
    if (index == -1) {
      return values[len - 1]; //GT: normRank >= 1.0; GE: normRank > 1.0
    }
    return values[index];
  }

  /**
   * Gets the normalized rank based on the given value.
   * @param cumWeights the given cumulative weights
   * @param values the given values
   * @param value the given value
   * @param ltEq determines the search criterion used.
   * @return the normalized rank
   */
  public double getRank(final long[] cumWeights, final float[] values, final float value, final boolean ltEq) {
    final int len = values.length;
    final long N = cumWeights[len -1];
    final InequalitySearch crit = ltEq ? InequalitySearch.LE : InequalitySearch.LT;
    final int index = InequalitySearch.find(values,  0, len - 1, value, crit);
    if (index == -1) {
      return 0; //LT: value <= minValue; LE: value < minValue
    }
    return (double)cumWeights[index] / N;
  }

  @Test
  public void checkIterator() {
    int k = 4;
    boolean hra = false;
    int numV = 3;
    int dup = 2;
    println("");
    println("CHECK ReqSketchSortedViewIterator");
    println("  k: " + k + ", hra: " + hra);
    ReqSketchBuilder bldr = ReqSketch.builder();
    ReqSketch sketch = bldr.build();
    int n = numV * dup; //Total values including duplicates
    println("  numV: " + numV + ", dup: " + dup);

    float[] arr = new float[n];
    int h = 0;
    for (int i = 0; i < numV; i++) {
      float flt = (i + 1) * 10;
      for (int j = 1; j <= dup; j++) { arr[h++] = flt; }
    }
    for (int i = 0; i < n; i++) { sketch.update(arr[i]); }

    ReqSketchSortedViewIterator itr = sketch.getSortedView().iterator();
    println("");
    String[] header = {"Value", "Wt", "CumWtNotInc", "nRankNotInc", "CumWtInc", "nRankInc"};
    String hfmt = "%12s%12s%12s%12s%12s%12s\n";
    String fmt = "%12.1f%12d%12d%12.3f%12d%12.3f\n";
    printf(hfmt, (Object[]) header);
    while (itr.next()) {
      float v = itr.getValue();
      long wt = itr.getWeight();
      long cumWtNotInc   = itr.getCumulativeWeight(false);
      double nRankNotInc = itr.getNormalizedRank(false);
      long cumWtInc      = itr.getCumulativeWeight(true);
      double nRankInc    = itr.getNormalizedRank(true);
      printf(fmt, v, wt, cumWtNotInc, nRankNotInc, cumWtInc, nRankInc);
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
