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

package org.apache.datasketches;

import static org.apache.datasketches.CrossCheckQuantilesTest.PrimType.DOUBLE;
import static org.apache.datasketches.CrossCheckQuantilesTest.PrimType.FLOAT;
import static org.apache.datasketches.CrossCheckQuantilesTest.SkType.CLASSIC;
import static org.apache.datasketches.CrossCheckQuantilesTest.SkType.KLL;
import static org.apache.datasketches.CrossCheckQuantilesTest.SkType.REQ;
import static org.apache.datasketches.CrossCheckQuantilesTest.SkType.REQ_NO_DEDUP;
import static org.apache.datasketches.CrossCheckQuantilesTest.SkType.REQ_SV;
import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.QuantileSearchCriteria.NON_INCLUSIVE;
import static org.apache.datasketches.QuantileSearchCriteria.NON_INCLUSIVE_STRICT;
import static org.testng.Assert.assertEquals;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.datasketches.req.ReqSketch;
import org.apache.datasketches.req.ReqSketchBuilder;
import org.apache.datasketches.req.ReqSketchSortedView;
import org.testng.annotations.Test;

public class CrossCheckQuantilesTest {

  enum SkType { REQ, REQ_SV, REQ_NO_DEDUP, KLL, CLASSIC }

  enum PrimType { DOUBLE, FLOAT }

  final int k = 32; //all sketches are in exact mode
  final boolean hra = false; //for the REQ sketch

  @Test
  public void checkQuantileSketches() {
    checkQAndR(REQ, FLOAT, NON_INCLUSIVE); //must do REQ first to compute expected results!
    checkQAndR(REQ, FLOAT, INCLUSIVE);

    checkQAndR(REQ_SV, FLOAT, NON_INCLUSIVE);
    checkQAndR(REQ_SV, FLOAT, INCLUSIVE);

    checkQAndR(REQ_NO_DEDUP, FLOAT, NON_INCLUSIVE);
    checkQAndR(REQ_NO_DEDUP, FLOAT, INCLUSIVE);

    checkQAndR(KLL, FLOAT, NON_INCLUSIVE);
    checkQAndR(KLL, FLOAT, INCLUSIVE);

    checkQAndR(CLASSIC, DOUBLE, NON_INCLUSIVE);
    checkQAndR(CLASSIC, DOUBLE, INCLUSIVE);
    println("");
  }

  double[] testRankResults_NI = null;
  double[] testRankResults_I = null;
  float[] testQuantileFResults_NI = null;
  float[] testQuantileFResults_I = null;
  double[] testQuantileDResults_NI = null;
  double[] testQuantileDResults_I = null;

  private void checkQAndR(final SkType skType, final PrimType type, final QuantileSearchCriteria inclusive) {
    String head = "CHECK " + skType.toString();
    if (inclusive == INCLUSIVE) { println("\n---------- " + head + " INCLUSIVE ----------\n"); }
    else { println("\n########## " + head + " NON-INCLUSIVE ##########\n"); }

    //CREATE EMPTY SKETCHES
    ReqSketchBuilder reqBldr = ReqSketch.builder();
    reqBldr.setK(k).setHighRankAccuracy(hra).setLessThanOrEqual(inclusive == INCLUSIVE ? true : false);
    ReqSketch reqSk = reqBldr.build();

    KllFloatsSketch kllSk = KllFloatsSketch.newHeapInstance(k);

    UpdateDoublesSketch udSk = DoublesSketch.builder().setK(k).build();

    //SKETCH INPUT.
    float[] baseFVals  = {10,20,30,40,50};
    double[] baseDVals = {10,20,30,40,50};
    int[] baseDups     = { 1, 4, 6, 2, 1}; //number of duplicates per base value

    //RAW TEST INPUT. This simulates what the Sorted View might see.
    //This checks the search algos without the sketch and created by hand
    float[] rawFVals =  {10,20,20,30,30, 30, 40, 50}; //note grouping by twos
    long[] rawCumWts =  { 1, 3, 5, 7, 9, 11, 13, 14};

    //COMPUTE N
    int N = 0;
    for (int i : baseDups) { N += i; }
    int numV = baseFVals.length; //num of distinct input values

    //CREATE SKETCH INPUT ARRAYS
    float[] skFValues = new float[N];
    double[] skDValues = new double[N];
    int n = 0;
    for (int bv = 0; bv < baseFVals.length; bv++) {
      float bvF = baseFVals[bv];
      double bvD = baseDVals[bv];
      for (int i = 0; i < baseDups[bv]; i++) {
        skFValues[n] = bvF;
        skDValues[n] = bvD;
        n++;
      }
    }

    //CREATE getRank TEST VALUES
    int numTV = 2 * numV + 1;
    float[] testFValues = new float[numTV];
    double[] testDValues = new double[numTV];
    for (int i = 0; i < numTV; i++) {
      testFValues[i] = 5F * (i + 1);
      testDValues[i] = 5.0 * (i + 1);
    }

    //CREATE getQuantile() TEST NORMALIZED RANKS
    //One for each value in the stream + one inbetween and zero
    int numTR = 2 * N + 1;
    double[] testRanks = new double[numTR];
    testRanks[0] = 0;
    for (int i = 1; i < numTR; i++) {
      testRanks[i] = (double) i / (numTR - 1);
    }

    //TEST RESULT ARRAYS, NI = non inclusive, I = inclusive
    if (testRankResults_NI == null) {
      testRankResults_NI = new double[numTV];
      testRankResults_I  = new double[numTV];
      testQuantileFResults_NI = new float[numTR];
      testQuantileFResults_I  = new float[numTR];
      testQuantileDResults_NI = new double[numTR];
      testQuantileDResults_I  = new double[numTR];
    }

    println("Sketch Input with exact weights and ranks:");
    println("  Sketch only keeps individual value weights per level.");
    println("  Cumulative Weights are computed in Sorted View.");
    println("  Normalized Ranks are computed on the fly.");
    println("");
    printf("%16s%16s%16s\n", "Value", "CumWeight", "NormalizedRank");

    //LOAD THE SKETCHES and PRINT
    for (int i = 0; i < N; i++) {
      printf("%16.1f%16d%16.3f\n", skFValues[i], i + 1, (i + 1.0)/N);
      reqSk.update(skFValues[i]);
      kllSk.update(skFValues[i]);
      udSk.update((skDValues[i]));
    }
    println("");

    //REQ SORTED VIEW DATA:
    ReqSketchSortedView rssv = null;
    if (skType.toString().startsWith("REQ")) {
      rssv = new ReqSketchSortedView(reqSk);
      println(rssv.toString(1, 16));
    }

    /**************************************/

    println(skType.toString() + " GetQuantile(NormalizedRank), INCLUSIVE = " + inclusive.toString());
    println("  CumWeight is for illustration");
    println("  Convert NormalizedRank to CumWeight (CW).");
    println("  Search RSSV CumWeights[] array:");
    println("    Non Inclusive (uses GT): arr[A] <= CW <  arr[B], return B");
    println("    Inclusive     (uses GE): arr[A] <  CW <= arr[B], return B");
    println("  Return Values[B]");
    println("");
    printf("%16s%16s%16s%16s\n", "NormalizedRank", "CumWeight", "Quantile", "True_Quantile");
    for (int i = 0; i < numTR; i++) {
      double testRank = testRanks[i];
      float qF; //float result
      double qD;//double result
      switch (skType) {
        case REQ: { //this case creates the expected values for all the others
          qF = reqSk.getQuantile(testRank, inclusive);
          qD = qF;
          if (inclusive == INCLUSIVE) {
            testQuantileFResults_I[i] = qF;
            testQuantileDResults_I[i] = qD;
          }
          else if (inclusive == NON_INCLUSIVE){
            testQuantileFResults_NI[i] = qF;
            testQuantileDResults_NI[i] = qD;
          }
          break;
        }
        case REQ_SV: {
          qF = rssv.getQuantile(testRank, inclusive);
          qD = 0;
          if (inclusive == INCLUSIVE) { assertEquals(qF, testQuantileFResults_I[i]); }
          else { assertEquals(qF, testQuantileFResults_NI[i]); };
          break;
        }
        case REQ_NO_DEDUP: {
          qF = this.getQuantile(rawCumWts, rawFVals, testRank, inclusive);
          qD = 0;
          if (inclusive == INCLUSIVE) { assertEquals(qF, testQuantileFResults_I[i]); }
          else { assertEquals(qF, testQuantileFResults_NI[i]); };
          break;
        }
//        case KLL: {
//          qF = kllSk.getQuantile(testRank, inclusive);
//          qD = 0;
//          if (inclusive) { assertEquals(qF, testQuantileFResults_I[i]); }
//          else { assertEquals(qF, testQuantileFResults_NI[i]); };
//          break;
//        }
//        case CLASSIC: {
//          qF = 0;
//          qD = kllSk.getQuantile(testRank, inclusive);
//          if (inclusive) { assertEquals(qD, testQuantileDResults_I[i]); }
//          else { assertEquals(qD, testQuantileDResults_NI[i]); };
//          break;
//        }
        default: qD = qF = 0; break;
      }
      if (inclusive == INCLUSIVE) {
        if (type == PrimType.DOUBLE) {
          printf("%16.3f%16.3f%16.1f%16.1f\n", testRank, testRank * N, qD, testQuantileDResults_I[i]);
        } else { //float
          printf("%16.3f%16.3f%16.1f%16.1f\n", testRank, testRank * N, qF, testQuantileFResults_I[i]);
        }
      } else if (inclusive == NON_INCLUSIVE){
        if (type == PrimType.DOUBLE) {
          printf("%16.3f%16.3f%16.1f%16.1f\n", testRank, testRank * N, qD, testQuantileDResults_NI[i]);
        } else { //float
          printf("%16.3f%16.3f%16.1f%16.1f\n", testRank, testRank * N, qF, testQuantileFResults_NI[i]);
        }
      }
    }

    //GET RANK
    println("");
    println(skType.toString() + " GetRank(Value), INCLUSIVE = " + inclusive);
    println("  Search RSSV Values[] array:");
    println("    Non Inclusive (uses LT): arr[A] <  V <= arr[B], return A");
    println("    Inclusive     (uses LE): arr[A] <= V <  arr[B], return A");
    println("  Convert CumWeights[A] to NormRank,");
    println("  Return NormRank");
    printf("%16s%16s%16s\n", "ValueIn", "NormalizedRank", "True-NormRank");

    double r; //result
    for (int i = 0; i < numTV; i++) {
      float testValue = testFValues[i];
      switch (skType) {
        case REQ: {
          r = reqSk.getRank(testValue, inclusive);
          if (inclusive == INCLUSIVE) { testRankResults_I[i] = r; }
          else if (inclusive == NON_INCLUSIVE) { testRankResults_NI[i] = r; }
          break;
        }
        case REQ_SV: {
          r = rssv.getRank(testValue,  inclusive);
          if (inclusive == INCLUSIVE) { assertEquals(r, testRankResults_I[i]); }
          else if (inclusive == NON_INCLUSIVE) { assertEquals(r, testRankResults_NI[i]); };
          break;
        }
        case REQ_NO_DEDUP: {
          r = getRank(rawCumWts, rawFVals, testValue, inclusive);
          if (inclusive == INCLUSIVE) { assertEquals(r, testRankResults_I[i]); }
          else if (inclusive == NON_INCLUSIVE) { assertEquals(r, testRankResults_NI[i]); };
          break;
        }
//        case KLL: {
//          r = kllSk.getRank(testValue,  inclusive);
//          if (inclusive == INCLUSIVE) { assertEquals(r, testRankResults_I[i]); }
//          else if (inclusive == NON_INCLUSIVE) { assertEquals(r, testRankResults_NI[i]); };
//          break;
//        }
//        case CLASSIC: {
//          r = udSk.getRank(testValue,  inclusive);
//          if (inclusive == INCLUSIVE) { assertEquals(r, testRankResults_I[i]); }
//          else if (inclusive == NON_INCLUSIVE) { assertEquals(r, testRankResults_NI[i]); };
//          break;
//        }
        default: r = 0; break;
      }
      if (inclusive == INCLUSIVE) {
        printf("%16.1f%16.3f%16.3f\n", testValue, r, testRankResults_I[i]);
      } else if (inclusive == NON_INCLUSIVE) {
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
  public float getQuantile(final long[] cumWeights, final float[] values, final double normRank,
      final QuantileSearchCriteria inclusive) {
    final int len = cumWeights.length;
    final long N = cumWeights[len -1];
    final long rank = (int)(normRank * N); //denormalize
    final InequalitySearch crit = inclusive == INCLUSIVE ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, rank, crit);
    if (index == -1) {
      if (inclusive == NON_INCLUSIVE_STRICT) { return Float.NaN; } //GT: normRank == 1.0;
      if (inclusive == NON_INCLUSIVE) { return values[len - 1]; }
    }
    return values[index];
  }

  /**
   * Gets the normalized rank based on the given value.
   * @param cumWeights the given cumulative weights
   * @param values the given values
   * @param value the given value
   * @param inclusive determines the search criterion used.
   * @return the normalized rank
   */
  public double getRank(final long[] cumWeights, final float[] values, final float value,
      final QuantileSearchCriteria inclusive) {
    final int len = values.length;
    final long N = cumWeights[len -1];
    final InequalitySearch crit = inclusive == INCLUSIVE ? InequalitySearch.LE : InequalitySearch.LT;
    final int index = InequalitySearch.find(values,  0, len - 1, value, crit);
    if (index == -1) {
      return 0; //LT: value <= minValue; LE: value < minValue
    }
    return (double)cumWeights[index] / N; //normalize
  }

  private final static boolean enablePrinting = false;

  /**
   * @param format the format
   * @param args the args
   */
  private static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
