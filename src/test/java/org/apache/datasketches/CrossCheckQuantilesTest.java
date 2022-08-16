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


import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.QuantileSearchCriteria.NON_INCLUSIVE;
import static org.apache.datasketches.QuantileSearchCriteria.NON_INCLUSIVE_STRICT;
import static org.apache.datasketches.ReflectUtility.KLL_DOUBLES_SV_CTOR;
import static org.apache.datasketches.ReflectUtility.KLL_FLOATS_SV_CTOR;
import static org.apache.datasketches.ReflectUtility.REQ_SV_CTOR;
import static org.testng.Assert.assertEquals;

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllDoublesSketchSortedView;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.kll.KllFloatsSketchSortedView;
import org.apache.datasketches.req.ReqSketch;
import org.apache.datasketches.req.ReqSketchSortedView;
import org.testng.annotations.Test;

public class CrossCheckQuantilesTest {

  final int k = 32; //all sketches are in exact mode

  //These test sets are specifically designed to test some tough corner cases so don't mess with them
  //  unless you know what you are doing.
  //These sets must start with 10 and be multiples of 10.
  final float[][] svFValues =
    {
      {10},                      //set 0
      {10,10},                   //set 1
      {10,20,20,30,30,30,40,50}  //set 2
    };

  final double[][] svDValues =
    {
      {10},
      {10,10},
      {10,20,20,30,30,30,40,50}
    };

  //these are value weights and will be converted to cumulative.
  final long[][] svWeights =
    {
      {1},
      {1,1},
      {1,2,2,2,2,2,2,1}
    };

  int numSets;

  long[][] svCumWeights;

  long[] totalN;

  float[][] skFStreamValues;
  double[][] skDStreamValues;

  ReqSketch reqFloatsSk = null;
  KllFloatsSketch kllFloatsSk = null;
  KllDoublesSketch kllDoublesSk = null;

  ReqSketchSortedView reqFloatsSV = null;
  KllFloatsSketchSortedView kllFloatsSV = null;
  KllDoublesSketchSortedView kllDoublesSV = null;

  public CrossCheckQuantilesTest() {}

  @Test
  public void runTests() throws Exception {
    buildDataSets();
    for (int set = 0; set < numSets; set++) {
      buildSVs(set);
      buildSketches(set);
      println("TEST getRank across all sketches and their Sorted Views");
      checkGetRank(set, INCLUSIVE);
      checkGetRank(set, NON_INCLUSIVE);
      checkGetRank(set, NON_INCLUSIVE_STRICT);
      println("");
      println("TEST getQuantile across all sketches and their Sorted Views");
      checkGetQuantile(set, INCLUSIVE);
      checkGetQuantile(set, NON_INCLUSIVE);
      checkGetQuantile(set, NON_INCLUSIVE_STRICT);
    }
  }

  private void checkGetRank(int set, QuantileSearchCriteria crit) {
      float maxFloatvalue = getMaxFloatValue(set);
      double trueRank;
      double testRank;
      for (float v = 5f; v <= maxFloatvalue + 5f; v += 5f) {
        trueRank = getStdFloatRank(svCumWeights[set], svFValues[set],v, crit);

        testRank = reqFloatsSV.getRank(v, crit);
        assertEquals(testRank, trueRank);
        testRank = reqFloatsSk.getRank(v, crit);
        assertEquals(testRank, trueRank);

        testRank = kllFloatsSV.getRank(v, crit);
        assertEquals(testRank, trueRank);
        testRank = kllFloatsSk.getRank(v, crit);
        assertEquals(testRank, trueRank);

        println("Floats  set: " + set + ", value: " + v + ", rank: " + trueRank + ", crit: " + crit.toString());
      }
      double maxDoubleValue = getMaxDoubleValue(set);
      for (double v = 5; v <= maxDoubleValue + 5; v += 5) {
        trueRank = getStdDoubleRank(svCumWeights[set], svDValues[set],v, crit);

        testRank = kllDoublesSV.getRank(v, crit);
        assertEquals(testRank, trueRank);
        testRank = kllDoublesSk.getRank(v, crit);
        assertEquals(testRank, trueRank);

        println("Doubles set: " + set + ", value: " + v + ", rank: " + trueRank + ", crit: " + crit.toString());
      }
  }

  private void checkGetQuantile(int set, QuantileSearchCriteria crit) {
    int twoN = (int)totalN[set] * 2;
    double dTwoN = twoN;
    float trueFQ;
    float testFQ;
    for (int i = 0; i <= twoN; i++) {
      double normRank = i / dTwoN;
      trueFQ = getStdFloatQuantile(svCumWeights[set], svFValues[set], normRank, crit);

      testFQ = reqFloatsSV.getQuantile(normRank, crit);
      assertEquals(testFQ, trueFQ);
      testFQ = reqFloatsSk.getQuantile(normRank, crit);
      assertEquals(testFQ, trueFQ);

      testFQ = kllFloatsSV.getQuantile(normRank, crit);
      assertEquals(testFQ, trueFQ);
      testFQ = kllFloatsSk.getQuantile(normRank, crit);
      assertEquals(testFQ, trueFQ);

      println("Floats  set: " + set + ", rank: " + normRank + ", Q: " + trueFQ + ", crit: " + crit.toString());
    }
    double trueDQ;
    double testDQ;
    for (int i = 0; i <= twoN; i++) {
      double normRank = i / dTwoN;
      trueDQ = getStdDoubleQuantile(svCumWeights[set], svDValues[set], normRank, crit);

      testDQ = kllDoublesSV.getQuantile(normRank, crit);
      assertEquals(testDQ, trueDQ);
      testDQ = kllDoublesSk.getQuantile(normRank, crit);
      assertEquals(testDQ, trueDQ);

      println("Doubles set: " + set + ", rank: " + normRank + ", Q: " + trueDQ + ", crit: " + crit.toString());
    }
  }

  private double getMaxDoubleValue(int set) {
    int streamLen = skDStreamValues[set].length;
    return skDStreamValues[set][streamLen -1];
  }

  private float getMaxFloatValue(int set) {
    int streamLen = skFStreamValues[set].length;
    return skFStreamValues[set][streamLen -1];
  }

  /**
   * Gets the quantile based on the given normalized rank.
   * @param cumWeights the given natural cumulative weights. The last value must be N.
   * @param values the given values
   * @param normRank the given normalized rank, which must be in the range [0.0, 1.0], inclusive.
   * @param inclusive determines the search criterion used.
   * @return the quantile
   */
  private float getStdFloatQuantile(
      final long[] cumWeights,
      final float[] values,
      final double normRank,
      final QuantileSearchCriteria inclusive) {

    final int len = cumWeights.length;
    final long N = cumWeights[len -1];
    final long rank = (int)(normRank * N); //denormalize
    final InequalitySearch crit = inclusive == INCLUSIVE
        ? InequalitySearch.GE
        : InequalitySearch.GT; //includes both NON_INCLUSIVE and NON_INCLUSIVE_STRICT
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, rank, crit);
    if (index == -1) {
      if (inclusive == NON_INCLUSIVE_STRICT) { return Float.NaN; } //GT: normRank == 1.0;
      if (inclusive == NON_INCLUSIVE) { return values[len - 1]; }
    }
    return values[index];
  }

  /**
   * Gets the quantile based on the given normalized rank.
   * @param cumWeights the given natural cumulative weights. The last value must be N.
   * @param values the given values
   * @param normRank the given normalized rank, which must be in the range [0.0, 1.0], inclusive.
   * @param inclusive determines the search criterion used.
   * @return the quantile
   */
  private double getStdDoubleQuantile(
      final long[] cumWeights,
      final double[] values,
      final double normRank,
      final QuantileSearchCriteria inclusive) {

    final int len = cumWeights.length;
    final long N = cumWeights[len -1];
    final long rank = (int)(normRank * N); //denormalize
    final InequalitySearch crit = inclusive == INCLUSIVE
        ? InequalitySearch.GE
        : InequalitySearch.GT; //includes both NON_INCLUSIVE and NON_INCLUSIVE_STRICT
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, rank, crit);
    if (index == -1) {
      if (inclusive == NON_INCLUSIVE_STRICT) { return Double.NaN; } //GT: normRank == 1.0;
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
  private double getStdFloatRank(
      final long[] cumWeights,
      final float[] values,
      final float value,
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

  /**
   * Gets the normalized rank based on the given value.
   * @param cumWeights the given cumulative weights
   * @param values the given values
   * @param value the given value
   * @param inclusive determines the search criterion used.
   * @return the normalized rank
   */
  private double getStdDoubleRank(
      final long[] cumWeights,
      final double[] values,
      final double value,
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


  /*******BUILD & LOAD SKETCHES***********/

  private void buildSketches(int set) {
    reqFloatsSk = ReqSketch.builder().setK(k).build();
    kllFloatsSk = KllFloatsSketch.newHeapInstance(k);
    kllDoublesSk = KllDoublesSketch.newHeapInstance(k);
    int count = skFStreamValues[set].length;
    for (int i = 0; i < count; i++) {
      reqFloatsSk.update(skFStreamValues[set][i]);
      kllFloatsSk.update(skFStreamValues[set][i]);
      kllDoublesSk.update(skDStreamValues[set][i]);
    }
  }

  /*******BUILD & LOAD SVs***********/

  private void buildSVs(int set) throws Exception {
    reqFloatsSV = getRawReqSV(svFValues[set], svCumWeights[set], totalN[set]);
    kllFloatsSV = getRawKllFloatsSV(svFValues[set], svCumWeights[set], totalN[set]);
    kllDoublesSV = getRawKllDoublesSV(svDValues[set], svCumWeights[set], totalN[set]);
  }

  private final ReqSketchSortedView getRawReqSV(
      final float[] values, final long[] cumWeights, final long totalN) throws Exception {
    return (ReqSketchSortedView) REQ_SV_CTOR.newInstance(values, cumWeights, totalN);
  }

  private final KllFloatsSketchSortedView getRawKllFloatsSV(
      final float[] values, final long[] cumWeights, final long totalN) throws Exception {
    return (KllFloatsSketchSortedView) KLL_FLOATS_SV_CTOR.newInstance(values, cumWeights, totalN);
  }

  private final KllDoublesSketchSortedView getRawKllDoublesSV(
      final double[] values, final long[] cumWeights, final long totalN) throws Exception {
    return (KllDoublesSketchSortedView) KLL_DOUBLES_SV_CTOR.newInstance(values, cumWeights, totalN);
  }

  /********BUILD DATA SETS**********/

  private void buildDataSets() {
    numSets = svWeights.length;
    svCumWeights = new long[numSets][];
    totalN = new long[numSets];
    skFStreamValues = new float[numSets][];
    skDStreamValues = new double[numSets][];
    for (int i = 0; i < numSets; i++) {
      svCumWeights[i] = convertToCumWeights(svWeights[i]);
      int len = svCumWeights[i].length;
      int totalCount = (int)svCumWeights[i][len -1];
      totalN[i] = totalCount;
      skFStreamValues[i] = convertToFloatStream(svFValues[i], svWeights[i], totalCount);
      skDStreamValues[i] = convertToDoubleStream(svDValues[i], svWeights[i], totalCount);

    }
    println("");
  }

  private float[] convertToFloatStream(
      final float[] svFValueArr,
      final long[] svWeightsArr,
      final int totalCount) {
    float[] out = new float[totalCount];
    int len = svWeightsArr.length;
    int i = 0;
    for (int j = 0; j < len; j++) {
      float f = svFValueArr[j];
      int wt = (int)svWeightsArr[j];
      for (int w = 0; w < wt; w++) {
        out[i++] = f;
      }
    }
    return out;
  }

  private double[] convertToDoubleStream(
      final double[] svDValueArr,
      final long[] svWeightsArr,
      final int totalCount) {
    double[] out = new double[totalCount];
    int len = svWeightsArr.length;
    int i = 0;
    for (int j = 0; j < len; j++) {
      double d = svDValueArr[j];
      int wt = (int)svWeightsArr[j];
      for (int w = 0; w < wt; w++) {
        out[i++] = d;
      }
    }
    return out;
  }

  private long[] convertToCumWeights(final long[] weights) {
    final int len = weights.length;
    final long[] out = new long[len];
    out[0] = weights[0];
    for (int i = 1; i < len; i++) {
      out[i] = weights[i] + out[i - 1];
    }
    return out;
  }

  /*******************/

  private final static boolean enablePrinting = false;

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
