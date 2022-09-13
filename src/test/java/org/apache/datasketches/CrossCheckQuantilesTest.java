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
import static org.apache.datasketches.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.ReflectUtility.CLASSIC_DOUBLES_SV_CTOR;
import static org.apache.datasketches.ReflectUtility.KLL_DOUBLES_SV_CTOR;
import static org.apache.datasketches.ReflectUtility.KLL_FLOATS_SV_CTOR;
import static org.apache.datasketches.ReflectUtility.REQ_SV_CTOR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllDoublesSketchSortedView;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.kll.KllFloatsSketchSortedView;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchSortedView;
import org.apache.datasketches.req.ReqSketch;
import org.apache.datasketches.req.ReqSketchSortedView;
import org.testng.annotations.Test;

/**
 * This test suite runs a common set of tests against all of the quantiles-type sketches in the library.
 * Although the unit tests for each of the sketches is quite extensive, the purpose of this test is to make
 * sure that key corner cases are in fact handled the same way by all of the sketches.
 *
 * <p>These tests are not about estimation accuracy, per se, as each of the different quantile sketches have very
 * different algortithms for selecting the data to be retained in the sketch and thus will have very different error
 * properties. These tests are primarily interested in making sure that the internal search and comparison algorithms
 * used within the sketches are producing the correct results for exact queries based on a chosen search
 * criteria. The search criteria are selected from the enum {@link QuantileSearchCriteria}. The corner cases of
 * interest here are to make sure that each of the search criteria behave correctly for the following.
 * <ul>
 * <li>A sketch with a single value.</li>
 * <li>A sketch with two identical values<li>
 * <li>A sketch with multiple duplicates and where the duplicates have weights greater than one.</li>
 * </ul>
 *
 * @author Lee Rhodes
 */
public class CrossCheckQuantilesTest {

  final static int k = 32; //all sketches are in exact mode

  //These test sets are specifically designed for the corner cases mentioned in the class javadoc.
  //  Please don't mess with them  unless you know what you are doing.
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
  UpdateDoublesSketch classicDoublesSk = null;

  ReqSketchSortedView reqFloatsSV = null;
  KllFloatsSketchSortedView kllFloatsSV = null;
  KllDoublesSketchSortedView kllDoublesSV = null;
  DoublesSketchSortedView classicDoublesSV = null;

  public CrossCheckQuantilesTest() {}

  @Test
  public void runTests() throws Exception {
    buildDataSets();
    for (int set = 0; set < numSets; set++) {
      buildSVs(set);
      buildSketches(set);
      println("");
      println("TEST getRank, Set " + set + ", all Criteria, across all sketches and their Sorted Views:");
      checkGetRank(set, INCLUSIVE);
      checkGetRank(set, EXCLUSIVE);
      println("");
      println("TEST getQuantile, Set " + set + ", all Criteria, across all sketches and their Sorted Views:");
      checkGetQuantile(set, INCLUSIVE);
      checkGetQuantile(set, EXCLUSIVE);
    }
  }

  private void checkGetRank(int set, QuantileSearchCriteria crit) {
    double trueRank;
    double testRank;
    float maxFloatvalue = getMaxFloatValue(set);
    println("");
    for (float v = 5f; v <= maxFloatvalue + 5f; v += 5f) {
      trueRank = getTrueFloatRank(svCumWeights[set], svFValues[set],v, crit);
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
    println("");
    double maxDoubleValue = getMaxDoubleValue(set);
    for (double v = 5; v <= maxDoubleValue + 5; v += 5) {
      trueRank = getTrueDoubleRank(svCumWeights[set], svDValues[set],v, crit);

      testRank = kllDoublesSV.getRank(v, crit);
      assertEquals(testRank, trueRank);
      testRank = kllDoublesSk.getRank(v, crit);
      assertEquals(testRank, trueRank);

      testRank = classicDoublesSV.getRank(v, crit);
      assertEquals(testRank, trueRank);
      testRank = classicDoublesSk.getRank(v, crit);
      assertEquals(testRank, trueRank);

      println("Doubles set: " + set + ", value: " + v + ", rank: " + trueRank + ", crit: " + crit.toString());
    }
  }

  private void checkGetQuantile(int set, QuantileSearchCriteria crit) {
    int twoN = (int)totalN[set] * 2;
    double dTwoN = twoN;
    float trueFQ;
    float testFQ;
    println("");
    for (int i = 0; i <= twoN; i++) {
      double normRank = i / dTwoN;
      trueFQ = getTrueFloatQuantile(svCumWeights[set], svFValues[set], normRank, crit);

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
    println("");
    double trueDQ;
    double testDQ;
    for (int i = 0; i <= twoN; i++) {
      double normRank = i / dTwoN;
      trueDQ = getTrueDoubleQuantile(svCumWeights[set], svDValues[set], normRank, crit);

      testDQ = kllDoublesSV.getQuantile(normRank, crit);
      assertEquals(testDQ, trueDQ);
      testDQ = kllDoublesSk.getQuantile(normRank, crit);
      assertEquals(testDQ, trueDQ);

      testDQ = classicDoublesSV.getQuantile(normRank, crit);
      assertEquals(testDQ, trueDQ);
      testDQ = classicDoublesSk.getQuantile(normRank, crit);
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
   * <ul><li><b>getQuantile(rank, INCLUSIVE) or q(r, GE)</b><br>
   * := Given r, return the quantile, q, of the smallest rank that is strictly Greater than or Equal to r.</li>
   * <li><br>getQuantile(rank, EXCLUSIVE) or q(r, GT)</b><br>
   * := Given r, return the quantile, q, of the smallest rank that is strictly Greater Than r.</li>
   * </ul>
   *
   * @param cumWeights the given natural cumulative weights. The last value must be N.
   * @param quantiles the given quantile array
   * @param givenNormR the given normalized rank, which must be in the range [0.0, 1.0], inclusive.
   * @param inclusive determines the search criterion used.
   * @return the quantile
   */
  private float getTrueFloatQuantile(
      final long[] cumWeights,
      final float[] quantiles,
      final double givenNormR,
      final QuantileSearchCriteria inclusive) {
    final int len = cumWeights.length;
    final long N = cumWeights[len - 1];
    float result = Float.NaN;

    for (int i = 0; i < len; i++) {
      if (i == len - 1) { //at top or single element array
        double topR = (double)cumWeights[i] / N;
        float topQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (givenNormR <= topR) { result = topQ; break; }
          fail("normRank > 1.0");
        }
        //EXCLUSIVE
        if (givenNormR < topR ) { result = topQ; break; }
        if (1.0 < givenNormR) { fail("normRank > 1.0"); }
        result = Float.NaN; // R == 1.0
        break;
      }
      else { //always at least two valid entries
        double loR = (double)cumWeights[i] / N;
        double hiR = (double)cumWeights[i + 1] / N;
        float loQ = quantiles[i];
        float hiQ = quantiles[i + 1];
        if (inclusive == INCLUSIVE) {
          if (i == 0) { //at bottom, starting up
            if (givenNormR <= loR) { result = loQ; break; }
          }
          if (loR < givenNormR && givenNormR <= hiR) { result = hiQ; break; }
          continue;
        }
        //EXCLUSIVE
        if (i == 0) { //at bottom, starting up
          if (givenNormR < loR) { result = loQ; break; }
        }
        if (loR <= givenNormR && givenNormR < hiR) { result = hiQ; break; }
        continue;
      }
    }
    return result;
  }

  /**
   * Gets the quantile based on the given normalized rank.
   * <ul><li><b>getQuantile(rank, INCLUSIVE) or q(r, GE)</b><br>
   * := Given r, return the quantile, q, of the smallest rank that is strictly Greater than or Equal to r.</li>
   * <li><br>getQuantile(rank, EXCLUSIVE) or q(r, GT)</b><br>
   * := Given r, return the quantile, q, of the smallest rank that is strictly Greater Than r.</li>
   * </ul>
   *
   * @param cumWeights the given natural cumulative weights. The last value must be N.
   * @param quantiles the given quantile array
   * @param givenNormR the given normalized rank, which must be in the range [0.0, 1.0], inclusive.
   * @param inclusive determines the search criterion used.
   * @return the quantile
   */
  private double getTrueDoubleQuantile(
      final long[] cumWeights,
      final double[] quantiles,
      final double givenNormR,
      final QuantileSearchCriteria inclusive) {
    final int len = cumWeights.length;
    final long N = cumWeights[len - 1];
    double result = Double.NaN;

    for (int i = 0; i < len; i++) {
      if (i == len - 1) { //at top or single element array
        double topR = (double)cumWeights[i] / N;
        double topQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (givenNormR <= topR) { result = topQ; break; }
          fail("normRank > 1.0");
        }
        //EXCLUSIVE
        if (givenNormR < topR ) { result = topQ; break; }
        if (1.0 < givenNormR) { fail("normRank > 1.0"); }
        result = Double.NaN; // R == 1.0
        break;
      }
      else { //always at least two valid entries
        double loR = (double)cumWeights[i] / N;
        double hiR = (double)cumWeights[i + 1] / N;
        double loQ = quantiles[i];
        double hiQ = quantiles[i + 1];
        if (inclusive == INCLUSIVE) {
          if (i == 0) { //at bottom, starting up
            if (givenNormR <= loR) { result = loQ; break; }
          }
          if (loR < givenNormR && givenNormR <= hiR) { result = hiQ; break; }
          continue;
        }
        //EXCLUSIVE) {
        if (i == 0) { //at bottom, starting up
          if (givenNormR < loR) { result = loQ; break; }
        }
        if (loR <= givenNormR && givenNormR < hiR) { result = hiQ; break; }
        continue;
      }
    }
    return result;
  }

  @SuppressWarnings("unused")
  /**
   * Gets the normalized rank based on the given value.
   * <ul><li><b>getRank(quantile, INCLUSIVE) or r(q, LE)</b><br>
   * := Given q, return the rank, r, of the largest quantile that is less than or equal to q.</li>
   * <li><b>getRank(quantile, EXCLUSIVE) or r(q, LT)</b>
   * := Given q, return the rank, r, of the largest quantile that is strictly Less Than q.</li>
   * </ul>
   *
   * @param cumWeights the given cumulative weights
   * @param quantiles the given quantile array
   * @param givenQ the given quantile
   * @param inclusive determines the search criterion used.
   * @return the normalized rank
   */
  private double getTrueFloatRank(
      final long[] cumWeights,
      final float[] quantiles,
      final float givenQ,
      final QuantileSearchCriteria inclusive) {
    final int len = quantiles.length;
    final long N = cumWeights[len -1];
    double result = Double.NaN;

    for (int i = len; i-- > 0; ) {
      if (i == 0) { //at bottom or single element array
        double bottomR = (double)cumWeights[i] / N;
        float bottomQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (givenQ <  bottomQ) { result = 0; break; }
          result = bottomR;
          break;
        }
        //EXCLUSIVE
        if (givenQ <= bottomQ) { result = 0; break; }
        if (bottomQ < givenQ) { result = bottomR; break; }
      }
      else { //always at least two valid entries
        double loR = (double)cumWeights[i - 1] / N;
        //double hiR = (double)cumWeights[i] / N;
        float loQ = quantiles[i - 1];
        float hiQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (i == len - 1) { //at top, starting down
            if (hiQ <= givenQ) { result = 1.0; break; }
          }
          if (loQ <= givenQ && givenQ < hiQ) { result = loR; break; }
          continue;
        }
        //EXCLUSIVE
        if (i == len - 1) { //at top, starting down
          if (hiQ < givenQ) { result = 1.0; break; }
        }
        if (loQ < givenQ && givenQ <= hiQ) { result = loR; break; }
        continue;
      }
    }
    return result;
  }

  @SuppressWarnings("unused")
  /**
   * Gets the normalized rank based on the given value.
   * <ul><li><b>getRank(quantile, INCLUSIVE) or r(q, LE)</b><br>
   * := Given q, return the rank, r, of the largest quantile that is less than or equal to q.</li>
   * <li><b>getRank(quantile, EXCLUSIVE) or r(q, LT)</b>
   * := Given q, return the rank, r, of the largest quantile that is strictly Less Than q.</li>
   * </ul>
   *
   * @param cumWeights the given cumulative weights
   * @param quantiles the given quantile array
   * @param givenQ the given quantile
   * @param inclusive determines the search criterion used.
   * @return the normalized rank
   */
  private double getTrueDoubleRank(
      final long[] cumWeights,
      final double[] quantiles,
      final double givenQ,
      final QuantileSearchCriteria inclusive) {
    final int len = quantiles.length;
    final long N = cumWeights[len -1];
    double result = Double.NaN;

    for (int i = len; i-- > 0; ) {
      if (i == 0) { //at bottom or single element array
        double bottomR = (double)cumWeights[i] / N;
        double bottomQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (givenQ <  bottomQ) { result = 0; break; }
          result = bottomR;
          break;
        }
        //EXCLUSIVE
        if (givenQ <= bottomQ) { result = 0; break; }
        if (bottomQ < givenQ) { result = bottomR; break; }
      }
      else { //always at least two valid entries
        double loR = (double)cumWeights[i - 1] / N;
        //double hiR = (double)cumWeights[i] / N;
        double loQ = quantiles[i - 1];
        double hiQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (i == len - 1) { //at top, starting down
            if (hiQ <= givenQ) { result = 1.0; break; }
          }
          if (loQ <= givenQ && givenQ < hiQ) { result = loR; break; }
          continue;
        }
        //EXCLUSIVE
        if (i == len - 1) { //at top, starting down
          if (hiQ < givenQ) { result = 1.0; break; }
        }
        if (loQ < givenQ && givenQ <= hiQ) { result = loR; break; }
        continue;
      }
    }
    return result;
  }

  /*******BUILD & LOAD SKETCHES***********/

  private void buildSketches(int set) {
    reqFloatsSk = ReqSketch.builder().setK(k).build();
    kllFloatsSk = KllFloatsSketch.newHeapInstance(k);
    kllDoublesSk = KllDoublesSketch.newHeapInstance(k);
    classicDoublesSk = DoublesSketch.builder().setK(k).build();
    int count = skFStreamValues[set].length;
    for (int i = 0; i < count; i++) {
      reqFloatsSk.update(skFStreamValues[set][i]);
      kllFloatsSk.update(skFStreamValues[set][i]);
      kllDoublesSk.update(skDStreamValues[set][i]);
      classicDoublesSk.update(skDStreamValues[set][i]);
    }
  }

  /*******BUILD & LOAD SVs***********/

  private void buildSVs(int set) throws Exception {
    reqFloatsSV = getRawReqSV(svFValues[set], svCumWeights[set], totalN[set]);
    kllFloatsSV = getRawKllFloatsSV(svFValues[set], svCumWeights[set], totalN[set]);
    kllDoublesSV = getRawKllDoublesSV(svDValues[set], svCumWeights[set], totalN[set]);
    classicDoublesSV = getRawClassicDoublesSV(svDValues[set], svCumWeights[set], totalN[set]);
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

  private final DoublesSketchSortedView getRawClassicDoublesSV(
      final double[] values, final long[] cumWeights, final long totalN) throws Exception {
    return (DoublesSketchSortedView) CLASSIC_DOUBLES_SV_CTOR.newInstance(values, cumWeights, totalN);
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

  private final static boolean enablePrinting = true;

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
