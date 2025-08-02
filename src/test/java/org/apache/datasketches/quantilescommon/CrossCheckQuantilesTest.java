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

package org.apache.datasketches.quantilescommon;

import static org.apache.datasketches.common.Util.longToFixedLengthString;
import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.quantilescommon.LinearRanksAndQuantiles.getTrueDoubleQuantile;
import static org.apache.datasketches.quantilescommon.LinearRanksAndQuantiles.getTrueDoubleRank;
import static org.apache.datasketches.quantilescommon.LinearRanksAndQuantiles.getTrueFloatQuantile;
import static org.apache.datasketches.quantilescommon.LinearRanksAndQuantiles.getTrueFloatRank;
import static org.apache.datasketches.quantilescommon.LinearRanksAndQuantiles.getTrueItemQuantile;
import static org.apache.datasketches.quantilescommon.LinearRanksAndQuantiles.getTrueItemRank;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

import static org.testng.Assert.assertEquals;

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.kll.KllItemsSketch;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.datasketches.req.ReqSketch;
import org.testng.annotations.Test;

/**
 * This test suite runs a common set of tests against all of the quantiles-type sketches in the library.
 * Although the unit tests for each of the sketches is quite extensive, the purpose of this test is to make
 * sure that key corner cases are in fact handled the same way by all of the sketches.
 *
 * <p>These tests are not about estimation accuracy, per se, as each of the different quantile sketches have very
 * different algorithms for selecting the data to be retained in the sketch and thus will have very different error
 * properties. These tests are primarily interested in making sure that the internal search and comparison algorithms
 * used within the sketches are producing the correct results for exact queries based on a chosen search
 * criteria. The search criteria are selected from the enum {@link QuantileSearchCriteria}. The corner cases of
 * interest here are to make sure that each of the search criteria behave correctly for the following.</p>
 * <ul>
 * <li>A sketch with a single value.</li>
 * <li>A sketch with two identical values<li>
 * <li>A sketch with multiple duplicates and where the duplicates have weights greater than one.
 * Note that the case with weights greater than one is only tested via the Sorted Views. The data loaded into the
 * sketches will all have weights of one.</li>
 * </ul>
 *
 * @author Lee Rhodes
 */
public class CrossCheckQuantilesTest {
  private final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
  private final Comparator<String> comparator = Comparator.naturalOrder();
  private final static int k = 32; //all sketches are in exact mode

  //These test sets are specifically designed for the corner cases mentioned in the class javadoc.
  //  Please don't mess with them  unless you know what you are doing.
  //These sets must start with 10 and be multiples of 10.
  final float[][] svFValues =
    {
      {10},                      //set 0
      {10,10},                   //set 1
      {10,20,30,40},             //set 2
      {10,20,20,30,30,30,40,50}, //set 3
      {10,10,20,20,30,30,40,40}  //set 4
    };

  final double[][] svDValues =
    {
      {10},
      {10,10},
      {10,20,30,40},
      {10,20,20,30,30,30,40,50},
      {10,10,20,20,30,30,40,40}
    };

  final String[][] svIValues =
    {
      {"10"},
      {"10","10"},
      {"10","20","30","40"},
      {"10","20","20","30","30","30","40","50"},
      {"10","10","20","20","30","30","40","40"}
    };

  //these are value weights and will be converted to cumulative.
  final long[][] svWeights =
    {
      {1},
      {1,1},
      {2,2,2,2},
      {2,2,2,2,2,2,2,2},
      {2,1,2,1,2,1,2,1}
    };

  final float[]  svMaxFValues = { 10, 10, 40, 50, 40 };
  final float[]  svMinFValues = { 10, 10, 10, 10, 10 };
  final double[] svMaxDValues = { 10, 10, 40, 50, 40 };
  final double[] svMinDValues = { 10, 10, 10, 10, 10 };
  final String[] svMaxIValues = { "10", "10", "40", "50", "40" };
  final String[] svMinIValues = { "10", "10", "10", "10", "10" };


  int numSets;

  long[][] svCumWeights;

  long[] totalN;

  float[][] skFStreamValues;
  double[][] skDStreamValues;
  String[][] skIStreamValues;

  ReqSketch reqFloatsSk = null;
  KllFloatsSketch kllFloatsSk = null;
  KllDoublesSketch kllDoublesSk = null;
  UpdateDoublesSketch classicDoublesSk = null;
  KllItemsSketch<String> kllItemsSk = null;
  ItemsSketch<String> itemsSk = null;

  FloatsSketchSortedView floatsSV = null;
  DoublesSketchSortedView doublesSV = null;
  ItemsSketchSortedView<String> classicItemsSV = null;
  ItemsSketchSortedView<String> kllItemsSV = null;

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

  private void checkGetRank(final int set, final QuantileSearchCriteria crit) {
    double trueRank;
    double testRank;

    println(LS + "FLOATS getRank Test SV vs Sk");
    final float maxFloatvalue = getMaxFloatValue(set);
    for (float v = 5f; v <= (maxFloatvalue + 5f); v += 5f) {
      trueRank = getTrueFloatRank(svCumWeights[set], svFValues[set],v, crit);

      testRank = floatsSV.getRank(v, crit);
      assertEquals(testRank, trueRank);
      testRank = reqFloatsSk.getRank(v, crit);
      assertEquals(testRank, trueRank);
      testRank = kllFloatsSk.getRank(v, crit);
      assertEquals(testRank, trueRank);

      println("Floats  set: " + set + ", value: " + v + ", rank: " + trueRank + ", crit: " + crit.toString());
    }

    println(LS + "DOUBLES getRank Test SV vs Sk");
    final double maxDoubleValue = getMaxDoubleValue(set);
    for (double v = 5; v <= (maxDoubleValue + 5); v += 5) {
      trueRank = getTrueDoubleRank(svCumWeights[set], svDValues[set],v, crit);

      testRank = doublesSV.getRank(v, crit);
      assertEquals(testRank, trueRank);
      testRank = kllDoublesSk.getRank(v, crit);
      assertEquals(testRank, trueRank);
      testRank = classicDoublesSk.getRank(v, crit);
      assertEquals(testRank, trueRank);

      println("Doubles set: " + set + ", value: " + v + ", rank: " + trueRank + ", crit: " + crit.toString());
    }

    println(LS + "ITEMS getRank Test SV vs Sk");
    int maxItemValue;
    try { maxItemValue = Integer.parseInt(getMaxItemValue(set)); }
    catch (final NumberFormatException e) { throw new SketchesArgumentException(e.toString()); }
    for (int v = 5; v <= (maxItemValue + 5); v += 5) {
      final String s = longToFixedLengthString(v, 2);
      trueRank = getTrueItemRank(svCumWeights[set], svIValues[set], s, crit, comparator);

      testRank = kllItemsSV.getRank(s, crit);
      assertEquals(testRank, trueRank);
      testRank = kllItemsSk.getRank(s, crit);
      assertEquals(testRank, trueRank);
      testRank = classicItemsSV.getRank(s, crit);
      assertEquals(testRank, trueRank);
      testRank = itemsSk.getRank(s, crit);
      assertEquals(testRank, trueRank);

      println("Items set: " + set + ", value: " + s + ", rank: " + trueRank + ", crit: " + crit.toString());
    }
  }

  private void checkGetQuantile(final int set, final QuantileSearchCriteria crit) {
    final int twoN = (int)totalN[set] * 2;
    final double dTwoN = twoN;
    float trueFQ;
    float testFQ;

    println(LS + "FLOATS getQuantile Test SV vs Sk");
    for (int i = 0; i <= twoN; i++) {
      final double normRank = i / dTwoN;
      trueFQ = getTrueFloatQuantile(svCumWeights[set], svFValues[set], normRank, crit);

      testFQ = floatsSV.getQuantile(normRank, crit);
      assertEquals(testFQ, trueFQ);
      testFQ = reqFloatsSk.getQuantile(normRank, crit);
      assertEquals(testFQ, trueFQ);
      testFQ = kllFloatsSk.getQuantile(normRank, crit);
      assertEquals(testFQ, trueFQ);

      println("Floats  set: " + set + ", rank: " + normRank + ", Q: " + trueFQ + ", crit: " + crit.toString());
    }

    println(LS + "DOUBLES getQuantile Test SV vs Sk");
    double trueDQ;
    double testDQ;
    for (int i = 0; i <= twoN; i++) {
      final double normRank = i / dTwoN;
      trueDQ = getTrueDoubleQuantile(svCumWeights[set], svDValues[set], normRank, crit);

      testDQ = doublesSV.getQuantile(normRank, crit);
      assertEquals(testDQ, trueDQ);
      testDQ = kllDoublesSk.getQuantile(normRank, crit);
      assertEquals(testDQ, trueDQ);
      testDQ = classicDoublesSk.getQuantile(normRank, crit);
      assertEquals(testDQ, trueDQ);

      println("Doubles set: " + set + ", rank: " + normRank + ", Q: " + trueDQ + ", crit: " + crit.toString());
    }

    println(LS + "ITEMS getQuantile Test SV vs Sk");
    String trueIQ;
    String testIQ;
    for (int i = 0; i <= twoN; i++) {
      final double normRank = i / dTwoN;
      trueIQ = getTrueItemQuantile(svCumWeights[set], svIValues[set], normRank, crit);

      testIQ = kllItemsSV.getQuantile(normRank, crit);
      assertEquals(testIQ, trueIQ);
      testIQ = kllItemsSk.getQuantile(normRank, crit);
      assertEquals(testIQ, trueIQ);
      testIQ = classicItemsSV.getQuantile(normRank, crit);
      assertEquals(testIQ, trueIQ);
      testIQ = itemsSk.getQuantile(normRank, crit);
      assertEquals(testIQ, trueIQ);

      println("Items set: " + set + ", rank: " + normRank + ", Q: " + trueIQ + ", crit: " + crit.toString());
    }
  }

  private double getMaxDoubleValue(final int set) {
    final int streamLen = skDStreamValues[set].length;
    return skDStreamValues[set][streamLen -1];
  }

  private float getMaxFloatValue(final int set) {
    final int streamLen = skFStreamValues[set].length;
    return skFStreamValues[set][streamLen -1];
  }

  private String getMaxItemValue(final int set) {
    final int streamLen = skIStreamValues[set].length;
    return skIStreamValues[set][streamLen -1];
  }

  /*******BUILD & LOAD SKETCHES***********/

  private void buildSketches(final int set) {
    reqFloatsSk = ReqSketch.builder().setK(k).build();
    kllFloatsSk = KllFloatsSketch.newHeapInstance(k);
    kllDoublesSk = KllDoublesSketch.newHeapInstance(k);
    classicDoublesSk = DoublesSketch.builder().setK(k).build();
    kllItemsSk = KllItemsSketch.newHeapInstance(k, comparator, serDe);
    itemsSk = ItemsSketch.getInstance(String.class, k, comparator);

    final int count = skFStreamValues[set].length;
    for (int i = 0; i < count; i++) {
      reqFloatsSk.update(skFStreamValues[set][i]);
      kllFloatsSk.update(skFStreamValues[set][i]);
      kllDoublesSk.update(skDStreamValues[set][i]);
      classicDoublesSk.update(skDStreamValues[set][i]);
      kllItemsSk.update(skIStreamValues[set][i]);
      itemsSk.update(skIStreamValues[set][i]);
    }
  }

  /*******BUILD & LOAD SVs***********/

  private void buildSVs(final int set) throws Exception {
    floatsSV = new FloatsSketchSortedView(svFValues[set], svCumWeights[set], totalN[set],
        svMaxFValues[set], svMinFValues[set]);
    doublesSV = new DoublesSketchSortedView(svDValues[set], svCumWeights[set], totalN[set],
        svMaxDValues[set], svMinDValues[set]);
    final String svImax = svIValues[set][svIValues[set].length - 1];
    final String svImin = svIValues[set][0];

    kllItemsSV = new ItemsSketchSortedView<>(svIValues[set], svCumWeights[set], totalN[set],
        comparator, svImax, svImin, String.class, .01, svCumWeights[set].length);

    classicItemsSV = new ItemsSketchSortedView<>(svIValues[set], svCumWeights[set], totalN[set],
        comparator, svImax, svImin, String.class, .01, svCumWeights[set].length);
  }

  /********BUILD DATA SETS**********/

  private void buildDataSets() {
    numSets = svWeights.length;
    svCumWeights = new long[numSets][];
    totalN = new long[numSets];
    skFStreamValues = new float[numSets][];
    skDStreamValues = new double[numSets][];
    skIStreamValues = new String[numSets][];
    for (int i = 0; i < numSets; i++) {
      svCumWeights[i] = convertToCumWeights(svWeights[i]);
      final int len = svCumWeights[i].length;
      final int totalCount = (int)svCumWeights[i][len -1];
      totalN[i] = totalCount;
      skFStreamValues[i] = convertToFloatStream(svFValues[i], svWeights[i], totalCount);
      skDStreamValues[i] = convertToDoubleStream(svDValues[i], svWeights[i], totalCount);
      skIStreamValues[i] = convertToItemStream(svIValues[i], svWeights[i], totalCount);
    }
    println("");
  }

  private static float[] convertToFloatStream(
      final float[] svFValueArr,
      final long[] svWeightsArr,
      final int totalCount) {
    final float[] out = new float[totalCount];
    final int len = svWeightsArr.length;
    int i = 0;
    for (int j = 0; j < len; j++) {
      final float f = svFValueArr[j];
      final int wt = (int)svWeightsArr[j];
      for (int w = 0; w < wt; w++) {
        out[i++] = f;
      }
    }
    return out;
  }

  private static double[] convertToDoubleStream(
      final double[] svDValueArr,
      final long[] svWeightsArr,
      final int totalCount) {
    final double[] out = new double[totalCount];
    final int len = svWeightsArr.length;
    int i = 0;
    for (int j = 0; j < len; j++) {
      final double d = svDValueArr[j];
      final int wt = (int)svWeightsArr[j];
      for (int w = 0; w < wt; w++) {
        out[i++] = d;
      }
    }
    return out;
  }

  private static String[] convertToItemStream(
      final String[] svIValueArr,
      final long[] svWeightsArr,
      final int totalCount) {
    final String[] out = new String[totalCount];
    final int len = svWeightsArr.length;
    int i = 0;
    for (int j = 0; j < len; j++) {
      final String s = svIValueArr[j];
      final int wt = (int)svWeightsArr[j];
      for (int w = 0; w < wt; w++) {
        out[i++] = s;
      }
    }
    return out;
  }

  private static long[] convertToCumWeights(final long[] weights) {
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
