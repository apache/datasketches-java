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

package org.apache.datasketches.kll;

import static java.lang.Math.ceil;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.*;
import static org.apache.datasketches.kll.KllSketch.SketchType.*;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.kll.KllSketch.SketchType;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.DoublesSortedView;
import org.apache.datasketches.quantilescommon.GenericSortedView;
import org.apache.datasketches.quantilescommon.GenericSortedViewIterator;
import org.testng.annotations.Test;

@SuppressWarnings("unused")
public class KllItemsSketchTest {
  private static final double PMF_EPS_FOR_K_8 = 0.35; // PMF rank error (epsilon) for k=8
  private static final double PMF_EPS_FOR_K_128 = 0.025; // PMF rank error (epsilon) for k=128
  private static final double PMF_EPS_FOR_K_256 = 0.013; // PMF rank error (epsilon) for k=256
  private static final double NUMERIC_NOISE_TOLERANCE = 1E-6;
  public ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

  @Test
  public void empty() {
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sketch.update(null); // this must not change anything
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getNumRetained(), 0);
    try { sketch.getRank("", INCLUSIVE); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getQuantile(0.5); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getQuantiles(new double[] {0}); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getPMF(new String[] {""}); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getCDF(new String[] {""}); fail(); } catch (SketchesArgumentException e) {}
    assertNotNull(sketch.toString(true, true));
    assertNotNull(sketch.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantileInvalidArg() {
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sketch.update("A");
    sketch.getQuantile(-1.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantilesInvalidArg() {
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sketch.update("A");
    sketch.getQuantiles(new double[] {2.0});
  }

  @Test
  public void oneValue() {
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sketch.update("A");
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 1);
    assertEquals(sketch.getNumRetained(), 1);
    assertEquals(sketch.getRank("A", EXCLUSIVE), 0.0);
    assertEquals(sketch.getRank("B", EXCLUSIVE), 1.0);
    assertEquals(sketch.getRank("A", EXCLUSIVE), 0.0);
    assertEquals(sketch.getRank("B", EXCLUSIVE), 1.0);
    assertEquals(sketch.getRank("@", INCLUSIVE), 0.0);
    assertEquals(sketch.getRank("A", INCLUSIVE), 1.0);
    assertEquals(sketch.getMinItem(),"A");
    assertEquals(sketch.getMaxItem(), "A");
    assertEquals(sketch.getQuantile(0.5, EXCLUSIVE), "A");
    assertEquals(sketch.getQuantile(0.5, INCLUSIVE), "A");
  }

  @Test
  public void tenValues() {
    final String[] tenStr = {"A","B","C","D","E","F","G","H","I","J"};
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= 10; i++) { sketch.update(tenStr[i - 1]); }
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 10);
    assertEquals(sketch.getNumRetained(), 10);
    for (int i = 1; i <= 10; i++) {
      assertEquals(sketch.getRank(tenStr[i - 1], EXCLUSIVE), (i - 1) / 10.0);
      assertEquals(sketch.getRank(tenStr[i - 1], INCLUSIVE), i / 10.0);
    }
    final String[] qArr = tenStr;
    double[] rOut = sketch.getRanks(qArr); //inclusive
    for (int i = 0; i < qArr.length; i++) {
      assertEquals(rOut[i], (i + 1) / 10.0);
    }
    rOut = sketch.getRanks(qArr, EXCLUSIVE); //exclusive
    for (int i = 0; i < qArr.length; i++) {
      assertEquals(rOut[i], i / 10.0);
    }

    for (int i = 0; i <= 10; i++) {
      double rank = i/10.0;
      String q = rank == 1.0 ? tenStr[i-1] : tenStr[i];
      assertEquals(sketch.getQuantile(rank, EXCLUSIVE), q);
      q = rank == 0 ? tenStr[i] : tenStr[i - 1];
      assertEquals(sketch.getQuantile(rank, INCLUSIVE), q);
    }

    {
      // getQuantile() and getQuantiles() equivalence EXCLUSIVE
      final String[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, EXCLUSIVE);
      for (int i = 0; i <= 10; i++) {
        assertEquals(sketch.getQuantile(i / 10.0, EXCLUSIVE), quantiles[i]);
      }
    }
    {
      // getQuantile() and getQuantiles() equivalence INCLUSIVE
      final String[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1}, INCLUSIVE);
      for (int i = 0; i <= 10; i++) {
        assertEquals(sketch.getQuantile(i / 10.0, INCLUSIVE), quantiles[i]);
      }
    }
  }

  @Test
  public void manyValuesEstimationMode() {
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final int n = 1_000_000;
    final int digits = Util.numDigits(n);

    for (int i = 1; i <= n; i++) {
      sketch.update(Util.intToFixedLengthString(i, digits));
      assertEquals(sketch.getN(), i);
    }

    // test getRank
    for (int i = 1; i <= n; i++) {
      final double trueRank = (double) i / n;
      String s = Util.intToFixedLengthString(i, digits);
      double r = sketch.getRank(s);
      assertEquals(r, trueRank, PMF_EPS_FOR_K_256, "for value " + s);
    }

    // test getPMF
    String s = Util.intToFixedLengthString(n/2, digits);
    final double[] pmf = sketch.getPMF(new String[] {s}); // split at median
    assertEquals(pmf.length, 2);
    assertEquals(pmf[0], 0.5, PMF_EPS_FOR_K_256);
    assertEquals(pmf[1], 0.5, PMF_EPS_FOR_K_256);

    assertEquals(sketch.getMinItem(), Util.intToFixedLengthString(1, digits));
    assertEquals(sketch.getMaxItem(), Util.intToFixedLengthString(n, digits));

 // check at every 0.1 percentage point
    final double[] fractions = new double[1001];
    final double[] reverseFractions = new double[1001]; // check that ordering doesn't matter
    for (int i = 0; i <= 1000; i++) {
      fractions[i] = (double) i / 1000;
      reverseFractions[1000 - i] = fractions[i];
    }
    final String[] quantiles = sketch.getQuantiles(fractions);
    final String[] reverseQuantiles = sketch.getQuantiles(reverseFractions);
    String previousQuantile = "";
    for (int i = 0; i <= 1000; i++) {
      final String quantile = sketch.getQuantile(fractions[i]);
      assertEquals(quantile, quantiles[i]);
      assertEquals(quantile, reverseQuantiles[1000 - i]);
      assertTrue(Util.le(previousQuantile, quantile, Comparator.naturalOrder()));
      previousQuantile = quantile;
    }
  }

  @Test
  public void getRankGetCdfGetPmfConsistency() {
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final int n = 1000;
    final int digits = Util.numDigits(n);
    final String[] quantiles = new String[n];
    for (int i = 0; i < n; i++) {
      final String str = Util.intToFixedLengthString(i, digits);
      sketch.update(str);
      quantiles[i] = str;
    }
    { //EXCLUSIVE
      final double[] ranks = sketch.getCDF(quantiles, EXCLUSIVE);
      final double[] pmf = sketch.getPMF(quantiles, EXCLUSIVE);
      double sumPmf = 0;
      for (int i = 0; i < n; i++) {
        assertEquals(ranks[i], sketch.getRank(quantiles[i], EXCLUSIVE), NUMERIC_NOISE_TOLERANCE, "rank vs CDF for value " + i);
        sumPmf += pmf[i];
        assertEquals(ranks[i], sumPmf, NUMERIC_NOISE_TOLERANCE, "CDF vs PMF for value " + i);
      }
      sumPmf += pmf[n];
      assertEquals(sumPmf, 1.0, NUMERIC_NOISE_TOLERANCE);
      assertEquals(ranks[n], 1.0, NUMERIC_NOISE_TOLERANCE);
    }
    { // INCLUSIVE (default)
      final double[] ranks = sketch.getCDF(quantiles, INCLUSIVE);
      final double[] pmf = sketch.getPMF(quantiles, INCLUSIVE);
      double sumPmf = 0;
      for (int i = 0; i < n; i++) {
        assertEquals(ranks[i], sketch.getRank(quantiles[i], INCLUSIVE), NUMERIC_NOISE_TOLERANCE,
            "rank vs CDF for value " + i);
        sumPmf += pmf[i];
        assertEquals(ranks[i], sumPmf, NUMERIC_NOISE_TOLERANCE, "CDF vs PMF for value " + i);
      }
      sumPmf += pmf[n];
      assertEquals(sumPmf, 1.0, NUMERIC_NOISE_TOLERANCE);
      assertEquals(ranks[n], 1.0, NUMERIC_NOISE_TOLERANCE);
    }
  }

  @Test
  public void merge() {
    final KllItemsSketch<String> sketch1 = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final KllItemsSketch<String> sketch2 = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final int n = 10000;
    final int digits = Util.numDigits(2 * n);
    for (int i = 0; i < n; i++) {
      sketch1.update(Util.intToFixedLengthString(i, digits));
      sketch2.update(Util.intToFixedLengthString(2 * n - i - 1, digits));
    }

    assertEquals(sketch1.getMinItem(), Util.intToFixedLengthString(0, digits));
    assertEquals(sketch1.getMaxItem(), Util.intToFixedLengthString(n - 1, digits));

    assertEquals(sketch2.getMinItem(), Util.intToFixedLengthString(n, digits));
    assertEquals(sketch2.getMaxItem(), Util.intToFixedLengthString(2 * n - 1, digits));

    sketch1.merge(sketch2);

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2L * n);
    assertEquals(sketch1.getMinItem(), Util.intToFixedLengthString(0, digits));
    assertEquals(sketch1.getMaxItem(), Util.intToFixedLengthString(2 * n - 1, digits));
    String upperBound = Util.intToFixedLengthString(n + (int)ceil(n * PMF_EPS_FOR_K_256), digits);
    String lowerBound = Util.intToFixedLengthString(n - (int)ceil(n * PMF_EPS_FOR_K_256), digits);
    String median = sketch1.getQuantile(0.5);
    assertTrue(Util.le(median, upperBound, Comparator.naturalOrder()));
    assertTrue(Util.le(lowerBound, median, Comparator.naturalOrder()));
  }

  @Test
  public void mergeLowerK() {
    final KllItemsSketch<String> sketch1 = KllItemsSketch.newHeapInstance(256, Comparator.naturalOrder(), serDe);
    final KllItemsSketch<String> sketch2 = KllItemsSketch.newHeapInstance(128, Comparator.naturalOrder(), serDe);
    final int n = 10000;
    final int digits = Util.numDigits(2 * n);
    for (int i = 0; i < n; i++) {
      sketch1.update(Util.intToFixedLengthString(i, digits));
      sketch2.update(Util.intToFixedLengthString(2 * n - i - 1, digits));
    }

    assertEquals(sketch1.getMinItem(), Util.intToFixedLengthString(0, digits));
    assertEquals(sketch1.getMaxItem(), Util.intToFixedLengthString(n - 1, digits));

    assertEquals(sketch2.getMinItem(), Util.intToFixedLengthString(n, digits));
    assertEquals(sketch2.getMaxItem(), Util.intToFixedLengthString(2 * n - 1, digits));

    assertTrue(sketch1.getNormalizedRankError(false) < sketch2.getNormalizedRankError(false));
    assertTrue(sketch1.getNormalizedRankError(true) < sketch2.getNormalizedRankError(true));
    sketch1.merge(sketch2);

    // sketch1 must get "contaminated" by the lower K in sketch2
    assertEquals(sketch1.getNormalizedRankError(false), sketch2.getNormalizedRankError(false));
    assertEquals(sketch1.getNormalizedRankError(true), sketch2.getNormalizedRankError(true));

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2 * n);
    assertEquals(sketch1.getMinItem(), Util.intToFixedLengthString(0, digits));
    assertEquals(sketch1.getMaxItem(), Util.intToFixedLengthString(2 * n - 1, digits));
    String upperBound = Util.intToFixedLengthString(n + (int)ceil(2 * n * PMF_EPS_FOR_K_128), digits);
    String lowerBound = Util.intToFixedLengthString(n - (int)ceil(2 * n * PMF_EPS_FOR_K_128), digits);
    String median = sketch1.getQuantile(0.5);
    assertTrue(Util.le(median, upperBound, Comparator.naturalOrder()));
    assertTrue(Util.le(lowerBound, median, Comparator.naturalOrder()));
  }

  @Test
  public void mergeEmptyLowerK() {
    final KllItemsSketch<String> sketch1 = KllItemsSketch.newHeapInstance(256, Comparator.naturalOrder(), serDe);
    final KllItemsSketch<String> sketch2 = KllItemsSketch.newHeapInstance(128, Comparator.naturalOrder(), serDe);
    final int n = 10000;
    final int digits = Util.numDigits(n);
    for (int i = 0; i < n; i++) {
      sketch1.update(Util.intToFixedLengthString(i, digits)); //sketch2 is empty
    }

    // rank error should not be affected by a merge with an empty sketch with lower K
    final double rankErrorBeforeMerge = sketch1.getNormalizedRankError(true);
    sketch1.merge(sketch2);
    assertEquals(sketch1.getNormalizedRankError(true), rankErrorBeforeMerge);
    {
    assertFalse(sketch1.isEmpty());
    assertTrue(sketch2.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch1.getMinItem(), Util.intToFixedLengthString(0, digits));
    assertEquals(sketch1.getMaxItem(), Util.intToFixedLengthString(n - 1, digits));
    String upperBound = Util.intToFixedLengthString(n / 2 + (int)ceil(n * PMF_EPS_FOR_K_256), digits);
    String lowerBound = Util.intToFixedLengthString(n / 2 - (int)ceil(n * PMF_EPS_FOR_K_256), digits);
    String median = sketch1.getQuantile(0.5);
    assertTrue(Util.le(median, upperBound, Comparator.naturalOrder()));
    assertTrue(Util.le(lowerBound, median, Comparator.naturalOrder()));
    }
    {
    //merge the other way
    sketch2.merge(sketch1);
    assertFalse(sketch1.isEmpty());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch2.getN(), n);
    assertEquals(sketch1.getMinItem(), Util.intToFixedLengthString(0, digits));
    assertEquals(sketch1.getMaxItem(), Util.intToFixedLengthString(n - 1, digits));
    assertEquals(sketch2.getMinItem(), Util.intToFixedLengthString(0, digits));
    assertEquals(sketch2.getMaxItem(), Util.intToFixedLengthString(n - 1, digits));
    String upperBound = Util.intToFixedLengthString(n / 2 + (int)ceil(n * PMF_EPS_FOR_K_128), digits);
    String lowerBound = Util.intToFixedLengthString(n / 2 - (int)ceil(n * PMF_EPS_FOR_K_128), digits);
    String median = sketch2.getQuantile(0.5);
    assertTrue(Util.le(median, upperBound, Comparator.naturalOrder()));
    assertTrue(Util.le(lowerBound, median, Comparator.naturalOrder()));
    }
  }

  @Test
  public void mergeExactModeLowerK() {
    final KllItemsSketch<String> sketch1 = KllItemsSketch.newHeapInstance(256, Comparator.naturalOrder(), serDe);
    final KllItemsSketch<String> sketch2 = KllItemsSketch.newHeapInstance(128, Comparator.naturalOrder(), serDe);
    final int n = 10000;
    final int digits = Util.numDigits(n);
    for (int i = 0; i < n; i++) {
      sketch1.update(Util.intToFixedLengthString(i, digits));
    }
    sketch2.update(Util.intToFixedLengthString(1, digits));

    // rank error should not be affected by a merge with a sketch in exact mode with lower K
    final double rankErrorBeforeMerge = sketch1.getNormalizedRankError(true);
    sketch1.merge(sketch2);
    assertEquals(sketch1.getNormalizedRankError(true), rankErrorBeforeMerge);
  }

  @Test
  public void mergeMinMinValueFromOther() {
    final KllItemsSketch<String> sketch1 = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final KllItemsSketch<String> sketch2 = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sketch1.update(Util.intToFixedLengthString(1, 1));
    sketch2.update(Util.intToFixedLengthString(2, 1));
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinItem(), Util.intToFixedLengthString(1, 1));
  }

  @Test
  public void mergeMinAndMaxFromOther() {
    final KllItemsSketch<String> sketch1 = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final KllItemsSketch<String> sketch2 = KllItemsSketch.newHeapInstance(10, Comparator.naturalOrder(), serDe);
    final int n = 1_000_000;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= 1_000_000; i++) {
      sketch1.update(Util.intToFixedLengthString(i, digits)); //sketch2 is empty
    }
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinItem(), Util.intToFixedLengthString(1, digits));
    assertEquals(sketch2.getMaxItem(), Util.intToFixedLengthString(n, digits));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooSmall() {
    KllItemsSketch.newHeapInstance(KllSketch.DEFAULT_M - 1, Comparator.naturalOrder(), serDe);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooLarge() {
    KllItemsSketch.newHeapInstance(KllSketch.MAX_K + 1, Comparator.naturalOrder(), serDe);
  }

  @Test
  public void minK() {
    final KllItemsSketch<String> sketch =
        KllItemsSketch.newHeapInstance(KllSketch.DEFAULT_M,Comparator.naturalOrder(), serDe);
    final int n = 1000;
    final int digits = Util.numDigits(n);
    for (int i = 0; i < n; i++) {
      sketch.update(Util.intToFixedLengthString(i, digits));
    }
    assertEquals(sketch.getK(), KllSketch.DEFAULT_M);
    String upperBound = Util.intToFixedLengthString(n / 2 + (int)ceil(n * PMF_EPS_FOR_K_8), digits);
    String lowerBound = Util.intToFixedLengthString(n / 2 - (int)ceil(n * PMF_EPS_FOR_K_8), digits);
    String median = sketch.getQuantile(0.5);
    assertTrue(Util.le(median, upperBound, Comparator.naturalOrder()));
    assertTrue(Util.le(lowerBound, median, Comparator.naturalOrder()));
  }

  @Test
  public void maxK() {
    final KllItemsSketch<String> sketch =
        KllItemsSketch.newHeapInstance(KllSketch.MAX_K,Comparator.naturalOrder(), serDe);
    final int n = 1000;
    final int digits = Util.numDigits(n);
    for (int i = 0; i < n; i++) {
      sketch.update(Util.intToFixedLengthString(i, digits));
    }
    assertEquals(sketch.getK(), KllSketch.MAX_K);
    String upperBound = Util.intToFixedLengthString(n / 2 + (int)ceil(n * PMF_EPS_FOR_K_256), digits);
    String lowerBound = Util.intToFixedLengthString(n / 2 - (int)ceil(n * PMF_EPS_FOR_K_256), digits);
    String median = sketch.getQuantile(0.5);
    assertTrue(Util.le(median, upperBound, Comparator.naturalOrder()));
    assertTrue(Util.le(lowerBound, median, Comparator.naturalOrder()));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void outOfOrderSplitPoints() {
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final String s0 = Util.intToFixedLengthString(0, 1);
    final String s1 = Util.intToFixedLengthString(1, 1);
    sketch.update(s0);
    sketch.getCDF(new String[] {s1, s0});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void nullSplitPoint() {
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sketch.update(Util.intToFixedLengthString(0, 1));
    sketch.getCDF(new String[] {null});
  }

  @Test
  public void getQuantiles() {
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sketch.update("A");
    sketch.update("B");
    sketch.update("C");
    sketch.update("D");
    String[] quantiles1 = sketch.getQuantiles(new double[] {0.0, 0.5, 1.0}, EXCLUSIVE);
    String[] quantiles2 = sketch.getPartitionBoundaries(2, EXCLUSIVE).boundaries;
    assertEquals(quantiles1, quantiles2);
    quantiles1 = sketch.getQuantiles(new double[] {0.0, 0.5, 1.0}, INCLUSIVE);
    quantiles2 = sketch.getPartitionBoundaries(2, INCLUSIVE).boundaries;
    assertEquals(quantiles1, quantiles2);
  }

  @Test
  public void checkReset() {
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    final int n = 100;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= n; i++) { sketch.update(Util.intToFixedLengthString(i, digits)); }
    long n1 = sketch.getN();
    String min1 = sketch.getMinItem();
    String max1 = sketch.getMaxItem();
    sketch.reset();
    for (int i = 1; i <= 100; i++) { sketch.update(Util.intToFixedLengthString(i, digits)); }
    long n2 = sketch.getN();
    String min2 = sketch.getMinItem();
    String max2 = sketch.getMaxItem();
    assertEquals(n2, n1);
    assertEquals(min2, min1);
    assertEquals(max2, max1);
  }

  @Test
  public void checkReadOnlyUpdate() {
    KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    Memory mem = Memory.wrap(sk1.toByteArray());
    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(mem, Comparator.naturalOrder(), serDe);
    try { sk2.update("A"); fail(); } catch (SketchesArgumentException e) { }
  }

  @Test
  public void checkNewDirectInstanceAndSmallSize() {
    KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    Memory mem = Memory.wrap(sk1.toByteArray());
    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(mem, Comparator.naturalOrder(), serDe);
    int sizeBytes = sk2.currentSerializedSizeBytes(false);
    assertEquals(sizeBytes, 8);

    sk1.update("A");
    mem = Memory.wrap(sk1.toByteArray());
    sk2 = KllItemsSketch.wrap(mem, Comparator.naturalOrder(), serDe);
    sizeBytes = sk2.currentSerializedSizeBytes(false);
    assertEquals(sizeBytes, 8 + 5);

    sk1.update("B");
    mem = Memory.wrap(sk1.toByteArray());
    sk2 = KllItemsSketch.wrap(mem, Comparator.naturalOrder(), serDe);
    sizeBytes = sk2.currentSerializedSizeBytes(false);
    assertEquals(sizeBytes, 20 + 4 + 2 * 5 + 2 * 5);
  }

  @Test
  public void sortedView() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    sk.update("A");
    sk.update("AB");
    sk.update("ABC");

    final GenericSortedView<String> view = sk.getSortedView();
    final GenericSortedViewIterator<String> itr = view.iterator();
    assertEquals(itr.next(), true);
    assertEquals(itr.getQuantile(), "A");
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getCumulativeWeight(EXCLUSIVE), 0);
    assertEquals(itr.getCumulativeWeight(INCLUSIVE), 1);
    assertEquals(itr.next(), true);
    assertEquals(itr.getQuantile(), "AB");
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getCumulativeWeight(EXCLUSIVE), 1);
    assertEquals(itr.getCumulativeWeight(INCLUSIVE), 2);
    assertEquals(itr.next(), true);
    assertEquals(itr.getQuantile(), "ABC");
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getCumulativeWeight(EXCLUSIVE), 2);
    assertEquals(itr.getCumulativeWeight(INCLUSIVE), 3);
    assertEquals(itr.next(), false);
  }

  @Test //also visual
  public void checkCDF_PDF() {
    final double[] cdfI = {.25, .50, .75, 1.0, 1.0 };
    final double[] cdfE = {0.0, .25, .50, .75, 1.0 };
    final double[] pmfI = {.25, .25, .25, .25, 0.0 };
    final double[] pmfE = {0.0, .25, .25, .25, .25 };
    final double toll = 1E-10;
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    final String[] strIn = {"A", "AB", "ABC", "ABCD"};
    for (int i = 0; i < strIn.length; i++) { sketch.update(strIn[i]); }
    String[] sp = new String[] {"A", "AB", "ABC", "ABCD"};
    println("SplitPoints:");
    for (int i = 0; i < sp.length; i++) {
      printf("%10s", sp[i]);
    }
    println("");
    println("INCLUSIVE:");
    double[] cdf = sketch.getCDF(sp, INCLUSIVE);
    double[] pmf = sketch.getPMF(sp, INCLUSIVE);
    printf("%10s%10s\n", "CDF", "PMF");
    for (int i = 0; i < cdf.length; i++) {
      printf("%10.2f%10.2f\n", cdf[i], pmf[i]);
      assertEquals(cdf[i], cdfI[i], toll);
      assertEquals(pmf[i], pmfI[i], toll);
    }
    println("EXCLUSIVE");
    cdf = sketch.getCDF(sp, EXCLUSIVE);
    pmf = sketch.getPMF(sp, EXCLUSIVE);
    printf("%10s%10s\n", "CDF", "PMF");
    for (int i = 0; i < cdf.length; i++) {
      printf("%10.2f%10.2f\n", cdf[i], pmf[i]);
      assertEquals(cdf[i], cdfE[i], toll);
      assertEquals(pmf[i], pmfE[i], toll);
    }
  }

  @Test
  public void checkWrapCase1Floats() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    final int n = 21;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= n; i++) { sk.update(Util.intToFixedLengthString(i, digits)); }

    Memory mem = Memory.wrap(sk.toByteArray());
    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(mem, Comparator.naturalOrder(), serDe);

    assertTrue(mem.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

  @Test
  public void checkReadOnlyExceptions() {
    int[] intArr = new int[0];
    int intV = 2;
    int idx = 1;
    KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    Memory mem = Memory.wrap(sk1.toByteArray());
    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(mem, Comparator.naturalOrder(), serDe);
    try { sk2.setLevelsArray(intArr);              fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setLevelsArrayAt(idx,intV);          fail(); } catch (SketchesArgumentException e) { }
  }

  @Test
  public void checkIsSameResource() {
    int cap = 128;
    WritableMemory wmem = WritableMemory.allocate(cap);
    WritableMemory reg1 = wmem.writableRegion(0, 64);
    WritableMemory reg2 = wmem.writableRegion(64, 64);
    assertFalse(reg1 == reg2);
    assertFalse(reg1.isSameResource(reg2));

    WritableMemory reg3 = wmem.writableRegion(0, 64);
    assertFalse(reg1 == reg3);
    assertTrue(reg1.isSameResource(reg3));

    byte[] byteArr1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe).toByteArray();
    reg1.putByteArray(0, byteArr1, 0, byteArr1.length);
    KllItemsSketch<String> sk1 = KllItemsSketch.wrap(reg1, Comparator.naturalOrder(), serDe);

    byte[] byteArr2 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe).toByteArray();
    reg2.putByteArray(0, byteArr2, 0, byteArr2.length);
    assertFalse(sk1.isSameResource(reg2));

    byte[] byteArr3 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe).toByteArray();
    reg3.putByteArray(0, byteArr3, 0, byteArr3.length);
    assertTrue(sk1.isSameResource(reg3));
  }

  // New added tests specially for KllItemsSketch
  @Test
  public void checkHeapifyEmpty() {
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    Memory mem = Memory.wrap(sk1.toByteArray());
    KllMemoryValidate memVal = new KllMemoryValidate(mem, SketchType.ITEMS_SKETCH, serDe);
    assertEquals(memVal.sketchStructure, COMPACT_EMPTY);
    assertEquals(mem.getCapacity(), 8);
    final KllItemsSketch<String> sk2 = KllItemsSketch.heapify(mem, Comparator.naturalOrder(), serDe);
    assertEquals(sk2.sketchStructure, UPDATABLE);
    assertEquals(sk2.getN(), 0);
    assertFalse(sk2.isReadOnly());
    try { sk2.getMinItem(); fail(); } catch (SketchesArgumentException e) { }
    try { sk2.getMaxItem(); fail(); } catch (SketchesArgumentException e) { }
    println(sk1.toString(true, true));
    println("");
    println(KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe));
  }

  @Test
  public void checkHeapifySingleItem() {
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    sk1.update("A");
    Memory mem = Memory.wrap(sk1.toByteArray());
    KllMemoryValidate memVal = new KllMemoryValidate(mem, SketchType.ITEMS_SKETCH, serDe);
    assertEquals(memVal.sketchStructure, COMPACT_SINGLE);
    assertEquals(mem.getCapacity(), memVal.sketchBytes);
    final KllItemsSketch<String> sk2 = KllItemsSketch.heapify(mem, Comparator.naturalOrder(), serDe);
    assertEquals(sk2.sketchStructure, UPDATABLE);
    assertEquals(sk2.getN(), 1);
    assertFalse(sk2.isReadOnly());
    assertEquals(sk2.getMinItem(), "A");
    assertEquals(sk2.getMaxItem(), "A");
    println(sk1.toString(true, true));
    println("");
    println(KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe));
  }

  @Test
  public void checkHeapifyFewItems() {
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    sk1.update("A");
    sk1.update("AB");
    sk1.update("ABC");
    Memory mem = Memory.wrap(sk1.toByteArray());
    KllMemoryValidate memVal = new KllMemoryValidate(mem, SketchType.ITEMS_SKETCH, serDe);
    assertEquals(memVal.sketchStructure, COMPACT_FULL);
    assertEquals(mem.getCapacity(), memVal.sketchBytes);
    println(sk1.toString(true, true));
    println("");
    println(KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe));
  }

  @Test
  public void checkHeapifyManyItems() {
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    final int n = 109;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= n; i++) {
      sk1.update(Util.intToFixedLengthString(i, digits));
    }
    Memory mem = Memory.wrap(sk1.toByteArray());
    KllMemoryValidate memVal = new KllMemoryValidate(mem, SketchType.ITEMS_SKETCH, serDe);
    assertEquals(memVal.sketchStructure, COMPACT_FULL);
    assertEquals(mem.getCapacity(), memVal.sketchBytes);
    println(sk1.toString(true, true));
    println("");
    println(KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe));
  }

  @Test
  public void checkWrapCausingLevelsCompaction() {
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    final int n = 109;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= n; i++) {
      sk1.update(Util.intToFixedLengthString(i, digits));
    }
    Memory mem = Memory.wrap(sk1.toByteArray());
    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(mem, Comparator.naturalOrder(), serDe);
    assertTrue(mem.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isDirect()); //not off-heap
    println(sk1.toString(true, true));
    println("");
    println(sk2.toString(true, true));
    println("");
    println(KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe));
  }

  @Test
  public void checkExceptions() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    try { sk.getTotalItemsByteArr(); fail(); } catch (SketchesArgumentException e) { }
    try { sk.getTotalItemsNumBytes(); fail(); } catch (SketchesArgumentException e) { }
    try { sk.setWritableMemory(null); fail(); } catch (SketchesArgumentException e) { }
    byte[] byteArr = sk.toByteArray();
    final KllItemsSketch<String> sk2 = KllItemsSketch.wrap(Memory.wrap(byteArr), Comparator.naturalOrder(), serDe);
    try { sk2.incN(); fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setItemsArray(null); fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setItemsArrayAt(0, null); fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setLevelZeroSorted(false); fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setMaxItem(null); fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setMinItem(null); fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setMinK(0); fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setN(0); fail(); } catch (SketchesArgumentException e) { }
  }

  @Test
  public void checkSortedViewAfterReset() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    sk.update("1");
    GenericSortedView<String> sv = sk.getSortedView();
    String ssv = sv.getQuantile(1.0, INCLUSIVE);
    assertEquals(ssv, "1");
    sk.reset();
    try { sk.getSortedView(); fail(); } catch (SketchesArgumentException e) { }
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
