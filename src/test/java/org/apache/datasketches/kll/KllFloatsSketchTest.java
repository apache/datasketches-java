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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.Math.min;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_FLOATS_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.kll.KllHelper;
import org.apache.datasketches.kll.KllSketch;
import org.apache.datasketches.quantilescommon.FloatsSortedView;
import org.apache.datasketches.quantilescommon.FloatsSortedViewIterator;
import org.testng.annotations.Test;

public class KllFloatsSketchTest {
  private static final String LS = System.getProperty("line.separator");
  private static final double PMF_EPS_FOR_K_8 = KllSketch.getNormalizedRankError(8, true);
  private static final double PMF_EPS_FOR_K_128 = KllSketch.getNormalizedRankError(128, true);
  private static final double PMF_EPS_FOR_K_256 = KllSketch.getNormalizedRankError(256, true);
  private static final double NUMERIC_NOISE_TOLERANCE = 1E-6;

  @Test
  public void empty() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(Float.NaN); // this must not change anything
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getNumRetained(), 0);
    try { sketch.getRank(0); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getMinItem(); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getMaxItem(); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getQuantile(0.5); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getQuantiles(new double[] {0}); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getPMF(new float[] {0}); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getCDF(new float[] {0}); fail(); } catch (final SketchesArgumentException e) {}
    assertNotNull(sketch.toString(true, true));
    assertNotNull(sketch.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantileInvalidArg() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(1);
    sketch.getQuantile(-1.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantilesInvalidArg() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(1);
    sketch.getQuantiles(new double[] {2.0});
  }

  @Test
  public void oneValue() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(1);
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 1);
    assertEquals(sketch.getNumRetained(), 1);
    assertEquals(sketch.getRank(0.0f, EXCLUSIVE), 0.0);
    assertEquals(sketch.getRank(1.0f, EXCLUSIVE), 0.0);
    assertEquals(sketch.getRank(2.0f, EXCLUSIVE), 1.0);
    assertEquals(sketch.getRank(0.0f, INCLUSIVE), 0.0);
    assertEquals(sketch.getRank(1.0f, INCLUSIVE), 1.0);
    assertEquals(sketch.getRank(2.0f, INCLUSIVE), 1.0);
    assertEquals(sketch.getMinItem(), 1.0f);
    assertEquals(sketch.getMaxItem(), 1.0f);
    assertEquals(sketch.getQuantile(0.5, EXCLUSIVE), 1.0f);
    assertEquals(sketch.getQuantile(0.5, INCLUSIVE), 1.0f);
  }

  @Test
  public void tenValues() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 10; i++) { sketch.update(i); }
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 10);
    assertEquals(sketch.getNumRetained(), 10);
    for (int i = 1; i <= 10; i++) {
      assertEquals(sketch.getRank(i, EXCLUSIVE), (i - 1) / 10.0);
      assertEquals(sketch.getRank(i, INCLUSIVE), i / 10.0);
    }
    final float[] qArr = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    double[] rOut = sketch.getRanks(qArr); //inclusive
    for (int i = 0; i < qArr.length; i++) {
      assertEquals(rOut[i], (i + 1) / 10.0);
    }
    rOut = sketch.getRanks(qArr, EXCLUSIVE); //exclusive
    for (int i = 0; i < qArr.length; i++) {
      assertEquals(rOut[i], i / 10.0);
    }

    for (int i = 0; i >= 10; i++) {
      final double rank = i/10.0;
      float q = rank == 1.0 ? i : i + 1;
      assertEquals(sketch.getQuantile(rank, EXCLUSIVE), q);
      q = (float)(rank == 0 ? i + 1.0 : i);
      assertEquals(sketch.getQuantile(rank, INCLUSIVE), q);
    }

    {
      // getQuantile() and getQuantiles() equivalence EXCLUSIVE
      final float[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, EXCLUSIVE);
      for (int i = 0; i <= 10; i++) {
        assertEquals(sketch.getQuantile(i / 10.0, EXCLUSIVE), quantiles[i]);
      }
    }
    {
      // getQuantile() and getQuantiles() equivalence INCLUSIVE
      final float[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, INCLUSIVE);
      for (int i = 0; i <= 10; i++) {
        assertEquals(sketch.getQuantile(i / 10.0, INCLUSIVE), quantiles[i]);
      }
    }
  }

  @Test
  public void manyValuesEstimationMode() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    final int n = 1_000_000;

    for (int i = 0; i < n; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getN(), n);

    // test getRank
    for (int i = 0; i < n; i++) {
      final double trueRank = (double) i / n;
      assertEquals(sketch.getRank(i), trueRank, PMF_EPS_FOR_K_256, "for value " + i);
    }

    // test getPMF
    final double[] pmf = sketch.getPMF(new float[] {n / 2.0F}); // split at median
    assertEquals(pmf.length, 2);
    assertEquals(pmf[0], 0.5, PMF_EPS_FOR_K_256);
    assertEquals(pmf[1], 0.5, PMF_EPS_FOR_K_256);

    assertEquals(sketch.getMinItem(), 0f); // min value is exact
    assertEquals(sketch.getMaxItem(), n - 1f); // max value is exact

    // check at every 0.1 percentage point
    final double[] fractions = new double[1001];
    final double[] reverseFractions = new double[1001]; // check that ordering doesn't matter
    for (int i = 0; i <= 1000; i++) {
      fractions[i] = (double) i / 1000;
      reverseFractions[1000 - i] = fractions[i];
    }
    final float[] quantiles = sketch.getQuantiles(fractions);
    final float[] reverseQuantiles = sketch.getQuantiles(reverseFractions);
    float previousQuantile = 0;
    for (int i = 0; i <= 1000; i++) {
      final float quantile = sketch.getQuantile(fractions[i]);
      assertEquals(quantile, quantiles[i]);
      assertEquals(quantile, reverseQuantiles[1000 - i]);
      assertTrue(previousQuantile <= quantile);
      previousQuantile = quantile;
    }
  }

  @Test
  public void getRankGetCdfGetPmfConsistency() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    final int n = 1000;
    final float[] values = new float[n];
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      values[i] = i;
    }
    { // inclusive = false (default)
      final double[] ranks = sketch.getCDF(values);
      final double[] pmf = sketch.getPMF(values);
      double sumPmf = 0;
      for (int i = 0; i < n; i++) {
        assertEquals(ranks[i], sketch.getRank(values[i]), NUMERIC_NOISE_TOLERANCE,
            "rank vs CDF for value " + i);
        sumPmf += pmf[i];
        assertEquals(ranks[i], sumPmf, NUMERIC_NOISE_TOLERANCE, "CDF vs PMF for value " + i);
      }
      sumPmf += pmf[n];
      assertEquals(sumPmf, 1.0, NUMERIC_NOISE_TOLERANCE);
      assertEquals(ranks[n], 1.0, NUMERIC_NOISE_TOLERANCE);
    }
    { // inclusive = true
      final double[] ranks = sketch.getCDF(values, INCLUSIVE);
      final double[] pmf = sketch.getPMF(values, INCLUSIVE);
      double sumPmf = 0;
      for (int i = 0; i < n; i++) {
        assertEquals(ranks[i], sketch.getRank(values[i], INCLUSIVE), NUMERIC_NOISE_TOLERANCE,
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
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance();
    final KllFloatsSketch sketch2 = KllFloatsSketch.newHeapInstance();
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i * 1.0f);
      sketch2.update(((2 * n) - i - 1) * 1.0f);
    }

    assertEquals(sketch1.getMinItem(), 0.0f);
    assertEquals(sketch1.getMaxItem(), (n - 1) * 1.0f);

    assertEquals(sketch2.getMinItem(), n * 1.0f);
    assertEquals(sketch2.getMaxItem(), ((2 * n) - 1) * 1.0f);

    sketch1.merge(sketch2);

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2L * n);
    assertEquals(sketch1.getMinItem(), 0.0f);
    assertEquals(sketch1.getMaxItem(), ((2 * n) - 1) * 1.0f);
    assertEquals(sketch1.getQuantile(0.5), n * 1.0f, 2 * n * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeLowerK() {
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance(256);
    final KllFloatsSketch sketch2 = KllFloatsSketch.newHeapInstance(128);
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update((2 * n) - i - 1);
    }

    assertEquals(sketch1.getMinItem(), 0.0f);
    assertEquals(sketch1.getMaxItem(), n - 1f);

    assertEquals(sketch2.getMinItem(), n);
    assertEquals(sketch2.getMaxItem(), (2f * n) - 1.0f);

    assertTrue(sketch1.getNormalizedRankError(false) < sketch2.getNormalizedRankError(false));
    assertTrue(sketch1.getNormalizedRankError(true) < sketch2.getNormalizedRankError(true));
    sketch1.merge(sketch2);

    // sketch1 must get "contaminated" by the lower K in sketch2
    assertEquals(sketch1.getNormalizedRankError(false), sketch2.getNormalizedRankError(false));
    assertEquals(sketch1.getNormalizedRankError(true), sketch2.getNormalizedRankError(true));

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2 * n);
    assertEquals(sketch1.getMinItem(), 0.0f);
    assertEquals(sketch1.getMaxItem(), (2.0f * n) - 1.0f);
    assertEquals(sketch1.getQuantile(0.5), n, 2 * n * PMF_EPS_FOR_K_128);
  }

  @Test
  public void mergeEmptyLowerK() {
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance(256);
    final KllFloatsSketch sketch2 = KllFloatsSketch.newHeapInstance(128);
    final int n = 10_000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
    }

    // rank error should not be affected by a merge with an empty sketch with lower K
    final double rankErrorBeforeMerge = sketch1.getNormalizedRankError(true);
    sketch1.merge(sketch2);
    assertEquals(sketch1.getNormalizedRankError(true), rankErrorBeforeMerge);

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch1.getMinItem(), 0.0f);
    assertEquals(sketch1.getMaxItem(), n - 1.0f);
    assertEquals(sketch1.getQuantile(0.5), n / 2.0f, n * PMF_EPS_FOR_K_256);

    //merge the other way
    sketch2.merge(sketch1);
    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch1.getMinItem(), 0f);
    assertEquals(sketch1.getMaxItem(), n - 1.0f);
    assertEquals(sketch1.getQuantile(0.5), n / 2.0f, n * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeExactModeLowerK() {
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance(256);
    final KllFloatsSketch sketch2 = KllFloatsSketch.newHeapInstance(128);
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
    }
    sketch2.update(1);

    // rank error should not be affected by a merge with a sketch in exact mode with lower K
    final double rankErrorBeforeMerge = sketch1.getNormalizedRankError(true);
    sketch1.merge(sketch2);
    assertEquals(sketch1.getNormalizedRankError(true), rankErrorBeforeMerge);
  }

  @Test
  public void mergeMinMinValueFromOther() {
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance();
    final KllFloatsSketch sketch2 = KllFloatsSketch.newHeapInstance();
    sketch1.update(1);
    sketch2.update(2);
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinItem(), 1.0F);
  }

  @Test
  public void mergeMinAndMaxFromOther() {
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance();
    for (int i = 1; i <= 1_000_000; i++) {
      sketch1.update(i);
    }
    final KllFloatsSketch sketch2 = KllFloatsSketch.newHeapInstance(10);
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinItem(), 1.0F);
    assertEquals(sketch2.getMaxItem(), 1_000_000.0F);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooSmall() {
    KllFloatsSketch.newHeapInstance(KllSketch.DEFAULT_M - 1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooLarge() {
    KllFloatsSketch.newHeapInstance(KllSketch.MAX_K + 1);
  }

  @Test
  public void minK() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance(KllSketch.DEFAULT_M);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getK(), KllSketch.DEFAULT_M);
    assertEquals(sketch.getQuantile(.5), 500.0f, 1000 * PMF_EPS_FOR_K_8);
  }

  @Test
  public void maxK() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance(KllSketch.MAX_K);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getK(), KllSketch.MAX_K);
    assertEquals(sketch.getQuantile(0.5), 500, 1000 * PMF_EPS_FOR_K_256);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void outOfOrderSplitPoints() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(0);
    sketch.getCDF(new float[] {1.0f, 0.0f});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void nanSplitPoint() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(0);
    sketch.getCDF(new float[] {Float.NaN});
  }

  @Test
  public void checkReset() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    final long n1 = sk.getN();
    final float min1 = sk.getMinItem();
    final float max1 = sk.getMaxItem();
    sk.reset();
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    final long n2 = sk.getN();
    final float min2 = sk.getMinItem();
    final float max2 = sk.getMaxItem();
    assertEquals(n2, n1);
    assertEquals(min2, min1);
    assertEquals(max2, max1);
  }

  @Test
  public void checkReadOnlyUpdate() {
    final KllFloatsSketch sk1 = KllFloatsSketch.newHeapInstance(20);
    final MemorySegment seg = MemorySegment.ofArray(sk1.toByteArray());
    final KllFloatsSketch sk2 = KllFloatsSketch.wrap(seg);
    try { sk2.update(1); fail(); } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkNewDirectInstanceAndSize() {
    final MemorySegment wseg = MemorySegment.ofArray(new byte[3000]);
    KllFloatsSketch.newDirectInstance(wseg);
    try { KllFloatsSketch.newDirectInstance(null); fail(); }
    catch (final NullPointerException e) { }
    final int updateSize = KllSketch.getMaxSerializedSizeBytes(200, 0, KLL_FLOATS_SKETCH, true);
    final int compactSize = KllSketch.getMaxSerializedSizeBytes(200, 0, KLL_FLOATS_SKETCH, false);
    assertTrue(compactSize < updateSize);
  }

  @Test
  public void sortedView() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(3);
    sk.update(1);
    sk.update(2);

    final FloatsSortedView view = sk.getSortedView();
    final FloatsSortedViewIterator itr = view.iterator();
    assertEquals(itr.next(), true);
    assertEquals(itr.getQuantile(), 1);
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getNaturalRank(EXCLUSIVE), 0);
    assertEquals(itr.getNaturalRank(INCLUSIVE), 1);
    assertEquals(itr.next(), true);
    assertEquals(itr.getQuantile(), 2);
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getNaturalRank(EXCLUSIVE), 1);
    assertEquals(itr.getNaturalRank(INCLUSIVE), 2);
    assertEquals(itr.next(), true);
    assertEquals(itr.getQuantile(), 3);
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getNaturalRank(EXCLUSIVE), 2);
    assertEquals(itr.getNaturalRank(INCLUSIVE), 3);
    assertEquals(itr.next(), false);
  }

  @Test //also visual
  public void checkCDF_PDF() {
    final double[] cdfI = {.25, .50, .75, 1.0, 1.0 };
    final double[] cdfE = {0.0, .25, .50, .75, 1.0 };
    final double[] pmfI = {.25, .25, .25, .25, 0.0 };
    final double[] pmfE = {0.0, .25, .25, .25, .25 };
    final double toll = 1E-10;
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    final float[] floatIn = {10, 20, 30, 40};
    for (int i = 0; i < floatIn.length; i++) { sketch.update(floatIn[i]); }
    final float[] sp = { 10, 20, 30, 40 };
    println("SplitPoints:");
    for (int i = 0; i < sp.length; i++) {
      printf("%10.2f", sp[i]);
    }
    println("");
    println("INCLUSIVE:");
    double[] cdf = sketch.getCDF(sp, INCLUSIVE);
    double[] pmf = sketch.getPMF(sp, INCLUSIVE);
    printf("%10s%10s" + LS, "CDF", "PMF");
    for (int i = 0; i < cdf.length; i++) {
      printf("%10.2f%10.2f" + LS, cdf[i], pmf[i]);
      assertEquals(cdf[i], cdfI[i], toll);
      assertEquals(pmf[i], pmfI[i], toll);
    }
    println("EXCLUSIVE");
    cdf = sketch.getCDF(sp, EXCLUSIVE);
    pmf = sketch.getPMF(sp, EXCLUSIVE);
    printf("%10s%10s" + LS, "CDF", "PMF");
    for (int i = 0; i < cdf.length; i++) {
      printf("%10.2f%10.2f" + LS, cdf[i], pmf[i]);
      assertEquals(cdf[i], cdfE[i], toll);
      assertEquals(pmf[i], pmfE[i], toll);
    }
  }

  @Test
  public void checkWrapCase1Floats() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }

    final MemorySegment seg = MemorySegment.ofArray(sk.toByteArray()).asReadOnly();
    final KllFloatsSketch sk2 = KllFloatsSketch.wrap(seg);

    assertTrue(seg.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isOffHeap());
  }

  @Test
  public void checkWritableWrapCase6And2Floats() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }

    final MemorySegment wseg = MemorySegment.ofArray(KllHelper.toByteArray(sk, true));
    final KllFloatsSketch sk2 = KllFloatsSketch.wrap(wseg);

    assertFalse(wseg.isReadOnly());
    assertFalse(sk2.isReadOnly());
    assertFalse(sk2.isOffHeap());
  }

  @Test
  public void checkKllSketchCase5Floats() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }

    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    final KllFloatsSketch sk2 = KllFloatsSketch.wrap(wseg);

    assertFalse(wseg.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isOffHeap());
  }

  @Test
  public void checkKllSketchCase3Floats() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }

    final MemorySegment seg = MemorySegment.ofArray(KllHelper.toByteArray(sk, true)).asReadOnly();
    final MemorySegment wseg = (MemorySegment) seg;
    final KllFloatsSketch sk2 = KllFloatsSketch.wrap(wseg);

    assertTrue(wseg.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isOffHeap());
  }

  @Test
  public void checkKllSketchCase7Floats() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }

    final MemorySegment seg = MemorySegment.ofArray(KllHelper.toByteArray(sk, true)).asReadOnly();
    final MemorySegment wseg = (MemorySegment) seg;
    final KllFloatsSketch sk2 = KllFloatsSketch.wrap(wseg);

    assertTrue(wseg.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isOffHeap());
  }

  @Test
  public void checkReadOnlyExceptions() {
    final int[] intArr = {};
    final int intV = 2;
    final int idx = 1;
    final KllFloatsSketch sk1 = KllFloatsSketch.newHeapInstance(20);
    final MemorySegment seg = MemorySegment.ofArray(sk1.toByteArray());
    final KllFloatsSketch sk2 = KllFloatsSketch.wrap(seg);
    try { sk2.setLevelsArray(intArr);              fail(); } catch (final SketchesArgumentException e) { }
    try { sk2.setLevelsArrayAt(idx,intV);          fail(); } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkIsSameResource() {
    final int cap = 128;
    final MemorySegment wseg = MemorySegment.ofArray(new byte[cap]);
    final MemorySegment slice1 = wseg.asSlice(0, 64);
    final MemorySegment slice2 = wseg.asSlice(64, 64);
    assertFalse(slice1 == slice2);
    assertFalse(MemorySegmentStatus.isSameResource(slice1, slice2));

    final MemorySegment slice3 = wseg.asSlice(0, 64);
    assertFalse(slice1 == slice3);
    assertTrue(MemorySegmentStatus.isSameResource(slice1, slice3));

    final byte[] byteArr1 = KllFloatsSketch.newHeapInstance(20).toByteArray();
    MemorySegment.copy(byteArr1, 0, slice1, JAVA_BYTE, 0, byteArr1.length);
    final KllFloatsSketch sk1 = KllFloatsSketch.wrap(slice1);

    final byte[] byteArr2 = KllFloatsSketch.newHeapInstance(20).toByteArray();
    MemorySegment.copy(byteArr2, 0, slice2, JAVA_BYTE, 0, byteArr2.length);
    assertFalse(sk1.isSameResource(slice2));

    final byte[] byteArr3 = KllFloatsSketch.newHeapInstance(20).toByteArray();
    MemorySegment.copy(byteArr3, 0, slice3, JAVA_BYTE, 0, byteArr3.length);
    assertTrue(sk1.isSameResource(slice3));
  }

  @Test
  public void checkSortedViewAfterReset() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    sk.update(1.0f);
    final FloatsSortedView sv = sk.getSortedView();
    final float fsv = sv.getQuantile(1.0, INCLUSIVE);
    assertEquals(fsv, 1.0f);
    sk.reset();
    try { sk.getSortedView(); fail(); } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkVectorUpdate() {
    final boolean withLevels = false;
    final boolean withLevelsAndItems = true;
    final int k = 20;
    final int n = 108;
    final int maxVsz = 40;  //max vector size
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(k);
    int j = 1;
    int rem;
    while ((rem = (n - j) + 1) > 0) {
      final int vecSz = min(rem, maxVsz);
      final float[] v = new float[vecSz];
      for (int i = 0; i < vecSz; i++) { v[i] = j++; }
      sk.update(v, 0, vecSz);
    }
    println(LS + "#<<< END STATE # >>>");
    println(sk.toString(withLevels, withLevelsAndItems));
    println("");
    assertEquals(sk.getN(), 108);
    assertEquals(sk.getMaxItem(), 108F);
    assertEquals(sk.getMinItem(), 1F);
  }

  @Test
  public void vectorizedUpdates() {
    final int trials = 1;
    final int M = 1; //number of vectors
    final int N = 1000; //vector size
    final int K = 256;
    final float[] values = new float[N];
    float vIn = 1.0F;
    long totN = 0;
    final long startTime = System.nanoTime();
    for (int t = 0; t < trials; t++) {
      final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance(K);
      for (int m = 0; m < M; m++) {
        for (int n = 0; n < N; n++) {
          values[n] = vIn++;  //fill vector
        }
        sketch.update(values, 0, N); //vector input
      }
      totN = sketch.getN();
      assertEquals(totN, M * N);
      assertEquals(sketch.getMinItem(), 1.0F);
      assertEquals(sketch.getMaxItem(), totN);
      assertEquals(sketch.getQuantile(0.5), (float)(totN / 2.0), totN * PMF_EPS_FOR_K_256 * 2.0); //wider tolerance
    }
    final long runTime = System.nanoTime() - startTime;
    println("Vectorized Updates");
    printf("  Vector size : %,12d" + LS, N);
    printf("  Num Vectors : %,12d" + LS, M);
    printf("  Total Input : %,12d" + LS, totN);
    printf("  Run Time mS : %,12.3f" + LS, runTime / 1e6);
    final double trialTime = runTime / (1e6 * trials);
    printf("  mS / Trial  : %,12.3f" + LS, trialTime);
    final double updateTime = runTime / (1.0 * totN * trials);
    printf("  nS / Update : %,12.3f" + LS, updateTime);
  }

  @Test
  public void nonVectorizedUpdates() {
    final int trials = 1;
    final int M = 1; //number of vectors
    final int N = 1000; //vector size
    final int K = 256;
    final float[] values = new float[N];
    float vIn = 1.0F;
    long totN = 0;
    final long startTime = System.nanoTime();
    for (int t = 0; t < trials; t++) {
      final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance(K);
      for (int m = 0; m < M; m++) {
        for (int n = 0; n < N; n++) {
          values[n] = vIn++; //fill vector
        }
        for (int i = 0; i < N; i++) {
          sketch.update(values[i]); //single item input
        }
      }
      totN = sketch.getN();
      assertEquals(totN, M * N);
      assertEquals(sketch.getMinItem(), 1.0);
      assertEquals(sketch.getMaxItem(), totN);
      assertEquals(sketch.getQuantile(0.5), (float)(totN / 2.0), totN * PMF_EPS_FOR_K_256 * 2.0); //wider tolerance
    }
    final long runTime = System.nanoTime() - startTime;
    println("Vectorized Updates");
    printf("  Vector size : %,12d" + LS, N);
    printf("  Num Vectors : %,12d" + LS, M);
    printf("  Total Input : %,12d" + LS, totN);
    printf("  Run Time mS : %,12.3f" + LS, runTime / 1e6);
    final double trialTime = runTime / (1e6 * trials);
    printf("  mS / Trial  : %,12.3f" + LS, trialTime);
    final double updateTime = runTime / (1.0 * totN * trials);
    printf("  nS / Update : %,12.3f" + LS, updateTime);
  }

  @Test
  public void deterministicMerge() throws NoSuchAlgorithmException {
    KllFloatsSketch t1 = KllFloatsSketch.newHeapInstance();
    KllFloatsSketch t2 = KllFloatsSketch.newHeapInstance();
    for(int i=0; i<1000; i++) {
      t1.update(1f*i);
      t2.update(2f*i);
    }
    byte[] tb1 = t1.toByteArray();
    byte[] tb2 = t2.toByteArray();

    MessageDigest md5 = MessageDigest.getInstance("MD5");
    HashSet<BigInteger> digests = new HashSet<>();
    for(int i=0; i<100; i++) {
      Random random = new Random(5);
      byte[] h1 = Arrays.copyOf(tb1, tb1.length);
      byte[] h2 = Arrays.copyOf(tb2, tb2.length);

      KllFloatsSketch start = KllFloatsSketch.newHeapInstance();
      KllFloatsSketch kll1 = KllFloatsSketch.heapify(MemorySegment.ofArray(h1));
      KllFloatsSketch kll2 = KllFloatsSketch.heapify(MemorySegment.ofArray(h2));
      start.merge(kll1, random);
      start.merge(kll2, random);

      BigInteger digest = new BigInteger(md5.digest(start.toByteArray()));
      digests.add(digest);
    }
    assertEquals(digests.size(), 1);
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
