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

import static org.apache.datasketches.kll.KllSketch.SketchType.LONGS_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllHeapLongsSketch;
import org.apache.datasketches.kll.KllHelper;
import org.apache.datasketches.kll.KllLongsSketch;
import org.apache.datasketches.kll.KllPreambleUtil;
import org.apache.datasketches.kll.KllSketch;
import org.apache.datasketches.kll.KllSketch.SketchStructure;
import org.testng.annotations.Test;

public class KllDirectLongsSketchTest {

  private static final double PMF_EPS_FOR_K_8 = 0.35; // PMF rank error (epsilon) for k=8
  private static final double PMF_EPS_FOR_K_128 = 0.025; // PMF rank error (epsilon) for k=128
  private static final double PMF_EPS_FOR_K_256 = 0.013; // PMF rank error (epsilon) for k=256
  private static final double NUMERIC_NOISE_TOLERANCE = 1E-6;

  @Test
  public void empty() {
    final KllLongsSketch sketch = getUpdatableDirectLongSketch(200, 0);
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getNumRetained(), 0);
    try { sketch.getRank(0); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getMinItem(); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getMaxItem(); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getQuantile(0.5); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getQuantiles(new double[] {0.0, 1.0}); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getPMF(new long[] {0}); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch.getCDF(new long[0]); fail(); } catch (final SketchesArgumentException e) {}
    assertNotNull(sketch.toString(true, true));
    assertNotNull(sketch.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantileInvalidArg() {
    final KllLongsSketch sketch = getUpdatableDirectLongSketch(200, 0);
    sketch.update(1);
    sketch.getQuantile(-1.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantilesInvalidArg() {
    final KllLongsSketch sketch = getUpdatableDirectLongSketch(200, 0);
    sketch.update(1);
    sketch.getQuantiles(new double[] {2.0});
  }

  @Test
  public void oneValue() {
    final KllLongsSketch sketch = getUpdatableDirectLongSketch(200, 0);
    sketch.update(1);
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 1);
    assertEquals(sketch.getNumRetained(), 1);
    assertEquals(sketch.getRank(1, EXCLUSIVE), 0.0);
    assertEquals(sketch.getRank(2, EXCLUSIVE), 1.0);
    assertEquals(sketch.getMinItem(), 1L);
    assertEquals(sketch.getMaxItem(), 1L);
    assertEquals(sketch.getQuantile(0.5, EXCLUSIVE), 1L);
  }

  @Test
  public void manyValuesEstimationMode() {
    final KllLongsSketch sketch = getUpdatableDirectLongSketch(200, 0);
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
    final double[] pmf = sketch.getPMF(new long[] {n / 2}); // split at median
    assertEquals(pmf.length, 2);
    assertEquals(pmf[0], 0.5, PMF_EPS_FOR_K_256);
    assertEquals(pmf[1], 0.5, PMF_EPS_FOR_K_256);
    assertEquals(sketch.getMinItem(), 0); // min value is exact
    assertEquals(sketch.getMaxItem(), n - 1L); // max value is exact

    // check at every 0.1 percentage point
    final double[] ranks = new double[1001];
    final double[] reverseRanks = new double[1001]; // check that ordering doesn't matter
    for (int i = 0; i <= 1000; i++) {
      ranks[i] = (double) i / 1000;
      reverseRanks[1000 - i] = ranks[i];
    }
    final long[] quantiles = sketch.getQuantiles(ranks);
    final long[] reverseQuantiles = sketch.getQuantiles(reverseRanks);
    long previousQuantile = 0;
    for (int i = 0; i <= 1000; i++) {
      final long quantile = sketch.getQuantile(ranks[i]);
      assertEquals(quantile, quantiles[i]);
      assertEquals(quantile, reverseQuantiles[1000 - i]);
      assertTrue(previousQuantile <= quantile);
      previousQuantile = quantile;
    }
  }

  @Test
  public void getRankGetCdfGetPmfConsistency() {
    final KllLongsSketch sketch = getUpdatableDirectLongSketch(200, 0);
    final int n = 1000;
    final long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      values[i] = i;
    }
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

  @Test
  public void merge() {
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(200, 0);
    final KllLongsSketch sketch2 = getUpdatableDirectLongSketch(200, 0);
    final int n = 10_000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update((2 * n) - i - 1);
    }

    assertEquals(sketch1.getMinItem(), 0);
    assertEquals(sketch1.getMaxItem(), n - 1);

    assertEquals(sketch2.getMinItem(), n);
    assertEquals(sketch2.getMaxItem(), (2 * n) - 1);

    sketch1.merge(sketch2);

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2L * n);
    assertEquals(sketch1.getMinItem(), 0);
    assertEquals(sketch1.getMaxItem(), (2 * n) - 1L);
    assertEquals(sketch1.getQuantile(0.5), n, n * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeLowerK() {
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(256, 0);
    final KllLongsSketch sketch2 = getUpdatableDirectLongSketch(128, 0);
    final int n = 10_000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update((2 * n) - i - 1);
    }

    assertEquals(sketch1.getMinItem(), 0);
    assertEquals(sketch1.getMaxItem(), n - 1);

    assertEquals(sketch2.getMinItem(), n);
    assertEquals(sketch2.getMaxItem(), (2 * n) - 1);

    assertTrue(sketch1.getNormalizedRankError(false) < sketch2.getNormalizedRankError(false));
    assertTrue(sketch1.getNormalizedRankError(true) < sketch2.getNormalizedRankError(true));
    sketch1.merge(sketch2);

    // sketch1 must get "contaminated" by the lower K in sketch2
    assertEquals(sketch1.getNormalizedRankError(false), sketch2.getNormalizedRankError(false));
    assertEquals(sketch1.getNormalizedRankError(true), sketch2.getNormalizedRankError(true));

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2 * n);
    assertEquals(sketch1.getMinItem(), 0);
    assertEquals(sketch1.getMaxItem(), (2 * n) - 1);
    assertEquals(sketch1.getQuantile(0.5), n, n * PMF_EPS_FOR_K_128);
  }

  @Test
  public void mergeEmptyLowerK() {
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(256, 0);
    final KllLongsSketch sketch2 = getUpdatableDirectLongSketch(128, 0);
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
    assertEquals(sketch1.getMinItem(), 0);
    assertEquals(sketch1.getMaxItem(), n - 1);
    assertEquals(sketch1.getQuantile(0.5), n / 2, (n / 2) * PMF_EPS_FOR_K_256);

    //merge the other way
    sketch2.merge(sketch1);
    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch1.getMinItem(), 0);
    assertEquals(sketch1.getMaxItem(), n - 1);
    assertEquals(sketch1.getQuantile(0.5), n / 2, (n / 2) * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeExactModeLowerK() {
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(256, 0);
    final KllLongsSketch sketch2 = getUpdatableDirectLongSketch(128, 0);
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
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(200, 0);
    final KllLongsSketch sketch2 = getUpdatableDirectLongSketch(200, 0);
    sketch1.update(1);
    sketch2.update(2);
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinItem(), 1L);
  }

  @Test
  public void mergeMinAndMaxFromOther() {
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(200, 0);
    final KllLongsSketch sketch2 = getUpdatableDirectLongSketch(200, 0);
    final int n = 1_000_000;
    for (int i = 1; i <= n; i++) {
      sketch1.update(i);
    }
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinItem(), 1L);
    assertEquals(sketch2.getMaxItem(), 1_000_000L);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooSmall() {
    getUpdatableDirectLongSketch(KllSketch.DEFAULT_M - 1, 0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooLarge() {
    getUpdatableDirectLongSketch(KllSketch.MAX_K + 1, 0);
  }

  @Test
  public void minK() {
    final KllLongsSketch sketch = getUpdatableDirectLongSketch(KllSketch.DEFAULT_M, 0);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getK(), KllSketch.DEFAULT_M);
    assertEquals(sketch.getQuantile(0.5), 500, 500 * PMF_EPS_FOR_K_8);
  }

  @Test
  public void maxK() {
    final KllLongsSketch sketch = getUpdatableDirectLongSketch(KllSketch.MAX_K, 0);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getK(), KllSketch.MAX_K);
    assertEquals(sketch.getQuantile(0.5), 500, 500 * PMF_EPS_FOR_K_256);
  }

  @Test
  public void serializeDeserializeEmptyViaCompactHeapify() {
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(200, 0);
    final byte[] bytes = sketch1.toByteArray(); //compact
    final KllLongsSketch sketch2 = KllLongsSketch.heapify(MemorySegment.ofArray(bytes));
    assertEquals(bytes.length, sketch1.currentSerializedSizeBytes(false));
    assertTrue(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    try { sketch2.getMinItem(); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch2.getMaxItem(); fail(); } catch (final SketchesArgumentException e) {}
    assertEquals(sketch2.currentSerializedSizeBytes(false),
        sketch1.currentSerializedSizeBytes(false));
  }

  @Test
  public void serializeDeserializeEmptyViaUpdatableWritableWrap() {
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(200, 0);
    final byte[] bytes = KllHelper.toByteArray(sketch1, true);
    final KllLongsSketch sketch2 =
        KllLongsSketch.wrap(MemorySegment.ofArray(bytes));
    assertEquals(bytes.length, sketch1.currentSerializedSizeBytes(true));
    assertTrue(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    try { sketch2.getMinItem(); fail(); } catch (final SketchesArgumentException e) {}
    try { sketch2.getMaxItem(); fail(); } catch (final SketchesArgumentException e) {}
    assertEquals(sketch2.currentSerializedSizeBytes(true),
        sketch1.currentSerializedSizeBytes(true));
  }

  @Test
  public void serializeDeserializeOneValueViaCompactHeapify() {
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(200, 0);
    sketch1.update(1);
    final byte[] bytes = sketch1.toByteArray();
    final KllLongsSketch sketch2 = KllLongsSketch.heapify(MemorySegment.ofArray(bytes));
    assertEquals(bytes.length, sketch1.currentSerializedSizeBytes(false));
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), 1);
    assertEquals(sketch2.getN(), 1);
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertTrue(sketch2.getMinItem() < Long.MAX_VALUE);
    assertTrue(sketch2.getMaxItem() > Long.MIN_VALUE);
    assertEquals(sketch2.currentSerializedSizeBytes(false), 8 + Long.BYTES);
  }

  @Test
  public void serializeDeserializeOneValueViaUpdatableWritableWrap() {
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(200, 0);
    sketch1.update(1);
    final byte[] bytes = KllHelper.toByteArray(sketch1, true);
    final KllLongsSketch sketch2 =
        KllLongsSketch.wrap(MemorySegment.ofArray(bytes));
    assertEquals(bytes.length, sketch1.currentSerializedSizeBytes(true));
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), 1);
    assertEquals(sketch2.getN(), 1);
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinItem(), 1L);
    assertEquals(sketch2.getMaxItem(), 1L);
    assertEquals(sketch2.currentSerializedSizeBytes(false), 8 + Long.BYTES);
    assertEquals(sketch2.currentSerializedSizeBytes(true), bytes.length);
  }

  @Test
  public void serializeDeserializeFullViaCompactHeapify() {
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(200, 1000);
    final byte[] byteArr1 = sketch1.toByteArray(); //compact
    final KllLongsSketch sketch2 =  KllLongsSketch.heapify(MemorySegment.ofArray(byteArr1));
    assertEquals(byteArr1.length, sketch1.currentSerializedSizeBytes(false));
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinItem(), sketch1.getMinItem());
    assertEquals(sketch2.getMaxItem(), sketch1.getMaxItem());
    assertEquals(sketch2.currentSerializedSizeBytes(false), sketch1.currentSerializedSizeBytes(false));
  }

  @Test
  public void serializeDeserializeFullViaUpdatableWritableWrap() {
    final KllLongsSketch sketch1 = getUpdatableDirectLongSketch(200, 0);
    final int n = 1000;
    for (int i = 1; i <= n; i++) {
      sketch1.update(i);
    }
    final byte[] bytes = KllHelper.toByteArray(sketch1, true); //updatable
    final KllLongsSketch sketch2 =
        KllLongsSketch.wrap(MemorySegment.ofArray(bytes));
    assertEquals(bytes.length, sketch1.currentSerializedSizeBytes(true));
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinItem(), sketch1.getMinItem());
    assertEquals(sketch2.getMaxItem(), sketch1.getMaxItem());
    assertEquals(sketch2.currentSerializedSizeBytes(true), sketch1.currentSerializedSizeBytes(true));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void outOfOrderSplitPoints() {
    final KllLongsSketch sketch = getUpdatableDirectLongSketch(200, 0);
    sketch.update(0);
    sketch.getCDF(new long[] {1, 0});
  }

  @Test
  public void checkSimpleMergeDirect() { //used for troubleshooting
    final int k = 20;
    final int n1 = 21;
    final int n2 = 43;
    final KllLongsSketch sk1 = KllLongsSketch.newHeapInstance(k);
    final KllLongsSketch sk2 = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= n1; i++) {
      sk1.update(i);
    }
    for (int i = 1; i <= n2; i++) {
      sk2.update(i + 100);
    }
    println("SK1:");
    println(sk1.toString(true, true));
    println("SK2:");
    println(sk2.toString(true, true));
    final MemorySegment wseg1 = MemorySegment.ofArray(KllHelper.toByteArray(sk1, true));
    final MemorySegment wseg2 = MemorySegment.ofArray(KllHelper.toByteArray(sk2, true));
    final KllLongsSketch dsk1 = KllLongsSketch.wrap(wseg1);
    final KllLongsSketch dsk2 = KllLongsSketch.wrap(wseg2);
    println("BEFORE MERGE");
    println(dsk1.toString(true, true));
    dsk1.merge(dsk2);
    println("AFTER MERGE");
    println(dsk1.toString(true, true));
  }

  @Test
  public void checkSketchInitializeDirectLongUpdatableMemorySegment() {
    final int k = 20; //don't change this
    KllLongsSketch sk;
    KllLongsSketch sk2;
    byte[] compBytes;
    MemorySegment wseg;

    println("#### CASE: LONG FULL DIRECT FROM UPDATABLE");
    sk2 = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= (k + 1); i++) { sk2.update(i); }
    //println(sk2.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wseg = MemorySegment.ofArray(compBytes);
    println(KllPreambleUtil.toString(compBytes, LONGS_SKETCH, true));
    sk = KllLongsSketch.wrap(wseg);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 3);
    assertEquals(sk.getMaxItem(), 21);
    assertEquals(sk.getMinItem(), 1);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: LONG EMPTY HEAPIFIED FROM UPDATABLE");
    sk2 = KllLongsSketch.newHeapInstance(k);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wseg = MemorySegment.ofArray(compBytes);
    println(KllPreambleUtil.toString(compBytes, LONGS_SKETCH, true));
    sk = KllLongsSketch.wrap(wseg);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (final SketchesArgumentException e) {}
    try { sk.getMinItem(); fail(); } catch (final SketchesArgumentException e) {}
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: LONG SINGLE HEAPIFIED FROM UPDATABLE");
    sk2 = KllLongsSketch.newHeapInstance(k);
    sk2.update(1);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wseg = MemorySegment.ofArray(compBytes);
    println(KllPreambleUtil.toString(compBytes, LONGS_SKETCH, true));
    sk = KllLongsSketch.wrap(wseg);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    assertEquals(sk.getMaxItem(), 1L);
    assertEquals(sk.getMinItem(), 1L);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test
  public void checkGetWritableMemory() {
    final KllLongsSketch sketch = getUpdatableDirectLongSketch(200, 200);
    assertEquals(sketch.getK(), 200);
    assertEquals(sketch.getN(), 200);
    assertFalse(sketch.isEmpty());
    assertTrue(sketch.isMemorySegmentUpdatableFormat());
    assertFalse(sketch.isEstimationMode());
    assertTrue(sketch.isLongsSketch());
    assertFalse(sketch.isLevelZeroSorted());
    assertFalse(sketch.isDoublesSketch());

    final MemorySegment wseg = sketch.getMemorySegment();
    final KllLongsSketch sk = KllHeapLongsSketch.heapifyImpl(wseg);
    assertEquals(sk.getK(), 200);
    assertEquals(sk.getN(), 200);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isMemorySegmentUpdatableFormat());
    assertFalse(sk.isEstimationMode());
    assertTrue(sk.isLongsSketch());
    assertFalse(sk.isLevelZeroSorted());
    assertFalse(sk.isDoublesSketch());
  }

  @Test
  public void checkReset() {
    final MemorySegment dstSeg = MemorySegment.ofArray(new byte[3000]);
    final KllLongsSketch sk = KllLongsSketch.newDirectInstance(20, dstSeg, null);
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    final long n1 = sk.getN();
    final long min1 = sk.getMinItem();
    final long max1 = sk.getMaxItem();
    sk.reset();
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    final long n2 = sk.getN();
    final long min2 = sk.getMinItem();
    final long max2 = sk.getMaxItem();
    assertEquals(n2, n1);
    assertEquals(min2, min1);
    assertEquals(max2, max1);
  }

  @Test
  public void checkHeapify() {
    final MemorySegment dstSeg = MemorySegment.ofArray(new byte[6000]);
    final KllLongsSketch sk = KllLongsSketch.newDirectInstance(20, dstSeg, null);
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    final KllLongsSketch sk2 = KllHeapLongsSketch.heapifyImpl(dstSeg);
    assertEquals(sk2.getMinItem(), 1L);
    assertEquals(sk2.getMaxItem(), 100L);
  }

  @Test
  public void checkMergeKllLongsSketch() {
    final MemorySegment dstSeg = MemorySegment.ofArray(new byte[6000]);
    final KllLongsSketch sk = KllLongsSketch.newDirectInstance(20, dstSeg, null);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    final KllLongsSketch sk2 = KllLongsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++ ) { sk2.update(i + 100); }
    sk.merge(sk2);
    assertEquals(sk.getMinItem(), 1L);
    assertEquals(sk.getMaxItem(), 121L);
  }

  @Test
  public void checkReverseMergeKllLongsSketch() {
    final MemorySegment dstSeg = MemorySegment.ofArray(new byte[6000]);
    final KllLongsSketch sk = KllLongsSketch.newDirectInstance(20, dstSeg, null);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    final KllLongsSketch sk2 = KllLongsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++ ) { sk2.update(i + 100); }
    sk2.merge(sk);
    assertEquals(sk2.getMinItem(), 1L);
    assertEquals(sk2.getMaxItem(), 121L);
  }

  @Test
  public void checkWritableWrapOfCompactForm() {
    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++ ) { sk.update(i); }
    final MemorySegment srcSeg = MemorySegment.ofArray(sk.toByteArray());
    final KllLongsSketch sk2 = KllLongsSketch.wrap(srcSeg);
    assertEquals(sk2.getMinItem(), 1L);
    assertEquals(sk2.getMaxItem(), 21L);
  }

  @Test
  public void checkReadOnlyExceptions() {
    final int k = 20;
    final long[] fltArr = {};
    final long fltV = 1;
    final int idx = 1;
    final boolean bool = true;
    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    final KllLongsSketch sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    try { sk2.incN(1);                         fail(); } catch (final SketchesArgumentException e) { }
    try { sk2.incNumLevels();                  fail(); } catch (final SketchesArgumentException e) { }
    try { sk2.setLongItemsArray(fltArr);       fail(); } catch (final SketchesArgumentException e) { }
    try { sk2.setLongItemsArrayAt(idx, fltV);  fail(); } catch (final SketchesArgumentException e) { }
    try { sk2.setLevelZeroSorted(bool);        fail(); } catch (final SketchesArgumentException e) { }
    try { sk2.setMaxItem(fltV);                fail(); } catch (final SketchesArgumentException e) { }
    try { sk2.setMinItem(fltV);                fail(); } catch (final SketchesArgumentException e) { }
    try { sk2.setMinK(idx);                    fail(); } catch (final SketchesArgumentException e) { }
    try { sk2.setN(idx);                       fail(); } catch (final SketchesArgumentException e) { }
    try { sk2.setNumLevels(idx);               fail(); } catch (final SketchesArgumentException e) { }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMergeExceptions() {
    final KllLongsSketch sk1 = KllLongsSketch.newHeapInstance(20);
    final MemorySegment srcSeg1 = MemorySegment.ofArray(sk1.toByteArray());
    final KllLongsSketch sk2 = KllLongsSketch.wrap(srcSeg1);
    sk2.merge(sk1);
  }

  @Test
  public void checkVectorUpdate() {
    final MemorySegment dstSeg = MemorySegment.ofArray(new byte[6000]);
    final KllLongsSketch sk = KllLongsSketch.newDirectInstance(20, dstSeg, null);
    final long[] v = new long[21];
    for (int i = 0; i < 21; i++) { v[i] = i + 1; }
    sk.update(v, 0, 21);
    println(sk.toString(true, true));
    final int[] levelsArr = sk.getLevelsArray(SketchStructure.UPDATABLE);
    assertEquals(levelsArr[0], 22);
    final long[] longsArr = sk.getLongItemsArray();
    assertEquals(longsArr[22], 21);
  }

  @Test
  public void checkWeightedUpdate() {
    final MemorySegment dstSeg = MemorySegment.ofArray(new byte[6000]);
    final KllLongsSketch sk = KllLongsSketch.newDirectInstance(8, dstSeg, null);
    for (int i = 0; i < 16; i++) {
      sk.update(i + 1, 16);
    }
    println(sk.toString(true, true));
    assertEquals(sk.getN(), 256);
    assertEquals(sk.getMaxItem(), 16L);
    assertEquals(sk.getMinItem(), 1L);
  }

  private static KllLongsSketch getUpdatableDirectLongSketch(final int k, final int n) {
    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    final byte[] byteArr = KllHelper.toByteArray(sk, true);
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    return KllLongsSketch.wrap(wseg);
  }

  @Test
  public void checkMergeExceptionsWrongType() {
    final KllLongsSketch sk1 = KllLongsSketch.newHeapInstance(20);
    final KllDoublesSketch sk2 = KllDoublesSketch.newHeapInstance(20);
    try { sk1.merge(sk2); fail(); } catch (final ClassCastException e) { }
    try { sk2.merge(sk1); fail(); } catch (final ClassCastException e) { }
  }

  private final static boolean enablePrinting = false;

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
