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

import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class KllDirectDoublesSketchTest {

  private static final double PMF_EPS_FOR_K_8 = 0.35; // PMF rank error (epsilon) for k=8
  private static final double PMF_EPS_FOR_K_128 = 0.025; // PMF rank error (epsilon) for k=128
  private static final double PMF_EPS_FOR_K_256 = 0.013; // PMF rank error (epsilon) for k=256
  private static final double NUMERIC_NOISE_TOLERANCE = 1E-6;
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void empty() {
    final KllDoublesSketch sketch = getUpdatableDirectDoublesSketch(200, 0);
    sketch.update(Double.NaN); // this must not change anything
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getNumRetained(), 0);
    try { sketch.getRank(0.5); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getQuantile(0.5); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getQuantiles(new double[] {0.0, 1.0}); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getPMF(new double[] {0}); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getCDF(new double[0]); fail(); } catch (SketchesArgumentException e) {}
    assertNotNull(sketch.toString(true, true));
    assertNotNull(sketch.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantileInvalidArg() {
    final KllDoublesSketch sketch = getUpdatableDirectDoublesSketch(200, 0);
    sketch.update(1);
    sketch.getQuantile(-1.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantilesInvalidArg() {
    final KllDoublesSketch sketch = getUpdatableDirectDoublesSketch(200, 0);
    sketch.update(1);
    sketch.getQuantiles(new double[] {2.0});
  }

  @Test
  public void oneValue() {
    final KllDoublesSketch sketch = getUpdatableDirectDoublesSketch(200, 0);
    sketch.update(1);
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 1);
    assertEquals(sketch.getNumRetained(), 1);
    assertEquals(sketch.getRank(1, EXCLUSIVE), 0.0);
    assertEquals(sketch.getRank(2, EXCLUSIVE), 1.0);
    assertEquals(sketch.getMinItem(), 1.0);
    assertEquals(sketch.getMaxItem(), 1.0);
    assertEquals(sketch.getQuantile(0.5, EXCLUSIVE), 1.0);
  }

  @Test
  public void manyValuesEstimationMode() {
    final KllDoublesSketch sketch = getUpdatableDirectDoublesSketch(200, 0);
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
    final double[] pmf = sketch.getPMF(new double[] {n / 2.0}); // split at median
    assertEquals(pmf.length, 2);
    assertEquals(pmf[0], 0.5, PMF_EPS_FOR_K_256);
    assertEquals(pmf[1], 0.5, PMF_EPS_FOR_K_256);
    assertEquals(sketch.getMinItem(), 0.0); // min value is exact
    assertEquals(sketch.getMaxItem(), n - 1.0); // max value is exact

    // check at every 0.1 percentage point
    final double[] ranks = new double[1001];
    final double[] reverseFractions = new double[1001]; // check that ordering doesn't matter
    for (int i = 0; i <= 1000; i++) {
      ranks[i] = (double) i / 1000;
      reverseFractions[1000 - i] = ranks[i];
    }
    final double[] quantiles = sketch.getQuantiles(ranks);
    final double[] reverseQuantiles = sketch.getQuantiles(reverseFractions);
    double previousQuantile = 0.0;
    for (int i = 0; i <= 1000; i++) {
      final double quantile = sketch.getQuantile(ranks[i]);
      assertEquals(quantile, quantiles[i]);
      assertEquals(quantile, reverseQuantiles[1000 - i]);
      assertTrue(previousQuantile <= quantile);
      previousQuantile = quantile;
    }
  }

  @Test
  public void getRankGetCdfGetPmfConsistency() {
    final KllDoublesSketch sketch = getUpdatableDirectDoublesSketch(200, 0);
    final int n = 1000;
    final double[] values = new double[n];
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
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(200, 0);
    final KllDoublesSketch sketch2 = getUpdatableDirectDoublesSketch(200, 0);
    final int n = 10_000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i * 1.0);
      sketch2.update((2 * n - i - 1) * 1.0);
    }

    assertEquals(sketch1.getMinItem(), 0.0);
    assertEquals(sketch1.getMaxItem(), (n - 1) * 1.0);

    assertEquals(sketch2.getMinItem(), n * 1.0);
    assertEquals(sketch2.getMaxItem(), (2 * n - 1) * 1.0);

    sketch1.merge(sketch2);

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2L * n);
    assertEquals(sketch1.getMinItem(), 0.0);
    assertEquals(sketch1.getMaxItem(), (2 * n - 1) * 1.0);
    assertEquals(sketch1.getQuantile(0.5), n * 1.0, n * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeLowerK() {
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(256, 0);
    final KllDoublesSketch sketch2 = getUpdatableDirectDoublesSketch(128, 0);
    final int n = 10_000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update(2 * n - i - 1);
    }

    assertEquals(sketch1.getMinItem(), 0.0f);
    assertEquals(sketch1.getMaxItem(), n - 1f);

    assertEquals(sketch2.getMinItem(), n);
    assertEquals(sketch2.getMaxItem(), 2f * n - 1f);

    assertTrue(sketch1.getNormalizedRankError(false) < sketch2.getNormalizedRankError(false));
    assertTrue(sketch1.getNormalizedRankError(true) < sketch2.getNormalizedRankError(true));
    sketch1.merge(sketch2);

    // sketch1 must get "contaminated" by the lower K in sketch2
    assertEquals(sketch1.getNormalizedRankError(false), sketch2.getNormalizedRankError(false));
    assertEquals(sketch1.getNormalizedRankError(true), sketch2.getNormalizedRankError(true));

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2 * n);
    assertEquals(sketch1.getMinItem(), 0.0);
    assertEquals(sketch1.getMaxItem(), 2.0 * n - 1.0);
    assertEquals(sketch1.getQuantile(0.5), n, n * PMF_EPS_FOR_K_128);
  }

  @Test
  public void mergeEmptyLowerK() {
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(256, 0);
    final KllDoublesSketch sketch2 = getUpdatableDirectDoublesSketch(128, 0);
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
    assertEquals(sketch1.getMinItem(), 0.0);
    assertEquals(sketch1.getMaxItem(), n - 1.0);
    assertEquals(sketch1.getQuantile(0.5), n / 2.0, n / 2 * PMF_EPS_FOR_K_256);

    //merge the other way
    sketch2.merge(sketch1);
    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch1.getMinItem(), 0.0);
    assertEquals(sketch1.getMaxItem(), n - 1.0);
    assertEquals(sketch1.getQuantile(0.5), n / 2.0, n / 2 * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeExactModeLowerK() {
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(256, 0);
    final KllDoublesSketch sketch2 = getUpdatableDirectDoublesSketch(128, 0);
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
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(200, 0);
    final KllDoublesSketch sketch2 = getUpdatableDirectDoublesSketch(200, 0);
    sketch1.update(1);
    sketch2.update(2);
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinItem(), 1.0);
  }

  @Test
  public void mergeMinAndMaxFromOther() {
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(200, 0);
    final KllDoublesSketch sketch2 = getUpdatableDirectDoublesSketch(200, 0);
    for (int i = 1; i <= 1_000_000; i++) {
      sketch1.update(i);
    }
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinItem(), 1);
    assertEquals(sketch2.getMaxItem(), 1_000_000);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooSmall() {
    getUpdatableDirectDoublesSketch(KllSketch.DEFAULT_M - 1, 0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooLarge() {
    getUpdatableDirectDoublesSketch(KllSketch.MAX_K + 1, 0);
  }

  @Test
  public void minK() {
    final KllDoublesSketch sketch = getUpdatableDirectDoublesSketch(KllSketch.DEFAULT_M, 0);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getK(), KllSketch.DEFAULT_M);
    assertEquals(sketch.getQuantile(0.5), 500, 500 * PMF_EPS_FOR_K_8);
  }

  @Test
  public void maxK() {
    final KllDoublesSketch sketch = getUpdatableDirectDoublesSketch(KllSketch.MAX_K, 0);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getK(), KllSketch.MAX_K);
    assertEquals(sketch.getQuantile(0.5), 500, 500 * PMF_EPS_FOR_K_256);
  }

  @Test
  public void serializeDeserializeEmptyViaCompactHeapify() {
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(200, 0);
    final byte[] bytes = sketch1.toByteArray(); //compact
    final KllDoublesSketch sketch2 = KllDoublesSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.currentSerializedSizeBytes(false));
    assertTrue(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    try { sketch2.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sketch2.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    assertEquals(sketch2.currentSerializedSizeBytes(false),
        sketch1.currentSerializedSizeBytes(false));
  }

  @Test
  public void serializeDeserializeEmptyViaUpdatableWritableWrap() {
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(200, 0);
    final byte[] bytes = KllHelper.toByteArray(sketch1, true);
    final KllDoublesSketch sketch2 =
        KllDoublesSketch.writableWrap(WritableMemory.writableWrap(bytes),memReqSvr);
    assertEquals(bytes.length, sketch1.currentSerializedSizeBytes(true));
    assertTrue(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    try { sketch2.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sketch2.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    assertEquals(sketch2.currentSerializedSizeBytes(true),
        sketch1.currentSerializedSizeBytes(true));
  }

  @Test
  public void serializeDeserializeOneValueViaCompactHeapify() {
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(200, 0);
    sketch1.update(1);
    final byte[] bytes = sketch1.toByteArray();
    final KllDoublesSketch sketch2 = KllDoublesSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.currentSerializedSizeBytes(false));
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), 1);
    assertEquals(sketch2.getN(), 1);
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertTrue(Double.isFinite(sketch2.getMinItem()));
    assertTrue(Double.isFinite(sketch2.getMaxItem()));
    assertEquals(sketch2.currentSerializedSizeBytes(false), 8 + Double.BYTES);
  }

  @Test
  public void serializeDeserializeOneValueViaUpdatableWritableWrap() {
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(200, 0);
    sketch1.update(1);
    final byte[] bytes = KllHelper.toByteArray(sketch1, true);
    final KllDoublesSketch sketch2 =
        KllDoublesSketch.writableWrap(WritableMemory.writableWrap(bytes),memReqSvr);
    assertEquals(bytes.length, sketch1.currentSerializedSizeBytes(true));
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), 1);
    assertEquals(sketch2.getN(), 1);
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinItem(), 1.0);
    assertEquals(sketch2.getMaxItem(), 1.0);
    assertEquals(sketch2.currentSerializedSizeBytes(false), 8 + Double.BYTES);
    assertEquals(sketch2.currentSerializedSizeBytes(true), bytes.length);
  }

  @Test
  public void serializeDeserializeFullViaCompactHeapify() {
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(200, 1000);
    final byte[] byteArr1 = sketch1.toByteArray(); //compact
    final KllDoublesSketch sketch2 =  KllDoublesSketch.heapify(Memory.wrap(byteArr1));
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
    final KllDoublesSketch sketch1 = getUpdatableDirectDoublesSketch(200, 0);
    final int n = 1000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
    }
    final byte[] bytes = KllHelper.toByteArray(sketch1, true); //updatable
    final KllDoublesSketch sketch2 =
        KllDoublesSketch.writableWrap(WritableMemory.writableWrap(bytes),memReqSvr);
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
    final KllDoublesSketch sketch = getUpdatableDirectDoublesSketch(200, 0);
    sketch.update(0);
    sketch.getCDF(new double[] {1, 0});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void nanSplitPoint() {
    final KllDoublesSketch sketch = getUpdatableDirectDoublesSketch(200, 0);
    sketch.update(0);
    sketch.getCDF(new double[] {Double.NaN});
  }

  @Test
  public void checkSimpleMergeDirect() { //used for troubleshooting
    int k = 20;
    int n1 = 21;
    int n2 = 43;
    KllDoublesSketch sk1 = KllDoublesSketch.newHeapInstance(k);
    KllDoublesSketch sk2 =  KllDoublesSketch.newHeapInstance(k);
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
    WritableMemory wmem1 = WritableMemory.writableWrap(KllHelper.toByteArray(sk1, true));
    WritableMemory wmem2 = WritableMemory.writableWrap(KllHelper.toByteArray(sk2, true));
    KllDoublesSketch dsk1 = KllDoublesSketch.writableWrap(wmem1, memReqSvr);
    KllDoublesSketch dsk2 = KllDoublesSketch.writableWrap(wmem2, memReqSvr);
    println("BEFORE MERGE");
    println(dsk1.toString(true, true));
    dsk1.merge(dsk2);
    println("AFTER MERGE");
    println(dsk1.toString(true, true));
  }

  @Test
  public void checkSketchInitializeDirectDoubleUpdatableMem() {
    int k = 20; //don't change this
    KllDoublesSketch sk;
    KllDoublesSketch sk2;
    byte[] compBytes;
    WritableMemory wmem;

    println("#### CASE: DOUBLE FULL DIRECT FROM UPDATABLE");
    sk2 = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk2.update(i); }
    //println(sk2.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(compBytes, DOUBLES_SKETCH, true));
    sk = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 33);
    assertEquals(sk.levelsArr.length, 3);
    assertEquals(sk.getMaxItem(), 21.0);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: DOUBLE EMPTY HEAPIFIED FROM UPDATABLE");
    sk2 = KllDoublesSketch.newHeapInstance(k);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(compBytes, DOUBLES_SKETCH, true));
    sk = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.levelsArr.length, 2);
    try { sk.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sk.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: DOUBLE SINGLE HEAPIFIED FROM UPDATABLE");
    sk2 = KllDoublesSketch.newHeapInstance(k);
    sk2.update(1);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(compBytes, DOUBLES_SKETCH, true));
    sk = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.levelsArr.length, 2);
    assertEquals(sk.getMaxItem(), 1.0);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test
  public void checkGetWritableMemory() {
    final KllDoublesSketch sketch = getUpdatableDirectDoublesSketch(200, 200);
    assertEquals(sketch.getK(), 200);
    assertEquals(sketch.getN(), 200);
    assertFalse(sketch.isEmpty());
    assertTrue(sketch.isMemoryUpdatableFormat());
    assertFalse(sketch.isEstimationMode());
    assertTrue(sketch.isDoublesSketch());
    assertFalse(sketch.isLevelZeroSorted());
    assertFalse(sketch.isFloatsSketch());

    final WritableMemory wmem = sketch.getWritableMemory();
    final KllDoublesSketch sk = KllHeapDoublesSketch.heapifyImpl(wmem);
    assertEquals(sk.getK(), 200);
    assertEquals(sk.getN(), 200);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isMemoryUpdatableFormat());
    assertFalse(sk.isEstimationMode());
    assertTrue(sk.isDoublesSketch());
    assertFalse(sk.isLevelZeroSorted());
    assertFalse(sk.isFloatsSketch());
  }

  @Test
  public void checkReset() {
    WritableMemory dstMem = WritableMemory.allocate(6000);
    KllDoublesSketch sk = KllDoublesSketch.newDirectInstance(20, dstMem, memReqSvr);
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    long n1 = sk.getN();
    double min1 = sk.getMinItem();
    double max1 = sk.getMaxItem();
    sk.reset();
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    long n2 = sk.getN();
    double min2 = sk.getMinItem();
    double max2 = sk.getMaxItem();
    assertEquals(n2, n1);
    assertEquals(min2, min1);
    assertEquals(max2, max1);
  }

  @Test
  public void checkHeapify() {
    WritableMemory dstMem = WritableMemory.allocate(6000);
    KllDoublesSketch sk = KllDoublesSketch.newDirectInstance(20, dstMem, memReqSvr);
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    KllDoublesSketch sk2 = KllHeapDoublesSketch.heapifyImpl(dstMem);
    assertEquals(sk2.getMinItem(), 1.0);
    assertEquals(sk2.getMaxItem(), 100.0);
  }

  @Test
  public void checkMergeKllDoublesSketch() {
    WritableMemory dstMem = WritableMemory.allocate(6000);
    KllDoublesSketch sk = KllDoublesSketch.newDirectInstance(20, dstMem, memReqSvr);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    KllDoublesSketch sk2 = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++ ) { sk2.update(i + 100); }
    sk.merge(sk2);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getMaxItem(), 121.0);
  }

  @Test
  public void checkReverseMergeKllDoubleSketch() {
    WritableMemory dstMem = WritableMemory.allocate(6000);
    KllDoublesSketch sk = KllDoublesSketch.newDirectInstance(20, dstMem, memReqSvr);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    KllDoublesSketch sk2 = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++ ) { sk2.update(i + 100); }
    sk2.merge(sk);
    assertEquals(sk2.getMinItem(), 1.0);
    assertEquals(sk2.getMaxItem(), 121.0);
  }

  @Test
  public void checkWritableWrapOfCompactForm() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++ ) { sk.update(i); }
    WritableMemory srcMem = WritableMemory.writableWrap(sk.toByteArray());
    KllDoublesSketch sk2 = KllDoublesSketch.writableWrap(srcMem, memReqSvr);
    assertEquals(sk2.getMinItem(), 1.0);
    assertEquals(sk2.getMaxItem(), 21.0);
  }

  @Test
  public void checkReadOnlyExceptions() {
    int k = 20;
    double[] dblArr = new double[0];
    double dblV = 1.0f;
    int idx = 1;
    boolean bool = true;
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    try { sk2.incN(1);                           fail(); } catch (SketchesArgumentException e) { }
    try { sk2.incNumLevels();                    fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setDoubleItemsArray(dblArr);       fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setDoubleItemsArrayAt(idx, dblV);  fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setLevelZeroSorted(bool);          fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setMaxItem(dblV);                  fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setMinItem(dblV);                  fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setMinK(idx);                      fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setN(idx);                         fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setNumLevels(idx);                 fail(); } catch (SketchesArgumentException e) { }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMergeExceptions() {
    KllDoublesSketch sk1 = KllDoublesSketch.newHeapInstance(20);
    WritableMemory srcMem1 = WritableMemory.writableWrap(sk1.toByteArray());
    KllDoublesSketch sk2 = KllDoublesSketch.writableWrap(srcMem1, memReqSvr);
    sk2.merge(sk1);
  }

  @Test
  public void checkMergeExceptionsWrongType() {
    KllFloatsSketch sk1 = KllFloatsSketch.newHeapInstance(20);
    KllDoublesSketch sk2 = KllDoublesSketch.newHeapInstance(20);
    try { sk1.merge(sk2); fail(); } catch (ClassCastException e) { }
    try { sk2.merge(sk1); fail(); } catch (ClassCastException e) { }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void checkVectorUpdate() {
    WritableMemory dstMem = WritableMemory.allocate(6000);
    KllDoublesSketch sk = KllDoublesSketch.newDirectInstance(20, dstMem, memReqSvr);
    final double[] v = new double[21];
    for (int i = 0; i < 21; i++) { v[i] = i + 1; }
    sk.update(v, 0, 21);
  }

  @Test
  public void checkWeightedUpdate() {
    WritableMemory dstMem = WritableMemory.allocate(6000);
    KllDoublesSketch sk = KllDoublesSketch.newDirectInstance(8, dstMem, memReqSvr);
    for (int i = 0; i < 16; i++) {
      sk.update(i + 1, 16);
    }
    println(sk.toString(true, true));
  }

  private static KllDoublesSketch getUpdatableDirectDoublesSketch(final int k, final int n) {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    byte[] byteArr = KllHelper.toByteArray(sk, true);
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    KllDoublesSketch ddsk = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    return ddsk;
  }

  private final static boolean enablePrinting = false;

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
