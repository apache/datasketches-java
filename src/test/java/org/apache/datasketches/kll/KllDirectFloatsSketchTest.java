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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class KllDirectFloatsSketchTest {

  private static final double PMF_EPS_FOR_K_8 = 0.35; // PMF rank error (epsilon) for k=8
  private static final double PMF_EPS_FOR_K_128 = 0.025; // PMF rank error (epsilon) for k=128
  private static final double PMF_EPS_FOR_K_256 = 0.013; // PMF rank error (epsilon) for k=256
  private static final double NUMERIC_NOISE_TOLERANCE = 1E-6;
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void empty() {
    final KllFloatsSketch sketch = getDFSketch(200, 0);
    sketch.update(Float.NaN); // this must not change anything
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getNumRetained(), 0);
    assertTrue(Double.isNaN(sketch.getRank(0)));
    assertTrue(Float.isNaN(sketch.getMinValue()));
    assertTrue(Float.isNaN(sketch.getMaxValue()));
    assertTrue(Float.isNaN(sketch.getQuantile(0.5)));
    assertNull(sketch.getQuantiles(new double[] {0}));
    assertNull(sketch.getPMF(new float[] {0}));
    assertNotNull(sketch.toString(true, true));
    assertNotNull(sketch.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantileInvalidArg() {
    final KllFloatsSketch sketch = getDFSketch(200, 0);
    sketch.update(1);
    sketch.getQuantile(-1.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantilesInvalidArg() {
    final KllFloatsSketch sketch = getDFSketch(200, 0);
    sketch.update(1);
    sketch.getQuantiles(new double[] {2.0});
  }

  @Test
  public void oneItem() {
    final KllFloatsSketch sketch = getDFSketch(200, 0);
    sketch.update(1);
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 1);
    assertEquals(sketch.getNumRetained(), 1);
    assertEquals(sketch.getRank(1), 0.0);
    assertEquals(sketch.getRank(2), 1.0);
    assertEquals(sketch.getMinValue(), 1f);
    assertEquals(sketch.getMaxValue(), 1f);
    assertEquals(sketch.getQuantile(0.5), 1f);
  }

  @Test
  public void manyItemsEstimationMode() {
    final KllFloatsSketch sketch = getDFSketch(200, 0);
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
    final double[] pmf = sketch.getPMF(new float[] {n / 2}); // split at median
    assertEquals(pmf.length, 2);
    assertEquals(pmf[0], 0.5, PMF_EPS_FOR_K_256);
    assertEquals(pmf[1], 0.5, PMF_EPS_FOR_K_256);

    assertEquals(sketch.getMinValue(), 0f); // min value is exact
    assertEquals(sketch.getQuantile(0), 0f); // min value is exact
    assertEquals(sketch.getMaxValue(), n - 1f); // max value is exact
    assertEquals(sketch.getQuantile(1), n - 1f); // max value is exact

    // check at every 0.1 percentage point
    final double[] fractions = new double[1001];
    final double[] reverseFractions = new double[1001]; // check that ordering doesn't matter
    for (int i = 0; i <= 1000; i++) {
      fractions[i] = (double) i / 1000;
      reverseFractions[1000 - i] = fractions[i];
    }
    final float[] quantiles = sketch.getQuantiles(fractions);
    final float[] reverseQuantiles = sketch.getQuantiles(reverseFractions);
    double previousQuantile = 0;
    for (int i = 0; i <= 1000; i++) {
      final double quantile = sketch.getQuantile(fractions[i]);
      assertEquals(quantile, quantiles[i]);
      assertEquals(quantile, reverseQuantiles[1000 - i]);
      assertTrue(previousQuantile <= quantile);
      previousQuantile = quantile;
    }
  }

  @Test
  public void getRankGetCdfGetPmfConsistency() {
    final KllFloatsSketch sketch = getDFSketch(200, 0);
    final int n = 1000;
    final float[] values = new float[n];
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
    final KllFloatsSketch sketch1 = getDFSketch(200, 0);
    final KllFloatsSketch sketch2 = getDFSketch(200, 0);
    final int n = 10_000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i * 1.0F);
      sketch2.update((2 * n - i - 1) * 1.0F);
    }

    assertEquals(sketch1.getMinValue(), 0.0);
    assertEquals(sketch1.getMaxValue(), (n - 1) * 1.0);

    assertEquals(sketch2.getMinValue(), n * 1.0);
    assertEquals(sketch2.getMaxValue(), (2 * n - 1) * 1.0);

    sketch1.merge(sketch2);

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2L * n);
    assertEquals(sketch1.getMinValue(), 0.0);
    assertEquals(sketch1.getMaxValue(), (2 * n - 1) * 1.0F);
    assertEquals(sketch1.getQuantile(0.5), n * 1.0F, n * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeLowerK() {
    final KllFloatsSketch sketch1 = getDFSketch(256, 0);
    final KllFloatsSketch sketch2 = getDFSketch(128, 0);
    final int n = 10_000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update(2 * n - i - 1);
    }

    assertEquals(sketch1.getMinValue(), 0.0f);
    assertEquals(sketch1.getMaxValue(), n - 1f);

    assertEquals(sketch2.getMinValue(), n);
    assertEquals(sketch2.getMaxValue(), 2f * n - 1f);

    assertTrue(sketch1.getNormalizedRankError(false) < sketch2.getNormalizedRankError(false));
    assertTrue(sketch1.getNormalizedRankError(true) < sketch2.getNormalizedRankError(true));
    sketch1.merge(sketch2);

    // sketch1 must get "contaminated" by the lower K in sketch2
    assertEquals(sketch1.getNormalizedRankError(false), sketch2.getNormalizedRankError(false));
    assertEquals(sketch1.getNormalizedRankError(true), sketch2.getNormalizedRankError(true));

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2 * n);
    assertEquals(sketch1.getMinValue(), 0f);
    assertEquals(sketch1.getMaxValue(), 2f * n - 1f);
    assertEquals(sketch1.getQuantile(0.5), n, n * PMF_EPS_FOR_K_128);
  }

  @Test
  public void mergeEmptyLowerK() {
    final KllFloatsSketch sketch1 = getDFSketch(256, 0);
    final KllFloatsSketch sketch2 = getDFSketch(128, 0);
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
    }

    // rank error should not be affected by a merge with an empty sketch with lower K
    final double rankErrorBeforeMerge = sketch1.getNormalizedRankError(true);
    sketch1.merge(sketch2);
    assertEquals(sketch1.getNormalizedRankError(true), rankErrorBeforeMerge);

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch1.getMinValue(), 0f);
    assertEquals(sketch1.getMaxValue(), n - 1f);
    assertEquals(sketch1.getQuantile(0.5), n / 2f, n / 2 * PMF_EPS_FOR_K_256);

    //merge the other way
    sketch2.merge(sketch1);
    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch1.getMinValue(), 0f);
    assertEquals(sketch1.getMaxValue(), n - 1f);
    assertEquals(sketch1.getQuantile(0.5), n / 2f, n / 2 * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeExactModeLowerK() {
    final KllFloatsSketch sketch1 = getDFSketch(256, 0);
    final KllFloatsSketch sketch2 = getDFSketch(128, 0);
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
    final KllFloatsSketch sketch1 = getDFSketch(200, 0);
    final KllFloatsSketch sketch2 = getDFSketch(200, 0);
    sketch1.update(1);
    sketch2.update(2);
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinValue(), 1.0F);
  }

  @Test
  public void mergeMinAndMaxFromOther() {
    final KllFloatsSketch sketch1 = getDFSketch(200, 0);
    final KllFloatsSketch sketch2 = getDFSketch(200, 0);
    for (int i = 1; i <= 1_000_000; i++) {
      sketch1.update(i);
    }
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinValue(), 1F);
    assertEquals(sketch2.getMaxValue(), 1_000_000F);
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooSmall() {
    final KllFloatsSketch sketch1 = getDFSketch(KllSketch.DEFAULT_M - 1, 0);
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooLarge() {
    final KllFloatsSketch sketch1 = getDFSketch(KllSketch.MAX_K + 1, 0);
  }

  @Test
  public void minK() {
    final KllFloatsSketch sketch = getDFSketch(KllSketch.DEFAULT_M, 0);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getK(), KllSketch.DEFAULT_M);
    assertEquals(sketch.getQuantile(0.5), 500, 500 * PMF_EPS_FOR_K_8);
  }

  @Test
  public void maxK() {
    final KllFloatsSketch sketch = getDFSketch(KllSketch.MAX_K, 0);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getK(), KllSketch.MAX_K);
    assertEquals(sketch.getQuantile(0.5), 500, 500 * PMF_EPS_FOR_K_256);
  }

  @Test
  public void serializeDeserializeEmptyViaCompactHeapify() {
    final KllFloatsSketch sketch1 = getDFSketch(200, 0);
    final byte[] bytes = sketch1.toByteArray();
    final KllFloatsSketch sketch2 = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getCurrentCompactSerializedSizeBytes());
    assertTrue(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertTrue(Double.isNaN(sketch2.getMinValue()));
    assertTrue(Double.isNaN(sketch2.getMaxValue()));
    assertEquals(sketch2.getCurrentCompactSerializedSizeBytes(), sketch1.getCurrentCompactSerializedSizeBytes());
  }

  @Test
  public void serializeDeserializeEmptyViaUpdatableWritableWrap() {
    final KllFloatsSketch sketch1 = getDFSketch(200, 0);
    final byte[] bytes = KllHelper.toUpdatableByteArrayImpl(sketch1);
    final KllFloatsSketch sketch2 =
        KllFloatsSketch.writableWrap(WritableMemory.writableWrap(bytes),memReqSvr);
    assertEquals(bytes.length, sketch1.getCurrentUpdatableSerializedSizeBytes());
    assertTrue(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertTrue(Double.isNaN(sketch2.getMinValue()));
    assertTrue(Double.isNaN(sketch2.getMaxValue()));
    assertEquals(sketch2.getCurrentCompactSerializedSizeBytes(), sketch1.getCurrentCompactSerializedSizeBytes());
  }

  @Test
  public void serializeDeserializeOneItemViaCompactHeapify() {
    final KllFloatsSketch sketch1 = getDFSketch(200, 0);
    sketch1.update(1);
    final byte[] bytes = sketch1.toByteArray();
    final KllFloatsSketch sketch2 = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getCurrentCompactSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), 1);
    assertEquals(sketch2.getN(), 1);
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertFalse(Double.isNaN(sketch2.getMinValue()));
    assertFalse(Double.isNaN(sketch2.getMaxValue()));
    assertEquals(sketch2.getCurrentCompactSerializedSizeBytes(), 8 + Float.BYTES);
  }

  @Test
  public void serializeDeserializeOneItemViaUpdatableWritableWrap() {
    final KllFloatsSketch sketch1 = getDFSketch(200, 0);
    sketch1.update(1);
    final byte[] bytes = KllHelper.toUpdatableByteArrayImpl(sketch1);
    final KllFloatsSketch sketch2 =
        KllFloatsSketch.writableWrap(WritableMemory.writableWrap(bytes),memReqSvr);
    assertEquals(bytes.length, sketch1.getCurrentUpdatableSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), 1);
    assertEquals(sketch2.getN(), 1);
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertFalse(Double.isNaN(sketch2.getMinValue()));
    assertFalse(Double.isNaN(sketch2.getMaxValue()));
    assertEquals(sketch2.getCurrentCompactSerializedSizeBytes(), 8 + Float.BYTES);
  }

  @Test
  public void serializeDeserializeFullViaCompactHeapify() {
    final KllFloatsSketch sketch1 = getDFSketch(200, 0);
    final int n = 1000;
    for (int i = 0; i < n; i++) { sketch1.update(i); }
    final byte[] bytes = sketch1.toByteArray();
    final KllFloatsSketch sketch2 =  KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getCurrentCompactSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinValue(), sketch1.getMinValue());
    assertEquals(sketch2.getMaxValue(), sketch1.getMaxValue());
    assertEquals(sketch2.getCurrentCompactSerializedSizeBytes(), sketch1.getCurrentCompactSerializedSizeBytes());
  }

  @Test
  public void serializeDeserializeFullViaUpdatableWritableWrap() {
    final KllFloatsSketch sketch1 = getDFSketch(200, 0);
    final int n = 1000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
    }
    final byte[] bytes = KllHelper.toUpdatableByteArrayImpl(sketch1);
    final KllFloatsSketch sketch2 =
        KllFloatsSketch.writableWrap(WritableMemory.writableWrap(bytes),memReqSvr);
    assertEquals(bytes.length, sketch1.getCurrentUpdatableSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinValue(), sketch1.getMinValue());
    assertEquals(sketch2.getMaxValue(), sketch1.getMaxValue());
    assertEquals(sketch2.getCurrentCompactSerializedSizeBytes(), sketch1.getCurrentCompactSerializedSizeBytes());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void outOfOrderSplitPoints() {
    final KllFloatsSketch sketch = getDFSketch(200, 0);
    sketch.update(0);
    sketch.getCDF(new float[] {1, 0});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void nanSplitPoint() {
    final KllFloatsSketch sketch = getDFSketch(200, 0);
    sketch.update(0);
    sketch.getCDF(new float[] {Float.NaN});
  }

  @Test
  public void getQuantiles() {
    final KllFloatsSketch sketch = getDFSketch(200, 0);
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    final float[] quantiles1 = sketch.getQuantiles(new double[] {0, 0.5, 1});
    final float[] quantiles2 = sketch.getQuantiles(3);
    assertEquals(quantiles1, quantiles2);
    assertEquals(quantiles1[0], 1f);
    assertEquals(quantiles1[1], 2f);
    assertEquals(quantiles1[2], 3f);
  }

  @Test
  public void checkSimpleMergeDirect() { //used for troubleshooting
    int k = 20;
    int n1 = 21;
    int n2 = 43;
    KllFloatsSketch sk1 = KllFloatsSketch.newHeapInstance(k);
    KllFloatsSketch sk2 = KllFloatsSketch.newHeapInstance(k);
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
    WritableMemory wmem1 = WritableMemory.writableWrap(KllHelper.toUpdatableByteArrayImpl(sk1));
    WritableMemory wmem2 = WritableMemory.writableWrap(KllHelper.toUpdatableByteArrayImpl(sk2));
    KllFloatsSketch dsk1 = KllFloatsSketch.writableWrap(wmem1, memReqSvr);
    KllFloatsSketch dsk2 = KllFloatsSketch.writableWrap(wmem2, memReqSvr);
    println("BEFORE MERGE");
    println(dsk1.toString(true, true));
    dsk1.merge(dsk2);
    println("AFTER MERGE");
    println(dsk1.toString(true, true));
  }

  @Test
  public void checkSketchInitializeDirectDoubleUpdatableMem() {
    int k = 20; //don't change this
    KllFloatsSketch sk;
    KllFloatsSketch sk2;
    byte[] compBytes;
    WritableMemory wmem;

    println("#### CASE: DOUBLE FULL DIRECT FROM UPDATABLE");
    sk2 = KllFloatsSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk2.update(i); }
    //println(sk2.toString(true, true));
    compBytes = KllHelper.toUpdatableByteArrayImpl(sk2);
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(compBytes, true));
    sk = KllFloatsSketch.writableWrap(wmem, memReqSvr);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getFloatItemsArray().length, 33);
    assertEquals(sk.getLevelsArray().length, 3);
    assertEquals(sk.getMaxFloatValue(), 21.0);
    assertEquals(sk.getMinFloatValue(), 1.0);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: DOUBLE EMPTY HEAPIFIED FROM UPDATABLE");
    sk2 = KllFloatsSketch.newHeapInstance(k);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toUpdatableByteArrayImpl(sk2);
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(compBytes, true));
    sk = KllFloatsSketch.writableWrap(wmem, memReqSvr);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxFloatValue(), Double.NaN);
    assertEquals(sk.getMinFloatValue(), Double.NaN);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: DOUBLE SINGLE HEAPIFIED FROM UPDATABLE");
    sk2 = KllFloatsSketch.newHeapInstance(k);
    sk2.update(1);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toUpdatableByteArrayImpl(sk2);
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(compBytes, true));
    sk = KllFloatsSketch.writableWrap(wmem, memReqSvr);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxFloatValue(), 1.0);
    assertEquals(sk.getMinFloatValue(), 1.0);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test
  public void checkGetWritableMemory() {
    final KllFloatsSketch sketch = getDFSketch(200, 200);
    assertEquals(sketch.getK(), 200);
    assertEquals(sketch.getN(), 200);
    assertFalse(sketch.isEmpty());
    assertTrue(sketch.isMemoryUpdatableFormat());
    assertFalse(sketch.isEstimationMode());
    assertTrue(sketch.isFloatsSketch());
    assertFalse(sketch.isLevelZeroSorted());
    assertFalse(sketch.isDoublesSketch());

    final WritableMemory wmem = sketch.getWritableMemory();
    final KllFloatsSketch sk = KllHeapFloatsSketch.heapifyImpl(wmem);
    assertEquals(sk.getK(), 200);
    assertEquals(sk.getN(), 200);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isMemoryUpdatableFormat());
    assertFalse(sk.isEstimationMode());
    assertTrue(sk.isFloatsSketch());
    assertFalse(sk.isLevelZeroSorted());
    assertFalse(sk.isDoublesSketch());
  }

  @Test
  public void checkReset() {
    WritableMemory dstMem = WritableMemory.allocate(3000);
    KllFloatsSketch sk = KllFloatsSketch.newDirectInstance(20, dstMem, memReqSvr);
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    long n1 = sk.getN();
    float min1 = sk.getMinValue();
    float max1 = sk.getMaxValue();
    sk.reset();
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    long n2 = sk.getN();
    float min2 = sk.getMinValue();
    float max2 = sk.getMaxValue();
    assertEquals(n2, n1);
    assertEquals(min2, min1);
    assertEquals(max2, max1);
  }

  @Test
  public void checkHeapify() {
    WritableMemory dstMem = WritableMemory.allocate(6000);
    KllFloatsSketch sk = KllFloatsSketch.newDirectInstance(20, dstMem, memReqSvr);
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    KllFloatsSketch sk2 = KllHeapFloatsSketch.heapifyImpl(dstMem);
    assertEquals(sk2.getMinValue(), 1.0);
    assertEquals(sk2.getMaxValue(), 100.0);
  }

  @Test
  public void checkMergeKllFloatsSketch() {
    WritableMemory dstMem = WritableMemory.allocate(6000);
    KllFloatsSketch sk = KllFloatsSketch.newDirectInstance(20, dstMem, memReqSvr);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    KllFloatsSketch sk2 = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++ ) { sk2.update(i + 100); }
    sk.merge(sk2);
    assertEquals(sk.getMinValue(), 1.0);
    assertEquals(sk.getMaxValue(), 121.0);
  }

  @Test
  public void checkReverseMergeKllFloatsSketch() {
    WritableMemory dstMem = WritableMemory.allocate(6000);
    KllFloatsSketch sk = KllFloatsSketch.newDirectInstance(20, dstMem, memReqSvr);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    KllFloatsSketch sk2 = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++ ) { sk2.update(i + 100); }
    sk2.merge(sk);
    assertEquals(sk2.getMinValue(), 1.0);
    assertEquals(sk2.getMaxValue(), 121.0);
  }

  @Test
  public void checkWritableWrapOfCompactForm() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++ ) { sk.update(i); }
    WritableMemory srcMem = WritableMemory.writableWrap(sk.toByteArray());
    KllFloatsSketch sk2 = KllFloatsSketch.writableWrap(srcMem, memReqSvr);
    assertEquals(sk2.getMinValue(), 1.0F);
    assertEquals(sk2.getMaxValue(), 21.0F);
  }

  @Test
  public void chickReadOnlyExceptions() {
    int k = 20;
    float[] fltArr = new float[0];
    float fltV = 1.0f;
    int idx = 1;
    boolean bool = true;
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(k);
    KllFloatsSketch sk2 = KllFloatsSketch.wrap(Memory.wrap(sk.toByteArray()));
    try { sk2.incN();                          fail(); } catch (SketchesArgumentException e) { }
    try { sk2.incNumLevels();                  fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setFloatItemsArray(fltArr);      fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setFloatItemsArrayAt(idx, fltV); fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setLevelZeroSorted(bool);        fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setMaxFloatValue(fltV);          fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setMinFloatValue(fltV);          fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setMinK(idx);                    fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setN(idx);                       fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setNumLevels(idx);               fail(); } catch (SketchesArgumentException e) { }
    try { sk2.getDoubleSingleItem();           fail(); } catch (SketchesArgumentException e) { }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMergeExceptions() {
    KllFloatsSketch sk1 = KllFloatsSketch.newHeapInstance(20);
    WritableMemory srcMem1 = WritableMemory.writableWrap(sk1.toByteArray());
    KllFloatsSketch sk2 = KllFloatsSketch.writableWrap(srcMem1, memReqSvr);
    sk2.merge(sk1);
  }

  @Test
  public void checkMergeExceptionsWrongType() {
    KllFloatsSketch sk1 = KllFloatsSketch.newHeapInstance(20);
    KllDoublesSketch sk2 = KllDoublesSketch.newHeapInstance(20);
    try { sk1.merge(sk2); fail(); } catch (SketchesArgumentException e) { }
    try { sk2.merge(sk1); fail(); } catch (SketchesArgumentException e) { }
  }

  private static KllFloatsSketch getDFSketch(final int k, final int n) {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(k);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    byte[] byteArr = KllHelper.toUpdatableByteArrayImpl(sk);
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    KllFloatsSketch dfsk = KllFloatsSketch.writableWrap(wmem, memReqSvr);
    return dfsk;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
