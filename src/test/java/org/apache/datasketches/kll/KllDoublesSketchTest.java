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

public class KllDoublesSketchTest {
  private static final double PMF_EPS_FOR_K_8 = 0.35; // PMF rank error (epsilon) for k=8
  private static final double PMF_EPS_FOR_K_128 = 0.025; // PMF rank error (epsilon) for k=128
  private static final double PMF_EPS_FOR_K_256 = 0.013; // PMF rank error (epsilon) for k=256
  private static final double NUMERIC_NOISE_TOLERANCE = 1E-6;
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void empty() {
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(Double.NaN); // this must not change anything
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getNumRetained(), 0);
    assertTrue(Double.isNaN(sketch.getRank(0)));
    assertTrue(Double.isNaN(sketch.getMinValue()));
    assertTrue(Double.isNaN(sketch.getMaxValue()));
    assertTrue(Double.isNaN(sketch.getQuantile(0.5)));
    assertNull(sketch.getQuantiles(new double[] {0}));
    assertNull(sketch.getPMF(new double[] {0}));
    assertNotNull(sketch.toString(true, true));
    assertNotNull(sketch.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantileInvalidArg() {
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(1);
    sketch.getQuantile(-1.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void getQuantilesInvalidArg() {
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(1);
    sketch.getQuantiles(new double[] {2.0});
  }

  @Test
  public void oneItem() {
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(1);
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 1);
    assertEquals(sketch.getNumRetained(), 1);
    assertEquals(sketch.getRank(1), 0.0);
    assertEquals(sketch.getRank(2), 1.0);
    assertEquals(sketch.getMinValue(), 1.0);
    assertEquals(sketch.getMaxValue(), 1.0);
    assertEquals(sketch.getQuantile(0.5), 1.0);
  }

  @Test
  public void manyItemsEstimationMode() {
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
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
    final double[] pmf = sketch.getPMF(new double[] {n / 2}); // split at median
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
    final double[] quantiles = sketch.getQuantiles(fractions);
    final double[] reverseQuantiles = sketch.getQuantiles(reverseFractions);
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
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
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
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance();
    final KllDoublesSketch sketch2 = KllDoublesSketch.newHeapInstance();
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i * 1.0);
      sketch2.update((2 * n - i - 1) * 1.0);
    }

    assertEquals(sketch1.getMinValue(), 0.0);
    assertEquals(sketch1.getMaxValue(), (n - 1) * 1.0);

    assertEquals(sketch2.getMinValue(), n * 1.0);
    assertEquals(sketch2.getMaxValue(), (2 * n - 1) * 1.0);

    sketch1.merge(sketch2);

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2L * n);
    assertEquals(sketch1.getMinValue(), 0.0);
    assertEquals(sketch1.getMaxValue(), (2 * n - 1) * 1.0);
    assertEquals(sketch1.getQuantile(0.5), n * 1.0, n * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeLowerK() {
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance(256);
    final KllDoublesSketch sketch2 = KllDoublesSketch.newHeapInstance(128);
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update(2 * n - i - 1);
    }

    assertEquals(sketch1.getMinValue(), 0.0f);
    assertEquals(sketch1.getMaxValue(), n - 1f);

    assertEquals(sketch2.getMinValue(), n);
    assertEquals(sketch2.getMaxValue(), 2f * n - 1.0);

    assertTrue(sketch1.getNormalizedRankError(false) < sketch2.getNormalizedRankError(false));
    assertTrue(sketch1.getNormalizedRankError(true) < sketch2.getNormalizedRankError(true));
    sketch1.merge(sketch2);

    // sketch1 must get "contaminated" by the lower K in sketch2
    assertEquals(sketch1.getNormalizedRankError(false), sketch2.getNormalizedRankError(false));
    assertEquals(sketch1.getNormalizedRankError(true), sketch2.getNormalizedRankError(true));

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2 * n);
    assertEquals(sketch1.getMinValue(), 0);
    assertEquals(sketch1.getMaxValue(), 2f * n - 1.0);
    assertEquals(sketch1.getQuantile(0.5), n, n * PMF_EPS_FOR_K_128);
  }

  @Test
  public void mergeEmptyLowerK() {
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance(256);
    final KllDoublesSketch sketch2 = KllDoublesSketch.newHeapInstance(128);
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
    assertEquals(sketch1.getMinValue(), 0);
    assertEquals(sketch1.getMaxValue(), n - 1.0);
    assertEquals(sketch1.getQuantile(0.5), n / 2.0, n / 2 * PMF_EPS_FOR_K_256);

    //merge the other way
    sketch2.merge(sketch1);
    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch1.getMinValue(), 0f);
    assertEquals(sketch1.getMaxValue(), n - 1.0);
    assertEquals(sketch1.getQuantile(0.5), n / 2.0, n / 2 * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeExactModeLowerK() {
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance(256);
    final KllDoublesSketch sketch2 = KllDoublesSketch.newHeapInstance(128);
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
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance();
    final KllDoublesSketch sketch2 = KllDoublesSketch.newHeapInstance();
    sketch1.update(1);
    sketch2.update(2);
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinValue(), 1.0);
  }

  @Test
  public void mergeMinAndMaxFromOther() {
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance();
    for (int i = 1; i <= 1_000_000; i++) {
      sketch1.update(i);
    }
    final KllDoublesSketch sketch2 = KllDoublesSketch.newHeapInstance();
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinValue(), 1);
    assertEquals(sketch2.getMaxValue(), 1_000_000);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooSmall() {
    KllDoublesSketch.newHeapInstance(KllSketch.DEFAULT_M - 1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooLarge() {
    KllDoublesSketch.newHeapInstance(KllSketch.MAX_K + 1);
  }

  @Test
  public void minK() {
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(KllSketch.DEFAULT_M);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getK(), KllSketch.DEFAULT_M);
    assertEquals(sketch.getQuantile(0.5), 500, 500 * PMF_EPS_FOR_K_8);
  }

  @Test
  public void maxK() {
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(KllSketch.MAX_K);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getK(), KllSketch.MAX_K);
    assertEquals(sketch.getQuantile(0.5), 500, 500 * PMF_EPS_FOR_K_256);
  }

  @Test
  public void serializeDeserializeEmpty() {
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance();
    final byte[] bytes = sketch1.toByteArray();
    final KllDoublesSketch sketch2 = KllDoublesSketch.heapify(Memory.wrap(bytes));
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
  public void serializeDeserializeOneItem() {
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance();
    sketch1.update(1);
    final byte[] bytes = sketch1.toByteArray();
    final KllDoublesSketch sketch2 = KllDoublesSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getCurrentCompactSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), 1);
    assertEquals(sketch2.getN(), 1);
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertFalse(Double.isNaN(sketch2.getMinValue()));
    assertFalse(Double.isNaN(sketch2.getMaxValue()));
    assertEquals(sketch2.getCurrentCompactSerializedSizeBytes(), 8 + Double.BYTES);
  }

  //@Test //not implemented from C++ yet
  //
  //public void deserializeOneItemV1() throws Exception {
  //  final byte[] bytes = getResourceBytes("kll_sketch_float_one_item_v1.sk");
  //  final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(bytes));
  //  assertFalse(sketch.isEmpty());
  //  assertFalse(sketch.isEstimationMode());
  //  assertEquals(sketch.getN(), 1);
  //  assertEquals(sketch.getNumRetained(), 1);
  //}

  @Test
  public void serializeDeserialize() {
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance();
    final int n = 1000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
    }
    final byte[] bytes = sketch1.toByteArray();
    final KllDoublesSketch sketch2 = KllDoublesSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getCurrentCompactSerializedSizeBytes());
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
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(0);
    sketch.getCDF(new double[] {1, 0});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void nanSplitPoint() {
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(0);
    sketch.getCDF(new double[] {Double.NaN});
  }

  @Test
  public void getQuantiles() {
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    final double[] quantiles1 = sketch.getQuantiles(new double[] {0, 0.5, 1});
    final double[] quantiles2 = sketch.getQuantiles(3);
    assertEquals(quantiles1, quantiles2);
    assertEquals(quantiles1[0], 1.0);
    assertEquals(quantiles1[1], 2.0);
    assertEquals(quantiles1[2], 3.0);
  }

  @Test
  public void checkReset() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    long n1 = sk.getN();
    double min1 = sk.getMinValue();
    double max1 = sk.getMaxValue();
    sk.reset();
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    long n2 = sk.getN();
    double min2 = sk.getMinValue();
    double max2 = sk.getMaxValue();
    assertEquals(n2, n1);
    assertEquals(min2, min1);
    assertEquals(max2, max1);
  }

  @Test
  public void coverInheritanceArtifacts() {
    float[] fltArr = new float[0];
    float fltV = 1.0f;
    int idx = 1;
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    try { sk.getFloatItemsArray();           fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMaxFloatValue();             fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMinFloatValue();             fail(); } catch (SketchesArgumentException e) { }
    try { sk.setFloatItemsArray(fltArr);     fail(); } catch (SketchesArgumentException e) { }
    try { sk.setFloatItemsArrayAt(idx,fltV); fail(); } catch (SketchesArgumentException e) { }
    try { sk.setMaxFloatValue(fltV);         fail(); } catch (SketchesArgumentException e) { }
    try { sk.setMinFloatValue(fltV);         fail(); } catch (SketchesArgumentException e) { }
  }

  @Test
  public void checkReadOnlyUpdate() {
    KllDoublesSketch sk1 = KllDoublesSketch.newHeapInstance(20);
    Memory mem = Memory.wrap(sk1.toByteArray());
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(mem);
    try { sk2.update(1); fail(); } catch (SketchesArgumentException e) { }
  }

  @Test
  public void checkNewDirectInstanceAndSize() {
    WritableMemory wmem = WritableMemory.allocate(3000);
    KllDoublesSketch.newDirectInstance(wmem, memReqSvr);
    try { KllDoublesSketch.newDirectInstance(null, memReqSvr); fail(); }
    catch (NullPointerException e) { }
    try { KllFloatsSketch.newDirectInstance(wmem, null); fail(); }
    catch (NullPointerException e) { }
    int updateSize = KllDoublesSketch.getMaxSerializedSizeBytes(200, 0, true);
    int compactSize = KllDoublesSketch.getMaxSerializedSizeBytes(200, 0, false);
    assertTrue(compactSize < updateSize);
  }

}
