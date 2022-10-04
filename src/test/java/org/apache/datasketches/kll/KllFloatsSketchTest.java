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

import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.Util.getResourceBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.FloatsSortedView;
import org.apache.datasketches.FloatsSortedViewIterator;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

@SuppressWarnings("deprecation")
public class KllFloatsSketchTest {
  private static final double PMF_EPS_FOR_K_8 = 0.35; // PMF rank error (epsilon) for k=8
  private static final double PMF_EPS_FOR_K_128 = 0.025; // PMF rank error (epsilon) for k=128
  private static final double PMF_EPS_FOR_K_256 = 0.013; // PMF rank error (epsilon) for k=256
  private static final double NUMERIC_NOISE_TOLERANCE = 1E-6;
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void empty() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(Float.NaN); // this must not change anything
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getNumRetained(), 0);
    assertTrue(Double.isNaN(sketch.getRank(0)));
    assertTrue(Float.isNaN(sketch.getMinQuantile()));
    assertTrue(Float.isNaN(sketch.getMaxQuantile()));
    assertTrue(Float.isNaN(sketch.getQuantile(0.5)));
    assertNull(sketch.getQuantiles(new double[] {0}));
    assertNull(sketch.getPMF(new float[] {0}));
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
    assertEquals(sketch.getRank(1f, EXCLUSIVE), 0.0);
    assertEquals(sketch.getRank(2, EXCLUSIVE), 1.0);
    assertEquals(sketch.getRank(1, EXCLUSIVE), 0.0);
    assertEquals(sketch.getRank(2, EXCLUSIVE), 1.0);
    assertEquals(sketch.getRank(0, INCLUSIVE), 0.0);
    assertEquals(sketch.getRank(1, INCLUSIVE), 1.0);
    assertEquals(sketch.getMinQuantile(), 1f);
    assertEquals(sketch.getMaxQuantile(), 1f);
    assertEquals(sketch.getQuantile(0.5, EXCLUSIVE), 1f);
  }

  @Test
  public void tenValues() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    for (int i = 1; i <= 10; i++) { sketch.update(i); }
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 10);
    assertEquals(sketch.getNumRetained(), 10);
    for (int i = 1; i <= 10; i++) {
      assertEquals(sketch.getRank(i, EXCLUSIVE), (i - 1) / 10.0);
      assertEquals(sketch.getRank(i, INCLUSIVE), (i) / 10.0);
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
    // inclusive = false (default)
    assertEquals(sketch.getQuantile(0, EXCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.1, EXCLUSIVE), 2);
    assertEquals(sketch.getQuantile(0.2, EXCLUSIVE), 3);
    assertEquals(sketch.getQuantile(0.3, EXCLUSIVE), 4);
    assertEquals(sketch.getQuantile(0.4, EXCLUSIVE), 5);
    assertEquals(sketch.getQuantile(0.5, EXCLUSIVE), 6);
    assertEquals(sketch.getQuantile(0.6, EXCLUSIVE), 7);
    assertEquals(sketch.getQuantile(0.7, EXCLUSIVE), 8);
    assertEquals(sketch.getQuantile(0.8, EXCLUSIVE), 9);
    assertEquals(sketch.getQuantile(0.9, EXCLUSIVE), 10);
    assertEquals(sketch.getQuantile(1, EXCLUSIVE), 10);
    // inclusive = true
    assertEquals(sketch.getQuantile(0, INCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.1, INCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.2, INCLUSIVE), 2);
    assertEquals(sketch.getQuantile(0.3, INCLUSIVE), 3);
    assertEquals(sketch.getQuantile(0.4, INCLUSIVE), 4);
    assertEquals(sketch.getQuantile(0.5, INCLUSIVE), 5);
    assertEquals(sketch.getQuantile(0.6, INCLUSIVE), 6);
    assertEquals(sketch.getQuantile(0.7, INCLUSIVE), 7);
    assertEquals(sketch.getQuantile(0.8, INCLUSIVE), 8);
    assertEquals(sketch.getQuantile(0.9, INCLUSIVE), 9);
    assertEquals(sketch.getQuantile(1, INCLUSIVE), 10);

    // getQuantile() and getQuantiles() equivalence
    {
      // exclusive
      final float[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1});
      for (int i = 0; i < 10; i++) {
        assertEquals(sketch.getQuantile(i / 10.0, EXCLUSIVE), quantiles[i + 1]);
      }
    }
    {
      // inclusive
      final float[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1}, INCLUSIVE);
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
      assertEquals(sketch.getN(), i + 1);
    }

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

    assertEquals(sketch.getMinQuantile(), 0f);
    assertEquals(sketch.getMaxQuantile(), n - 1f);

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
    { // inclusive = false (default)
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
      sketch2.update((2 * n - i - 1) * 1.0f);
    }

    assertEquals(sketch1.getMinQuantile(), 0.0f);
    assertEquals(sketch1.getMaxQuantile(), (n - 1) * 1.0f);

    assertEquals(sketch2.getMinQuantile(), n * 1.0f);
    assertEquals(sketch2.getMaxQuantile(), (2 * n - 1) * 1.0f);

    sketch1.merge(sketch2);

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2L * n);
    assertEquals(sketch1.getMinQuantile(), 0.0f);
    assertEquals(sketch1.getMaxQuantile(), (2 * n - 1) * 1.0f);
    assertEquals(sketch1.getQuantile(0.5), n * 1.0f, n * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeLowerK() {
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance(256);
    final KllFloatsSketch sketch2 = KllFloatsSketch.newHeapInstance(128);
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update(2 * n - i - 1);
    }

    assertEquals(sketch1.getMinQuantile(), 0.0f);
    assertEquals(sketch1.getMaxQuantile(), n - 1f);

    assertEquals(sketch2.getMinQuantile(), n);
    assertEquals(sketch2.getMaxQuantile(), 2f * n - 1f);

    assertTrue(sketch1.getNormalizedRankError(false) < sketch2.getNormalizedRankError(false));
    assertTrue(sketch1.getNormalizedRankError(true) < sketch2.getNormalizedRankError(true));
    sketch1.merge(sketch2);

    // sketch1 must get "contaminated" by the lower K in sketch2
    assertEquals(sketch1.getNormalizedRankError(false), sketch2.getNormalizedRankError(false));
    assertEquals(sketch1.getNormalizedRankError(true), sketch2.getNormalizedRankError(true));

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2 * n);
    assertEquals(sketch1.getMinQuantile(), 0f);
    assertEquals(sketch1.getMaxQuantile(), 2f * n - 1f);
    assertEquals(sketch1.getQuantile(0.5), n, n * PMF_EPS_FOR_K_128);
  }

  @Test
  public void mergeEmptyLowerK() {
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance(256);
    final KllFloatsSketch sketch2 = KllFloatsSketch.newHeapInstance(128);
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
    assertEquals(sketch1.getMinQuantile(), 0f);
    assertEquals(sketch1.getMaxQuantile(), n - 1f);
    assertEquals(sketch1.getQuantile(0.5), n / 2f, n / 2 * PMF_EPS_FOR_K_256);

    //merge the other way
    sketch2.merge(sketch1);
    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch1.getMinQuantile(), 0f);
    assertEquals(sketch1.getMaxQuantile(), n - 1f);
    assertEquals(sketch1.getQuantile(0.5), n / 2f, n / 2 * PMF_EPS_FOR_K_256);
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
    assertEquals(sketch2.getMinQuantile(), 1.0F);
  }

  @Test
  public void mergeMinAndMaxFromOther() {
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance();
    for (int i = 1; i <= 1_000_000; i++) {
      sketch1.update(i);
    }
    final KllFloatsSketch sketch2 = KllFloatsSketch.newHeapInstance(10);
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinQuantile(), 1F);
    assertEquals(sketch2.getMaxQuantile(), 1_000_000F);
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
    float q = sketch.getQuantile(.5);
    assertEquals(q, 500, 500 * PMF_EPS_FOR_K_8);
  }

  @Test
  public void maxK() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance(KllSketch.MAX_K);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getK(), KllSketch.MAX_K);
    assertEquals(sketch.getQuantile(0.5), 500, 500 * PMF_EPS_FOR_K_256);
  }

  @Test
  public void serializeDeserializeEmpty() {
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance();
    final byte[] bytes = sketch1.toByteArray();
    final KllFloatsSketch sketch2 = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getCurrentCompactSerializedSizeBytes());
    assertTrue(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertTrue(Float.isNaN(sketch2.getMinQuantile()));
    assertTrue(Float.isNaN(sketch2.getMaxQuantile()));
    assertEquals(sketch2.getCurrentCompactSerializedSizeBytes(), sketch1.getCurrentCompactSerializedSizeBytes());
  }

  @Test
  public void serializeDeserializeOneValue() {
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance();
    sketch1.update(1);
    final byte[] bytes = sketch1.toByteArray();
    final KllFloatsSketch sketch2 = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getCurrentCompactSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), 1);
    assertEquals(sketch2.getN(), 1);
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertFalse(Float.isNaN(sketch2.getMinQuantile()));
    assertFalse(Float.isNaN(sketch2.getMaxQuantile()));
    assertEquals(sketch2.getCurrentCompactSerializedSizeBytes(), 8 + Float.BYTES);
  }

  @Test
  public void deserializeOneValueV1() throws Exception {
    final byte[] bytes = getResourceBytes("kll_sketch_float_one_item_v1.sk");
    final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertFalse(sketch.isEmpty());
    assertFalse(sketch.isEstimationMode());
    assertEquals(sketch.getN(), 1);
    assertEquals(sketch.getNumRetained(), 1);
  }

  @Test
  public void serializeDeserialize() {
    final KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance();
    final int n = 1000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
    }
    final byte[] bytes = sketch1.toByteArray();
    final KllFloatsSketch sketch2 = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getCurrentCompactSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinQuantile(), sketch1.getMinQuantile());
    assertEquals(sketch2.getMaxQuantile(), sketch1.getMaxQuantile());
    assertEquals(sketch2.getCurrentCompactSerializedSizeBytes(), sketch1.getCurrentCompactSerializedSizeBytes());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void outOfOrderSplitPoints() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(0);
    sketch.getCDF(new float[] {1, 0});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void nanSplitPoint() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(0);
    sketch.getCDF(new float[] {Float.NaN});
  }

  @Test
  public void getQuantiles() {
    final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    final float[] quantiles1 = sketch.getQuantiles(new double[] {0, 0.5, 1}, EXCLUSIVE);
    final float[] quantiles2 = sketch.getQuantiles(3, EXCLUSIVE);
    assertEquals(quantiles1, quantiles2);
    assertEquals(quantiles1[0], 1f);
    assertEquals(quantiles1[1], 2f);
    assertEquals(quantiles1[2], 3f);
  }

  @Test
  public void checkReset() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    long n1 = sk.getN();
    float min1 = sk.getMinQuantile();
    float max1 = sk.getMaxQuantile();
    sk.reset();
    for (int i = 1; i <= 100; i++) { sk.update(i); }
    long n2 = sk.getN();
    float min2 = sk.getMinQuantile();
    float max2 = sk.getMaxQuantile();
    assertEquals(n2, n1);
    assertEquals(min2, min1);
    assertEquals(max2, max1);
  }

  @Test
  public void coverInheritanceArtifacts() {
    double[] dblArr = new double[0];
    double dblV = 1.0;
    int idx = 1;
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    try { sk.getDoubleValuesArray();           fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMaxDoubleQuantile();             fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMinDoubleQuantile();             fail(); } catch (SketchesArgumentException e) { }
    try { sk.setDoubleValuesArray(dblArr);     fail(); } catch (SketchesArgumentException e) { }
    try { sk.setDoubleValuesArrayAt(idx,dblV); fail(); } catch (SketchesArgumentException e) { }
    try { sk.setMaxDoubleQuantile(dblV);         fail(); } catch (SketchesArgumentException e) { }
    try { sk.setMinDoubleQuantile(dblV);         fail(); } catch (SketchesArgumentException e) { }
  }

  @Test
  public void checkReadOnlyUpdate() {
    KllFloatsSketch sk1 = KllFloatsSketch.newHeapInstance(20);
    Memory mem = Memory.wrap(sk1.toByteArray());
    KllFloatsSketch sk2 = KllFloatsSketch.wrap(mem);
    try { sk2.update(1); fail(); } catch (SketchesArgumentException e) { }
  }

  @Test
  public void checkNewDirectInstanceAndSize() {
    WritableMemory wmem = WritableMemory.allocate(3000);
    KllFloatsSketch.newDirectInstance(wmem, memReqSvr);
    try { KllFloatsSketch.newDirectInstance(null, memReqSvr); fail(); }
    catch (NullPointerException e) { }
    try { KllFloatsSketch.newDirectInstance(wmem, null); fail(); }
    catch (NullPointerException e) { }
    int updateSize = KllFloatsSketch.getMaxSerializedSizeBytes(200, 0, true);
    int compactSize = KllFloatsSketch.getMaxSerializedSizeBytes(200, 0, false);
    assertTrue(compactSize < updateSize);
  }

  @Test
  public void sortedView() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(3);
    sk.update(1);
    sk.update(2);
    {
      final FloatsSortedView view = sk.getSortedView();
      final FloatsSortedViewIterator itr = view.iterator();
      assertEquals(itr.next(), true);
      assertEquals(itr.getQuantile(), 1);
      assertEquals(itr.getWeight(), 1);
      assertEquals(itr.getCumulativeWeight(EXCLUSIVE), 0);
      assertEquals(itr.getCumulativeWeight(INCLUSIVE), 1);
      assertEquals(itr.next(), true);
      assertEquals(itr.getQuantile(), 2);
      assertEquals(itr.getWeight(), 1);
      assertEquals(itr.getCumulativeWeight(EXCLUSIVE), 1);
      assertEquals(itr.getCumulativeWeight(INCLUSIVE), 2);
      assertEquals(itr.next(), true);
      assertEquals(itr.getQuantile(), 3);
      assertEquals(itr.getWeight(), 1);
      assertEquals(itr.getCumulativeWeight(EXCLUSIVE), 2);
      assertEquals(itr.getCumulativeWeight(INCLUSIVE), 3);
      assertEquals(itr.next(), false);
    }
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
    float[] sp = new float[] { 10, 20, 30, 40 };
    println("SplitPoints:");
    for (int i = 0; i < sp.length; i++) {
      printf("%10.2f", sp[i]);
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
