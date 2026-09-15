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

package org.apache.datasketches.tdigest;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.testng.annotations.Test;

public class TDigestDoubleTest {

  @Test
  public void empty() {
    final TDigestDouble td = new TDigestDouble((short) 100);
    assertTrue(td.isEmpty());
    assertEquals(td.getK(), 100);
    assertEquals(td.getTotalWeight(), 0);
    assertThrows(SketchesStateException.class, () -> td.getMinValue());
    assertThrows(SketchesStateException.class, () -> td.getMaxValue());
    assertThrows(SketchesStateException.class, () -> td.getRank(0));
    assertThrows(SketchesStateException.class, () -> td.getQuantile(0.5));
    assertThrows(SketchesStateException.class, () -> td.getPMF(new double[]{0}));
    assertThrows(SketchesStateException.class, () -> td.getCDF(new double[]{0}));
  }

  @Test
  public void oneValue() {
    final TDigestDouble td = new TDigestDouble();
    td.update(1);
    assertFalse(td.isEmpty());
    assertEquals(td.getK(), 200);
    assertEquals(td.getTotalWeight(), 1);
    assertEquals(td.getMinValue(), 1);
    assertEquals(td.getMaxValue(), 1);
    assertEquals(td.getRank(0.99), 0);
    assertEquals(td.getRank(1), 0.5);
    assertEquals(td.getRank(1.01), 1);
    assertEquals(td.getQuantile(0), 1);
    assertEquals(td.getQuantile(0.5), 1);
    assertEquals(td.getQuantile(1), 1);
  }

  @Test
  public void repeatedValuesAtSingletonInterpolationBoundary() {
    final TDigestDouble td = new TDigestDouble();
    for (int i = 0; i < 20; i++) { td.update(1); }
    assertEquals(td.getQuantile(0.9), 1.0);
  }

  @Test
  public void emptySplitPointsDefineOneBin() {
    final TDigestDouble td = new TDigestDouble();
    td.update(1);
    assertEquals(td.getCDF(new double[0]), new double[] {1});
    assertEquals(td.getPMF(new double[0]), new double[] {1});
  }

  @Test
  public void manyValues() {
    final TDigestDouble td = new TDigestDouble();
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      td.update(i);
    }
    assertFalse(td.isEmpty());
    assertEquals(td.getTotalWeight(), n);
    assertEquals(td.getMinValue(), 0);
    assertEquals(td.getMaxValue(), n - 1);
    assertEquals(td.getRank(0), 0, 0.0001);
    assertEquals(td.getRank(n / 4), 0.25, 0.0001);
    assertEquals(td.getRank(n / 2), 0.5, 0.0001);
    assertEquals(td.getRank((n * 3) / 4), 0.75, 0.0001);
    assertEquals(td.getRank(n), 1);
    assertEquals(td.getQuantile(0), 0);
    assertEquals(td.getQuantile(0.5), n / 2, (n / 2) * 0.03);
    assertEquals(td.getQuantile(0.9), n * 0.9, n * 0.9 * 0.01);
    assertEquals(td.getQuantile(0.95), n * 0.95, n * 0.95 * 0.01);
    assertEquals(td.getQuantile(1), n - 1);
    final double[] pmf = td.getPMF(new double[] {n / 2});
    assertEquals(pmf.length, 2);
    assertEquals(pmf[0], 0.5, 0.0001);
    assertEquals(pmf[1], 0.5, 0.0001);
    final double[] cdf = td.getCDF(new double[] {n / 2});
    assertEquals(cdf.length, 2);
    assertEquals(cdf[0], 0.5, 0.0001);
    assertEquals(cdf[1], 1.0);
  }

  @Test
  public void mergeSmall() {
    final TDigestDouble td1 = new TDigestDouble();
    td1.update(1);
    td1.update(2);
    final TDigestDouble td2 = new TDigestDouble();
    td2.update(2);
    td2.update(3);
    td1.merge(td2);
    assertEquals(td1.getTotalWeight(), 4);
    assertEquals(td1.getMinValue(), 1);
    assertEquals(td1.getMaxValue(), 3);
  }

  @Test
  public void mergeLarge() {
    final int n = 10000;
    final TDigestDouble td1 = new TDigestDouble();
    final TDigestDouble td2 = new TDigestDouble();
    for (int i = 0; i < (n / 2); i++) {
      td1.update(i);
      td2.update((n / 2) + i);
    }
    td1.merge(td2);
    assertEquals(td1.getTotalWeight(), n);
    assertEquals(td1.getMinValue(), 0);
    assertEquals(td1.getMaxValue(), n - 1);
  }

  @Test
  public void serializeDeserializeEmpty() {
    final TDigestDouble td1 = new TDigestDouble();
    final byte[] bytes = td1.toByteArray();
    final TDigestDouble td2 = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
    assertEquals(td2.getK(), td1.getK());
    assertEquals(td2.getTotalWeight(), td1.getTotalWeight());
    assertEquals(td2.isEmpty(), td1.isEmpty());
  }

  @Test
  public void serializeDeserializeNonEmpty() {
    final TDigestDouble td1 = new TDigestDouble();
    for (int i = 0; i < 10000; i++) {
      td1.update(i);
    }
    final byte[] bytes = td1.toByteArray();
    final TDigestDouble td2 = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
    assertEquals(td2.getK(), td1.getK());
    assertEquals(td2.getTotalWeight(), td1.getTotalWeight());
    assertEquals(td2.isEmpty(), td1.isEmpty());
    assertEquals(td2.getMinValue(), td1.getMinValue());
    assertEquals(td2.getMaxValue(), td1.getMaxValue());
    assertEquals(td2.getRank(5000), td1.getRank(5000));
    assertEquals(td2.getQuantile(0.5), td1.getQuantile(0.5));
  }

  @Test
  public void updateIgnoresNaNAndInfinity() {
    final TDigestDouble td = new TDigestDouble();
    td.update(Double.NaN);
    td.update(Double.POSITIVE_INFINITY);
    td.update(Double.NEGATIVE_INFINITY);
    assertTrue(td.isEmpty());
    td.update(1);
    td.update(Double.POSITIVE_INFINITY);
    td.update(Double.NEGATIVE_INFINITY);
    assertEquals(td.getTotalWeight(), 1);
    assertEquals(td.getMinValue(), 1.0);
    assertEquals(td.getMaxValue(), 1.0);
  }

  // issue #702
  @Test
  public void extremeValuesDoNotProduceNaN() {
    final TDigestDouble td = new TDigestDouble();
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      td.update(((i & 1) == 0) ? Double.MAX_VALUE : -Double.MAX_VALUE);
    }
    final byte[] bytes = td.toByteArray(); // compresses as a side effect
    assertEquals(td.getTotalWeight(), n);
    assertTrue(Double.isFinite(td.getQuantile(0.25)));
    assertTrue(Double.isFinite(td.getQuantile(0.5)));
    assertTrue(Double.isFinite(td.getQuantile(0.75)));
    // all serialized centroid means must be finite, otherwise heapify throws
    final TDigestDouble td2 = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
    assertEquals(td2.getTotalWeight(), n);
    assertEquals(td2.getMinValue(), -Double.MAX_VALUE);
    assertEquals(td2.getMaxValue(), Double.MAX_VALUE);
  }

  @Test
  public void sameSignExtremeQuantilesStayFinite() {
    for (final double sign : new double[] {1, -1}) {
      final TDigestDouble td = new TDigestDouble();
      td.update(sign * Math.nextDown(Double.MAX_VALUE));
      td.update(sign * Double.MAX_VALUE);
      final MemorySegment seg = MemorySegment.ofArray(td.toByteArray());
      // The independently rounded normalized weights used to overflow the sum of
      // two finite terms, even though the result must lie between these means.
      seg.set(ValueLayout.JAVA_LONG_UNALIGNED, 40, (1L << 52) - 1);
      seg.set(ValueLayout.JAVA_LONG_UNALIGNED, 56, 1L << 52);
      final TDigestDouble restored = TDigestDouble.heapify(seg);
      for (final double rank : new double[] {0.25, 0.5, 0.75}) {
        final double quantile = restored.getQuantile(rank);
        assertTrue(Double.isFinite(quantile), "non-finite quantile: " + quantile);
        assertTrue(quantile >= restored.getMinValue());
        assertTrue(quantile <= restored.getMaxValue());
      }
    }
  }

  @Test
  public void mergedExtremeCentroidStaysFinite() {
    final double lower = Math.nextDown(Double.MAX_VALUE);
    final TDigestDouble td = new TDigestDouble((short) 10);
    td.update(lower);
    td.update(lower);
    td.update(Double.MAX_VALUE);
    td.update(Double.MAX_VALUE);
    final MemorySegment seg = MemorySegment.ofArray(td.toByteArray());
    // Merging the middle centroids overflows (value - mean) * weight. The old
    // fallback also overflowed because its independently rounded ratios summed above one.
    seg.set(ValueLayout.JAVA_LONG_UNALIGNED, 40, 1L << 55);
    seg.set(ValueLayout.JAVA_LONG_UNALIGNED, 56, (1L << 52) + 1);
    seg.set(ValueLayout.JAVA_LONG_UNALIGNED, 72, 5L << 52);
    seg.set(ValueLayout.JAVA_LONG_UNALIGNED, 88, 1L << 55);
    final TDigestDouble source = TDigestDouble.heapify(seg);
    final TDigestDouble merged = new TDigestDouble((short) 10);
    merged.merge(source);
    // Round-tripping validates every stored mean, including centroids away from the queried rank.
    final TDigestDouble restored = TDigestDouble.heapify(MemorySegment.ofArray(merged.toByteArray()));
    assertEquals(restored.getTotalWeight(), source.getTotalWeight());
    assertEquals(restored.getMinValue(), lower);
    assertEquals(restored.getMaxValue(), Double.MAX_VALUE);
    assertTrue(Double.isFinite(restored.getQuantile(0.5)));
  }

  @Test
  public void quantileWithTwoSampleLastCentroid() {
    final TDigestDouble td = new TDigestDouble();
    td.update(0);
    td.update(50);
    td.update(90);
    final MemorySegment seg = MemorySegment.ofArray(td.toByteArray());
    seg.set(ValueLayout.JAVA_DOUBLE_UNALIGNED, 24, 100);
    seg.set(ValueLayout.JAVA_LONG_UNALIGNED, 72, 2);
    final TDigestDouble restored = TDigestDouble.heapify(seg);
    assertEquals(restored.getTotalWeight(), 4);
    assertEquals(restored.getQuantile(0.75), 100.0);
  }

  // serialized layout: preamble 16 bytes, min 8 bytes, max 8 bytes,
  // then (mean 8 bytes, weight 8 bytes) per centroid
  private static byte[] serializeNonEmpty() {
    final TDigestDouble td = new TDigestDouble();
    for (int i = 0; i < 1000; i++) {
      td.update(i);
    }
    return td.toByteArray();
  }

  @Test
  public void deserializeNaNCentroidMean() {
    final byte[] bytes = serializeNonEmpty();
    MemorySegment.ofArray(bytes).set(ValueLayout.JAVA_DOUBLE_UNALIGNED, 32, Double.NaN);
    assertThrows(SketchesArgumentException.class, () -> TDigestDouble.heapify(MemorySegment.ofArray(bytes)));
  }

  @Test
  public void deserializeInfiniteCentroidMean() {
    final byte[] bytes = serializeNonEmpty();
    MemorySegment.ofArray(bytes).set(ValueLayout.JAVA_DOUBLE_UNALIGNED, 32, Double.NEGATIVE_INFINITY);
    assertThrows(SketchesArgumentException.class, () -> TDigestDouble.heapify(MemorySegment.ofArray(bytes)));
  }

  @Test
  public void deserializeZeroCentroidWeight() {
    final byte[] bytes = serializeNonEmpty();
    MemorySegment.ofArray(bytes).set(ValueLayout.JAVA_LONG_UNALIGNED, 40, 0L);
    assertThrows(SketchesArgumentException.class, () -> TDigestDouble.heapify(MemorySegment.ofArray(bytes)));
  }

  @Test
  public void deserializeNaNMinValue() {
    final byte[] bytes = serializeNonEmpty();
    MemorySegment.ofArray(bytes).set(ValueLayout.JAVA_DOUBLE_UNALIGNED, 16, Double.NaN);
    assertThrows(SketchesArgumentException.class, () -> TDigestDouble.heapify(MemorySegment.ofArray(bytes)));
  }

  @Test
  public void deserializeNaNSingleValue() {
    final TDigestDouble td = new TDigestDouble();
    td.update(1);
    final byte[] bytes = td.toByteArray();
    // single-value layout: preamble 8 bytes, then the value
    MemorySegment.ofArray(bytes).set(ValueLayout.JAVA_DOUBLE_UNALIGNED, 8, Double.NaN);
    assertThrows(SketchesArgumentException.class, () -> TDigestDouble.heapify(MemorySegment.ofArray(bytes)));
  }

  @Test
  public void rankBelowFirstCentroidMean() {
    // the format allows a first centroid of weight greater than 1, so the left tail of
    // getRank() must stay normalized just like the right tail
    final byte[] bytes = serializeNonEmpty();
    MemorySegment.ofArray(bytes).set(ValueLayout.JAVA_DOUBLE_UNALIGNED, 16, -1); // min
    MemorySegment.ofArray(bytes).set(ValueLayout.JAVA_LONG_UNALIGNED, 40, 100L); // first weight
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
    final double totalWeight = td.getTotalWeight();
    assertEquals(td.getRank(-1), 0.5 / totalWeight);
    assertEquals(td.getRank(-0.5), (1.0 + (((100 / 2.0) - 1.0) * 0.5)) / totalWeight);
    double previous = 0;
    for (int i = 0; i <= 100; i++) {
      final double rank = td.getRank(-1 + (i / 100.0));
      assertTrue((rank >= 0) && (rank <= 1), "rank out of [0, 1]: " + rank);
      assertTrue(rank >= previous, "rank not monotonic: " + rank + " after " + previous);
      previous = rank;
    }
  }

  @Test
  public void quantilesAreMonotonic() {
    final TDigestDouble td = new TDigestDouble((short) 100);
    for (int i = 0; i < 10000; i++) {
      td.update(i);
    }
    double previous = td.getMinValue();
    for (int i = 0; i <= 1000; i++) {
      final double quantile = td.getQuantile(i / 1000.0);
      assertTrue(quantile >= previous, "quantile not monotonic: " + quantile + " after " + previous);
      assertTrue((quantile >= td.getMinValue()) && (quantile <= td.getMaxValue()),
          "quantile out of [min, max]: " + quantile);
      previous = quantile;
    }
  }

  @Test
  public void quantileAboveLastCentroidMean() {
    final byte[] bytes = serializeNonEmpty();
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    final int numCentroids = seg.get(ValueLayout.JAVA_INT_UNALIGNED, 8);
    final long lastWeightOffset = 40 + ((numCentroids - 1) * 16L);
    seg.set(ValueLayout.JAVA_LONG_UNALIGNED, lastWeightOffset, 100L);
    final TDigestDouble td = TDigestDouble.heapify(seg);
    double previous = td.getMinValue();
    for (int i = 0; i <= 1000; i++) {
      final double quantile = td.getQuantile(i / 1000.0);
      assertTrue(quantile >= previous, "quantile not monotonic: " + quantile + " after " + previous);
      assertTrue(quantile <= td.getMaxValue(), "quantile above max: " + quantile);
      previous = quantile;
    }
  }
}
