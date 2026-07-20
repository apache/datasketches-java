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

import static org.apache.datasketches.common.TestUtil.resPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.common.TestUtil;
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
  public void deserializeFromReferenceImplementationDouble() {
    final byte[] bytes = TestUtil.getFileBytes(resPath, "tdigest_ref_k100_n10000_double.sk");
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
    final int n = 10000;
    assertEquals(td.getK(), 100);
    assertEquals(td.getTotalWeight(), n);
    assertEquals(td.getMinValue(), 0);
    assertEquals(td.getMaxValue(), n - 1);
    assertEquals(td.getRank(0), 0, 0.0001);
    assertEquals(td.getRank(n / 4), 0.25, 0.0001);
    assertEquals(td.getRank(n / 2), 0.5, 0.0001);
    assertEquals(td.getRank((n * 3) / 4), 0.75, 0.0001);
    assertEquals(td.getRank(n), 1);
  }

  @Test
  public void deserializeFromReferenceImplementationFloat() {
    final byte[] bytes = TestUtil.getFileBytes(resPath, "tdigest_ref_k100_n10000_float.sk");
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
    final int n = 10000;
    assertEquals(td.getK(), 100);
    assertEquals(td.getTotalWeight(), n);
    assertEquals(td.getMinValue(), 0);
    assertEquals(td.getMaxValue(), n - 1);
    assertEquals(td.getRank(0), 0, 0.0001);
    assertEquals(td.getRank(n / 4), 0.25, 0.0001);
    assertEquals(td.getRank(n / 2), 0.5, 0.0001);
    assertEquals(td.getRank((n * 3) / 4), 0.75, 0.0001);
    assertEquals(td.getRank(n), 1);
  }
}
