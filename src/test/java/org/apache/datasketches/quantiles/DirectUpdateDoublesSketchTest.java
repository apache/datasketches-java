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

package org.apache.datasketches.quantiles;

import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DirectUpdateDoublesSketchTest {


  @BeforeMethod
  public void setUp() {
    DoublesSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

  @Test
  public void checkSmallMinMax () {
    final int k = 32;
    final int n = 8;
    final UpdateDoublesSketch qs1 = buildDQS(k, n);
    final UpdateDoublesSketch qs2 = buildDQS(k, n);
    final UpdateDoublesSketch qs3 = buildDQS(k, n);

    for (int i = n; i >= 1; i--) {
      qs1.update(i);
      qs2.update(10+i);
      qs3.update(i);
    }
    assertEquals(qs1.getQuantile (0.0, EXCLUSIVE), 1.0);
    assertEquals(qs1.getQuantile (0.5, EXCLUSIVE), 5.0);
    assertEquals(qs1.getQuantile (1.0, EXCLUSIVE), 8.0);

    assertEquals(qs2.getQuantile (0.0, EXCLUSIVE), 11.0);
    assertEquals(qs2.getQuantile (0.5, EXCLUSIVE), 15.0);
    assertEquals(qs2.getQuantile (1.0, EXCLUSIVE), 18.0);

    assertEquals(qs3.getQuantile (0.0, EXCLUSIVE), 1.0);
    assertEquals(qs3.getQuantile (0.5, EXCLUSIVE), 5.0);
    assertEquals(qs3.getQuantile (1.0, EXCLUSIVE), 8.0);

    final double[] queries = {0.0, 0.5, 1.0};

    final double[] resultsA = qs1.getQuantiles(queries, EXCLUSIVE);
    assertEquals(resultsA[0], 1.0);
    assertEquals(resultsA[1], 5.0);
    assertEquals(resultsA[2], 8.0);

    final DoublesUnion union1 = DoublesUnion.heapify(qs1);
    union1.union(qs2);
    final DoublesSketch result1 = union1.getResult();

    final DoublesUnion union2 = DoublesUnion.heapify(qs2);
    union2.union(qs3);
    final DoublesSketch result2 = union2.getResult();

    final double[] resultsB = result1.getQuantiles(queries, EXCLUSIVE);
    printResults(resultsB);
    assertEquals(resultsB[0], 1.0);
    assertEquals(resultsB[1], 11.0);
    assertEquals(resultsB[2], 18.0);

    final double[] resultsC = result2.getQuantiles(queries, EXCLUSIVE);
    assertEquals(resultsC[0], 1.0);
    assertEquals(resultsC[1], 11.0);
    assertEquals(resultsC[2], 18.0);
  }

  static void printResults(final double[] results) {
    println(results[0] + ", " + results[1] + ", " + results[2]);
  }

  @Test
  public void wrapEmptyUpdateSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    final MemorySegment seg = MemorySegment.ofBuffer(ByteBuffer.wrap(s1.toByteArray()).order(ByteOrder.nativeOrder()));
    final UpdateDoublesSketch s2 = DirectUpdateDoublesSketch.wrapInstance(seg, null);
    assertTrue(s2.isEmpty());

    assertEquals(s2.getN(), 0);
    assertTrue(Double.isNaN(s2.isEmpty() ? Double.NaN : s2.getMinItem()));
    assertTrue(Double.isNaN(s2.isEmpty() ? Double.NaN : s2.getMaxItem()));

    s2.reset(); // empty: a no-op
    assertEquals(s2.getN(), 0);
  }

  @Test
  public void checkPutCombinedBuffer() {
    final int k = PreambleUtil.DEFAULT_K;
    final int cap = 32 + ((2 * k) << 3);
    MemorySegment seg = MemorySegment.ofArray(new byte[cap]);
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(k).build(seg);
    seg = qs.getMemorySegment();
    assertEquals(seg.byteSize(), cap);
    assertTrue(qs.isEmpty());

    final int n = 16;
    final double[] data = new double[n];
    for (int i = 0; i < n; ++i) {
      data[i] = i + 1;
    }
    qs.putBaseBufferCount(n);
    qs.putN(n);
    qs.putCombinedBuffer(data);

    final double[] combBuf = qs.getCombinedBuffer();
    assertEquals(combBuf, data);

    // shouldn't have changed min/max values
    assertTrue(Double.isNaN(qs.getMinItem()));
    assertTrue(Double.isNaN(qs.getMaxItem()));
  }

  @Test
  public void checkMisc() {
    final int k = PreambleUtil.DEFAULT_K;
    final int n = 48;
    final int cap = 32 + ((2 * k) << 3);
    MemorySegment seg = MemorySegment.ofArray(new byte[cap]);
    UpdateDoublesSketch qs = DoublesSketch.builder().setK(k).build(seg);
    seg = qs.getMemorySegment();
    assertEquals(seg.byteSize(), cap);
    double[] combBuf = qs.getCombinedBuffer();
    assertEquals(combBuf.length, 2 * k);
    qs = buildAndLoadDQS(k, n);
    qs.update(Double.NaN);
    final int n2 = (int)qs.getN();
    assertEquals(n2, n);
    combBuf = qs.getCombinedBuffer();
    assertEquals(combBuf.length, ceilingPowerOf2(n)); // since n < k
    println(qs.toString(true, true));
    qs.reset();
    assertEquals(qs.getN(), 0);
    qs.putBaseBufferCount(0);
  }

  @SuppressWarnings("unused")
  @Test
  public void variousExceptions() {
    final MemorySegment seg = MemorySegment.ofArray(new byte[8]);
    try {
      final int flags = PreambleUtil.COMPACT_FLAG_MASK;
      DirectUpdateDoublesSketch.checkCompact(2, 0);
      fail();
    } catch (final SketchesArgumentException e) {} //OK
    try {
      final int flags = PreambleUtil.COMPACT_FLAG_MASK;
      DirectUpdateDoublesSketch.checkCompact(3, flags);
      fail();
    } catch (final SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketch.checkPreLongs(3);
      fail();
    } catch (final SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketch.checkPreLongs(0);
      fail();
    } catch (final SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketch.checkDirectFlags(PreambleUtil.COMPACT_FLAG_MASK);
      fail();
    } catch (final SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketch.checkEmptyAndN(true, 1);
      fail();
    } catch (final SketchesArgumentException e) {} //OK
  }

  @Test
  public void checkCheckDirectSegCapacity() {
    final int k = 128;
    DirectUpdateDoublesSketch.checkDirectSegCapacity(k, (2 * k) - 1, (4 + (2 * k)) * 8);
    DirectUpdateDoublesSketch.checkDirectSegCapacity(k, (2 * k) + 1, (4 + (3 * k)) * 8);
    DirectUpdateDoublesSketch.checkDirectSegCapacity(k, 0, 8);

    try {
      DirectUpdateDoublesSketch.checkDirectSegCapacity(k, 10000, 64);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
  }

  @Test
  public void serializeDeserialize() {
    final int sizeBytes = DoublesSketch.getUpdatableStorageBytes(128, 2000);
    final MemorySegment seg = MemorySegment.ofArray(new byte[sizeBytes]);
    final UpdateDoublesSketch sketch1 = DoublesSketch.builder().build(seg);
    for (int i = 0; i < 1000; i++) {
      sketch1.update(i);
    }

    final UpdateDoublesSketch sketch2 = UpdateDoublesSketch.wrap(seg, null);
    for (int i = 0; i < 1000; i++) {
      sketch2.update(i + 1000);
    }
    assertEquals(sketch2.getMinItem(), 0.0);
    assertEquals(sketch2.getMaxItem(), 1999.0);
    assertEquals(sketch2.getQuantile(0.5), 1000.0, 10.0);

    final byte[] arr2 = sketch2.toByteArray(false);
    assertEquals(arr2.length, sketch2.getSerializedSizeBytes());
    final DoublesSketch sketch3 = DoublesSketch.wrap(MemorySegment.ofArray(arr2), null);
    assertEquals(sketch3.getMinItem(), 0.0);
    assertEquals(sketch3.getMaxItem(), 1999.0);
    assertEquals(sketch3.getQuantile(0.5), 1000.0, 10.0);
  }

  @Test
  public void mergeTest() {
    final DoublesSketch dqs1 = buildAndLoadDQS(128, 256);
    final DoublesSketch dqs2 = buildAndLoadDQS(128, 256, 256);
    final DoublesUnion union = DoublesUnion.builder().setMaxK(128).build();
    union.union(dqs1);
    union.union(dqs2);
    final DoublesSketch result = union.getResult();
    final double median = result.getQuantile(0.5);
    println("Median: " + median);
    assertEquals(median, 258.0, .05 * 258);
  }

  @Test
  public void checkSimplePropagateCarryDirect() {
    final int k = 16;
    final int n = k * 2;

    final int segBytes = DoublesSketch.getUpdatableStorageBytes(k, n);
    final MemorySegment seg = MemorySegment.ofArray(new byte[segBytes]);
    final DoublesSketchBuilder bldr = DoublesSketch.builder();
    final UpdateDoublesSketch ds = bldr.setK(k).build(seg);
    for (int i = 1; i <= n; i++) { // 1 ... n
      ds.update(i);
    }
    double last = 0.0;
    for (int i = 0; i < k; i++) { //check the level 0
      final double d = seg.get(JAVA_DOUBLE_UNALIGNED, (4 + (2 * k) + i) << 3);
      assertTrue(d > 0);
      assertTrue(d > last);
      last = d;
    }
    //println(ds.toString(true, true));
  }

  @Test
  public void getRankAndGetCdfConsistency() {
    final int k = 128;
    final int n = 1_000_000;
    final int segBytes = DoublesSketch.getUpdatableStorageBytes(k, n);
    final MemorySegment seg = MemorySegment.ofArray(new byte[segBytes]);
    final UpdateDoublesSketch sketch = DoublesSketch.builder().build(seg);
    final double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      values[i] = i;
    }
    final double[] ranks = sketch.getCDF(values);
    for (int i = 0; i < n; i++) {
      assertEquals(ranks[i], sketch.getRank(values[i]));
    }
  }

  static UpdateDoublesSketch buildAndLoadDQS(final int k, final int n) {
    return buildAndLoadDQS(k, n, 0);
  }

  static UpdateDoublesSketch buildAndLoadDQS(final int k, final long n, final int startV) {
    final UpdateDoublesSketch qs = buildDQS(k, n);
    for (long i = 1; i <= n; i++) {
      qs.update(startV + i);
    }
    return qs;
  }

  static UpdateDoublesSketch buildDQS(final int k, final long n) {
    int cap = DoublesSketch.getUpdatableStorageBytes(k, n);
    if (cap < (2 * k)) { cap = 2 * k; }
    final DoublesSketchBuilder bldr = new DoublesSketchBuilder();
    bldr.setK(k);
    return bldr.build(MemorySegment.ofArray(new byte[cap]));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    print(s+LS);
  }

  /**
   * @param s value to print
   */
  static void print(final String s) {
    //System.err.print(s); //disable here
  }

}
