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

package org.apache.datasketches.quantiles2;

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DirectUpdateDoublesSketchTest {


  @BeforeMethod
  public void setUp() {
    DoublesSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

  @Test
  public void checkSmallMinMax () {
    int k = 32;
    int n = 8;
    UpdateDoublesSketch qs1 = buildDQS(k, n);
    UpdateDoublesSketch qs2 = buildDQS(k, n);
    UpdateDoublesSketch qs3 = buildDQS(k, n);

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

    double[] queries = {0.0, 0.5, 1.0};

    double[] resultsA = qs1.getQuantiles(queries, EXCLUSIVE);
    assertEquals(resultsA[0], 1.0);
    assertEquals(resultsA[1], 5.0);
    assertEquals(resultsA[2], 8.0);

    DoublesUnion union1 = DoublesUnion.heapify(qs1);
    union1.union(qs2);
    DoublesSketch result1 = union1.getResult();

    DoublesUnion union2 = DoublesUnion.heapify(qs2);
    union2.union(qs3);
    DoublesSketch result2 = union2.getResult();

    double[] resultsB = result1.getQuantiles(queries, EXCLUSIVE);
    printResults(resultsB);
    assertEquals(resultsB[0], 1.0);
    assertEquals(resultsB[1], 11.0);
    assertEquals(resultsB[2], 18.0);

    double[] resultsC = result2.getQuantiles(queries, EXCLUSIVE);
    assertEquals(resultsC[0], 1.0);
    assertEquals(resultsC[1], 11.0);
    assertEquals(resultsC[2], 18.0);
  }

  static void printResults(double[] results) {
    println(results[0] + ", " + results[1] + ", " + results[2]);
  }

  @Test
  public void wrapEmptyUpdateSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    final WritableMemory mem
            = WritableMemory.writableWrap(ByteBuffer.wrap(s1.toByteArray()).order(ByteOrder.nativeOrder()));
    final UpdateDoublesSketch s2 = DirectUpdateDoublesSketch.wrapInstance(mem);
    assertTrue(s2.isEmpty());

    assertEquals(s2.getN(), 0);
    assertTrue(Double.isNaN(s2.isEmpty() ? Double.NaN : s2.getMinItem()));
    assertTrue(Double.isNaN(s2.isEmpty() ? Double.NaN : s2.getMaxItem()));

    s2.reset(); // empty: so should be a no-op
    assertEquals(s2.getN(), 0);
  }

  @Test
  public void checkPutCombinedBuffer() {
    final int k = PreambleUtil.DEFAULT_K;
    final int cap = 32 + ((2 * k) << 3);
    WritableMemory mem = WritableMemory.writableWrap(new byte[cap]);
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(k).build(mem);
    mem = qs.getMemory();
    assertEquals(mem.getCapacity(), cap);
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
    int k = PreambleUtil.DEFAULT_K;
    int n = 48;
    int cap = 32 + ((2 * k) << 3);
    WritableMemory mem = WritableMemory.writableWrap(new byte[cap]);
    UpdateDoublesSketch qs = DoublesSketch.builder().setK(k).build(mem);
    mem = qs.getMemory();
    assertEquals(mem.getCapacity(), cap);
    double[] combBuf = qs.getCombinedBuffer();
    assertEquals(combBuf.length, 2 * k);
    qs = buildAndLoadDQS(k, n);
    qs.update(Double.NaN);
    int n2 = (int)qs.getN();
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
    WritableMemory mem = WritableMemory.writableWrap(new byte[8]);
    try {
      int flags = PreambleUtil.COMPACT_FLAG_MASK;
      DirectUpdateDoublesSketchR.checkCompact(2, 0);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      int flags = PreambleUtil.COMPACT_FLAG_MASK;
      DirectUpdateDoublesSketchR.checkCompact(3, flags);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketchR.checkPreLongs(3);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketchR.checkPreLongs(0);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketchR.checkDirectFlags(PreambleUtil.COMPACT_FLAG_MASK);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketchR.checkEmptyAndN(true, 1);
      fail();
    } catch (SketchesArgumentException e) {} //OK
  }

  @Test
  public void checkCheckDirectMemCapacity() {
    final int k = 128;
    DirectUpdateDoublesSketchR.checkDirectMemCapacity(k, (2 * k) - 1, (4 + (2 * k)) * 8);
    DirectUpdateDoublesSketchR.checkDirectMemCapacity(k, (2 * k) + 1, (4 + (3 * k)) * 8);
    DirectUpdateDoublesSketchR.checkDirectMemCapacity(k, 0, 8);

    try {
      DirectUpdateDoublesSketchR.checkDirectMemCapacity(k, 10000, 64);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
  }

  @Test
  public void serializeDeserialize() {
    int sizeBytes = DoublesSketch.getUpdatableStorageBytes(128, 2000);
    WritableMemory mem = WritableMemory.writableWrap(new byte[sizeBytes]);
    UpdateDoublesSketch sketch1 = DoublesSketch.builder().build(mem);
    for (int i = 0; i < 1000; i++) {
      sketch1.update(i);
    }

    UpdateDoublesSketch sketch2 = UpdateDoublesSketch.wrap(mem);
    for (int i = 0; i < 1000; i++) {
      sketch2.update(i + 1000);
    }
    assertEquals(sketch2.getMinItem(), 0.0);
    assertEquals(sketch2.getMaxItem(), 1999.0);
    assertEquals(sketch2.getQuantile(0.5), 1000.0, 10.0);

    byte[] arr2 = sketch2.toByteArray(false);
    assertEquals(arr2.length, sketch2.getSerializedSizeBytes());
    DoublesSketch sketch3 = DoublesSketch.wrap(WritableMemory.writableWrap(arr2));
    assertEquals(sketch3.getMinItem(), 0.0);
    assertEquals(sketch3.getMaxItem(), 1999.0);
    assertEquals(sketch3.getQuantile(0.5), 1000.0, 10.0);
  }

  @Test
  public void mergeTest() {
    DoublesSketch dqs1 = buildAndLoadDQS(128, 256);
    DoublesSketch dqs2 = buildAndLoadDQS(128, 256, 256);
    DoublesUnion union = DoublesUnion.builder().setMaxK(128).build();
    union.union(dqs1);
    union.union(dqs2);
    DoublesSketch result = union.getResult();
    double median = result.getQuantile(0.5);
    println("Median: " + median);
    assertEquals(median, 258.0, .05 * 258);
  }

  @Test
  public void checkSimplePropagateCarryDirect() {
    final int k = 16;
    final int n = k * 2;

    final int memBytes = DoublesSketch.getUpdatableStorageBytes(k, n);
    final WritableMemory mem = WritableMemory.writableWrap(new byte[memBytes]);
    final DoublesSketchBuilder bldr = DoublesSketch.builder();
    final UpdateDoublesSketch ds = bldr.setK(k).build(mem);
    for (int i = 1; i <= n; i++) { // 1 ... n
      ds.update(i);
    }
    double last = 0.0;
    for (int i = 0; i < k; i++) { //check the level 0
      final double d = mem.getDouble((4 + (2 * k) + i) << 3);
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
    final int memBytes = DoublesSketch.getUpdatableStorageBytes(k, n);
    final WritableMemory mem = WritableMemory.writableWrap(new byte[memBytes]);
    final UpdateDoublesSketch sketch = DoublesSketch.builder().build(mem);
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

  static UpdateDoublesSketch buildAndLoadDQS(int k, int n) {
    return buildAndLoadDQS(k, n, 0);
  }

  static UpdateDoublesSketch buildAndLoadDQS(int k, long n, int startV) {
    UpdateDoublesSketch qs = buildDQS(k, n);
    for (long i = 1; i <= n; i++) {
      qs.update(startV + i);
    }
    return qs;
  }

  static UpdateDoublesSketch buildDQS(int k, long n) {
    int cap = DoublesSketch.getUpdatableStorageBytes(k, n);
    if (cap < (2 * k)) { cap = 2 * k; }
    DoublesSketchBuilder bldr = new DoublesSketchBuilder();
    bldr.setK(k);
    UpdateDoublesSketch dqs = bldr.build(WritableMemory.writableWrap(new byte[cap]));
    return dqs;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    print(s+LS);
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    //System.err.print(s); //disable here
  }

}
