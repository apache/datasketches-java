/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.quantiles.Util.LS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

public class DirectUpdateDoublesSketchTest {

  @BeforeMethod
  public void setUp() {
    DoublesSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

  @Test
  public void checkBigMinMax () {
    int k = 32;
    UpdateDoublesSketch qs1 = DoublesSketch.builder().setK(k).build();
    UpdateDoublesSketch qs2 = DoublesSketch.builder().setK(k).build();
    UpdateDoublesSketch qs3 = DoublesSketch.builder().setK(k).build();
    assertFalse(qs1.isEstimationMode());

    for (int i = 999; i >= 1; i--) {
      qs1.update(i);
      qs2.update(1000+i);
      qs3.update(i);
    }
    assertTrue(qs1.isEstimationMode());

    assertTrue(qs1.getQuantile(0.0) == 1.0);
    assertTrue(qs1.getQuantile(1.0) == 999.0);

    assertTrue(qs2.getQuantile(0.0) == 1001.0);
    assertTrue(qs2.getQuantile(1.0) == 1999.0);

    assertTrue((qs3.getQuantile(0.0) == 1.0));
    assertTrue(qs3.getQuantile(1.0) == 999.0);

    double[] queries = {0.0, 1.0};

    double[] resultsA = qs1.getQuantiles(queries);
    assertTrue(resultsA[0] == 1.0);
    assertTrue(resultsA[1] == 999.0);

    DoublesUnion union1 = DoublesUnion.heapify(qs1);
    union1.update(qs2);
    DoublesSketch result1 = union1.getResult();

    DoublesUnion union2 = DoublesUnion.heapify(qs2);
    union2.update(qs3);
    DoublesSketch result2 = union2.getResult();

    double[] resultsB = result1.getQuantiles(queries);
    assertTrue(resultsB[0] == 1.0);
    assertTrue(resultsB[1] == 1999.0);

    double[] resultsC = result2.getQuantiles(queries);
    assertTrue(resultsC[0] == 1.0);
    assertTrue(resultsC[1] == 1999.0);
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
    assert (qs1.getQuantile (0.0) == 1.0);
    assert (qs1.getQuantile (0.5) == 5.0);
    assert (qs1.getQuantile (1.0) == 8.0);

    assert (qs2.getQuantile (0.0) == 11.0);
    assert (qs2.getQuantile (0.5) == 15.0);
    assert (qs2.getQuantile (1.0) == 18.0);

    assert (qs3.getQuantile (0.0) == 1.0);
    assert (qs3.getQuantile (0.5) == 5.0);
    assert (qs3.getQuantile (1.0) == 8.0);

    double[] queries = {0.0, 0.5, 1.0};

    double[] resultsA = qs1.getQuantiles(queries);
    assert (resultsA[0] == 1.0);
    assert (resultsA[1] == 5.0);
    assert (resultsA[2] == 8.0);

    DoublesUnion union1 = DoublesUnion.heapify(qs1);
    union1.update(qs2);
    DoublesSketch result1 = union1.getResult();

    DoublesUnion union2 = DoublesUnion.heapify(qs2);
    union2.update(qs3);
    DoublesSketch result2 = union2.getResult();

    double[] resultsB = result1.getQuantiles(queries);
    printResults(resultsB);
    assert (resultsB[0] == 1.0);
    assert (resultsB[1] == 11.0);
    assert (resultsB[2] == 18.0);

    double[] resultsC = result2.getQuantiles(queries);
    assert (resultsC[0] == 1.0);
    assert (resultsC[1] == 11.0);
    assert (resultsC[2] == 18.0);
  }

  static void printResults(double[] results) {
    println(results[0] + ", " + results[1] + ", " + results[2]);
  }

  @Test
  public void wrapEmptyUpdateSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    final WritableMemory mem
            = WritableMemory.wrap(ByteBuffer.wrap(s1.toByteArray()).order(ByteOrder.nativeOrder()));
    final UpdateDoublesSketch s2 = DirectUpdateDoublesSketch.wrapInstance(mem);
    assertTrue(s2.isEmpty());

    assertEquals(s2.getN(), 0);
    assertTrue(Double.isNaN(s2.getMinValue()));
    assertTrue(Double.isNaN(s2.getMaxValue()));

    s2.reset(); // empty: so should be a no-op
    assertEquals(s2.getN(), 0);
  }

  @Test
  public void checkPutCombinedBuffer() {
    final int k = PreambleUtil.DEFAULT_K;
    final int cap = 32 + ((2 * k) << 3);
    WritableMemory mem = WritableMemory.wrap(new byte[cap]);
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
    assertTrue(Double.isNaN(qs.getMinValue()));
    assertTrue(Double.isNaN(qs.getMaxValue()));
  }

  @Test
  public void checkMisc() {
    int k = PreambleUtil.DEFAULT_K;
    int n = 48;
    int cap = 32 + ((2 * k) << 3);
    WritableMemory mem = WritableMemory.wrap(new byte[cap]);
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
    WritableMemory mem = WritableMemory.wrap(new byte[8]);
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
    WritableMemory mem = WritableMemory.wrap(new byte[sizeBytes]);
    UpdateDoublesSketch sketch1 = DoublesSketch.builder().build(mem);
    for (int i = 0; i < 1000; i++) {
      sketch1.update(i);
    }

    UpdateDoublesSketch sketch2 = UpdateDoublesSketch.wrap(mem);
    for (int i = 0; i < 1000; i++) {
      sketch2.update(i + 1000);
    }
    assertEquals(sketch2.getMinValue(), 0.0);
    assertEquals(sketch2.getMaxValue(), 1999.0);
    assertEquals(sketch2.getQuantile(0.5), 1000.0, 10.0);

    byte[] arr2 = sketch2.toByteArray(false);
    assertEquals(arr2.length, sketch2.getStorageBytes());
    DoublesSketch sketch3 = DoublesSketch.wrap(WritableMemory.wrap(arr2));
    assertEquals(sketch3.getMinValue(), 0.0);
    assertEquals(sketch3.getMaxValue(), 1999.0);
    assertEquals(sketch3.getQuantile(0.5), 1000.0, 10.0);
  }

  @Test
  public void mergeTest() {
    DoublesSketch dqs1 = buildAndLoadDQS(128, 256);
    DoublesSketch dqs2 = buildAndLoadDQS(128, 256, 256);
    DoublesUnion union = DoublesUnion.builder().setMaxK(128).build();
    union.update(dqs1);
    union.update(dqs2);
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
    final WritableMemory mem = WritableMemory.wrap(new byte[memBytes]);
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
    final WritableMemory mem = WritableMemory.wrap(new byte[memBytes]);
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
    for (int i=1; i<=n; i++) {
      qs.update(startV + i);
    }
    return qs;
  }

  static UpdateDoublesSketch buildDQS(int k, long n) {
    int cap = DoublesSketch.getUpdatableStorageBytes(k, n);
    if (cap < (2 * k)) { cap = 2 * k; }
    DoublesSketchBuilder bldr = new DoublesSketchBuilder();
    bldr.setK(k);
    UpdateDoublesSketch dqs = bldr.build(WritableMemory.wrap(new byte[cap]));
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
