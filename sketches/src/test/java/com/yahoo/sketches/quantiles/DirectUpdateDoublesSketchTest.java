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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.SketchesArgumentException;

public class DirectUpdateDoublesSketchTest {

  @BeforeMethod
  public void setUp() {
    DoublesSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

  @Test
  public void checkGetAdjustedEpsilon() {
  // note: there is a big fudge factor in these numbers, so they don't need to be computed exactly
  double absTol = 1e-14; // we just want to catch gross bugs
  int[] kArr = {2,16,1024,1 << 30};
  double[] epsArr = { // these were computed by an earlier ocaml version of the function
    0.821714930853465,
    0.12145410223356,
    0.00238930378957284,
    3.42875166500824e-09 };
  for (int i = 0; i < 4; i++) {
    assertEquals(epsArr[i],
                 Util.EpsilonFromK.getAdjustedEpsilon(kArr[i]),
                 absTol,
                 "adjustedFindEpsForK() doesn't match precomputed value");
  }
  for (int i = 0; i < 3; i++) {
    DoublesSketch qs = DoublesSketch.builder().build(kArr[i]);
    assertEquals(epsArr[i],
                 qs.getNormalizedRankError(),
                 absTol,
                 "getNormalizedCountError() doesn't match precomputed value");
  }
}

  @Test
  public void checkBigMinMax () {
    int k = 32;
    UpdateDoublesSketch qs1 = DoublesSketch.builder().build(k);
    UpdateDoublesSketch qs2 = DoublesSketch.builder().build(k);
    UpdateDoublesSketch qs3 = DoublesSketch.builder().build(k);
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

    DoublesUnion union1 = DoublesUnionBuilder.heapify(qs1);
    union1.update(qs2);
    DoublesSketch result1 = union1.getResult();

    DoublesUnion union2 = DoublesUnionBuilder.heapify(qs2);
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

    DoublesUnion union1 = DoublesUnionBuilder.heapify(qs1);
    union1.update(qs2);
    DoublesSketch result1 = union1.getResult();

    DoublesUnion union2 = DoublesUnionBuilder.heapify(qs2);
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
    final Memory mem
            = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray()));
    final UpdateDoublesSketch s2 = DirectUpdateDoublesSketch.wrapInstance(mem);
    assertTrue(s2.isEmpty());

    assertEquals(s2.getN(), 0);
    assertEquals(s2.getMinValue(), Double.POSITIVE_INFINITY);
    assertEquals(s2.getMaxValue(), Double.NEGATIVE_INFINITY);

    s2.reset(); // empty: so should be a no-op
    assertEquals(s2.getN(), 0);

    try {
      // no memory handler so this should fail
      s2.update(1.0);
      fail();
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkMisc() {
    int k = PreambleUtil.DEFAULT_K;
    int n = 48;
    int cap = 32 + ((2 * k) << 3);
    Memory mem = new NativeMemory(new byte[cap]);
    UpdateDoublesSketch qs = DoublesSketch.builder().initMemory(mem).build(k);
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
    //println(qs.toString(true, true));
    qs.reset();
    assertEquals(qs.getN(), 0);
    qs.putCombinedBufferItemCapacity(0);
    qs.putBaseBufferCount(0);
  }

  @SuppressWarnings("unused")
  @Test
  public void variousExceptions() {
    Memory mem = new NativeMemory(new byte[8]);
    try {
      int flags = PreambleUtil.COMPACT_FLAG_MASK;
      DirectUpdateDoublesSketch.checkCompact(2, 0);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      int flags = PreambleUtil.COMPACT_FLAG_MASK;
      DirectUpdateDoublesSketch.checkCompact(3, flags);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketch.checkPreLongs(3);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketch.checkPreLongs(0);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketch.checkDirectFlags(PreambleUtil.COMPACT_FLAG_MASK);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectUpdateDoublesSketch.checkEmptyAndN(true, 1);
      fail();
    } catch (SketchesArgumentException e) {} //OK
  }

  @Test
  public void checkCheckDirectMemCapacity() {
    final int k = 128;
    DirectUpdateDoublesSketch.checkDirectMemCapacity(k, 2 * k - 1, (4 + 2 * k) * 8);
    DirectUpdateDoublesSketch.checkDirectMemCapacity(k, 2 * k + 1, (4 + 3 * k) * 8);
    DirectUpdateDoublesSketch.checkDirectMemCapacity(k, 0, 8);

    try {
      DirectUpdateDoublesSketch.checkDirectMemCapacity(k, 10000, 64);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
  }

  @Test
  public void serializeDeserialize() {
    int sizeBytes = DoublesSketch.getUpdatableStorageBytes(128, 2000, true);
    Memory mem = new NativeMemory(new byte[sizeBytes]);
    UpdateDoublesSketch sketch1 = DoublesSketch.builder().initMemory(mem).build();
    for (int i = 0; i < 1000; i++) {
      sketch1.update(i);
    }

    UpdateDoublesSketch sketch2 = (UpdateDoublesSketch) DoublesSketch.wrap(mem);
    for (int i = 0; i < 1000; i++) {
      sketch2.update(i + 1000);
    }
    assertEquals(sketch2.getMinValue(), 0.0);
    assertEquals(sketch2.getMaxValue(), 1999.0);
    assertEquals(sketch2.getQuantile(0.5), 1000.0, 10.0);

    byte[] arr2 = sketch2.toByteArray(false);
    assertEquals(arr2.length, sketch2.getStorageBytes());
    DoublesSketch sketch3 = DoublesSketch.wrap(new NativeMemory(arr2));
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

    final int memBytes = DoublesSketch.getUpdatableStorageBytes(k, n, true);
    final Memory mem = new NativeMemory(new byte[memBytes]);
    final DoublesSketchBuilder bldr = DoublesSketch.builder();
    bldr.initMemory(mem);
    final UpdateDoublesSketch ds = bldr.build(k);
    for (int i = 1; i <= n; i++) { // 1 ... n
      ds.update(i);
    }
    double last = 0.0;
    for (int i = 0; i < k; i++) { //check the level 0
      final double d = mem.getDouble((4 + 2 * k + i) << 3);
      assertTrue(d > 0);
      assertTrue(d > last);
      last = d;
    }
    //println(ds.toString(true, true));
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
    int cap = DoublesSketch.getUpdatableStorageBytes(k, n, true);
    if (cap < 2 * k) { cap = 2 * k; }
    DoublesSketchBuilder bldr = new DoublesSketchBuilder();
    bldr.setK(k);
    bldr.initMemory(new NativeMemory(new byte[cap]));
    UpdateDoublesSketch dqs = bldr.build();
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
