/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.DoublesUpdateImpl.maybeGrowLevels;
import static com.yahoo.sketches.quantiles.Util.LS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRequest;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.SketchesArgumentException;

public class DirectDoublesSketchTest {

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
    DoublesSketch qs1 = DoublesSketch.builder().build(k);
    DoublesSketch qs2 = DoublesSketch.builder().build(k);
    DoublesSketch qs3 = DoublesSketch.builder().build(k);

    for (int i = 999; i >= 1; i--) {
      qs1.update(i);
      qs2.update(1000+i);
      qs3.update(i);
    }
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
    DoublesSketch qs1 = buildDQS(k, n);
    DoublesSketch qs2 = buildDQS(k, n);
    DoublesSketch qs3 = buildDQS(k, n);

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
    assert (resultsB[0] == 1.0);
    assert (resultsB[1] == 11.0);
    assert (resultsB[2] == 18.0);

    double[] resultsC = result2.getQuantiles(queries);
    assert (resultsC[0] == 1.0);
    assert (resultsC[1] == 11.0);
    assert (resultsC[2] == 18.0);
  }

  @Test
  public void checkMisc() {
    int k = DoublesSketch.DEFAULT_K;
    int n = 48;
    int cap = 32 + ((2 * k) << 3);
    Memory mem = new NativeMemory(new byte[cap]);
    DoublesSketch qs = DoublesSketch.builder().initMemory(mem).build(k);
    mem = qs.getMemory();
    assertEquals(mem.getCapacity(), cap);
    double[] combBuf = qs.getCombinedBuffer();
    assertEquals(combBuf.length, 2 * k);
    qs = buildAndLoadDQS(k, n);
    qs.update(Double.NaN);
    int n2 = (int)qs.getN();
    assertEquals(n2, n);
    combBuf = qs.getCombinedBuffer();
    assertEquals(combBuf.length, 2 * k);
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
      DirectDoublesSketch.newInstance(2, mem);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      int flags = PreambleUtil.COMPACT_FLAG_MASK;
      DirectDoublesSketch.checkCompact(2, 0);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      int flags = PreambleUtil.COMPACT_FLAG_MASK;
      DirectDoublesSketch.checkCompact(3, flags);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectDoublesSketch.checkPreLongs(3);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectDoublesSketch.checkPreLongs(0);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectDoublesSketch.checkDirectFlags(PreambleUtil.COMPACT_FLAG_MASK);
      fail();
    } catch (SketchesArgumentException e) {} //OK
    try {
      DirectDoublesSketch.checkEmptyAndN(true, 1);
      fail();
    } catch (SketchesArgumentException e) {} //OK
  }

  @Test
  public void checkCheckDirectMemCapacity() {
    int k = 128;
    DirectDoublesSketch.checkDirectMemCapacity(k, 2 * k - 1, (4 + 2 * k) * 8);
  }

  @Test//(expectedExceptions = IllegalArgumentException.class) //from MemoryUtil.requestMemoryHandler
  public void checkGrowCombinedBuffer() {
    int k = 128;
    int n = 2 * k -1;
    DoublesSketch qs = buildDQS(k, n);
    Memory mem = qs.getMemory();
    mem.setMemoryRequest(new MemoryManager());
    int spaceNeeded = maybeGrowLevels(k, n + 1);
    qs.growCombinedBuffer(qs.getCombinedBufferItemCapacity(), spaceNeeded);
    long newMemCap = qs.getMemory().getCapacity();
    assertEquals(newMemCap, spaceNeeded * 8 + 32);
  }

  static DoublesSketch buildAndLoadDQS(int k, int n) {
    return buildAndLoadDQS(k, n, 0);
  }

  static DoublesSketch buildAndLoadDQS(int k, long n, int startV) {
    DoublesSketch qs = buildDQS(k, n);
    for (int i=1; i<=n; i++) {
      qs.update(startV + i);
    }
    return qs;
  }

  static DoublesSketch buildDQS(int k, long n) {
    int cap = DoublesSketch.getUpdatableStorageBytes(k, n);
    DoublesSketchBuilder bldr = new DoublesSketchBuilder();
    bldr.setK(k);
    bldr.initMemory(new NativeMemory(new byte[cap]));
    DoublesSketch dqs = bldr.build();
    return dqs;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
    print("PRINTING: "+this.getClass().getName() + LS);
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

  //Allocates what was asked
  private static class MemoryManager implements MemoryRequest {

    @Override
    public Memory request(long capacityBytes) {
      Memory newMem = new NativeMemory(new byte[(int)capacityBytes]);
      //println("ReqCap: "+capacityBytes + ", Granted: "+newMem.getCapacity());
      return newMem;
    }

    @Override
    public Memory request(Memory origMem, long copyToBytes, long capacityBytes) {
      // not used.
      return null;
    }

    @Override
    public void free(Memory mem) {
     // not used.
    }

    @Override
    public void free(Memory memToFree, Memory newMem) {
      // not used.
    }

  }
}
