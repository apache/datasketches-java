/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.LS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.theta.Sketch;

public class DirectCompactDoublesSketchTest {
  @BeforeMethod
  public void setUp() {
    DoublesSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrapFromUpdateSketch() {
    final int k = 4;
    final int n = 27;
    final UpdateDoublesSketch qs = HeapUpdateDoublesSketchTest.buildAndLoadQS(k, n);

    final byte[] qsBytes = qs.toByteArray();
    final Memory qsMem = new NativeMemory(qsBytes);

    DirectCompactDoublesSketch.wrapInstance(qsMem);
    fail();
  }

  @Test
  public void wrapFromCompactSketch() {
    final int k = 8;
    final int n = 177;
    final DirectCompactDoublesSketch qs = buildAndLoadDCQS(k, n); // assuming ordered inserts

    final byte[] qsBytes = qs.toByteArray();
    final Memory qsMem = new NativeMemory(qsBytes);

    final DirectCompactDoublesSketch compactQs = DirectCompactDoublesSketch.wrapInstance(qsMem);
    DoublesSketchTest.testSketchEquality(qs, compactQs);
    assertEquals(qsBytes.length, compactQs.getStorageBytes());
  }

  @Test
  public void checkEmpty() {
    final int k = PreambleUtil.DEFAULT_K;
    final DirectCompactDoublesSketch qs1 = buildAndLoadDCQS(k, 0);
    final byte[] byteArr = qs1.toByteArray();
    final byte[] byteArr2 = qs1.toByteArray(true);
    final Memory mem = new NativeMemory(byteArr);
    final HeapCompactDoublesSketch qs2 = HeapCompactDoublesSketch.heapifyInstance(mem);
    assertTrue(qs2.isEmpty());
    assertEquals(byteArr.length, qs1.getStorageBytes());
    assertEquals(byteArr, byteArr2);
    assertEquals(qs2.getQuantile(0.0), Double.POSITIVE_INFINITY);
    assertEquals(qs2.getQuantile(1.0), Double.NEGATIVE_INFINITY);
    assertEquals(qs2.getQuantile(0.5), Double.NaN);
    final double[] quantiles = qs2.getQuantiles(new double[] {0.0, 0.5, 1.0});
    assertEquals(quantiles.length, 3);
    assertEquals(quantiles[0], Double.POSITIVE_INFINITY);
    assertEquals(quantiles[1], Double.NaN);
    assertEquals(quantiles[2], Double.NEGATIVE_INFINITY);
    //println(qs1.toString(true, true));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMemTooSmall() {
    final Memory mem = new NativeMemory(new byte[7]);
    HeapCompactDoublesSketch.heapifyInstance(mem);
  }

  @Test
  public void checkMerge() {
    // most approaches don't use getCombinedBuffer
  }

  static DirectCompactDoublesSketch buildAndLoadDCQS(final int k, final int n) {
    return buildAndLoadDCQS(k, n, 0);
  }

  static DirectCompactDoublesSketch buildAndLoadDCQS(final int k, final int n, final int startV) {
    final UpdateDoublesSketch qs = DoublesSketch.builder().build(k);
    for (int i = 1; i <= n; i++) {
      qs.update(startV + i);
    }
    final byte[] byteArr = new byte[qs.getCompactStorageBytes()];
    final NativeMemory mem = new NativeMemory(byteArr);
    return (DirectCompactDoublesSketch) qs.compact(mem);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
    print("PRINTING: " + this.getClass().getName() + LS);
  }

  static void println(final String s) {
    print(s + LS);
  }

  static void print(final String s) {
    //System.err.print(s); //disable here
  }
}
