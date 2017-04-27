/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.LS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

public class HeapCompactDoublesSketchTest {

  @BeforeMethod
  public void setUp() {
    DoublesSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

  @Test
  public void heapifyFromUpdateSketch() {
    final int k = 4;
    final int n = 45;
    final UpdateDoublesSketch qs = buildAndLoadQS(k, n); // assuming ordered inserts

    final byte[] qsBytes = qs.toByteArray();
    final Memory qsMem = Memory.wrap(qsBytes);

    final HeapCompactDoublesSketch compactQs = HeapCompactDoublesSketch.heapifyInstance(qsMem);
    DoublesSketchTest.testSketchEquality(qs, compactQs);

    assertNull(compactQs.getMemory());
  }

  @Test
  public void heapifyFromCompactSketch() {
    final int k = 8;
    final int n = 177;
    final UpdateDoublesSketch qs = buildAndLoadQS(k, n); // assuming ordered inserts

    final byte[] qsBytes = qs.compact().toByteArray();
    final Memory qsMem = Memory.wrap(qsBytes);

    final HeapCompactDoublesSketch compactQs = HeapCompactDoublesSketch.heapifyInstance(qsMem);
    DoublesSketchTest.testSketchEquality(qs, compactQs);
  }


  @Test
  public void checkEmpty() {
    final int k = PreambleUtil.DEFAULT_K;
    final UpdateDoublesSketch qs1 = buildAndLoadQS(k, 0);
    final byte[] byteArr = qs1.compact().toByteArray();
    final byte[] byteArr2 = qs1.toByteArray(true);
    final Memory mem = Memory.wrap(byteArr);
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
  public void checkMemTooSmall1() {
    final Memory mem = Memory.wrap(new byte[7]);
    HeapCompactDoublesSketch.heapifyInstance(mem);
  }


  static UpdateDoublesSketch buildAndLoadQS(final int k, final int n) {
    return buildAndLoadQS(k, n, 0);
  }

  static UpdateDoublesSketch buildAndLoadQS(final int k, final int n, final int startV) {
    final UpdateDoublesSketch qs = DoublesSketch.builder().build(k);
    for (int i = 1; i <= n; i++) {
      qs.update(startV + i);
    }
    return qs;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
    print("PRINTING: " + this.getClass().getName() + LS);
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    print(s + LS);
  }

  /**
   * @param s value to print
   */
  static void print(final String s) {
    //System.err.print(s); //disable here
  }

}
