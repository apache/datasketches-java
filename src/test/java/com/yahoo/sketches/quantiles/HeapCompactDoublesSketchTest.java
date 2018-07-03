/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static com.yahoo.sketches.quantiles.Util.LS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
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
    final UpdateDoublesSketch qs = buildAndLoadQS(k, n);
    final byte[] qsBytes = qs.toByteArray();
    final Memory qsMem = Memory.wrap(qsBytes);

    final HeapCompactDoublesSketch compactQs = HeapCompactDoublesSketch.heapifyInstance(qsMem);
    DoublesSketchTest.testSketchEquality(qs, compactQs);

    assertNull(compactQs.getMemory());
  }

  @Test
  public void createFromUnsortedUpdateSketch() {
    final int k = 4;
    final int n = 13;
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(k).build();
    for (int i = n; i > 0; --i) {
      qs.update(i);
    }
    final HeapCompactDoublesSketch compactQs = HeapCompactDoublesSketch.createFromUpdateSketch(qs);

    // don't expect equal but new base buffer should be sorted
    checkBaseBufferIsSorted(compactQs);
  }

  @Test
  public void heapifyFromCompactSketch() {
    final int k = 8;
    final int n = 177;
    final UpdateDoublesSketch qs = buildAndLoadQS(k, n); // assuming reverse ordered inserts

    final byte[] qsBytes = qs.compact().toByteArray();
    final Memory qsMem = Memory.wrap(qsBytes);

    final HeapCompactDoublesSketch compactQs = HeapCompactDoublesSketch.heapifyInstance(qsMem);
    DoublesSketchTest.testSketchEquality(qs, compactQs);
  }

  @Test
  public void checkHeapifyUnsortedCompactV2() {
    final int k = 64;
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(64).build();
    for (int i = 0; i < (3 * k); ++i) {
      qs.update(i);
    }
    assertEquals(qs.getBaseBufferCount(), k);
    final byte[] sketchBytes = qs.toByteArray(true);
    final WritableMemory mem = WritableMemory.wrap(sketchBytes);

    // modify to make v2, clear compact flag, and insert a -1 in the middle of the base buffer
    PreambleUtil.insertSerVer(mem, 2);
    PreambleUtil.insertFlags(mem, 0);
    final long tgtAddr = COMBINED_BUFFER + ((Double.BYTES * k) / 2);
    mem.putDouble(tgtAddr, -1.0);
    assert mem.getDouble(tgtAddr - Double.BYTES) > mem.getDouble(tgtAddr);

    // ensure the heapified base buffer is sorted
    final HeapCompactDoublesSketch qs2 = HeapCompactDoublesSketch.heapifyInstance(mem);
    checkBaseBufferIsSorted(qs2);
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
    assertTrue(Double.isNaN(qs2.getQuantile(0.0)));
    assertTrue(Double.isNaN(qs2.getQuantile(1.0)));
    assertTrue(Double.isNaN(qs2.getQuantile(0.5)));
    final double[] quantiles = qs2.getQuantiles(new double[] {0.0, 0.5, 1.0});
    assertNull(quantiles);
    //println(qs1.toString(true, true));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMemTooSmall1() {
    final Memory mem = Memory.wrap(new byte[7]);
    HeapCompactDoublesSketch.heapifyInstance(mem);
  }

  static void checkBaseBufferIsSorted(HeapCompactDoublesSketch qs) {
    final double[] combinedBuffer = qs.getCombinedBuffer();
    final int bbCount = qs.getBaseBufferCount();

    for (int i = 1; i < bbCount; ++i) {
      assert combinedBuffer[i - 1] <= combinedBuffer[i];
    }
  }

  static UpdateDoublesSketch buildAndLoadQS(final int k, final int n) {
    return buildAndLoadQS(k, n, 0);
  }

  static UpdateDoublesSketch buildAndLoadQS(final int k, final int n, final int startV) {
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(k).build();
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
