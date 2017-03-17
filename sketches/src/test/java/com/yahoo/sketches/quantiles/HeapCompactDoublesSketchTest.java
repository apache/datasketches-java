/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.HeapUpdateDoublesSketch.checkPreLongsFlagsSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.Util.LS;
import static com.yahoo.sketches.quantiles.Util.computeCombinedBufferItemCapacity;
import static com.yahoo.sketches.quantiles.Util.computeNumLevelsNeeded;
import static com.yahoo.sketches.quantiles.Util.lg;
import static java.lang.Math.floor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;

public class HeapCompactDoublesSketchTest {

  @BeforeMethod
  public void setUp() {
    DoublesSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

  @Test
  public void heapifyFromUpdateSketch() {
    //final int k = 128;
    //final int n = 1950413;
    final int k = 4;
    final int n = 27;
    final UpdateDoublesSketch qs = buildAndLoadQS(k, n); // assuming ordered inserts

    final byte[] qsBytes = qs.toByteArray();
    final Memory qsMem = new NativeMemory(qsBytes);

    final HeapCompactDoublesSketch compactQs = HeapCompactDoublesSketch.heapifyInstance(qsMem);
    testSketchEquality(qs, compactQs);
  }

  @Test
  public void heapifyFromCompactSketch() {
    final int k = 8;
    final int n = 177;
    final UpdateDoublesSketch qs = buildAndLoadQS(k, n); // assuming ordered inserts

    final byte[] qsBytes = qs.compact().toByteArray();
    final Memory qsMem = new NativeMemory(qsBytes);

    final HeapCompactDoublesSketch compactQs = HeapCompactDoublesSketch.heapifyInstance(qsMem);
    testSketchEquality(qs, compactQs);

    final DoublesSketch ds = DoublesSketch.heapify(qsMem);
    testSketchEquality(qs, ds);
  }


  static void testSketchEquality(final DoublesSketch sketch1,
                                         final DoublesSketch sketch2) {
    assertEquals(sketch1.getK(), sketch2.getK());
    assertEquals(sketch1.getN(), sketch2.getN());
    assertEquals(sketch1.getBitPattern(), sketch2.getBitPattern());
    assertEquals(sketch1.getMinValue(), sketch2.getMinValue());
    assertEquals(sketch1.getMaxValue(), sketch2.getMaxValue());

    final DoublesSketchAccessor accessor1 = DoublesSketchAccessor.wrap(sketch1);
    final DoublesSketchAccessor accessor2 = DoublesSketchAccessor.wrap(sketch2);

    // Compare base buffers. Already confirmed n and k match.
    for (int i = 0; i < accessor1.numItems(); ++i) {
      assertEquals(accessor1.get(i), accessor2.get(i));
    }

    // Iterate over levels comparing items
    long bitPattern = sketch1.getBitPattern();
    for (int lvl = 0; bitPattern != 0; ++lvl, bitPattern >>>= 1) {
      if ((bitPattern & 1) > 0) {
        accessor1.setLevel(lvl);
        accessor2.setLevel(lvl);
        for (int i = 0; i < accessor1.numItems(); ++i) {
          assertEquals(accessor1.get(i), accessor2.get(i));
        }
      }
    }
  }

  @Test
  public void checkEmpty() {
    final int k = PreambleUtil.DEFAULT_K;
    final UpdateDoublesSketch qs1 = buildAndLoadQS(k, 0);
    final byte[] byteArr = qs1.compact().toByteArray();
    final byte[] byteArr2 = qs1.toByteArray(true, true);
    final Memory mem = new NativeMemory(byteArr);
    final HeapCompactDoublesSketch qs2 = HeapCompactDoublesSketch.heapifyInstance(mem);
    assertTrue(qs2.isEmpty());
    assertEquals(byteArr.length, 8);
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
    final Memory mem = new NativeMemory(new byte[7]);
    final HeapCompactDoublesSketch qs2 = HeapCompactDoublesSketch.heapifyInstance(mem);
    qs2.getQuantile(0.5);
  }


  static UpdateDoublesSketch buildAndLoadQS(int k, int n) {
    return buildAndLoadQS(k, n, 0);
  }

  static UpdateDoublesSketch buildAndLoadQS(int k, int n, int startV) {
    UpdateDoublesSketch qs = DoublesSketch.builder().build(k);
    for (int i=1; i<=n; i++) {
      qs.update(startV + i);
    }
    return qs;
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

}
