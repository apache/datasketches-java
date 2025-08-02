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

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.quantilescommon.DoublesSortedView;
import org.apache.datasketches.quantilescommon.DoublesSortedViewIterator;
import org.testng.Assert;
import org.testng.annotations.Test;



public class DoublesSketchTest {

  @Test
  public void heapToDirect() {
    final UpdateDoublesSketch heapSketch = DoublesSketch.builder().build();
    for (int i = 0; i < 1000; i++) {
      heapSketch.update(i);
    }
    final DoublesSketch directSketch = DoublesSketch.wrap(MemorySegment.ofArray(heapSketch.toByteArray(false)));

    assertEquals(directSketch.getMinItem(), 0.0);
    assertEquals(directSketch.getMaxItem(), 999.0);
    assertEquals(directSketch.getQuantile(0.5), 500.0, 4.0);
  }

  @Test
  public void directToHeap() {
    final int sizeBytes = 10000;
    final UpdateDoublesSketch directSketch = DoublesSketch.builder().build(MemorySegment.ofArray(new byte[sizeBytes]));
    for (int i = 0; i < 1000; i++) {
      directSketch.update(i);
    }
    UpdateDoublesSketch heapSketch;
    heapSketch = (UpdateDoublesSketch) DoublesSketch.heapify(MemorySegment.ofArray(directSketch.toByteArray()));
    for (int i = 0; i < 1000; i++) {
      heapSketch.update(i + 1000);
    }
    assertEquals(heapSketch.getMinItem(), 0.0);
    assertEquals(heapSketch.getMaxItem(), 1999.0);
    assertEquals(heapSketch.getQuantile(0.5), 1000.0, 10.0);
  }

  @Test
  public void checkToByteArray() {
    final UpdateDoublesSketch ds = DoublesSketch.builder().build(); //k = 128
    ds.update(1);
    ds.update(2);
    final byte[] arr = ds.toByteArray(false);
    assertEquals(arr.length, ds.getCurrentUpdatableSerializedSizeBytes());
  }

  /**
   * Checks 2 DoublesSketches for equality, triggering an assert if unequal. Handles all
   * input sketches and compares only values on valid levels, allowing it to be used to compare
   * Update and Compact sketches.
   * @param sketch1 input sketch 1
   * @param sketch2 input sketch 2
   */
  static void testSketchEquality(final DoublesSketch sketch1,
                                 final DoublesSketch sketch2) {
    assertEquals(sketch1.getK(), sketch2.getK());
    assertEquals(sketch1.getN(), sketch2.getN());
    assertEquals(sketch1.getBitPattern(), sketch2.getBitPattern());
    assertEquals(sketch1.getMinItem(), sketch2.getMinItem());
    assertEquals(sketch1.getMaxItem(), sketch2.getMaxItem());

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
  public void checkIsSameResource() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[(k*16) +24]);
    final MemorySegment cseg = MemorySegment.ofArray(new byte[8]);
    final DirectUpdateDoublesSketch duds =
            (DirectUpdateDoublesSketch) DoublesSketch.builder().setK(k).build(seg);
    assertTrue(duds.isSameResource(seg));
    final DirectCompactDoublesSketch dcds = (DirectCompactDoublesSketch) duds.compact(cseg);
    assertTrue(dcds.isSameResource(cseg));

    final UpdateDoublesSketch uds = DoublesSketch.builder().setK(k).build();
    assertFalse(uds.isSameResource(seg));
  }

  @Test
  public void checkEmptyExceptions() {
    final int k = 16;
    final UpdateDoublesSketch uds = DoublesSketch.builder().setK(k).build();
    try { uds.getMaxItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { uds.getMinItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { uds.getRank(1.0); fail(); } catch (final IllegalArgumentException e) {}
    try { uds.getPMF(new double[] { 0, 0.5, 1.0 }); fail(); } catch (final IllegalArgumentException e) {}
    try { uds.getCDF(new double[] { 0, 0.5, 1.0 }); fail(); } catch (final IllegalArgumentException e) {}
  }

  @Test
  public void directSketchShouldMoveOntoHeapEventually() {
    final Arena arena = Arena.ofConfined();
    final MemorySegment wseg = arena.allocate(1000);
    final UpdateDoublesSketch sketch = DoublesSketch.builder().build(wseg);
    Assert.assertTrue(sketch.isSameResource(wseg));
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    Assert.assertFalse(sketch.isSameResource(wseg));
    arena.close();
    Assert.assertFalse(wseg.scope().isAlive());
  }

  @Test
  public void directSketchShouldMoveOntoHeapEventually2() {
    final Arena arena = Arena.ofConfined();
    int i = 0;
    final MemorySegment wseg = arena.allocate(50);
    final UpdateDoublesSketch sketch = DoublesSketch.builder().build(wseg);
    Assert.assertTrue(sketch.isSameResource(wseg));
    for (; i < 1000; i++) {
      if (sketch.isSameResource(wseg)) {
        sketch.update(i);
      } else {
        //println("MOVED OUT at i = " + i);
        break;
      }
    }
    arena.close();
    Assert.assertFalse(wseg.scope().isAlive());
  }

  @Test
  public void checkEmptyDirect() {
    try (Arena arena = Arena.ofConfined()) {
       final MemorySegment wseg = arena.allocate(1000);
      final UpdateDoublesSketch sketch = DoublesSketch.builder().build(wseg);
      sketch.toByteArray(); //exercises a specific path
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void sortedView() {
    final UpdateDoublesSketch sketch = DoublesSketch.builder().build();
    sketch.update(3);
    sketch.update(1);
    sketch.update(2);
    { // cumulative inclusive
      final DoublesSortedView view = sketch.getSortedView();
      final DoublesSortedViewIterator it = view.iterator();
      Assert.assertEquals(it.next(), true);
      Assert.assertEquals(it.getQuantile(), 1);
      Assert.assertEquals(it.getWeight(), 1);
      Assert.assertEquals(it.getNaturalRank(INCLUSIVE), 1);
      Assert.assertEquals(it.next(), true);
      Assert.assertEquals(it.getQuantile(), 2);
      Assert.assertEquals(it.getWeight(), 1);
      Assert.assertEquals(it.getNaturalRank(INCLUSIVE), 2);
      Assert.assertEquals(it.next(), true);
      Assert.assertEquals(it.getQuantile(), 3);
      Assert.assertEquals(it.getWeight(), 1);
      Assert.assertEquals(it.getNaturalRank(INCLUSIVE), 3);
      Assert.assertEquals(it.next(), false);
    }
  }

  @Test
  public void checkRankLBError() {
    final UpdateDoublesSketch sk = DoublesSketch.builder().build();
    final double eps = sk.getNormalizedRankError(false);
    println("" + (2 * eps));
    for (int i = 1; i <= 10000; i++) { sk.update(i); }
    final double rlb = sk.getRankLowerBound(.5);
    println(.5 - rlb);
    assertTrue((.5 - rlb) <= (2* eps));
  }

  @Test
  public void checkRankUBError() {
    final UpdateDoublesSketch sk = DoublesSketch.builder().build();
    final double eps = sk.getNormalizedRankError(false);
    println(""+ (2 * eps));
    for (int i = 1; i <= 10000; i++) { sk.update(i); }
    final double rub = sk.getRankUpperBound(.5);
    println(rub -.5);
    assertTrue((rub -.5) <= (2 * eps));
  }

  @Test
  public void checkGetRanks() {
    final UpdateDoublesSketch sk = DoublesSketch.builder().build();
    for (int i = 1; i <= 10000; i++) { sk.update(i); }
    final double[] qArr = {1000,2000,3000,4000,5000,6000,7000,8000,9000,10000};
    final double[] ranks = sk.getRanks(qArr, INCLUSIVE);
    for (int i = 0; i < qArr.length; i++) {
      final double rLB = sk.getRankLowerBound(ranks[i]);
      final double rUB = sk.getRankUpperBound(ranks[i]);
      assertTrue(rLB <= ranks[i]);
      assertTrue(rUB >= ranks[i]);
      println(rLB + ", " + ranks[i] + ", " + rUB);
    }
  }

  @Test
  public void checkToStringHeap() {
    final DoublesSketch sk = DoublesSketch.builder().setK(8).build();
    final int n = 32;
    for (int i = 1; i <= n; i++) {
      final double item = i;
      sk.update(item);
    }
    println(sk.toString(true, false));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  private final static boolean enablePrinting = false;

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
