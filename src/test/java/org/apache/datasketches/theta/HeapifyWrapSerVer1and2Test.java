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

package org.apache.datasketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Util;
import org.testng.annotations.Test;

@SuppressWarnings("resource")
public class HeapifyWrapSerVer1and2Test {
  private static final short defaultSeedHash = Util.computeSeedHash(Util.DEFAULT_UPDATE_SEED);

  //Heapify CompactSketch

  @Test
  public void checkHeapifyCompactSketchAssumedDefaultSeed() {
    final int k = 64;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    final CompactSketch csk = usk.compact();
    final MemorySegment cskSeg = MemorySegment.ofArray(csk.toByteArray()).asReadOnly();
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = CompactSketch.heapify(cskSeg); //don't check seedHash here
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash); //check seedHash here
  }

  @Test //Compact Assumed Different Seed
  public void checkHeapifyCompactSketchAssumedDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    final CompactSketch csk = usk.compact();
    final MemorySegment cskSeg = MemorySegment.ofArray(csk.toByteArray()).asReadOnly();
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = CompactSketch.heapify(cskSeg); //don't check seedHash here
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash); //check seedHash here
  }

  @Test //Compact Given Default Seed
  public void checkHeapifyCompactSketchGivenDefaultSeed() {
    final int k = 64;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    final CompactSketch csk = usk.compact();
    final MemorySegment cskSeg = MemorySegment.ofArray(csk.toByteArray()).asReadOnly();
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = CompactSketch.heapify(cskSeg, seed); //check seedHash here
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash); //check seedHash here
  }

  @Test //Compact Given Different Seed
  public void checkHeapifyCompactSketchGivenDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    final CompactSketch csk = usk.compact();
    final MemorySegment cskSeg = MemorySegment.ofArray(csk.toByteArray()).asReadOnly();
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = CompactSketch.heapify(cskSeg, seed); //check seedHash here
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  //Heapify Sketch

  @Test
  public void checkHeapifySketchAssumedDefaultSeed() {
    final int k = 64;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    final CompactSketch csk = usk.compact();
    final MemorySegment cskSeg = MemorySegment.ofArray(csk.toByteArray()).asReadOnly();
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = (CompactSketch) Sketch.heapify(cskSeg);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifySketchAssumedDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    final CompactSketch csk = usk.compact();
    final MemorySegment cskSeg = MemorySegment.ofArray(csk.toByteArray()).asReadOnly();
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = (CompactSketch) Sketch.heapify(cskSeg);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifySketchGivenDefaultSeed() {
    final int k = 64;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    final CompactSketch csk = usk.compact();
    final MemorySegment cskSeg = MemorySegment.ofArray(csk.toByteArray()).asReadOnly();
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = (CompactSketch) Sketch.heapify(cskSeg, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifySketchGivenDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    final CompactSketch csk = usk.compact();
    final MemorySegment cskSeg = MemorySegment.ofArray(csk.toByteArray()).asReadOnly();
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = (CompactSketch) Sketch.heapify(cskSeg, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  //Wrap CompactSketch

  @Test
  public void checkWrapCompactSketchAssumedDefaultSeed() {
    final int k = 64;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    MemorySegment offHeap;
    final CompactSketch csk = usk.compact();

    //SerialVersion3 test
    try(Arena arena = Arena.ofConfined()) {
      offHeap = putOffHeap(MemorySegment.ofArray(csk.toByteArray()), arena);
      cskResult = CompactSketch.wrap(offHeap);
      assertEquals(cskResult.getEstimate(), usk.getEstimate());
      assertEquals(cskResult.getSeedHash(), seedHash);
      assertTrue(cskResult.isOffHeap());
    }
  }

  @Test
  public void checkWrapCompactSketchAssumedDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    MemorySegment offHeap;
    final CompactSketch csk = usk.compact();

    //SerialVersion3 test
    try(Arena arena = Arena.ofConfined()) {
      offHeap = putOffHeap(MemorySegment.ofArray(csk.toByteArray()), arena);
      cskResult = CompactSketch.wrap(offHeap);
      assertEquals(cskResult.getEstimate(), usk.getEstimate());
      assertEquals(cskResult.getSeedHash(), seedHash);
      assertTrue(cskResult.isOffHeap());
    }
  }

  @Test
  public void checkWrapCompactSketchGivenDefaultSeed() {
    final int k = 64;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    MemorySegment offHeap;
    final CompactSketch csk = usk.compact();

    //SerialVersion3 test
    try(Arena arena = Arena.ofConfined()) {
      offHeap = putOffHeap(MemorySegment.ofArray(csk.toByteArray()), arena);
      cskResult = CompactSketch.wrap(offHeap, seed);
      assertEquals(cskResult.getEstimate(), usk.getEstimate());
      assertEquals(cskResult.getSeedHash(), seedHash);
      assertTrue(cskResult.isOffHeap());
    }
  }

  @Test
  public void checkWrapCompactSketchGivenDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    MemorySegment offHeap;
    final CompactSketch csk = usk.compact();

    //SerialVersion3 test
    try(Arena arena = Arena.ofConfined()) {
      offHeap = putOffHeap(MemorySegment.ofArray(csk.toByteArray()), arena);
      cskResult = CompactSketch.wrap(offHeap, seed);
      assertEquals(cskResult.getEstimate(), usk.getEstimate());
      assertEquals(cskResult.getSeedHash(), seedHash);
      assertTrue(cskResult.isOffHeap());
    }
  }

  //Wrap Sketch

  @Test
  public void checkWrapSketchAssumedDefaultSeed() {
    final int k = 64;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    MemorySegment offHeap;
    final CompactSketch csk = usk.compact();

    //SerialVersion3 test
    try(Arena arena = Arena.ofConfined()) {
      offHeap = putOffHeap(MemorySegment.ofArray(csk.toByteArray()), arena);
      cskResult = (CompactSketch) Sketch.wrap(offHeap);
      assertEquals(cskResult.getEstimate(), usk.getEstimate());
      assertEquals(cskResult.getSeedHash(), seedHash);
      assertTrue(cskResult.isOffHeap());
    }
  }

  @Test
  public void checkWrapSketchAssumedDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    MemorySegment offHeap;
    final CompactSketch csk = usk.compact();

    //SerialVersion3 test
    try(Arena arena = Arena.ofConfined()) {
      offHeap = putOffHeap(MemorySegment.ofArray(csk.toByteArray()), arena);
      cskResult = (CompactSketch) Sketch.wrap(offHeap);
      assertEquals(cskResult.getEstimate(), usk.getEstimate());
      assertEquals(cskResult.getSeedHash(), seedHash);
      assertTrue(cskResult.isOffHeap());
    }
  }

  @Test
  public void checkWrapSketchGivenDefaultSeed() {
    final int k = 64;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    MemorySegment offHeap;
    final CompactSketch csk = usk.compact();

    //SerialVersion3 test
    try(Arena arena = Arena.ofConfined()) {
      offHeap = putOffHeap(MemorySegment.ofArray(csk.toByteArray()), arena);
      cskResult = (CompactSketch) Sketch.wrap(offHeap, seed);
      assertEquals(cskResult.getEstimate(), usk.getEstimate());
      assertEquals(cskResult.getSeedHash(), seedHash);
      assertTrue(cskResult.isOffHeap());
    }
  }

  @Test
  public void checkWrapSketchGivenDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    MemorySegment offHeap;
    final CompactSketch csk = usk.compact();

    //SerialVersion3 test
    try(Arena arena = Arena.ofConfined()) {
      offHeap = putOffHeap(MemorySegment.ofArray(csk.toByteArray()), arena);
      cskResult = (CompactSketch) Sketch.wrap(offHeap, seed);
      assertEquals(cskResult.getEstimate(), usk.getEstimate());
      assertEquals(cskResult.getSeedHash(), seedHash);
      assertTrue(cskResult.isOffHeap());
    }
  }

  private static MemorySegment putOffHeap(final MemorySegment heapSeg, final Arena arena) {
    final long cap = heapSeg.byteSize();
    final MemorySegment wseg = arena.allocate(cap);
    MemorySegment.copy(heapSeg, 0, wseg, 0, cap);
    return wseg;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }
}

