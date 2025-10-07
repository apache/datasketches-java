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
public class HeapifyWrapSerVer3Test {
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

    cskResult = CompactSketch.heapify(cskSeg);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifyCompactSketchDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    final CompactSketch csk = usk.compact();
    final MemorySegment cskSeg = MemorySegment.ofArray(csk.toByteArray()).asReadOnly();
    CompactSketch cskResult;

    cskResult = CompactSketch.heapify(cskSeg, seed);
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

    cskResult = (CompactSketch) ThetaSketch.heapify(cskSeg);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifySketchDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    final CompactSketch csk = usk.compact();
    final MemorySegment cskSeg = MemorySegment.ofArray(csk.toByteArray()).asReadOnly();
    CompactSketch cskResult;

    cskResult = (CompactSketch) ThetaSketch.heapify(cskSeg, seed);
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

    try(Arena arena = Arena.ofConfined()) {
      offHeap = putOffHeap(MemorySegment.ofArray(csk.toByteArray()), arena);
      cskResult = CompactSketch.wrap(offHeap);
      assertEquals(cskResult.getEstimate(), usk.getEstimate());
      assertEquals(cskResult.getSeedHash(), seedHash);
      assertTrue(cskResult.isOffHeap());
    }
  }

  @Test
  public void checkWrapCompactSketchDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    MemorySegment offHeap;
    final CompactSketch csk = usk.compact();

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
      cskResult = (CompactSketch) ThetaSketch.wrap(offHeap);
      assertEquals(cskResult.getEstimate(), usk.getEstimate());
      assertEquals(cskResult.getSeedHash(), seedHash);
      assertTrue(cskResult.isOffHeap());
    }
  }

  @Test
  public void checkWrapSketchDifferentSeed() {
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
      cskResult = (CompactSketch) ThetaSketch.wrap(offHeap, seed);
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

