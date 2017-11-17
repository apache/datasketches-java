/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;

/**
 * The concept for these tests is that the "MemoryManager" classes below are proxies for the
 * implementation that <i>owns</i> the native memory allocations, thus is responsible for
 * allocating larger Memory when requested and the actual freeing of the old memory allocations.
 */
public class DirectQuantilesMemoryRequestTest {
  @Test
  public void checkLimitedMemoryScenarios() { //Requesting application
    final int k = 128;
    final int u = 40 * k;
    final int initBytes = ((2 * k) + 4) << 3; //just the BB

    //########## Owning Implementation
    // This part would actually be part of the Memory owning implemention so it is faked here
    try (WritableDirectHandle memHandler = WritableMemory.allocateDirect(initBytes)) {
      final WritableMemory mem1 = memHandler.get();
      println("Initial mem size: " + mem1.getCapacity());

      //########## Receiving Application
      // The receiving application has been given mem1 to use for a sketch,
      // but alas, it is not ultimately large enough.
      final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build(mem1);
      assertTrue(usk1.isEmpty());

      //Load the sketch
      for (int i = 0; i < u; i++) {
        // The sketch uses <></>he MemoryRequest, acquired from mem1, to acquire more memory as
        // needed. and requests via the MemoryRequest to free the old allocations.
        usk1.update(i);
      }
      final double result = usk1.getQuantile(0.5);
      println("Result: " + result);
      assertEquals(result, u / 2.0, 0.05 * u); //Success

      //########## Owning Implementation
      //The actual Memory has been re-allocated several times, so the above mem1 reference is invalid.
      println("\nFinal mem size: " + mem1.getCapacity());
    }
  }

  @Test
  public void checkGrowBaseBuf() {
    final int k = 128;
    final int u = 32; // don't need the BB to fill here
    final int initBytes = (4 + (u / 2)) << 3; // not enough to hold everything

    try (WritableDirectHandle memHandler = WritableMemory.allocateDirect(initBytes)) {
      //final MemoryManager memMgr = new MemoryManager();
      //final WritableMemory mem1 = memMgr.request(initBytes);
      final WritableMemory mem1 = memHandler.get();
      println("Initial mem size: " + mem1.getCapacity());
      final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build(mem1);
      for (int i = 1; i <= u; i++) {
        usk1.update(i);
      }
      final int currentSpace = usk1.getCombinedBufferItemCapacity();
      println("curCombBufItemCap: " + currentSpace);
      assertEquals(currentSpace, 2 * k);
    }
  }

  @Test
  public void checkGrowCombBuf() {
    final int k = 128;
    final int u = (2 * k) - 1; //just to fill the BB
    final int initBytes = ((2 * k) + 4) << 3; //just room for BB

    try (WritableDirectHandle memHandler = WritableMemory.allocateDirect(initBytes)) {
      //final MemoryManager memMgr = new MemoryManager();
      //final WritableMemory mem1 = memMgr.request(initBytes);
      final WritableMemory mem1 = memHandler.get();
      println("Initial mem size: " + mem1.getCapacity());
      final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build(mem1);
      for (int i = 1; i <= u; i++) {
        usk1.update(i);
      }
      final int currentSpace = usk1.getCombinedBufferItemCapacity();
      println("curCombBufItemCap: " + currentSpace);
      final double[] newCB = usk1.growCombinedBuffer(currentSpace, 3 * k);
      final int newSpace = usk1.getCombinedBufferItemCapacity();
      println("newCombBurItemCap: " + newSpace);
      assertEquals(newCB.length, 3 * k);
      //memMgr.free(mem1);
    }
  }

  @Test
  public void checkGrowFromWrappedEmptySketch() {
    final int k = 16;
    final int n = 0;
    final int initBytes = DoublesSketch.getUpdatableStorageBytes(k, n);
    final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build();
    final Memory origSketchMem = Memory.wrap(usk1.toByteArray());

    try (WritableDirectHandle memHandler = WritableMemory.allocateDirect(initBytes)) {
      // putN() -- force-increment, check regular update
      final WritableMemory mem = memHandler.get();
      origSketchMem.copyTo(0, mem, 0, initBytes);
      UpdateDoublesSketch usk2 = DirectUpdateDoublesSketch.wrapInstance(mem);
      assertEquals(usk2.getMemory().getCapacity(), initBytes);
      assertTrue(usk2.isEmpty());
      usk2.putN(5);
      assertEquals(usk2.getN(), 5);
      // will request a full base buffer
      usk2.update(1.0);
      assertEquals(usk2.getN(), 6);
      final int expectedSize = COMBINED_BUFFER + ((2 * k) << 3);
      assertEquals(usk2.getMemory().getCapacity(), expectedSize);

      //update
      origSketchMem.copyTo(0, mem, 0, initBytes);
      usk2 = DirectUpdateDoublesSketch.wrapInstance(mem);
      assertEquals(usk2.getMemory().getCapacity(), initBytes);
      assertEquals(usk2.getMinValue(), Double.NaN);
      usk2.update(5.0);
      double minV = usk2.getMinValue();
      assertEquals(minV, 5.0);
      double maxV = usk2.getMaxValue();
      assertEquals(maxV, 5.0);
      assertEquals(usk2.getMemory().getCapacity(), expectedSize);
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
