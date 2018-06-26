/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
    try (WritableDirectHandle wdh = WritableMemory.allocateDirect(initBytes)) {
      final WritableMemory wmem = wdh.get();
      println("Initial mem size: " + wmem.getCapacity());

      //########## Receiving Application
      // The receiving application has been given wmem to use for a sketch,
      // but alas, it is not ultimately large enough.
      final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build(wmem);
      assertTrue(usk1.isEmpty());

      //Load the sketch
      for (int i = 0; i < u; i++) {
        // The sketch uses The MemoryRequest, acquired from wmem, to acquire more memory as
        // needed, and requests via the MemoryRequest to free the old allocations.
        usk1.update(i);
      }
      final double result = usk1.getQuantile(0.5);
      println("Result: " + result);
      assertEquals(result, u / 2.0, 0.05 * u); //Success

      //########## Owning Implementation
      //The actual Memory has been re-allocated several times,
      // so the above wmem reference is invalid.
      println("\nFinal mem size: " + wmem.getCapacity());
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
    final int initBytes = DoublesSketch.getUpdatableStorageBytes(k, n); //8 bytes
    final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build();
    final Memory origSketchMem = Memory.wrap(usk1.toByteArray());

    try (WritableDirectHandle memHandle = WritableMemory.allocateDirect(initBytes)) {
      WritableMemory mem = memHandle.get();
      origSketchMem.copyTo(0, mem, 0, initBytes);
      UpdateDoublesSketch usk2 = DirectUpdateDoublesSketch.wrapInstance(mem);
      assertTrue(mem.isSameResource(usk2.getMemory()));
      assertEquals(mem.getCapacity(), initBytes);
      assertTrue(mem.isDirect());
      assertTrue(usk2.isEmpty());

      //update the sketch forcing it to grow on-heap
      for (int i = 1; i <= 5; i++) { usk2.update(i); }
      assertEquals(usk2.getN(), 5);
      WritableMemory mem2 = usk2.getMemory();
      assertFalse(mem.isSameResource(mem2));
      assertFalse(mem2.isDirect()); //should now be on-heap

      final int expectedSize = COMBINED_BUFFER + ((2 * k) << 3);
      assertEquals(mem2.getCapacity(), expectedSize);
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
