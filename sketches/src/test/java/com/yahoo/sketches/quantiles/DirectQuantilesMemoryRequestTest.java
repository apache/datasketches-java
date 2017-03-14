/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.AllocMemory;
import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRequest;
import com.yahoo.memory.NativeMemory;

/**
 * The concept for these tests is that the "MemoryManager" classes below are proxies for the
 * implementation that <i>owns</i> the native memory allocations, thus is responsible for
 * allocating larger Memory when requested and the actual freeing of the old memory allocations.
 */
public class DirectQuantilesMemoryRequestTest {

  //////////////////////////////////////////////////////
  //////////////////////////////////////////////////////
  private static class MemoryManager implements MemoryRequest { //Allocates what was requested
    NativeMemory last = null; //simple means of tracking the last Memory allocated

    @Override
    public Memory request(final long capacityBytes) {
      last = new AllocMemory(capacityBytes, this); //attach me
      println("\nReqCap: " + capacityBytes + ", Granted: " + last.getCapacity());
      return last;
    }

    @Override
    public Memory request(final Memory origMem, final long copyToBytes, final long capacityBytes) {
      final Memory newMem = request(capacityBytes);
      origMem.copy(0, newMem, 0, copyToBytes);
      println("\nOldCap: " + origMem.getCapacity() + ", ReqCap: " + capacityBytes
          + ", Granted: " + newMem.getCapacity());
      return newMem;
    }

    @Override
    public void free(final Memory mem) {
      println("\nmem Freed bytes : " + mem.getCapacity());
      mem.freeMemory();
    }

    @Override
    public void free(final Memory memToFree, final Memory newMem) {
      println("\nmemToFree  Freed bytes: " + memToFree.getCapacity());
      println("newMem Allocated bytes: " + newMem.getCapacity());
      memToFree.freeMemory();
    }
  }

  //////////////////////////////////////////////////////

  @Test
  public void checkLimitedMemoryScenarios() { //Requesting application
    final int k = 128;
    final int u = 40 * k;
    final int initBytes = (2 * k + 4) << 3; //just the BB

    //########## Owning Implementation
    //This part would actually be part of the Memory owning implemention so it is faked here
    final MemoryManager memMgr = new MemoryManager();
    final Memory mem1 = memMgr.request(initBytes); //allocate
    println("Initial mem size: " + mem1.getCapacity());

    //########## Receiving Application
    //The receiving application has been given mem1 to use for a sketch,
    // but alas, it is not ultimately large enough.
    final UpdateDoublesSketch usk1 = DoublesSketch.builder().initMemory(mem1).build(k); //.initMemory(mem1)
    assertTrue(usk1.isEmpty());

    //Load the sketch
    for (int i = 0; i < u; i++) {
      //The sketch uses the MemoryRequest, acquired from mem1, to acquire more memory as needed.
      // and requests via the MemoryRequest to free the old allocations.
      usk1.update(i);
    }
    final double result = usk1.getQuantile(0.5);
    println("Result: " + result);
    assertEquals(result, u / 2.0, 0.05 * u); //Success

    //########## Owning Implementation
    //The actual Memory has been re-allocated several times, so the above mem1 reference is invalid.
    final NativeMemory last = memMgr.last;
    println("\nFinal mem size: " + last.getCapacity());
    memMgr.free(last);
  }

  @Test
  public void checkGrowBaseBuf() {
    final int k = 128;
    final int u = 32; // don't need the BB to fill here
    final int initBytes = (4 + u / 2) << 3; // not enough to hold everything

    final MemoryManager memMgr = new MemoryManager();
    final Memory mem1 = memMgr.request(initBytes);
    println("Initial mem size: " + mem1.getCapacity());
    final UpdateDoublesSketch usk1 = DoublesSketch.builder().initMemory(mem1).build(k);
    for (int i = 1; i <= u; i++) { usk1.update(i); }
    final int currentSpace = usk1.getCombinedBufferItemCapacity();
    println("curCombBufItemCap: " + currentSpace);
    assertEquals(currentSpace, 2 * k);
  }

  @Test
  public void checkGrowCombBuf() {
    final int k = 128;
    final int u = 2 * k - 1; //just to fill the BB
    final int initBytes = (2 * k + 4) << 3; //just room for BB

    final MemoryManager memMgr = new MemoryManager();
    final Memory mem1 = memMgr.request(initBytes);
    println("Initial mem size: " + mem1.getCapacity());
    final UpdateDoublesSketch usk1 = DoublesSketch.builder().initMemory(mem1).build(k);
    for (int i = 1; i <= u; i++) { usk1.update(i); }
    final int currentSpace = usk1.getCombinedBufferItemCapacity();
    println("curCombBufItemCap: " + currentSpace);
    final double[] newCB = usk1.growCombinedBuffer(currentSpace, 3 * k);
    final int newSpace = usk1.getCombinedBufferItemCapacity();
    println("newCombBurItemCap: " + newSpace);
    assertEquals(newCB.length, 3 * k);
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
