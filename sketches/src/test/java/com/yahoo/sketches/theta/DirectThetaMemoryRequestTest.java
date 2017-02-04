/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.AllocMemory;
import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.MemoryRequest;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesStateException;

/**
 * The concept for these tests is that the "MemoryManager" classes below are proxies for the
 * implementation that <i>owns</i> the native memory allocations, thus is responsible for
 * allocating larger Memory when requested and the actual freeing of the old memory allocations.
 */
public class DirectThetaMemoryRequestTest {

  //////////////////////////////////////////////////////
  //////////////////////////////////////////////////////
  private static class MemoryManager implements MemoryRequest { //Allocates what was requested
    NativeMemory last = null; //simple means of tracking the last Memory allocated

    @Override
    public Memory request(long capacityBytes) {
      NativeMemory newMem = new AllocMemory(capacityBytes, this); //attach me
      last = newMem;
      println("\nReqCap: "+capacityBytes + ", Granted: "+newMem.getCapacity());
      return newMem;
    }

    @Override
    public Memory request(Memory origMem, long copyToBytes, long capacityBytes) {
      Memory newMem = request(capacityBytes);
      NativeMemory.copy(origMem, 0, newMem, 0, copyToBytes);
      println("\nOldCap: " + origMem.getCapacity() + ", ReqCap: " + capacityBytes
          + ", Granted: "+ newMem.getCapacity());
      return newMem;
    }

    @Override
    public void free(Memory mem) {
      if (mem instanceof NativeMemory) {
        println("\nmem Freed bytes : " + mem.getCapacity());
        ((NativeMemory)mem).freeMemory();
      } else if (mem instanceof MemoryRegion){
        println("\nThe original MemoryRegion can be reassigned.");
      }
    }

    @Override
    public void free(Memory memToFree, Memory newMem) {
      if (memToFree instanceof NativeMemory) {
        println("\nmemToFree  Freed bytes: " + memToFree.getCapacity());
        println("newMem Allocated bytes: " + newMem.getCapacity());
        ((NativeMemory)memToFree).freeMemory();
      } else if (memToFree instanceof MemoryRegion){
        println("\nThe original MemoryRegion can be reassigned.");
      }
    }
  }
//////////////////////////////////////////////////////
  @Test
  public void checkLimitedMemoryScenarios() { //Requesting application
    int k = 4096;
    int u = 2 * k; //large enough to force a rebuild

    //########## Owning Implementation
    //This part would actually be part of the Memory owning implemention so it is faked here
    MemoryManager memMgr = new MemoryManager();
    Memory mem1 = memMgr.request((k << 3) / 4); //allocate 1/4 size off-heap
    println("Initial mem size: " + mem1.getCapacity());

    //########## Receiving Application
    //The receiving application has been given mem1 to use for a sketch,
    // but alas, it is not ultimately large enough.
    UpdateSketch usk1 = UpdateSketch.builder().initMemory(mem1)
        .setResizeFactor(ResizeFactor.X8).build(k);
    assertTrue(usk1.isEmpty());

    //Load the sketch
    for (int i=0; i<u; i++) {
    //The sketch uses the MemoryRequest, acquired from mem1, to acquire more memory as needed.
    // and requests via the MemoryRequest to free the old allocations.
      usk1.update(i);
    }
    assertEquals(usk1.getEstimate(), u, 0.05 * u); //Success

    //########## Owning Implementation
    //The actual Memory has been re-allocated several times, so the above mem1 reference is invalid.
    NativeMemory last = memMgr.last;
    println("\nFinal mem size: " + last.getCapacity());
    memMgr.free(last);
  }

  @Test
  public void checkLimitedMemoryWithP() {
    int k = 4096;
    int u = 2048; //est mode
    float p = (float)0.5;
    MemoryManager memMgr = new MemoryManager();

    Memory mem1 = memMgr.request((k<<3) / 4); //allocate 1/4 size off-heap

    UpdateSketch usk1 =
        UpdateSketch.builder().initMemory(mem1).setResizeFactor(ResizeFactor.X2).setP(p).build(k);
    assertTrue(usk1.isEmpty());

    for (int i=0; i<u; i++) {
      usk1.update(i);
    }
    assertEquals(usk1.getEstimate(), u, 0.1*u);

    NativeMemory mem2 = new NativeMemory(usk1.toByteArray());
    Sketches.wrapSketch(mem2);

    NativeMemory nMem = (NativeMemory) usk1.getMemory();
    println("Freed: " + nMem.getCapacity());
    nMem.freeMemory();
  }

  //////////////////////////////////////////////////////
  //////////////////////////////////////////////////////
  private static class MemoryManager2 implements MemoryRequest { //Allocates 2X what was requested

    @Override
    public Memory request(long capacityBytes) {
      long newCap = capacityBytes * 2;
      println("ReqCap: " + capacityBytes + ", Granted: " + newCap);
      Memory newMem = new AllocMemory(newCap);
      newMem.setMemoryRequest(this);
      return newMem;
    }

    @Override
    public void free(Memory mem) {
      println("Freed : " + mem.getCapacity());
      ((NativeMemory)mem).freeMemory();
    }

    @Override
    public void free(Memory memToFree, Memory newMem) {
      if (memToFree instanceof NativeMemory) {
        NativeMemory nMem = (NativeMemory)memToFree;
        println("Freed : " + nMem.getCapacity());
        nMem.freeMemory();
      } // else reassign the old MemoryRegion
    }

    @Override
    public Memory request(Memory origMem, long copyToBytes, long capacityBytes) {
      throw new SketchesStateException("SHOULD NOT BE HERE");
    }
  }
//////////////////////////////////////////////////////
  @Test
  public void checkLimitedMemoryScenarios2() {
    int k = 4096;
    int u = 2*k;

    MemoryManager2 memMgr = new MemoryManager2(); //allocates 2X what was requested

    Memory mem1 = new AllocMemory((k<<3) / 8, memMgr); //allocate 1/8 size off-heap

    UpdateSketch usk1 = UpdateSketch.builder().initMemory(mem1).setResizeFactor(ResizeFactor.X2).build(k);
    assertTrue(usk1.isEmpty());

    for (int i=0; i<u; i++) {
      usk1.update(i);
    }
    assertEquals(usk1.getEstimate(), u, 0.05*u);
    NativeMemory nMem = (NativeMemory) usk1.getMemory();
    println(nMem.toHexString("TestMemory", 0, 128));

    println("Freed: " + nMem.getCapacity());
    nMem.freeMemory();
  }

  //////////////////////////////////////////////////////
  //////////////////////////////////////////////////////
  private static class BadMemoryManager implements MemoryRequest { //Allocates one less than requested

    @Override
    public Memory request(long capacityBytes) {
      long newCap = capacityBytes-1; //Too small
      println("ReqCap: "+capacityBytes + ", Granted: "+newCap);
      return new AllocMemory(newCap, this);
    }

    @Override
    public void free(Memory mem) {
      println("Freed : " + mem.getCapacity());
      ((NativeMemory)mem).freeMemory();
    }

    @Override
    public void free(Memory memToFree, Memory newMem) {
      if (memToFree instanceof NativeMemory) {
        NativeMemory nMem = (NativeMemory)memToFree;
        println("Freed : " + nMem.getCapacity());
        nMem.freeMemory();
      } // else reassign the old MemoryRegion
    }

    @Override
    public Memory request(Memory origMem, long copyToBytes, long capacityBytes) {
      throw new SketchesStateException("SHOULD NOT BE HERE");
    }
  }
  //////////////////////////////////////////////////////

  @Test
  public void checkBadMemoryAlloc() {
    int k = 4096;
    int u = 2*k;

    BadMemoryManager memMgr = new BadMemoryManager();

    NativeMemory mem1 = new AllocMemory((k<<3) / 4 + 24, memMgr); //allocate 1/4 size off-heap

    UpdateSketch usk1 = UpdateSketch.builder().initMemory(mem1).setResizeFactor(ResizeFactor.X2).build(k);
    assertTrue(usk1.isEmpty());
    try {
      for (int i=0; i<u; i++) {
        usk1.update(i);
      }
      fail("Expected IllegalArgumentException");
    }
    catch (IllegalArgumentException e) { //from MemoryUtil.requestMemoryHandler
      //e.printStackTrace();
      //pass
    }
    finally {
      println("Freed: " + mem1.getCapacity());
      mem1.freeMemory();
    }
  }

  //////////////////////////////////////////////////////
  //////////////////////////////////////////////////////
  private static class BadMemoryManager2 implements MemoryRequest { //Returns null Memory

    @Override
    public Memory request(long capacityBytes) {
      println("ReqCap: "+capacityBytes + ", Granted: null");
      return null;
    }

    @Override
    public void free(Memory mem) {
      println("Freed : " + mem.getCapacity());
      ((NativeMemory)mem).freeMemory();
    }

    @Override
    public void free(Memory memToFree, Memory newMem) {
      if (memToFree instanceof NativeMemory) {
        NativeMemory nMem = (NativeMemory)memToFree;
        println("Freed : " + nMem.getCapacity());
        nMem.freeMemory();
      } // else reassign the old MemoryRegion
    }

    @Override
    public Memory request(Memory origMem, long copyToBytes, long capacityBytes) {
      throw new SketchesStateException("SHOULD NOT BE HERE");
    }
  }
  //////////////////////////////////////////////////////

  @Test
  public void checkBadMemoryAlloc2() {
    int k = 4096;
    int u = 2*k;

    BadMemoryManager2 memMgr = new BadMemoryManager2();

    NativeMemory mem1 = new AllocMemory((k<<3) / 4 + 24, memMgr); //allocate 1/4 size off-heap

    UpdateSketch usk1 = UpdateSketch.builder().initMemory(mem1).setResizeFactor(ResizeFactor.X2).build(k);
    assertTrue(usk1.isEmpty());
    try {
      for (int i=0; i<u; i++) {
        usk1.update(i);
      }
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { //from MemoryUtil.requestMemoryHandler
      //e.printStackTrace();
      //pass
    }
    finally {
      mem1.freeMemory();
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
