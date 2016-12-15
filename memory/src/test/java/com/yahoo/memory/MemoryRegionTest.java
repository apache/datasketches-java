/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.memory;

import static com.yahoo.memory.CommonTest.atomicMethodTests;
import static com.yahoo.memory.CommonTest.setClearIsBitsTests;
import static com.yahoo.memory.CommonTest.setClearMemoryRegionsTests;
import static com.yahoo.memory.CommonTest.setGetArraysTests;
import static com.yahoo.memory.CommonTest.setGetPartialArraysWithOffsetTests;
import static com.yahoo.memory.CommonTest.setGetTests;
import static com.yahoo.memory.CommonTest.toHexStringAllMemTests;
import static java.lang.Math.min;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class MemoryRegionTest {

  @Test
  public void checkRegionCapacityAndFree() {
    int memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);
    assertEquals(memCapacity, mem.getCapacity());

    Memory region = new MemoryRegion(mem, 8, 32);
    assertEquals(32, region.getCapacity());

    mem.freeMemory();
  }

  @Test
  public void checkRegionBound() {
    int memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);

    try {
      region.toHexString("Force Assertion Error", memCapacity, 8);
      fail("Did Not Catch Assertion Error");
    }
    catch (AssertionError e) {
      //pass

    } finally {
      mem.freeMemory();
    }
  }

  @Test
  public void checkRegionSrcArrayBound() {
    int memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);
    try {
      byte[] srcArray = { 1, -2, 3, -4 };
      region.putByteArray(0L, srcArray, 0, 5);
      fail("Did Not Catch Assertion Error");
    }
    catch (AssertionError e) {
      //pass
    } finally {
      mem.freeMemory();
    }
  }

  @Test
  public void checkCopySmall() {
    int memCapacity = 64;
    int half = memCapacity/2;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);
    region.clear();

    for (int i=0; i<half; i++) {
      region.putByte(i, (byte) i);
    }

    region.copy(0, half, half);

    for (int i=0; i<half; i++) {
      assertEquals(region.getByte(i+half), (byte) i);
    }

    mem.freeMemory();
  }

  @Test
  public void checkCopyLarge() {
    int memCapacity = (2<<20) + 64;
    int memCapLongs = memCapacity / 8;
    int halfBytes = memCapacity / 2;
    int halfLongs = memCapLongs / 2;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);
    region.clear();

    for (int i=0; i<halfLongs; i++) {
      region.putLong(i*8,  i);
    }

    region.copy(0, halfBytes, halfBytes);

    for (int i=0; i<halfLongs; i++) {
      assertEquals(region.getLong((i+halfLongs)*8), i);
    }

    mem.freeMemory();
  }

  @Test
  public void checkCrossMemoryCopySmall() {
    int memCapacity = 64;
    NativeMemory mem1 = new AllocMemory(memCapacity);
    NativeMemory mem2 = new AllocMemory(memCapacity);
    Memory region1 = new MemoryRegion(mem1, 0, memCapacity);
    Memory region2 = new MemoryRegion(mem2, 0, memCapacity);

    for (int i=0; i<memCapacity; i++) {
      region1.putByte(i, (byte) i);
    }
    region2.clear();

    NativeMemory.copy(region1, 0, region2, 0, memCapacity);

    for (int i=0; i<memCapacity; i++) {
      assertEquals(mem2.getByte(i), (byte) i);
    }

    mem1.freeMemory();
    mem2.freeMemory();
  }

  @Test
  public void checkCopyWithinRegionOverlapAssert() {
    int memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);

    region.clear();
    println(region.toHexString("Clear 64", 0, memCapacity));

    for (int i=0; i<memCapacity/2; i++) {
      region.putByte(i, (byte) i);
    }
    println(region.toHexString("Set 1st 32 to ints ", 0, memCapacity));

    try {
      region.copy(0, memCapacity/4, memCapacity/2);
      fail("Did not catch assertion error");
    }
    catch (AssertionError e) {
      //pass
    } finally {
      mem.freeMemory();
    }
  }

 //Tests using CommonTests

  @Test
  public void checkToHexStringAllMem() {
    int memCapacity = 48; //must be 48
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);

    toHexStringAllMemTests(region); //requires println enabled in CommonTests to visually check

    mem.freeMemory();
  }

  @Test
  public void checkSetClearMemoryRegions() {
    int memCapacity = 64; //must be 64
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);

    setClearMemoryRegionsTests(region); //requires println enabled to visually check

    mem.freeMemory();
  }

  @Test
  public void checkSetGet() {
    int memCapacity = 16; //must be 16
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);
    assertEquals(region.getCapacity(), memCapacity);

    setGetTests(region);

    mem.freeMemory();
  }

  @Test
  public void checkSetGetArrays() {
    int memCapacity = 32;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);
    assertEquals(memCapacity, region.getCapacity());

    setGetArraysTests(region);

    mem.freeMemory();
  }

  @Test
  public void checkSetGetPartialArraysWithOffset() {
    int memCapacity = 32;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);
    assertEquals(memCapacity, region.getCapacity());

    setGetPartialArraysWithOffsetTests(region);

    mem.freeMemory();
  }

  @Test
  public void checkSetClearIsBits() {
    int memCapacity = 8;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);

    assertEquals(memCapacity, region.getCapacity());
    mem.clear();

    setClearIsBitsTests(region);

    mem.freeMemory();
  }

  @Test
  public void checkIncrements() {
    int memCapacity = 8;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);

    assertEquals(region.getCapacity(), memCapacity);

    atomicMethodTests(region);

    mem.freeMemory();
  }

  @Test
  public void checkReassign() {
     long[] arr = new long[2];
     Memory mem = new NativeMemory(arr);
     mem.putLong(0, 1L);
     mem.putLong(8, -2);
     MemoryRegion reg = new MemoryRegion(mem, 0, 8);
     assertEquals(reg.getLong(0), 1L);
     reg.reassign(8, 8);
     assertEquals(reg.getLong(0), -2L);
  }

  //////////////////////////////////////////////////////
  //////////////////////////////////////////////////////
  //this one allocates what was asked from MemoryRegion
  private static class MemoryRegionManager implements MemoryRequest {
    private Memory parent_ = null;
    private long capUsed_ = 0;  //a very simple memory management scheme!

    MemoryRegionManager(Memory parent) {
      parent_ = parent;
    }

    @Override
    public Memory request(long capacityBytes) {
      if (capacityBytes <= (parent_.getCapacity() - capUsed_)) {
        Memory newMem = new MemoryRegion(parent_, capUsed_, capacityBytes, this);
        capUsed_ += capacityBytes;
        return newMem;
      }
      return null; //could not satisfy the request
    }

    @Override
    public Memory request(Memory origMem, long copyToBytes, long capacityBytes) {
      if (capacityBytes <= (parent_.getCapacity() - capUsed_)) {
        MemoryRegion newMem = new MemoryRegion(parent_, capUsed_, capacityBytes);
        capUsed_ += capacityBytes;
        long minCopyToBytes = min(min(origMem.getCapacity(), copyToBytes), capacityBytes);
        NativeMemory.copy(origMem, 0, newMem, 0, minCopyToBytes);
        if (minCopyToBytes < capacityBytes) {
          newMem.clear(minCopyToBytes, capacityBytes - minCopyToBytes);
        }
        return newMem;
      }
      return null; //could not satisfy the request
    }

    @Override
    public void free(Memory mem) {
      //In a more sophisticated memory management scheme this would allow reallocation of
      // memory regions.
    }

    @Override
    public void free(Memory memToFree, Memory newMem) {
      //In a more sophisticated memory management scheme this would allow reallocation of
      // memory regions.
    }
  }
  //////////////////////////////////////////////////////

  @Test
  public void checkMemoryRegionRequest() {
    int parentCap = 256;
    byte[] memArr = new byte[parentCap];
    NativeMemory parent = new NativeMemory(memArr);
    MemoryRequest mr = new MemoryRegionManager(parent);
    //mark the memory so we can see it
    for (int i=0; i<parentCap; i++) memArr[i] = (byte) i;
    println(parent.toHexString("Parent", 0, 256));

    Memory reg1 = mr.request(128); //1st request
    reg1.setMemoryRequest(mr);
    println(reg1.toHexString("Region1", 0, (int)reg1.getCapacity()));

    Memory reg2 = reg1.getMemoryRequest().request(64); //2nd request via region1
    reg2.setMemoryRequest(mr);
    println(reg2.toHexString("Region2", 0, (int)reg2.getCapacity()));

    Memory reg3 = reg2.getMemoryRequest().request(64); //3rd request via region2
    println(reg3.toHexString("Region3", 0, (int)reg3.getCapacity()));

  }

  @Test
  public void checkIsReadOnly() {
    long[] srcArray = { 1, -2, 3, -4, 5, -6, 7, -8 };
    NativeMemory mem = new NativeMemory(srcArray);
    MemoryRegion mr = new MemoryRegion(mem, 0, mem.getCapacity());
    assertFalse(mr.isReadOnly());

    Memory readOnlyMem = mr.asReadOnlyMemory();
    assertTrue(readOnlyMem.isReadOnly());

    for (int i = 0; i < mem.getCapacity(); i++) {
      assertEquals(mem.getByte(i), readOnlyMem.getByte(i));
    }
    mem.freeMemory();
  }

  @Test(expectedExceptions = ReadOnlyMemoryException.class)
  public void checkWritesOnReadOnlyMemory() {
    long[] srcArray = { 1, -2, 3, -4, 5, -6, 7, -8 };
    NativeMemory mem = new NativeMemory(srcArray);
    MemoryRegion mr = new MemoryRegion(mem, 0, mem.getCapacity());
    Memory readOnlyMem = mr.asReadOnlyMemory();
    try {
      readOnlyMem.putLong(0, 10L);
    }
    finally {
      mem.freeMemory();
    }
  }

  @Test
  public void checkMisc() {
    byte[] arr = new byte[8];
    NativeMemory mem = new NativeMemory(arr);
    MemoryRegion reg = new MemoryRegion(mem, 0, mem.getCapacity());
    byte[] arr2 = (byte[]) reg.array();
    assertTrue(arr == arr2);
    assertTrue(reg.hasArray());
    assertTrue(reg.isAllocated());
    assertFalse(reg.hasByteBuffer());
    assertNull(reg.byteBuffer());
    assertTrue(mem.equals(reg.getParent()));

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
