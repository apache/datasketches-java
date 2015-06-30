/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.memory;

import static com.yahoo.sketches.memory.CommonTests.getAndAddSetTests;
import static com.yahoo.sketches.memory.CommonTests.setClearIsBitsTests;
import static com.yahoo.sketches.memory.CommonTests.setClearMemoryRegionsTests;
import static com.yahoo.sketches.memory.CommonTests.setGetArraysTests;
import static com.yahoo.sketches.memory.CommonTests.setGetPartialArraysWithOffsetTests;
import static com.yahoo.sketches.memory.CommonTests.setGetTests;
import static com.yahoo.sketches.memory.CommonTests.toHexStringAllMemTests;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.sketches.memory.AllocMemory;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.MemoryUtil;
import com.yahoo.sketches.memory.NativeMemory;

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
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkRegionBound() {
    int memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);
    
    try {
      region.toHexString("Force Assertion Error", memCapacity, 8);
      println("Did Not Catch Assertion Error");
      //throw new IllegalArgumentException("Did Not Catch Assertion Error 2");
    } 
    catch (AssertionError e) {
      println("Caught Assertion Error");
      throw new IllegalArgumentException("Cought Assertion Error");
    
    } finally {
      mem.freeMemory();
    }
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkRegionSrcArrayBound() {
    int memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);
    try {
      byte[] srcArray = { 1, -2, 3, -4 };
      region.putByteArray(0L, srcArray, 0, 5);
      println("Did Not Catch Assertion Error");
      //throw new IllegalArgumentException("Did Not Catch Assertion Error");
    } 
    catch (AssertionError e) {
      println("Caught Assertion Error");
      throw new IllegalArgumentException("Cought Assertion Error");
      
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
    
    MemoryUtil.copy(region1, 0, region2, 0, memCapacity);
    
    for (int i=0; i<memCapacity; i++) {
      assertEquals(mem2.getByte(i), (byte) i);
    }
    
    mem1.freeMemory();
    mem2.freeMemory();
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
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
      println("Did Not Catch Assertion Error");
    } 
    catch (AssertionError e) {
      println("Caught Assertion Error");
      throw new IllegalArgumentException("Cought Assertion Error");
    
    } finally {
      mem.freeMemory();
    }
  }  
  
 //Tests using CommonTests
  
  @Test
  public void checkToHexStringAllMem() {
    int memCapacity = 48;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);
    
    toHexStringAllMemTests(region);
    
    mem.freeMemory();
  }
  
  @Test
  public void checkSetClearMemoryRegions() {
    int memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);
    Memory region = new MemoryRegion(mem, 0, memCapacity);
    
    setClearMemoryRegionsTests(region);
    
    mem.freeMemory();
  }
  
  @Test
  public void checkSetGet() {
    int memCapacity = 16;
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
    
    getAndAddSetTests(region);
    
    mem.freeMemory();
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
  
}