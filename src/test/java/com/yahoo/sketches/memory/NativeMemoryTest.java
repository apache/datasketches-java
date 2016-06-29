/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.memory;

import static com.yahoo.sketches.memory.CommonTest.getAndAddSetTests;
import static com.yahoo.sketches.memory.CommonTest.setClearIsBitsTests;
import static com.yahoo.sketches.memory.CommonTest.setClearMemoryRegionsTests;
import static com.yahoo.sketches.memory.CommonTest.setGetArraysTests;
import static com.yahoo.sketches.memory.CommonTest.setGetPartialArraysWithOffsetTests;
import static com.yahoo.sketches.memory.CommonTest.setGetTests;
import static com.yahoo.sketches.memory.CommonTest.toHexStringAllMemTests;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.memory.AllocMemory;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.MemoryUtil;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * @author Lee Rhodes
 */
public class NativeMemoryTest {

  //Simple Native direct
  
  @Test
  public void checkNativeCapacityAndFree() {
    int memCapacity = 8;
    NativeMemory mem = new AllocMemory(memCapacity);
    assertEquals(memCapacity, mem.getCapacity());
    
    mem.freeMemory(); //intentional
    assertFalse(mem.isAllocated());
    
    mem.freeMemory(); //intentional, nothing to free
  }
  
  @Test
  public void checkNativeNotAllocatedMemory() {
    long memCapacity = 16;
    NativeMemory mem = new AllocMemory(memCapacity);
    
    mem.freeMemory(); //intential
    assertTrue(mem.getCapacity() == 0);
    assertFalse(mem.isAllocated());
  }
  
  //Simple Native array

  @Test
  public void checkByteArray() {
    byte[] srcArray = { 1, -2, 3, -4, 5, -6, 7, -8 };
    byte[] dstArray = new byte[8];
    NativeMemory mem = new NativeMemory(srcArray);
    mem.getByteArray(0, dstArray, 0, 8);

    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }
    //freeMemory not needed, array is on-heap
  }
  
  @Test
  public void checkLongArray() {
    long[] srcArray = { 1, -2, 3, -4, 5, -6, 7, -8 };
    long[] dstArray = new long[8];
    NativeMemory mem = new NativeMemory(srcArray);
    mem.getLongArray(0, dstArray, 0, 8);

    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }
    //freeMemory not needed, array is on-heap
  }
  
  @Test
  public void checkNativeBaseBound() {
    int memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);
    
    try {
      mem.toHexString("Force Assertion Error", memCapacity, 8);
      fail("Did Not Catch Assertion Error: Memory bound");
    } 
    catch (AssertionError e) {
      //pass
    } finally {
      mem.freeMemory();
    }
  }
  
  @Test
  public void checkNativeSrcArrayBound() {
    long memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);
    
    try {
      byte[] srcArray = { 1, -2, 3, -4 };
      mem.putByteArray(0L, srcArray, 0, 5);
      fail("Did Not Catch Assertion Error: array bound");
    } 
    catch (AssertionError e) {
      //pass
    } finally {
      mem.freeMemory();
    }
  }
  
  //Copy Within tests
  
  @Test
  public void checkCopyWithinNativeSmall() {
    int memCapacity = 64;
    int half = memCapacity/2;
    NativeMemory mem = new AllocMemory(memCapacity);
    mem.clear();
    
    for (int i=0; i<half; i++) {
      mem.putByte(i, (byte) i);
    }

    mem.copy(0, half, half);
    
    for (int i=0; i<half; i++) {
      assertEquals(mem.getByte(i+half), (byte) i);
    }
    
    mem.freeMemory();
  }
  
  @Test
  public void checkCopyWithinNativeLarge() {
    int memCapacity = (2<<20) + 64;
    int memCapLongs = memCapacity / 8;
    int halfBytes = memCapacity / 2;
    int halfLongs = memCapLongs / 2;
    NativeMemory mem = new AllocMemory(memCapacity);
    mem.clear();
    
    for (int i=0; i<halfLongs; i++) {
      mem.putLong(i*8,  i);
    }
    
    mem.copy(0, halfBytes, halfBytes);
    
    for (int i=0; i<halfLongs; i++) {
      assertEquals(mem.getLong((i+halfLongs)*8), i);
    }

    mem.freeMemory();
  }
  
  @Test
  public void checkCopyWithinNativeOverlap() {
    int memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);
    mem.clear();
    println(mem.toHexString("Clear 64", 0, memCapacity));
    
    for (int i=0; i<memCapacity/2; i++) {
      mem.putByte(i, (byte) i);
    }
    println(mem.toHexString("Set 1st 32 to ints ", 0, memCapacity));
    
    try {
      mem.copy(0, memCapacity/4, memCapacity/2);
      fail("Did Not Catch Assertion Error: copy overlap");
    } 
    catch (AssertionError e) {
      //pass
    } finally {
      mem.freeMemory();
    }
  }
  
  @Test
  public void checkCopyWithinNativeSrcBound() {
    int memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);

    try {
      mem.copy(32, 32, 33);  //hit source bound check
      fail("Did Not Catch Assertion Error: source bound");
    } 
    catch (AssertionError e) {
      //pass
    } finally {
      mem.freeMemory();
    }
  }
  
  @Test
  public void checkCopyWithinNativeDstBound() {
    int memCapacity = 64;
    NativeMemory mem = new AllocMemory(memCapacity);

    try {
      mem.copy(0, 32, 33);  //hit dst bound check
      fail("Did Not Catch Assertion Error: dst bound");
    } 
    catch (AssertionError e) {
      //pass
    } finally {
      mem.freeMemory();
    }
  }
  
  @Test
  public void checkCopyCrossNativeSmall() {
    int memCapacity = 64;
    NativeMemory mem1 = new AllocMemory(memCapacity);
    NativeMemory mem2 = new AllocMemory(memCapacity);
    
    for (int i=0; i<memCapacity; i++) {
      mem1.putByte(i, (byte) i);
    }
    mem2.clear();
    
    NativeMemory.copy(mem1, 0, mem2, 0, memCapacity);
    
    for (int i=0; i<memCapacity; i++) {
      assertEquals(mem2.getByte(i), (byte) i);
    }
    
    mem1.freeMemory();
    mem2.freeMemory();
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void checkDeprecatedCopyCrossNativeSmall() {
    int memCapacity = 64;
    NativeMemory mem1 = new AllocMemory(memCapacity);
    NativeMemory mem2 = new AllocMemory(memCapacity);
    
    for (int i=0; i<memCapacity; i++) {
      mem1.putByte(i, (byte) i);
    }
    mem2.clear();
    
    MemoryUtil.copy(mem1, 0, mem2, 0, memCapacity);
    
    for (int i=0; i<memCapacity; i++) {
      assertEquals(mem2.getByte(i), (byte) i);
    }
    
    mem1.freeMemory();
    mem2.freeMemory();
  }
  
  @Test
  public void checkCopyCrossNativeLarge() {
    int memCapacity = (2<<20) + 64;
    int memCapLongs = memCapacity / 8;
    NativeMemory mem1 = new AllocMemory(memCapacity);
    NativeMemory mem2 = new AllocMemory(memCapacity);
    
    for (int i=0; i<memCapLongs; i++) {
      mem1.putLong(i*8, i);
    }
    mem2.clear();
    
    NativeMemory.copy(mem1, 0, mem2, 0, memCapacity);
    
    for (int i=0; i<memCapLongs; i++) {
      assertEquals(mem2.getLong(i*8), i);
    }
    
    mem1.freeMemory();
    mem2.freeMemory();
  }

  @Test
  public void checkCopyCrossNativeAndByteArray() {
    int memCapacity = 64;
    NativeMemory mem1 = new AllocMemory(memCapacity);
    for (int i= 0; i<mem1.getCapacity(); i++) {
      mem1.putByte(i, (byte) i);
    }
    byte[] byteArr = new byte[64];
    NativeMemory mem2 = new NativeMemory(byteArr);
    mem2.clear();
    NativeMemory.copy(mem1, 8, mem2, 16, 16);
    
    for (int i=0; i<16; i++) {
      assertEquals(mem1.getByte(8+i), mem2.getByte(16+i));
    }
    
    println(mem2.toHexString("Mem2", 0, (int)mem2.getCapacity()));
    
    mem1.freeMemory();
    mem2.freeMemory();
  }
  
  @Test
  public void checkCopyCrossRegionsSameNative() {
    int memCapacity = 128;
    NativeMemory mem1 = new AllocMemory(memCapacity);
    for (int i= 0; i<mem1.getCapacity(); i++) {
      mem1.putByte(i, (byte) i);
    }
    println(mem1.toHexString("Mem1", 0, (int)mem1.getCapacity()));
    
    Memory reg1 = new MemoryRegion(mem1, 8, 16);
    println(reg1.toHexString("Reg1", 0, (int)reg1.getCapacity()));
    
    Memory reg2 = new MemoryRegion(mem1, 24, 16);
    println(reg2.toHexString("Reg2", 0, (int)reg2.getCapacity()));
    
    NativeMemory.copy(reg1, 0, reg2, 0, 16);
    
    for (int i=0; i<16; i++) {
      assertEquals(reg1.getByte(i), reg2.getByte(i));
      assertEquals(mem1.getByte(8+i), mem1.getByte(24+i));
    }
    
    println(mem1.toHexString("Mem1", 0, (int)mem1.getCapacity()));
    
    mem1.freeMemory();
  }
  
  @Test
  public void checkCopyCrossNativeArrayAndHierarchicalRegions() {
    int memCapacity = 64;
    NativeMemory mem1 = new AllocMemory(memCapacity);
    for (int i= 0; i<mem1.getCapacity(); i++) { //fill with numbers
      mem1.putByte(i, (byte) i);
    }
    println(mem1.toHexString("Mem1", 0, (int)mem1.getCapacity()));
    
    NativeMemory mem2 = new NativeMemory(new byte[64]);
    mem2.clear();
    
    Memory reg1 = new MemoryRegion(mem1, 8, 32);
    Memory reg1B = new MemoryRegion(reg1, 8, 16);
    println(reg1.toHexString("Reg1", 0, (int)reg1.getCapacity()));
    println(reg1B.toHexString("Reg1B", 0, (int)reg1B.getCapacity()));
    
    Memory reg2 = new MemoryRegion(mem2, 32, 16);
    NativeMemory.copy(reg1B, 0, reg2, 0, 16);
    println(reg2.toHexString("Reg2", 0, (int)reg2.getCapacity()));

    println(mem2.toHexString("Mem2", 0, (int)mem2.getCapacity()));
    
    mem1.freeMemory();
    mem2.freeMemory();
  }
  
  //Tests using CommonTests 
  
  @Test
  public void checkToHexStringAllMem() {
    int memCapacity = 48; //must be 48
    NativeMemory mem = new AllocMemory(memCapacity);
    
    toHexStringAllMemTests(mem); //requires println enabled to visually check
    
    mem.freeMemory();
  }
  
  @Test
  public void checkSetClearMemoryRegions() {
    int memCapacity = 64; //must be 64
    NativeMemory mem = new AllocMemory(memCapacity);
    
    setClearMemoryRegionsTests(mem); //requires println enabled to visually check
    
    mem.freeMemory();
  }
  
  @Test
  public void checkSetGet() {
    int memCapacity = 16; //must be 16
    NativeMemory mem = new AllocMemory(memCapacity);
    assertEquals(mem.getCapacity(), memCapacity);
    
    setGetTests(mem);

    mem.freeMemory();
  }
  
  @Test
  public void checkSetGetArrays() {
    int memCapacity = 32;
    NativeMemory mem = new AllocMemory(memCapacity);
    assertEquals(memCapacity, mem.getCapacity());
    
    setGetArraysTests(mem);
    
    mem.freeMemory();
  }
  
  @Test
  public void checkSetGetPartialArraysWithOffset() {
    int memCapacity = 32;
    NativeMemory mem = new AllocMemory(memCapacity);
    assertEquals(memCapacity, mem.getCapacity());
    
    setGetPartialArraysWithOffsetTests(mem);
    
    mem.freeMemory();
  }
  
  @Test
  public void checkSetClearIsBits() {
    int memCapacity = 8;
    NativeMemory mem = new AllocMemory(memCapacity);

    assertEquals(memCapacity, mem.getCapacity());
    mem.clear();
    
    setClearIsBitsTests(mem);
    
    mem.freeMemory();
  }
  
  @Test
  public void checkIncrements() {
    int memCapacity = 8;
    NativeMemory mem = new AllocMemory(memCapacity);

    assertEquals(mem.getCapacity(), memCapacity);
    
    getAndAddSetTests(mem);
    
    mem.freeMemory();
  }
  
  @Test
  public void checkByteBufferHeapAccess() {
    int memCapacity = 64;
    ByteBuffer byteBuf = ByteBuffer.allocate(memCapacity);
    NativeMemory mem = new NativeMemory(byteBuf);
    
    for (int i=0; i<memCapacity; i++) {
      byteBuf.put(i, (byte) i);
    }
    
    for (int i=0; i<memCapacity; i++) {
      assertEquals(mem.getByte(i), byteBuf.get(i));
    }
    
    println( mem.toHexString("HeapBB", 0, memCapacity));
  }
  
  @Test
  public void checkByteBufferDirectAccess() {
    int memCapacity = 64;
    ByteBuffer byteBuf = ByteBuffer.allocateDirect(memCapacity);
    NativeMemory mem = new NativeMemory(byteBuf);
    
    for (int i=0; i<memCapacity; i++) {
      byteBuf.put(i, (byte) i);
    }
    
    for (int i=0; i<memCapacity; i++) {
      assertEquals(mem.getByte(i), byteBuf.get(i));
    }
    
    println( mem.toHexString("HeapBB", 0, memCapacity));
  }
  
  @Test
  public void checkHasArrayAndBuffer() {
    byte[] byteArr = new byte[64];
    NativeMemory mem = new NativeMemory(byteArr);
    assertTrue(mem.hasArray());
    byte[] byteArr2 = (byte[]) mem.array();
    assertEquals(byteArr2.length, 64);
    
    ByteBuffer byteBuf = ByteBuffer.allocate(128);
    mem = new NativeMemory(byteBuf);
    assertTrue(mem.hasByteBuffer());
    ByteBuffer byteBuf2 = mem.byteBuffer();
    assertEquals(byteBuf2.capacity(), 128);
  }
  
  @Test
  public void checkBinarySearch() {
    int len = 100;
    long[] arr = new long[len];
    for (int i=0; i<len; i++) arr[i] = i;
    Memory mem = new NativeMemory(arr);
    mem.putLongArray(0, arr, 0, len);

    for (long i=0; i<len; i+=10) {
      int idx = MemoryUtil.binarySearchLongs(mem, 0, arr.length, i);
      assertEquals(mem.getLong(idx<<3), i);
    }
    
    int idx = MemoryUtil.binarySearchLongs(mem, 0, arr.length, -10);
    assertEquals(idx, -1);
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkLongArrExcep() {
    long[] arr = new long[0];
    new NativeMemory(arr);
  }
  
  @Test
  public void checkIsDirect() {
    byte[] byteArr = new byte[64];
    NativeMemory mem = new NativeMemory(byteArr);
    assertFalse(mem.isDirect());
    mem = new AllocMemory(64);
    assertTrue(mem.isDirect());
    mem.freeMemory();
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNullByteArray() {
    byte[] byteArr = null;
    NativeMemory mem = new NativeMemory(byteArr);
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
