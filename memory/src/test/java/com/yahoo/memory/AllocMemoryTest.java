/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.memory;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class AllocMemoryTest {

  @Test
  public void checkReallocateNativeMemory() {
    int longs = 1024;
    int bytes = longs << 3;
    NativeMemory mem1 = new AllocMemory(bytes);
    for (int i = 0; i<longs; i++) {
      mem1.putLong(i << 3, i);
    }
    long mem1add = mem1.getAddress(0);
    println("Add: " + mem1add + ", Cap: " + mem1.getCapacity());

    //reallocate at twice the size.
    NativeMemory mem2 = new AllocMemory(mem1, 2L * bytes, null);
    long mem2add = mem2.getAddress(0);
    boolean equal = (mem1add == mem2add);
    String eqStr = (equal)? ", EQUAL, " : ", NOT EQUAL, ";
    println("Add: " + mem2add + eqStr + "Cap: " + mem2.getCapacity());

    //Check the new mem up to original size
    for (int i = 0; i<longs; i++) {
      assertEquals(mem2.getLong(i << 3), i);
    }
    mem1.freeMemory();
    mem2.freeMemory();
  }

  @Test
  public void checkAllocateCopyClear() {
    int longs = 16;
    int bytes = longs << 3;
    NativeMemory mem1 = new AllocMemory(bytes);
    for (int i = 0; i<longs; i++) {
      mem1.putLong(i << 3, i);
    }
    long mem1add = mem1.getAddress(0);
    println("Add: " + mem1add + ", Cap: " + mem1.getCapacity());

    //new allocate at twice the size.
    NativeMemory mem2 = new AllocMemory(mem1, bytes, 2*bytes, null);
    long mem2add = mem2.getAddress(0);
    println("Add: " + mem2add + ", Cap: " + mem2.getCapacity());

    //Check the new mem up to copy size
    for (int i = 0; i<longs; i++) {
      assertEquals(mem2.getLong(i << 3), i);
    }
    mem1.freeMemory();
    mem2.freeMemory();
  }

  private static class DummyMemReq implements MemoryRequest {
    @Override public Memory request(long capacityBytes) { return null; }
    @Override public Memory request(Memory origMem, long copyToBytes, long capacityBytes) {
      return null;
    }
    @Override public void free(Memory mem) {}
    @Override public void free(Memory memToFree, Memory newMem) {}
  }

  @Test
  public void checkAllocateWithMemReq() {
    MemoryRequest req = new DummyMemReq();
    Memory mem = new AllocMemory(8, req);
    assertTrue(req.equals(mem.getMemoryRequest()));
    mem.getNativeMemory().freeMemory();
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
