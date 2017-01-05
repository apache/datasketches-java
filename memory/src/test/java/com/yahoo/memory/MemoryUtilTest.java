/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;


import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class MemoryUtilTest {

  @Test
  public void checkGoodCallback() {
    int k = 128;
    Memory mem = new NativeMemory(new byte[k]);
    mem.setMemoryRequest(new GoodMemoryManager());
    Memory newMem = MemoryUtil.requestMemoryHandler(mem, 2 * k);
    assertEquals(newMem.getCapacity(), 2 * k);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkNoCallback() {
    int k = 128;
    Memory mem = new NativeMemory(new byte[k]);
    MemoryUtil.requestMemoryHandler(mem, 2 * k);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBadCallback1() {
    int k = 128;
    Memory mem = new NativeMemory(new byte[k]);
    mem.setMemoryRequest(new BadMemoryManager1());
    MemoryUtil.requestMemoryHandler(mem, 2 * k);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBadCallback2() {
    int k = 128;
    Memory mem = new NativeMemory(new byte[k]);
    mem.setMemoryRequest(new BadMemoryManager2());
    MemoryUtil.requestMemoryHandler(mem, 2 * k);
  }

  //Allocates what was asked
  private static class GoodMemoryManager implements MemoryRequest {

    @Override
    public Memory request(long capacityBytes) {
      Memory newMem = new NativeMemory(new byte[(int)capacityBytes]);
      println("ReqCap: "+capacityBytes + ", Granted: "+newMem.getCapacity());
      return newMem;
    }

    @Override
    public Memory request(Memory origMem, long copyToBytes, long capacityBytes) {
      // not used.
      return null;
    }

    @Override
    public void free(Memory mem) {
     // not used.
    }

    @Override
    public void free(Memory memToFree, Memory newMem) {
      // not used.
    }

  }

  //Returns a null
  private static class BadMemoryManager1 implements MemoryRequest {

    @Override
    public Memory request(long capacityBytes) {
      return null;
    }

    @Override
    public Memory request(Memory origMem, long copyToBytes, long capacityBytes) {
      // not used.
      return null;
    }

    @Override
    public void free(Memory mem) {
      // not used.
    }

    @Override
    public void free(Memory memToFree, Memory newMem) {
      // not used.
    }

  }

  //Allocates too small
  private static class BadMemoryManager2 implements MemoryRequest {

    @Override
    public Memory request(long capacityBytes) {
      return new NativeMemory(new byte[(int)(capacityBytes - 1)]);
    }

    @Override
    public Memory request(Memory origMem, long copyToBytes, long capacityBytes) {
      // not used.
      return null;
    }

    @Override
    public void free(Memory mem) {
      // not used.
    }

    @Override
    public void free(Memory memToFree, Memory newMem) {
      // not used.
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
