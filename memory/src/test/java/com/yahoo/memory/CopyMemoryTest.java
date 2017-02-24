/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

import static org.testng.Assert.assertEquals;
//import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class CopyMemoryTest {

  @Test
  public void heapWSource() {
    int k1 = 1 << 20; //longs
    int k2 = 2 * k1;
    Memory srcMem = genMem(k1, false, false, false); //empty, RO, direct
    Memory dstMem = genMem(k2, true, false, false);
    srcMem.copy(0, dstMem, k1 << 3, k1 << 3);
    check(dstMem, k1, k1, 1);
  }

  @Test
  public void heapROSource() {
    int k1 = 1 << 20; //longs
    int k2 = 2 * k1;
    Memory srcMem = genMem(k1, false, true, false); //empty, RO, direct
    Memory dstMem = genMem(k2, true, false, false);
    srcMem.copy(0, dstMem, k1 << 3, k1 << 3);
    check(dstMem, k1, k1, 1);
  }

  @Test
  public void directWSource() {
    int k1 = 1 << 20; //longs
    int k2 = 2 * k1;
    Memory srcMem = genMem(k1, false, false, true); //empty, RO, direct
    Memory dstMem = genMem(k2, true, false, false);
    srcMem.copy(0, dstMem, k1 << 3, k1 << 3);
    check(dstMem, k1, k1, 1);
    srcMem.freeMemory();
    dstMem.freeMemory();
  }

  @Test
  public void directROSource() {
    int k1 = 1 << 20; //longs
    int k2 = 2 * k1;
    Memory srcMem = genMem(k1, false, true, true); //empty, RO, direct
    Memory dstMem = genMem(k2, true, false, false);
    srcMem.copy(0, dstMem, k1 << 3, k1 << 3);
    check(dstMem, k1, k1, 1);
    srcMem.freeMemory();
    dstMem.freeMemory();
  }

  @Test
  public void heapWSrcRegion() {
    int k1 = 1 << 5; //longs
    int k2 = 2 * k1;
    Memory srcMem = genRegion(k1/2, k1/2, k1, false, false, false); //empty, RO, direct
    Memory dstMem = genMem(k2, true, false, false);
    srcMem.copy(0, dstMem, k1 << 3, (k1/2) << 3);
    check(dstMem, k1, k1/2, k1/2 + 1);
    srcMem.freeMemory();
    dstMem.freeMemory();
  }

  @Test
  public void heapROSrcRegion() {
    int k1 = 1 << 20; //longs
    int k2 = 2 * k1;
    Memory srcMem = genRegion(k1/2, k1/2, k1, false, true, false); //empty, RO, direct
    //println(srcMem.toHexString("SrcMem", 0L, (int)srcMem.getCapacity()));
    Memory dstMem = genMem(k2, true, false, false);
    srcMem.copy(0, dstMem, k1 << 3, (k1/2) << 3);
    //println(dstMem.toHexString("DstMem", 0L, (int)dstMem.getCapacity()));
    check(dstMem, k1, k1/2, k1/2 + 1);
    srcMem.freeMemory();
    dstMem.freeMemory();
  }

  @Test
  public void directROSrcRegion() {
    int k1 = 1 << 20; //longs
    int k2 = 2 * k1;
    Memory srcMem = genRegion(k1/2, k1/2, k1, false, true, true); //empty, RO, direct
    //println(srcMem.toHexString("SrcMem", 0L, (int)srcMem.getCapacity()));
    Memory dstMem = genMem(k2, true, false, false);
    srcMem.copy(0, dstMem, k1 << 3, (k1/2) << 3);
    //println(dstMem.toHexString("DstMem", 0L, (int)dstMem.getCapacity()));
    check(dstMem, k1, k1/2, k1/2 + 1);
    srcMem.freeMemory();
    dstMem.freeMemory();
  }

  private static void check(Memory mem, int offsetLongs, int lengthLongs, int startValue) {
    int offBytes = offsetLongs << 3;
    for (long i = 0; i < lengthLongs; i++) {
      assertEquals(mem.getLong(offBytes + (i << 3)), i + startValue);
    }
  }

  private static Memory genMem(int longs, boolean empty, boolean readOnly, boolean direct) {
    NativeMemory mem;
    if (direct) {
      mem = new AllocMemory(longs << 3);
      if (empty) mem.clear();
    } else {
      mem = new NativeMemory(new long[longs]);
    }
    if (!empty) {
      for (int i = 0; i < longs; i++) { mem.putLong(i << 3, i + 1); }
    }
    if (readOnly) { return mem.asReadOnlyMemory(); }
    return mem;
  }

  private static Memory genRegion(int offsetLongs, int lengthLongs,
      int baseLongs, boolean empty, boolean readOnly, boolean direct) {
    Memory base = genMem(baseLongs, empty, false, direct);
    Memory region = new MemoryRegion(base, offsetLongs << 3, lengthLongs << 3);
    if (readOnly) { return region.asReadOnlyMemory(); }
    return region;
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
