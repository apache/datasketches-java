/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

//import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
//import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
//import static org.testng.Assert.fail;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class SameResourceTests {

  @Test
  public void checkSameResource1() { //Heap NativeMemory
    byte[] byteArr = new byte[64];
    NativeMemory mem1 = new NativeMemory(byteArr);
    NativeMemory mem2 = new NativeMemory(byteArr);
    assertTrue(MemoryUtil.isSameResource(mem1, mem2));
    mem2 = new NativeMemory(new byte[64]);
    assertFalse(MemoryUtil.isSameResource(mem1, mem2));
  }

  @Test
  public void checkSameResource2() { //Heap MemoryRegion
    byte[] byteArr = new byte[64];
    NativeMemory mem1 = new NativeMemory(byteArr);
    MemoryRegion reg1 = new MemoryRegion(mem1, 32, 32);
    MemoryRegion reg2 = new MemoryRegion(mem1, 32, 32);
    assertTrue(MemoryUtil.isSameResource(reg1, reg2));
    reg2 = new MemoryRegion(mem1, 0, 64);
    assertTrue(MemoryUtil.isSameResource(mem1, reg2));
    assertTrue(mem1.byteBuffer() == reg2.getByteBuffer());
    assertTrue(mem1.array() == reg2.getArray());
    reg2 = new MemoryRegion(reg1, 0, 32);
    assertTrue(MemoryUtil.isSameResource(reg1, reg2));
    reg2 = new MemoryRegion(mem1, 0, 32);
    assertFalse(MemoryUtil.isSameResource(mem1, reg2));
  }

  @Test
  public void checkSameResource3() { //Heap MemoryRegion as read only
    byte[] byteArr = new byte[64];
    NativeMemory mem1 = new NativeMemory(byteArr);
    MemoryRegion reg1 = (new MemoryRegion(mem1, 32, 32)).asReadOnlyMemory();
    MemoryRegion reg2 = (new MemoryRegion(mem1, 32, 32)).asReadOnlyMemory();
    assertTrue(MemoryUtil.isSameResource(reg1, reg2));
    reg2 = (new MemoryRegion(mem1, 0, 64)).asReadOnlyMemory();
    assertTrue(MemoryUtil.isSameResource(mem1, reg2));
    assertTrue(mem1.array() == reg2.getArray());
    assertTrue(mem1.byteBuffer() == reg2.getByteBuffer());
    reg2 = (new MemoryRegion(reg1, 0, 32)).asReadOnlyMemory();
    assertTrue(MemoryUtil.isSameResource(reg1, reg2));
    reg2 = (new MemoryRegion(mem1, 0, 32)).asReadOnlyMemory();
    assertFalse(MemoryUtil.isSameResource(mem1, reg2));
  }

  @Test
  public void checkSameResource4() { //Direct NativeMemory
    NativeMemory mem1 = new AllocMemory(64);
    NativeMemory mem2 = mem1;
    assertTrue(MemoryUtil.isSameResource(mem1, mem2));
    mem2 = new AllocMemory(64);
    assertFalse(MemoryUtil.isSameResource(mem1, mem2));
    mem1.freeMemory();
    mem2.freeMemory();
  }

  @Test
  public void checkSameResource5() { //Direct MemoryRegion
    NativeMemory mem1 = new AllocMemory(64);
    MemoryRegion reg1 = new MemoryRegion(mem1, 32, 32);
    MemoryRegion reg2 = new MemoryRegion(mem1, 32, 32);
    assertTrue(MemoryUtil.isSameResource(reg1, reg2));
    reg2 = new MemoryRegion(mem1, 0, 64);
    assertTrue(MemoryUtil.isSameResource(mem1, reg2));
    assertTrue(mem1.byteBuffer() == reg2.getByteBuffer());
    assertTrue(mem1.array() == reg2.getArray());
    assertTrue(reg2.array() == reg2.getArray());
    reg2 = new MemoryRegion(reg1, 0, 32);
    assertTrue(MemoryUtil.isSameResource(reg1, reg2));
    reg2 = new MemoryRegion(mem1, 0, 32);
    assertFalse(MemoryUtil.isSameResource(mem1, reg2));
    mem1.freeMemory();
  }

  @Test
  public void checkSameResource6() { //Direct MemoryRegion as read only
    NativeMemory mem1 = new AllocMemory(64);
    MemoryRegion reg1 = (new MemoryRegion(mem1, 32, 32)).asReadOnlyMemory();
    MemoryRegion reg2 = (new MemoryRegion(mem1, 32, 32)).asReadOnlyMemory();
    assertTrue(MemoryUtil.isSameResource(reg1, reg2));
    reg2 = (new MemoryRegion(mem1, 0, 64)).asReadOnlyMemory();
    assertTrue(MemoryUtil.isSameResource(mem1, reg2));
    assertTrue(mem1.array() == reg2.getArray());
    assertTrue(mem1.byteBuffer() == reg2.getByteBuffer());
    reg2 = (new MemoryRegion(reg1, 0, 32)).asReadOnlyMemory();
    assertTrue(MemoryUtil.isSameResource(reg1, reg2));
    reg2 = (new MemoryRegion(mem1, 0, 32)).asReadOnlyMemory();
    assertFalse(MemoryUtil.isSameResource(mem1, reg2));
    mem1.freeMemory();
  }

}
