/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hash;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;


/**
 * @author Lee Rhodes
 */
public class XxHashTest {

  @Test
  public void offsetChecks() {
    long seed = 12345;
    int blocks = 6;
    int cap = blocks * 16;

    long hash;

    WritableMemory wmem = WritableMemory.allocate(cap);
    for (int i = 0; i < cap; i++) { wmem.putByte(i, (byte)(-128 + i)); }

    for (int offset = 0; offset < 16; offset++) {
      int arrLen = cap - offset;
      hash = XxHash.hash(wmem, offset, arrLen, seed);
      assertTrue(hash != 0);
    }
  }

  @Test
  public void byteArrChecks() {
    long seed = 0;
    int offset = 0;
    int bytes = 16;

    for (int j = 1; j < bytes; j++) {
      byte[] in = new byte[bytes];

      WritableMemory wmem = WritableMemory.wrap(in);
      for (int i = 0; i < j; i++) { wmem.putByte(i, (byte) (-128 + i)); }

      long hash = XxHash.hash(wmem, offset, bytes, seed);
      assertTrue(hash != 0);
    }
  }

  @Test
  public void longCheck() {
    long seed = 0;
    long hash = XxHash.hash(123L, seed);
    assertTrue(hash != 0);
  }

  @Test
  public void collisionTest() {
    WritableMemory wmem = WritableMemory.allocate(128);
    wmem.putLong(0, 1);
    wmem.putLong(16, 42);
    wmem.putLong(32, 2);
    long h1 = XxHash.hash(wmem, 0, wmem.getCapacity(), 0);

    wmem.putLong(0, 1 + 0xBA79078168D4BAFL);
    wmem.putLong(32, 2 + 0x9C90005B80000000L);
    long h2 = XxHash.hash(wmem, 0, wmem.getCapacity(), 0);
    assertEquals(h1, h2);

    wmem.putLong(0, 1 + (0xBA79078168D4BAFL * 2));
    wmem.putLong(32, 2 + (0x9C90005B80000000L * 2));

    long h3 = XxHash.hash(wmem, 0, wmem.getCapacity(), 0);
    assertEquals(h2, h3);
  }

}
