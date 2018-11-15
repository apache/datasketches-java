/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hash;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
/**
 * @author Lee Rhodes
 */
public class MemoryMurmurHash3Test {

  @Test
  public void comparisonCheck() {
    long seed = 12345;
    int blocks = 6;
    int cap = blocks * 16;

    long[] hash1 = new long[2];
    long[] hash2;

    WritableMemory wmem = WritableMemory.allocate(cap);
    for (byte i = 0; i < cap; i++) { wmem.putByte(i, (byte)(-128 + i)); }

    for (int offset = 0; offset < 16; offset++) {
      int arrLen = cap - offset;
      hash1 = MemoryMurmurHash3.hash(wmem, offset, arrLen, seed, hash1);
      byte[] byteArr2 = new byte[arrLen];
      wmem.getByteArray(offset, byteArr2, 0, arrLen);
      hash2 = MurmurHash3.hash(byteArr2, seed);
      assertEquals(hash1, hash2);
    }

  }

  @Test
  public void longArrCheck() {
    long seed = 54321;
    int blocks = 6;
    int cap = blocks * 2;
    long rand = 0x87c37b91114253d5L;
    long[] hash1 = new long[2];
    long[] hash2;

    long[] longArr1 = new long[cap];
    for (int i = 0; i < cap; i++) { longArr1[i] = rand + i; }

    for (int offset = 0; offset < 2; offset++) {
      int arrLen = cap - offset;
      hash1 = MemoryMurmurHash3.hash(longArr1, offset, arrLen, seed, hash1);
      long[] longArr2 = new long[arrLen];
      for (int i = 0; i < arrLen; i++) { longArr2[i] = longArr1[i + offset]; }
      hash2 = MurmurHash3.hash(longArr2, seed);
      assertEquals(hash1, hash2);
    }

  }

}
