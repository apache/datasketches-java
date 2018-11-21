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
    for (int i = 0; i < cap; i++) { wmem.putByte(i, (byte)(-128 + i)); }

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
  public void bytesCheck() {
    long seed = 0;
    int offset = 0;
    int cap = 16;

    long[] hash1;
    long[] hash2 = new long[2];

    for (int j = 1; j < cap; j++) {
      byte[] in = new byte[cap];

      WritableMemory wmem = WritableMemory.wrap(in);
      for (int i = 0; i < j; i++) { wmem.putByte(i, (byte) (-128 + i)); }

      hash1 = MurmurHash3.hash(in, 0);
      hash2 = MemoryMurmurHash3.hash(wmem, offset, j, seed, hash1);

      printHashes(hash1, hash2);
    }
  }

  static void printHashes(long[] hash1, long[] hash2) {
    String match = ((hash1[0] == hash2[0]) && (hash1[1] == hash2[1]))? "true" : "false";
    String h1a = Long.toHexString(hash1[0]);
    String h1b = Long.toHexString(hash1[1]);
    String h2a = Long.toHexString(hash2[0]);
    String h2b = Long.toHexString(hash2[1]);
    String h1s = String.format("%16s %16s", h1a, h1b);
    String h2s = String.format("%16s %16s %5s", h2a, h2b, match);
    System.out.println(h1s);
    System.out.println(h2s);
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
