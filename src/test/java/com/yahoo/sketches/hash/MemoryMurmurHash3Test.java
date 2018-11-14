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
    long cap = 100;

    byte[] byteArr1 = new byte[(int) cap];
    WritableMemory wmem = WritableMemory.wrap(byteArr1);
    for (int i = 0; i < cap; i++) { byteArr1[i] = (byte) i; }

    long[] hash1 = new long[2];
    hash1 = MemoryMurmurHash3.hash(wmem, 0L, cap, 0L, hash1);

    long[] hash2 = MurmurHash3.hash(byteArr1, 0L);
    assertEquals(hash1, hash2);

    //now with offset
    long offsetBytes = 1;
    int len2 = (int) (cap - offsetBytes);

    long[] hash3 = new long[2];
    hash3 = MemoryMurmurHash3.hash(wmem, offsetBytes, len2, 0L, hash3);

    byte[] byteArr2 = new byte[len2];
    for (int i = 0; i < len2; i++) {
      byteArr2[i] = byteArr1[(int)(i + offsetBytes)];
    }

    long[] hash4 = MurmurHash3.hash(byteArr2, 0L);
    assertEquals(hash3, hash4);
  }

}
