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
  public void offsetChecks() {
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
  public void byteArrChecks() {
    long seed = 0;
    int offset = 0;
    int bytes = 16;

    long[] hash2 = new long[2];

    for (int j = 1; j < bytes; j++) {
      byte[] in = new byte[bytes];

      WritableMemory wmem = WritableMemory.wrap(in);
      for (int i = 0; i < j; i++) { wmem.putByte(i, (byte) (-128 + i)); }

      long[] hash1 = MurmurHash3.hash(in, 0);
      hash2 = MemoryMurmurHash3.hash(wmem, offset, bytes, seed, hash2);
      long[] hash3 = MemoryMurmurHash3.hash(in, seed);

      assertEquals(hash1, hash2);
      assertEquals(hash1, hash3);
    }
  }

  @Test
  public void charArrChecks() {
    long seed = 0;
    int offset = 0;
    int chars = 16;
    int bytes = chars << 1;

    long[] hash2 = new long[2];

    for (int j = 1; j < chars; j++) {
      char[] in = new char[chars];

      WritableMemory wmem = WritableMemory.wrap(in);
      for (int i = 0; i < j; i++) { wmem.putInt(i, i); }

      long[] hash1 = MurmurHash3.hash(in, 0);
      hash2 = MemoryMurmurHash3.hash(wmem, offset, bytes, seed, hash2);
      long[] hash3 = MemoryMurmurHash3.hash(in, seed);

      assertEquals(hash1, hash2);
      assertEquals(hash1, hash3);
    }
  }

  @Test
  public void intArrChecks() {
    long seed = 0;
    int offset = 0;
    int ints = 16;
    int bytes = ints << 2;

    long[] hash2 = new long[2];

    for (int j = 1; j < ints; j++) {
      int[] in = new int[ints];

      WritableMemory wmem = WritableMemory.wrap(in);
      for (int i = 0; i < j; i++) { wmem.putInt(i, i); }

      long[] hash1 = MurmurHash3.hash(in, 0);
      hash2 = MemoryMurmurHash3.hash(wmem, offset, bytes, seed, hash2);
      long[] hash3 = MemoryMurmurHash3.hash(in, seed);

      assertEquals(hash1, hash2);
      assertEquals(hash1, hash3);
    }
  }

  @Test
  public void longArrChecks() {
    long seed = 0;
    int offset = 0;
    int longs = 16;
    int bytes = longs << 3;

    long[] hash2 = new long[2];

    for (int j = 1; j < longs; j++) {
      long[] in = new long[longs];

      WritableMemory wmem = WritableMemory.wrap(in);
      for (int i = 0; i < j; i++) { wmem.putLong(i, i); }

      long[] hash1 = MurmurHash3.hash(in, 0);
      hash2 = MemoryMurmurHash3.hash(wmem, offset, bytes, seed, hash2);
      long[] hash3 = MemoryMurmurHash3.hash(in, seed);

      assertEquals(hash1, hash2);
      assertEquals(hash1, hash3);
    }
  }

  @Test
  public void longCheck() {
    long seed = 0;
    int offset = 0;
    int bytes = 8;

    long[] hash2 = new long[2];
    long[] in = { 1 };
    WritableMemory wmem = WritableMemory.wrap(in);

    long[] hash1 = MurmurHash3.hash(in, 0);
    hash2 = MemoryMurmurHash3.hash(wmem, offset, bytes, seed, hash2);
    long[] hash3 = MemoryMurmurHash3.hash(in, seed);

    assertEquals(hash1, hash2);
    assertEquals(hash1, hash3);
  }

  @Test
  public void doubleCheck() {
    long[] hash1 = checkDouble(-0.0);
    long[] hash2 = checkDouble(0.0);
    assertEquals(hash1, hash2);
    hash1 = checkDouble(Double.NaN);
    long nan = (0x7FFL << 52) + 1L;
    hash2 = checkDouble(Double.longBitsToDouble(nan));
    assertEquals(hash1, hash2);
    checkDouble(1.0);
  }

  private static long[] checkDouble(double dbl) {
    long seed = 0;
    int offset = 0;
    int bytes = 8;

    long[] hash2 = new long[2];

    final double d = (dbl == 0.0) ? 0.0 : dbl;   // canonicalize -0.0, 0.0
    final long data = Double.doubleToLongBits(d);// canonicalize all NaN forms
    final long[] dataArr = { data };

    WritableMemory wmem = WritableMemory.wrap(dataArr);
    long[] hash1 = MurmurHash3.hash(dataArr, 0);
    hash2 = MemoryMurmurHash3.hash(wmem, offset, bytes, seed, hash2);
    long[] hash3 = MemoryMurmurHash3.hash(dbl, seed, hash2);

    assertEquals(hash1, hash2);
    assertEquals(hash1, hash3);
    return hash1;
  }

}
