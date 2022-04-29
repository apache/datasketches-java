/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hash;

import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MurmurHash3v2;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class MurmurHash3v2Test {
  private Random rand = new Random();
  private static final int trials = 1 << 20;

  @Test
  public void compareLongArrLong() { //long[]
    int arrLen = 3;
    int iPer = 8 / Long.BYTES;
    long[] key = new long[arrLen];
    for (int i = 0; i < trials; i++) { //trials
      for (int j = 0; j < arrLen / iPer; j++) { //longs
        long r = rand.nextLong();
        key[j] = r;
      }
      long[] res1 = hashV1(key, 0);
      long[] res2 = hashV2(key, 0);
      assertEquals(res2, res1);
    }
  }

  @Test
  public void compareIntArr() { //int[]
    int bytes = Integer.BYTES;
    int arrLen = 6;
    int[] key = new int[arrLen];
    int iPer = 8 / bytes;
    int nLongs = arrLen / iPer;
    int shift = 64 / iPer;

    for (int i = 0; i < trials; i++) { //trials
      for (int j = 0; j < nLongs; j++) { //longs
        long r = rand.nextLong();
        for (int k = 0; k < iPer; k++) { //ints
          int shft = k * shift;
          key[k] = (int) (r >>> shft);
        }
      }
      long[] res1 = hashV1(key, 0);
      long[] res2 = hashV2(key, 0);
      assertEquals(res2, res1);
    }
  }

  @Test
  public void compareCharArr() { //char[]
    int bytes = Character.BYTES;
    int arrLen = 12;
    char[] key = new char[arrLen];
    int iPer = 8 / bytes;
    int nLongs = arrLen / iPer;
    int shift = 64 / iPer;

    for (int i = 0; i < trials; i++) { //trials
      for (int j = 0; j < nLongs; j++) { //longs
        long r = rand.nextLong();
        for (int k = 0; k < iPer; k++) { //char
          int shft = k * shift;
          key[k] = (char) (r >>> shft);
        }
      }
      long[] res1 = hashV1(key, 0);
      long[] res2 = hashV2(key, 0);
      assertEquals(res2, res1);
    }
  }

  @Test
  public void compareByteArr() { //byte[]
    int bytes = Byte.BYTES;
    int arrLen = 12;
    byte[] key = new byte[arrLen];
    int iPer = 8 / bytes;
    int nLongs = arrLen / iPer;
    int shift = 64 / iPer;

    for (int i = 0; i < trials; i++) { //trials
      for (int j = 0; j < nLongs; j++) { //longs
        long r = rand.nextLong();
        for (int k = 0; k < iPer; k++) { //bytes
          int shft = k * shift;
          key[k] = (byte) (r >>> shft);
        }
      }
      long[] res1 = hashV1(key, 0);
      long[] res2 = hashV2(key, 0);
      assertEquals(res2, res1);
    }
  }

  @Test
  public void compareLongVsLongArr() {
    int arrLen = 1;
    long[] key = new long[arrLen];
    long[] out = new long[2];
    for (int i = 0; i < trials; i++) { //trials
      long r = rand.nextLong();
      key[0] = r;
      long[] res1 = hashV1(key, 0);
      long[] res2 = hashV2(r, 0, out);
      assertEquals(res2, res1);
    }
  }

  private static final long[] hashV1(long[] key, long seed) {
    return MurmurHash3.hash(key, seed);
  }

  private static final long[] hashV1(int[] key, long seed) {
    return MurmurHash3.hash(key, seed);
  }

  private static final long[] hashV1(char[] key, long seed) {
    return MurmurHash3.hash(key, seed);
  }

  private static final long[] hashV1(byte[] key, long seed) {
    return MurmurHash3.hash(key, seed);
  }

  private static final long[] hashV2(long[] key, long seed) {
    return MurmurHash3v2.hash(key, seed);
  }

  private static final long[] hashV2(int[] key2, long seed) {
    return MurmurHash3v2.hash(key2, seed);
  }

  private static final long[] hashV2(char[] key, long seed) {
    return MurmurHash3v2.hash(key, seed);
  }

  private static final long[] hashV2(byte[] key, long seed) {
    return MurmurHash3v2.hash(key, seed);
  }

  //V2 single primitives

  private static final long[] hashV2(long key, long seed, long[] out) {
    return MurmurHash3v2.hash(key, seed, out);
  }

//  private static final long[] hashV2(double key, long seed, long[] out) {
//    return MurmurHash3v2.hash(key, seed, out);
//  }

//  private static final long[] hashV2(String key, long seed, long[] out) {
//    return MurmurHash3v2.hash(key, seed, out);
//  }



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
      hash1 = MurmurHash3v2.hash(wmem, offset, arrLen, seed, hash1);
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
    int bytes = 1024;

    long[] hash2 = new long[2];

    for (int j = 1; j < bytes; j++) {
      byte[] in = new byte[bytes];

      WritableMemory wmem = WritableMemory.writableWrap(in);
      for (int i = 0; i < j; i++) { wmem.putByte(i, (byte) (-128 + i)); }

      long[] hash1 = MurmurHash3.hash(in, 0);
      hash2 = MurmurHash3v2.hash(wmem, offset, bytes, seed, hash2);
      long[] hash3 = MurmurHash3v2.hash(in, seed);

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

      WritableMemory wmem = WritableMemory.writableWrap(in);
      for (int i = 0; i < j; i++) { wmem.putInt(i, i); }

      long[] hash1 = MurmurHash3.hash(in, 0);
      hash2 = MurmurHash3v2.hash(wmem, offset, bytes, seed, hash2);
      long[] hash3 = MurmurHash3v2.hash(in, seed);

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

      WritableMemory wmem = WritableMemory.writableWrap(in);
      for (int i = 0; i < j; i++) { wmem.putInt(i, i); }

      long[] hash1 = MurmurHash3.hash(in, 0);
      hash2 = MurmurHash3v2.hash(wmem, offset, bytes, seed, hash2);
      long[] hash3 = MurmurHash3v2.hash(in, seed);

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

      WritableMemory wmem = WritableMemory.writableWrap(in);
      for (int i = 0; i < j; i++) { wmem.putLong(i, i); }

      long[] hash1 = MurmurHash3.hash(in, 0);
      hash2 = MurmurHash3v2.hash(wmem, offset, bytes, seed, hash2);
      long[] hash3 = MurmurHash3v2.hash(in, seed);

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
    WritableMemory wmem = WritableMemory.writableWrap(in);

    long[] hash1 = MurmurHash3.hash(in, 0);
    hash2 = MurmurHash3v2.hash(wmem, offset, bytes, seed, hash2);
    long[] hash3 = MurmurHash3v2.hash(in, seed);

    assertEquals(hash1, hash2);
    assertEquals(hash1, hash3);
  }

  @Test
  public void checkEmptiesNulls() {
    long seed = 123;
    long[] hashOut = new long[2];
    try {
      MurmurHash3v2.hash(Memory.wrap(new long[0]), 0, 0, seed, hashOut);  //mem empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      Memory mem = null;
      MurmurHash3v2.hash(mem, 0, 0, seed, hashOut); //mem null
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      String s = "";
      MurmurHash3v2.hash(s, seed, hashOut); //string empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      String s = null;
      MurmurHash3v2.hash(s, seed, hashOut); //string null
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      byte[] barr = new byte[0];
      MurmurHash3v2.hash(barr, seed); //byte[] empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      byte[] barr = null;
      MurmurHash3v2.hash(barr, seed); //byte[] null
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      char[] carr = new char[0];
      MurmurHash3v2.hash(carr, seed); //char[] empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      char[] carr = null;
      MurmurHash3v2.hash(carr, seed); //char[] null
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      int[] iarr = new int[0];
      MurmurHash3v2.hash(iarr, seed); //int[] empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      int[] iarr = null;
      MurmurHash3v2.hash(iarr, seed); //int[] null
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      long[] larr = new long[0];
      MurmurHash3v2.hash(larr, seed); //long[] empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      long[] larr = null;
      MurmurHash3v2.hash(larr, seed); //long[] null
      fail();
    } catch (final IllegalArgumentException e) { } //OK
  }

  @Test
  public void checkStringLong() {
    long seed = 123;
    long[] hashOut = new long[2];
    String s = "123";
    assertTrue(MurmurHash3v2.hash(s, seed, hashOut)[0] != 0);
    long v = 123;
    assertTrue(MurmurHash3v2.hash(v, seed, hashOut)[0] != 0);
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

    WritableMemory wmem = WritableMemory.writableWrap(dataArr);
    long[] hash1 = MurmurHash3.hash(dataArr, 0);
    hash2 = MurmurHash3v2.hash(wmem, offset, bytes, seed, hash2);
    long[] hash3 = MurmurHash3v2.hash(dbl, seed, hash2);

    assertEquals(hash1, hash2);
    assertEquals(hash1, hash3);
    return hash1;
  }

}
