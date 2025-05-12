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

import static org.apache.datasketches.hash.XxHash.hashByteArr;
import static org.apache.datasketches.hash.XxHash.hashCharArr;
import static org.apache.datasketches.hash.XxHash.hashDoubleArr;
import static org.apache.datasketches.hash.XxHash.hashFloatArr;
import static org.apache.datasketches.hash.XxHash.hashIntArr;
import static org.apache.datasketches.hash.XxHash.hashLong;
import static org.apache.datasketches.hash.XxHash.hashLongArr;
import static org.apache.datasketches.hash.XxHash.hashShortArr;
import static org.apache.datasketches.hash.XxHash.hashString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class XxHash64Test {

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
      hash = wmem.xxHash64(offset, arrLen, seed);
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

      WritableMemory wmem = WritableMemory.writableWrap(in);
      for (int i = 0; i < j; i++) { wmem.putByte(i, (byte) (-128 + i)); }

      long hash =wmem.xxHash64(offset, bytes, seed);
      assertTrue(hash != 0);
    }
  }

  /*
   * This test is adapted from
   * <a href="https://github.com/OpenHFT/Zero-Allocation-Hashing/blob/master/
   * src/test/java/net/openhft/hashing/XxHashCollisionTest.java">
   * OpenHFT/Zero-Allocation-Hashing</a> to test hash compatibility with that implementation.
   * It is licensed under Apache License, version 2.0. See LICENSE.
   */
  @Test
  public void collisionTest() {
    WritableMemory wmem = WritableMemory.allocate(128);
    wmem.putLong(0, 1);
    wmem.putLong(16, 42);
    wmem.putLong(32, 2);
    long h1 = wmem.xxHash64(0, wmem.getCapacity(), 0);

    wmem.putLong(0, 1L + 0xBA79078168D4BAFL);
    wmem.putLong(32, 2L + 0x9C90005B80000000L);
    long h2 = wmem.xxHash64(0, wmem.getCapacity(), 0);
    assertEquals(h1, h2);

    wmem.putLong(0, 1L + (0xBA79078168D4BAFL * 2));
    wmem.putLong(32, 2L + (0x392000b700000000L)); //= (0x9C90005B80000000L * 2) fix overflow false pos

    long h3 = wmem.xxHash64(0, wmem.getCapacity(), 0);
    assertEquals(h2, h3);
  }
  
//  This test had to be disabled because the net.openhft.hashing.LongHashFunction is obsolete and depends on sun.misc.unsafe.  
//  /**
//   * This simple test compares the output of {@link Resource#xxHash64(long, long, long)} with the
//   * output of {@link net.openhft.hashing.LongHashFunction}, that itself is tested against the
//   * reference implementation in C.  This increases confidence that the xxHash function implemented
//   * in this package is in fact the same xxHash function implemented in C.
//   *
//   * @author Roman Leventov
//   * @author Lee Rhodes
//   */
//  @Test
//  public void testXxHash() {
//    Random random = ThreadLocalRandom.current();
//    for (int len = 0; len < 100; len++) {
//      byte[] bytes = new byte[len];
//      for (int i = 0; i < 10; i++) {
//        long zahXxHash = LongHashFunction.xx().hashBytes(bytes);
//        long memoryXxHash = Memory.wrap(bytes).xxHash64(0, len, 0);
//        assertEquals(memoryXxHash, zahXxHash);
//        random.nextBytes(bytes);
//      }
//    }
//  }

  private static final byte[] barr = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

  @Test
  public void testArrHashes() {
    WritableMemory wmem = WritableMemory.writableWrap(barr);
    long hash0 = wmem.xxHash64(8, 8, 0);
    long hash1 = hashByteArr(barr, 8, 8, 0);
    assertEquals(hash1, hash0);

    char[] carr = new char[8];
    wmem.getCharArray(0, carr, 0, 8);
    hash1 = hashCharArr(carr, 4, 4, 0);
    assertEquals(hash1, hash0);

    short[] sarr = new short[8];
    wmem.getShortArray(0, sarr, 0, 8);
    hash1 = hashShortArr(sarr, 4, 4, 0);
    assertEquals(hash1, hash0);

    int[] iarr = new int[4];
    wmem.getIntArray(0, iarr, 0, 4);
    hash1 = hashIntArr(iarr, 2, 2, 0);
    assertEquals(hash1, hash0);

    float[] farr = new float[4];
    wmem.getFloatArray(0, farr, 0, 4);
    hash1 = hashFloatArr(farr, 2, 2, 0);
    assertEquals(hash1, hash0);

    long[] larr = new long[2];
    wmem.getLongArray(0, larr, 0, 2);
    hash1 = hashLongArr(larr, 1, 1, 0);
    long in = wmem.getLong(8);
    long hash2 = hashLong(in, 00); //tests the single long hash
    assertEquals(hash1, hash0);
    assertEquals(hash2, hash0);

    double[] darr = new double[2];
    wmem.getDoubleArray(0, darr, 0, 2);
    hash1 = hashDoubleArr(darr, 1, 1, 0);
    assertEquals(hash1, hash0);
  }

  @Test
  public void testString() {
    String s = "Now is the time for all good men to come to the aid of their country.";
    char[] arr = s.toCharArray();
    long hash0 = hashString(s, 0, s.length(), 0);
    long hash1 = hashCharArr(arr, 0, arr.length, 0);
    assertEquals(hash1, hash0);
  }

}
