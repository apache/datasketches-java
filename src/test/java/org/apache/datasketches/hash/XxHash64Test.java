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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_CHAR_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.hash.XxHash.hashByteArr;
import static org.apache.datasketches.hash.XxHash.hashCharArr;
import static org.apache.datasketches.hash.XxHash.hashDoubleArr;
import static org.apache.datasketches.hash.XxHash.hashFloatArr;
import static org.apache.datasketches.hash.XxHash.hashIntArr;
import static org.apache.datasketches.hash.XxHash.hashLong;
import static org.apache.datasketches.hash.XxHash.hashLongArr;
import static org.apache.datasketches.hash.XxHash.hashShortArr;
import static org.apache.datasketches.hash.XxHash.hashString;
import static org.apache.datasketches.hash.XxHash64.hash;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class XxHash64Test {

  @Test
  public void offsetChecks() {
    final long seed = 12345;
    final int blocks = 6;
    final int cap = blocks * 16;

    long hash;

    final MemorySegment wseg = MemorySegment.ofArray(new byte[cap]);
    for (int i = 0; i < cap; i++) { wseg.set(JAVA_BYTE, i, (byte)(-128 + i)); }

    for (int offset = 0; offset < 16; offset++) {
      final int arrLen = cap - offset;
      hash = hash(wseg, offset, arrLen, seed);
      assertTrue(hash != 0);
    }
  }

  @Test
  public void byteArrChecks() {
    final long seed = 0;
    final int offset = 0;
    final int bytes = 16;

    for (int j = 1; j < bytes; j++) {
      final byte[] in = new byte[bytes];

      final MemorySegment wseg = MemorySegment.ofArray(in);
      for (int i = 0; i < j; i++) { wseg.set(JAVA_BYTE, i, (byte) (-128 + i)); }

      final long hash = hash(wseg, offset, bytes, seed);
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
    final MemorySegment wseg = MemorySegment.ofArray(new byte[128]);
    wseg.set(JAVA_LONG_UNALIGNED, 0, 1);
    wseg.set(JAVA_LONG_UNALIGNED, 16, 42);
    wseg.set(JAVA_LONG_UNALIGNED, 32, 2);
    final long h1 = hash(wseg, 0, wseg.byteSize(), 0);

    wseg.set(JAVA_LONG_UNALIGNED, 0, 1L + 0xBA79078168D4BAFL);
    wseg.set(JAVA_LONG_UNALIGNED, 32, 2L + 0x9C90005B80000000L);
    final long h2 = hash(wseg, 0, wseg.byteSize(), 0);
    assertEquals(h1, h2);

    wseg.set(JAVA_LONG_UNALIGNED, 0, 1L + (0xBA79078168D4BAFL * 2));
    wseg.set(JAVA_LONG_UNALIGNED, 32, 2L + (0x392000b700000000L)); //= (0x9C90005B80000000L * 2) fix overflow false pos

    final long h3 = hash(wseg, 0, wseg.byteSize(), 0);
    assertEquals(h2, h3);
  }

  private static final byte[] barr = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

  @Test
  public void testArrHashes() {
    final MemorySegment wseg = MemorySegment.ofArray(barr);
    final long hash0 = hash(wseg, 8, 8, 0);
    long hash1 = hashByteArr(barr, 8, 8, 0);
    assertEquals(hash1, hash0);

    final char[] carr = new char[8];
    MemorySegment.copy(wseg, JAVA_CHAR_UNALIGNED, 0, carr, 0, 8);
    hash1 = hashCharArr(carr, 4, 4, 0);
    assertEquals(hash1, hash0);

    final short[] sarr = new short[8];
    MemorySegment.copy(wseg, JAVA_SHORT_UNALIGNED, 0, sarr, 0, 8);
    hash1 = hashShortArr(sarr, 4, 4, 0);
    assertEquals(hash1, hash0);

    final int[] iarr = new int[4];
    MemorySegment.copy(wseg, JAVA_INT_UNALIGNED, 0, iarr, 0, 4);
    hash1 = hashIntArr(iarr, 2, 2, 0);
    assertEquals(hash1, hash0);

    final float[] farr = new float[4];
    MemorySegment.copy(wseg, JAVA_FLOAT_UNALIGNED, 0, farr, 0, 4);
    hash1 = hashFloatArr(farr, 2, 2, 0);
    assertEquals(hash1, hash0);

    final long[] larr = new long[2];
    MemorySegment.copy(wseg, JAVA_LONG_UNALIGNED, 0, larr, 0, 2);
    hash1 = hashLongArr(larr, 1, 1, 0);
    final long in = wseg.get(JAVA_LONG_UNALIGNED, 8);
    final long hash2 = hashLong(in, 00); //tests the single long hash
    assertEquals(hash1, hash0);
    assertEquals(hash2, hash0);

    final double[] darr = new double[2];
    MemorySegment.copy(wseg, JAVA_DOUBLE_UNALIGNED, 0, darr, 0, 2);
    hash1 = hashDoubleArr(darr, 1, 1, 0);
    assertEquals(hash1, hash0);
  }

  @Test
  public void testString() {
    final String s = "Now is the time for all good men to come to the aid of their country.";
    final char[] arr = s.toCharArray();
    final long hash0 = hashString(s, 0, s.length(), 0);
    final long hash1 = hashCharArr(arr, 0, arr.length, 0);
    assertEquals(hash1, hash0);
  }

}
