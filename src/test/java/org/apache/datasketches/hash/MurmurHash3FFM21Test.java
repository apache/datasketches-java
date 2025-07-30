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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.util.Random;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class MurmurHash3FFM21Test {
  private final Random rand = new Random();
  private static final int trials = 1 << 20;

  @Test
  public void compareLongArrLong() { //long[]
    final int arrLen = 3;
    final int iPer = 8 / Long.BYTES;
    final long[] key = new long[arrLen];
    for (int i = 0; i < trials; i++) { //trials
      for (int j = 0; j < (arrLen / iPer); j++) { //longs
        final long r = rand.nextLong();
        key[j] = r;
      }
      final long[] res1 = hashV1(key, 0);
      final long[] res2 = hashV2(key, 0);
      assertEquals(res2, res1);
    }
  }

  @Test
  public void compareIntArr() { //int[]
    final int bytes = Integer.BYTES;
    final int arrLen = 6;
    final int[] key = new int[arrLen];
    final int iPer = 8 / bytes;
    final int nLongs = arrLen / iPer;
    final int shift = 64 / iPer;

    for (int i = 0; i < trials; i++) { //trials
      for (int j = 0; j < nLongs; j++) { //longs
        final long r = rand.nextLong();
        for (int k = 0; k < iPer; k++) { //ints
          final int shft = k * shift;
          key[k] = (int) (r >>> shft);
        }
      }
      final long[] res1 = hashV1(key, 0);
      final long[] res2 = hashV2(key, 0);
      assertEquals(res2, res1);
    }
  }

  @Test
  public void compareCharArr() { //char[]
    final int bytes = Character.BYTES;
    final int arrLen = 12;
    final char[] key = new char[arrLen];
    final int iPer = 8 / bytes;
    final int nLongs = arrLen / iPer;
    final int shift = 64 / iPer;

    for (int i = 0; i < trials; i++) { //trials
      for (int j = 0; j < nLongs; j++) { //longs
        final long r = rand.nextLong();
        for (int k = 0; k < iPer; k++) { //char
          final int shft = k * shift;
          key[k] = (char) (r >>> shft);
        }
      }
      final long[] res1 = hashV1(key, 0);
      final long[] res2 = hashV2(key, 0);
      assertEquals(res2, res1);
    }
  }

  @Test
  public void compareByteArr() { //byte[]
    final int bytes = Byte.BYTES;
    final int arrLen = 12;
    final byte[] key = new byte[arrLen];
    final int iPer = 8 / bytes;
    final int nLongs = arrLen / iPer;
    final int shift = 64 / iPer;

    for (int i = 0; i < trials; i++) { //trials
      for (int j = 0; j < nLongs; j++) { //longs
        final long r = rand.nextLong();
        for (int k = 0; k < iPer; k++) { //bytes
          final int shft = k * shift;
          key[k] = (byte) (r >>> shft);
        }
      }
      final long[] res1 = hashV1(key, 0);
      final long[] res2 = hashV2(key, 0);
      assertEquals(res2, res1);
    }
  }

  @Test
  public void compareLongVsLongArr() {
    final int arrLen = 1;
    final long[] key = new long[arrLen];
    final long[] out = new long[2];
    for (int i = 0; i < trials; i++) { //trials
      final long r = rand.nextLong();
      key[0] = r;
      final long[] res1 = hashV1(key, 0);
      final long[] res2 = hashV2(r, 0, out);
      assertEquals(res2, res1);
    }
  }

  private static final long[] hashV1(final long[] key, final long seed) {
    return MurmurHash3.hash(key, seed);
  }

  private static final long[] hashV1(final int[] key, final long seed) {
    return MurmurHash3.hash(key, seed);
  }

  private static final long[] hashV1(final char[] key, final long seed) {
    return MurmurHash3.hash(key, seed);
  }

  private static final long[] hashV1(final byte[] key, final long seed) {
    return MurmurHash3.hash(key, seed);
  }

  private static final long[] hashV2(final long[] key, final long seed) {
    return MurmurHash3FFM21.hash(key, seed);
  }

  private static final long[] hashV2(final int[] key2, final long seed) {
    return MurmurHash3FFM21.hash(key2, seed);
  }

  private static final long[] hashV2(final char[] key, final long seed) {
    return MurmurHash3FFM21.hash(key, seed);
  }

  private static final long[] hashV2(final byte[] key, final long seed) {
    return MurmurHash3FFM21.hash(key, seed);
  }

  //V2 single primitives

  private static final long[] hashV2(final long key, final long seed, final long[] out) {
    return MurmurHash3FFM21.hash(key, seed, out);
  }

//  private static final long[] hashV2(double key, long seed, long[] out) {
//    return MurmurHash3v4.hash(key, seed, out);
//  }

//  private static final long[] hashV2(String key, long seed, long[] out) {
//    return MurmurHash3v4.hash(key, seed, out);
//  }

  @Test
  public void offsetChecks() {
    final long seed = 12345;
    final int blocks = 6;
    final int cap = blocks * 16;

    long[] hash1 = new long[2];
    long[] hash2;

    final MemorySegment wseg = MemorySegment.ofArray(new byte[cap]);
    for (int i = 0; i < cap; i++) { wseg.set(JAVA_BYTE, i, (byte)(-128 + i)); }

    for (int offset = 0; offset < 16; offset++) {
      final int arrLen = cap - offset;
      hash1 = MurmurHash3FFM21.hash(wseg, offset, arrLen, seed, hash1);
      final byte[] byteArr2 = new byte[arrLen];
      MemorySegment.copy(wseg, JAVA_BYTE, offset, byteArr2, 0, arrLen);
      hash2 = MurmurHash3.hash(byteArr2, seed);
      assertEquals(hash1, hash2);
    }
  }

  @Test
  public void byteArrChecks() {
    final long seed = 0;
    final int offset = 0;
    final int bytes = 1024;

    long[] hash2 = new long[2];

    for (int j = 1; j < bytes; j++) {
      final byte[] in = new byte[bytes];

      final MemorySegment wseg = MemorySegment.ofArray(in);
      for (int i = 0; i < j; i++) { wseg.set(JAVA_BYTE, i, (byte) (-128 + i)); }

      final long[] hash1 = MurmurHash3.hash(in, seed);
      hash2 = MurmurHash3FFM21.hash(wseg, offset, bytes, seed, hash2);
      final long[] hash3 = MurmurHash3FFM21.hash(in, seed);

      assertEquals(hash1, hash2);
      assertEquals(hash1, hash3);
    }
  }

  @Test
  public void charArrChecks() {
    final long seed = 0;
    final int offset = 0;
    final int chars = 16;
    final int bytes = chars << 1;

    long[] hash2 = new long[2];

    for (int j = 1; j < chars; j++) {
      final char[] in = new char[chars];

      final MemorySegment wseg = MemorySegment.ofArray(in);
      for (int i = 0; i < j; i++) { wseg.set(JAVA_INT_UNALIGNED, i, i); }

      final long[] hash1 = MurmurHash3.hash(in, 0);
      hash2 = MurmurHash3FFM21.hash(wseg, offset, bytes, seed, hash2);
      final long[] hash3 = MurmurHash3FFM21.hash(in, seed);

      assertEquals(hash1, hash2);
      assertEquals(hash1, hash3);
    }
  }

  @Test
  public void intArrChecks() {
    final long seed = 0;
    final int offset = 0;
    final int ints = 16;
    final int bytes = ints << 2;

    long[] hash2 = new long[2];

    for (int j = 1; j < ints; j++) {
      final int[] in = new int[ints];

      final MemorySegment wseg = MemorySegment.ofArray(in);
      for (int i = 0; i < j; i++) { wseg.set(JAVA_INT_UNALIGNED, i, i); }

      final long[] hash1 = MurmurHash3.hash(in, 0);
      hash2 = MurmurHash3FFM21.hash(wseg, offset, bytes, seed, hash2);
      final long[] hash3 = MurmurHash3FFM21.hash(in, seed);

      assertEquals(hash1, hash2);
      assertEquals(hash1, hash3);
    }
  }

  @Test
  public void longArrChecks() {
    final long seed = 0;
    final int offset = 0;
    final int longs = 16;
    final int bytes = longs << 3;

    long[] hash2 = new long[2];

    for (int j = 1; j < longs; j++) {
      final long[] in = new long[longs];

      final MemorySegment wseg = MemorySegment.ofArray(in);
      for (int i = 0; i < j; i++) { wseg.set(JAVA_LONG_UNALIGNED, i, i); }

      final long[] hash1 = MurmurHash3.hash(in, 0);
      hash2 = MurmurHash3FFM21.hash(wseg, offset, bytes, seed, hash2);
      final long[] hash3 = MurmurHash3FFM21.hash(in, seed);

      assertEquals(hash1, hash2);
      assertEquals(hash1, hash3);
    }
  }

  @Test
  public void longCheck() {
    final long seed = 0;
    final int offset = 0;
    final int bytes = 8;

    long[] hash2 = new long[2];
    final long[] in = { 1 };
    final MemorySegment wseg = MemorySegment.ofArray(in);

    final long[] hash1 = MurmurHash3.hash(in, 0);
    hash2 = MurmurHash3FFM21.hash(wseg, offset, bytes, seed, hash2);
    final long[] hash3 = MurmurHash3FFM21.hash(in, seed);

    assertEquals(hash1, hash2);
    assertEquals(hash1, hash3);
  }

  @Test
  public void checkEmptiesNulls() {
    final long seed = 123;
    final long[] hashOut = new long[2];
    try {
      MurmurHash3FFM21.hash(MemorySegment.ofArray(new long[0]), 0, 0, seed, hashOut);  //seg empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      final String s = "";
      MurmurHash3FFM21.hash(s, seed, hashOut); //string empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      final String s = null;
      MurmurHash3FFM21.hash(s, seed, hashOut); //string null
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      final byte[] barr = {};
      MurmurHash3FFM21.hash(barr, seed); //byte[] empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      final byte[] barr = null;
      MurmurHash3FFM21.hash(barr, seed); //byte[] null
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      final char[] carr = {};
      MurmurHash3FFM21.hash(carr, seed); //char[] empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      final char[] carr = null;
      MurmurHash3FFM21.hash(carr, seed); //char[] null
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      final int[] iarr = {};
      MurmurHash3FFM21.hash(iarr, seed); //int[] empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      final int[] iarr = null;
      MurmurHash3FFM21.hash(iarr, seed); //int[] null
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      final long[] larr = {};
      MurmurHash3FFM21.hash(larr, seed); //long[] empty
      fail();
    } catch (final IllegalArgumentException e) { } //OK
    try {
      final long[] larr = null;
      MurmurHash3FFM21.hash(larr, seed); //long[] null
      fail();
    } catch (final IllegalArgumentException e) { } //OK
  }

  @Test
  public void checkStringLong() {
    final long seed = 123;
    final long[] hashOut = new long[2];
    final String s = "123";
    assertTrue(MurmurHash3FFM21.hash(s, seed, hashOut)[0] != 0);
    final long v = 123;
    assertTrue(MurmurHash3FFM21.hash(v, seed, hashOut)[0] != 0);
  }

  @Test
  public void doubleCheck() {
    long[] hash1 = checkDouble(-0.0);
    long[] hash2 = checkDouble(0.0);
    assertEquals(hash1, hash2);
    hash1 = checkDouble(Double.NaN);
    final long nan = (0x7FFL << 52) + 1L;
    hash2 = checkDouble(Double.longBitsToDouble(nan));
    assertEquals(hash1, hash2);
    checkDouble(1.0);
  }

  private static long[] checkDouble(final double dbl) {
    final long seed = 0;
    final int offset = 0;
    final int bytes = 8;

    long[] hash2 = new long[2];

    final double d = (dbl == 0.0) ? 0.0 : dbl;   // canonicalize -0.0, 0.0
    final long data = Double.doubleToLongBits(d);// canonicalize all NaN forms
    final long[] dataArr = { data };

    final MemorySegment wseg = MemorySegment.ofArray(dataArr);
    final long[] hash1 = MurmurHash3.hash(dataArr, 0);
    hash2 = MurmurHash3FFM21.hash(wseg, offset, bytes, seed, hash2);
    final long[] hash3 = MurmurHash3FFM21.hash(dbl, seed, hash2);

    assertEquals(hash1, hash2);
    assertEquals(hash1, hash3);
    return hash1;
  }

}
