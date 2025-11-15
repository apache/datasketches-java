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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.hash.MurmurHash3FFM.hash;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests the MurmurHash3 against specific, known hash results given known
 * inputs obtained from the public domain C++ version 150.
 *
 * @author Lee Rhodes
 */
public class MurmurHash3FFMbTest {

  @Test
  public void checkByteArrRemainderGT8() { //byte[], remainder > 8
    final String keyStr = "The quick brown fox jumps over the lazy dog";
    final byte[] key = keyStr.getBytes(UTF_8);
    final long[] result = hash(key, 0);
    //Should be:
    final long h1 = 0xe34bbc7bbc071b6cL;
    final long h2 = 0x7a433ca9c49a9347L;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  @Test
  public void checkByteArrRemainderGT8withSegment() { //byte[], remainder > 8
    final String keyStr = "The quick brown fox jumps over the lazy dog";
    final byte[] key = keyStr.getBytes(UTF_8);
    final long[] out = new long[2];
    final MemorySegment seg = MemorySegment.ofArray(key);
    final long[] result = hash(seg, 0, seg.byteSize(), 0, out);
    //Should be:
    final long h1 = 0xe34bbc7bbc071b6cL;
    final long h2 = 0x7a433ca9c49a9347L;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  @Test
  public void checkByteArrChange1bit() { //byte[], change one bit
    final String keyStr = "The quick brown fox jumps over the lazy eog";
    final byte[] key = keyStr.getBytes(UTF_8);
    final long[] result = hash(key, 0);
    //Should be:
    final long h1 = 0x362108102c62d1c9L;
    final long h2 = 0x3285cd100292b305L;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  @Test
  public void checkByteArrRemainderLt8() { //byte[], test a remainder < 8
    final String keyStr = "The quick brown fox jumps over the lazy dogdogdog";
    final byte[] key = keyStr.getBytes(UTF_8);
    final long[] result = hash(key, 0);
    //Should be;
    final long h1 = 0x9c8205300e612fc4L;
    final long h2 = 0xcbc0af6136aa3df9L;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  @Test
  public void checkByteArrReaminderEQ8() { //byte[], test a remainder = 8
    final String keyStr = "The quick brown fox jumps over the lazy1";
    final byte[] key = keyStr.getBytes(UTF_8);
    final long[] result = hash(key, 0);
    //Should be:
    final long h1 = 0xe3301a827e5cdfe3L;
    final long h2 = 0xbdbf05f8da0f0392L;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  /**
   * This test should have the exact same output as Test4
   */
  @Test
  public void checkLongArrRemainderEQ8() { //long[], test a remainder = 8
    final String keyStr = "The quick brown fox jumps over the lazy1";
    final long[] key = stringToLongs(keyStr);
    final long[] result = hash(key, 0);
    //Should be:
    final long h1 = 0xe3301a827e5cdfe3L;
    final long h2 = 0xbdbf05f8da0f0392L;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  /**
   * This test should have the exact same output as Test4
   */
  @Test
  public void checkIntArrRemainderEQ8() { //int[], test a remainder = 8
    final String keyStr = "The quick brown fox jumps over the lazy1"; //40B
    final int[] key = stringToInts(keyStr);
    final long[] result = hash(key, 0);
    //Should be:
    final long h1 = 0xe3301a827e5cdfe3L;
    final long h2 = 0xbdbf05f8da0f0392L;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  @Test
  public void checkIntArrRemainderEQ0() { //int[], test a remainder = 0
    final String keyStr = "The quick brown fox jumps over t"; //32B
    final int[] key = stringToInts(keyStr);
    final long[] result = hash(key, 0);
    //Should be:
    final long h1 = 0xdf6af91bb29bdacfL;
    final long h2 = 0x91a341c58df1f3a6L;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  /**
   * Tests an odd remainder of int[].
   */
  @Test
  public void checkIntArrOddRemainder() { //int[], odd remainder
    final String keyStr = "The quick brown fox jumps over the lazy dog"; //43B
    final int[] key = stringToInts(keyStr);
    final long[] result = hash(key, 0);
    //Should be:
    final long h1 = 0x1eb232b0087543f5L;
    final long h2 = 0xfc4c1383c3ace40fL;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  /**
   * Tests an odd remainder of int[].
   */
  @Test
  public void checkCharArrOddRemainder() { //char[], odd remainder
    final String keyStr = "The quick brown fox jumps over the lazy dog.."; //45B
    final char[] key = keyStr.toCharArray();
    final long[] result = hash(key, 0);
    //Should be:
    final long h1 = 0xca77b498ea9ed953L;
    final long h2 = 0x8b8f8ec3a8f4657eL;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  /**
   * Tests an odd remainder of int[].
   */
  @Test
  public void checkCharArrRemainderEQ0() { //char[], remainder of 0
    final String keyStr = "The quick brown fox jumps over the lazy "; //40B
    final char[] key = keyStr.toCharArray();
    final long[] result = hash(key, 0);
    //Should be:
    final long h1 = 0x51b15e9d0887f9f1L;
    final long h2 = 0x8106d226786511ebL;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  @Test
  public void checkByteArrAllOnesZeros() { //byte[], test a ones byte and a zeros byte
    final byte[] key = {
      0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f, 0x77, 0x6e,
      0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d, 0x70, 0x73, 0x20, 0x6f, 0x76, 0x65,
      0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79, 0x20, 0x64, 0x6f, 0x67,
      (byte) 0xff, 0x64, 0x6f, 0x67, 0x00
    };
    final long[] result = MurmurHash3FFM.hash(key, 0);
    //Should be:
    final long h1 = 0xe88abda785929c9eL;
    final long h2 = 0x96b98587cacc83d6L;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  /**
   * This test demonstrates that the hash of byte[], char[], int[], or long[] will produce the
   * same hash result if, and only if, all the arrays have the same exact length in bytes, and if
   * the contents of the values in the arrays have the same byte endianness and overall order.
   */
  @Test
  public void checkCrossTypeHashConsistency() {
    long[] out;
    println("Bytes");
    final byte[] bArr = {1,2,3,4,5,6,7,8,   9,10,11,12,13,14,15,16,  17,18,19,20,21,22,23,24};
    final long[] out1 = hash(bArr, 0L);
    println(longToHexBytes(out1[0]));
    println(longToHexBytes(out1[1]));

    println("Chars");
    final char[] cArr = {0X0201, 0X0403, 0X0605, 0X0807,   0X0a09, 0X0c0b, 0X0e0d, 0X100f,
        0X1211, 0X1413, 0X1615, 0X1817};
    out = hash(cArr, 0L);
    Assert.assertEquals(out, out1);
    println(longToHexBytes(out[0]));
    println(longToHexBytes(out[1]));

    println("Ints");
    final int[] iArr = {0X04030201, 0X08070605,   0X0c0b0a09, 0X100f0e0d,   0X14131211,   0X18171615};
    out = hash(iArr, 0L);
    Assert.assertEquals(out, out1);
    println(longToHexBytes(out[0]));
    println(longToHexBytes(out[1]));

    println("Longs");
    final long[] lArr = {0X0807060504030201L, 0X100f0e0d0c0b0a09L, 0X1817161514131211L};
    out = hash(lArr, 0L);
    Assert.assertEquals(out, out1);
    println(longToHexBytes(out[0]));
    println(longToHexBytes(out[1]));
  }

  @Test
  public void checkEmptyOrNullExceptions() {
    try {
      final long[] arr = null;
      hash(arr, 1L);
      fail();
    }
    catch (final IllegalArgumentException e) { }
    try {
      final int[] arr = null; hash(arr, 1L); fail();
    } catch (final IllegalArgumentException e) { }
    try {
      final char[] arr = null; hash(arr, 1L); fail();
    } catch (final IllegalArgumentException e) { }
    try {
      final byte[] arr = null; hash(arr, 1L); fail();
    } catch (final IllegalArgumentException e) { }

    long[] out = new long[2];
    try {
      final String in = null; hash(in, 1L, out); fail();
    } catch (final IllegalArgumentException e) { }
    try {
      final MemorySegment seg = MemorySegment.ofArray(new byte[0]);
      hash(seg, 0L, 4L, 1L, out);
    } catch (final IllegalArgumentException e) { }
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment seg = arena.allocate(8);
      out = hash(seg, 0L, 4L, 1L, out);
    }
    assertTrue((out[0] != 0) && (out[1] != 0));
  }

  @Test
  public void checkHashTails() {
    long[] out = new long[2];
    final MemorySegment seg = MemorySegment.ofArray(new byte[32]);
    seg.fill((byte)85);

    for (int i = 16; i <= 32; i++) {
      out = hash(seg, 0, i, 1L, out);
    }
  }

  @Test
  public void checkSinglePrimitives() {
    long[] out = new long[2];
    out = hash(1L, 1L, out);
    out = hash(0.0, 1L, out);
    out = hash("123", 1L, out);
  }

  //Helper methods

  private static long[] stringToLongs(final String in) {
    final byte[] bArr = in.getBytes(UTF_8);
    final int inLen = bArr.length;
    final int outLen = (inLen / 8) + (((inLen % 8) != 0) ? 1 : 0);
    final long[] out = new long[outLen];

    for (int i = 0; i < (outLen - 1); i++ ) {
      for (int j = 0; j < 8; j++ ) {
        out[i] |= ((bArr[(i * 8) + j] & 0xFFL) << (j * 8));
      }
    }
    final int inTail = 8 * (outLen - 1);
    final int rem = inLen - inTail;
    for (int j = 0; j < rem; j++ ) {
      out[outLen - 1] |= ((bArr[inTail + j] & 0xFFL) << (j * 8));
    }
    return out;
  }

  private static int[] stringToInts(final String in) {
    final byte[] bArr = in.getBytes(UTF_8);
    final int inLen = bArr.length;
    final int outLen = (inLen / 4) + (((inLen % 4) != 0) ? 1 : 0);
    final int[] out = new int[outLen];

    for (int i = 0; i < (outLen - 1); i++ ) {
      for (int j = 0; j < 4; j++ ) {
        out[i] |= ((bArr[(i * 4) + j] & 0xFFL) << (j * 8));
      }
    }
    final int inTail = 4 * (outLen - 1);
    final int rem = inLen - inTail;
    for (int j = 0; j < rem; j++ ) {
      out[outLen - 1] |= ((bArr[inTail + j] & 0xFFL) << (j * 8));
    }
    return out;
  }

  /**
   * Returns a string of spaced hex bytes in Big-Endian order.
   * @param v the given long
   * @return string of spaced hex bytes in Big-Endian order.
   */
  private static String longToHexBytes(final long v) {
    final long mask = 0XFFL;
    final StringBuilder sb = new StringBuilder();
    for (int i = 8; i-- > 0; ) {
      final String s = Long.toHexString((v >>> (i * 8)) & mask);
      sb.append(zeroPad(s, 2)).append(" ");
    }
    return sb.toString();
  }

  /**
   * Prepend the given string with zeros. If the given string is equal or greater than the given
   * field length, it will be returned without modification.
   * @param s the given string
   * @param fieldLength desired total field length including the given string
   * @return the given string prepended with zeros.
   */
  private static final String zeroPad(final String s, final int fieldLength) {
    return characterPad(s, fieldLength, '0', false);
  }

  /**
   * Prepend or postpend the given string with the given character to fill the given field length.
   * If the given string is equal or greater than the given field length, it will be returned
   * without modification.
   * @param s the given string
   * @param fieldLength the desired field length
   * @param padChar the desired pad character
   * @param postpend if true append the padCharacters to the end of the string.
   * @return prepended or postpended given string with the given character to fill the given field
   * length.
   */
  private static final String characterPad(final String s, final int fieldLength, final char padChar,
      final boolean postpend) {
    final char[] chArr = s.toCharArray();
    final int sLen = chArr.length;
    if (sLen < fieldLength) {
      final char[] out = new char[fieldLength];
      final int blanks = fieldLength - sLen;

      if (postpend) {
        for (int i = 0; i < sLen; i++) {
          out[i] = chArr[i];
        }
        for (int i = sLen; i < fieldLength; i++) {
          out[i] = padChar;
        }
      } else { //prepend
        for (int i = 0; i < blanks; i++) {
          out[i] = padChar;
        }
        for (int i = blanks; i < fieldLength; i++) {
          out[i] = chArr[i - blanks];
        }
      }

      return String.valueOf(out);
    }
    return s;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
