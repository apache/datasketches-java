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
import static org.apache.datasketches.common.Util.longToHexBytes;
import static org.apache.datasketches.hash.MurmurHash3.hash;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests the MurmurHash3 against specific, known hash results given known
 * inputs obtained from the public domain C++ version 150.
 *
 * @author Lee Rhodes
 */
public class MurmurHash3Test {

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
  public void checkByteBufRemainderGT8() { //byte buffer, remainder > 8
    final String keyStr = "The quick brown fox jumps over the lazy dog";
    final byte[] key = keyStr.getBytes(UTF_8);

    //Should be:
    final long h1 = 0xe34bbc7bbc071b6cL;
    final long h2 = 0x7a433ca9c49a9347L;

    checkHashByteBuf(key, h1, h2);
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
  public void checkByteBufChange1bit() { //byte buffer, change one bit
    final String keyStr = "The quick brown fox jumps over the lazy eog";
    final byte[] key = keyStr.getBytes(UTF_8);

    //Should be:
    final long h1 = 0x362108102c62d1c9L;
    final long h2 = 0x3285cd100292b305L;

    checkHashByteBuf(key, h1, h2);
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
  public void checkByteBufRemainderLt8() { //byte buffer, test a remainder < 8
    final String keyStr = "The quick brown fox jumps over the lazy dogdogdog";
    final byte[] key = keyStr.getBytes(UTF_8);

    //Should be;
    final long h1 = 0x9c8205300e612fc4L;
    final long h2 = 0xcbc0af6136aa3df9L;

    checkHashByteBuf(key, h1, h2);
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

  @Test
  public void checkByteBufReaminderEQ8() { //byte buffer, test a remainder = 8
    final String keyStr = "The quick brown fox jumps over the lazy1";
    final byte[] key = keyStr.getBytes(UTF_8);

    //Should be:
    final long h1 = 0xe3301a827e5cdfe3L;
    final long h2 = 0xbdbf05f8da0f0392L;

    checkHashByteBuf(key, h1, h2);
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
    final long[] result = MurmurHash3.hash(key, 0);

    //Should be:
    final long h1 = 0xe88abda785929c9eL;
    final long h2 = 0x96b98587cacc83d6L;
    Assert.assertEquals(result[0], h1);
    Assert.assertEquals(result[1], h2);
  }

  @Test
  public void checkByteBufAllOnesZeros() { //byte[], test a ones byte and a zeros byte
    final byte[] key = {
      0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f, 0x77, 0x6e,
      0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d, 0x70, 0x73, 0x20, 0x6f, 0x76, 0x65,
      0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79, 0x20, 0x64, 0x6f, 0x67,
      (byte) 0xff, 0x64, 0x6f, 0x67, 0x00
    };

    final long h1 = 0xe88abda785929c9eL;
    final long h2 = 0x96b98587cacc83d6L;

    checkHashByteBuf(key, h1, h2);
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

    println("ByteBuffer");
    final ByteBuffer bBuf = ByteBuffer.wrap(bArr);
    out = hash(bBuf, 0L);
    Assert.assertEquals(out, out1);
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


  //Helper methods
  private static long[] stringToLongs(final String in) {
    final byte[] bArr = in.getBytes(UTF_8);
    final int inLen = bArr.length;
    final int outLen = (inLen / 8) + ((inLen % 8) != 0 ? 1 : 0);
    final long[] out = new long[outLen];

    for (int i = 0; i < (outLen - 1); i++ ) {
      for (int j = 0; j < 8; j++ ) {
        out[i] |= (bArr[(i * 8) + j] & 0xFFL) << (j * 8);
      }
    }
    final int inTail = 8 * (outLen - 1);
    final int rem = inLen - inTail;
    for (int j = 0; j < rem; j++ ) {
      out[outLen - 1] |= (bArr[inTail + j] & 0xFFL) << (j * 8);
    }
    return out;
  }

  private static int[] stringToInts(final String in) {
    final byte[] bArr = in.getBytes(UTF_8);
    final int inLen = bArr.length;
    final int outLen = (inLen / 4) + ((inLen % 4) != 0 ? 1 : 0);
    final int[] out = new int[outLen];

    for (int i = 0; i < (outLen - 1); i++ ) {
      for (int j = 0; j < 4; j++ ) {
        out[i] |= (bArr[(i * 4) + j] & 0xFFL) << (j * 8);
      }
    }
    final int inTail = 4 * (outLen - 1);
    final int rem = inLen - inTail;
    for (int j = 0; j < rem; j++ ) {
      out[outLen - 1] |= (bArr[inTail + j] & 0xFFL) << (j * 8);
    }
    return out;
  }

  /**
   * Tests {@link MurmurHash3#hash(ByteBuffer, long)} on the provided key.
   *
   * @param key byte array to hash
   * @param h1 first half of expected hash
   * @param h2 second half of expected hash
   */
  private static void checkHashByteBuf(final byte[] key, final long h1, final long h2) {
    // Include dummy byte at start, end to make sure position, limit are respected.
    final ByteBuffer buf = ByteBuffer.allocate(key.length + 2).order(ByteOrder.LITTLE_ENDIAN);
    buf.position(1);
    buf.put(key);
    buf.limit(1 + key.length);
    buf.position(1);

    final long[] result1 = MurmurHash3.hash(buf, 0);

    // Position, limit, order should not be changed.
    Assert.assertEquals(1, buf.position());
    Assert.assertEquals(1 + key.length, buf.limit());
    Assert.assertEquals(ByteOrder.LITTLE_ENDIAN, buf.order());

    // Check the actual hashes.
    Assert.assertEquals(result1[0], h1);
    Assert.assertEquals(result1[1], h2);
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
