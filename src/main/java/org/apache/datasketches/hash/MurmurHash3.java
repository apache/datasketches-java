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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.Memory;

/**
 * The MurmurHash3 is a fast, non-cryptographic, 128-bit hash function that has
 * excellent avalanche and 2-way bit independence properties.
 *
 * <p>
 * Austin Appleby's C++
 * <a href="https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp">
 * MurmurHash3_x64_128(...), final revision 150</a>,
 * which is in the Public Domain, was the inspiration for this implementation in Java.
 * </p>
 *
 * <p>
 * This java implementation pays close attention to the C++ algorithms in order to
 * maintain bit-wise compatibility, but the design is quite different. This implementation has also
 * been extended to include processing of arrays of longs, char or ints, which was not part of the
 * original C++ implementation. This implementation produces the same exact output hash bits as
 * the above C++ method given the same input.</p>
 *
 * <p>In addition, with this implementation, the hash of byte[], char[], int[], or long[] will
 * produce the same hash result if, and only if, all the arrays have the same exact length in
 * bytes, and if the contents of the values in the arrays have the same byte endianness and
 * overall order. There is a unit test for this class that demonstrates this.</p>
 *
 * <p>
 * The structure of this implementation also reflects a separation of code that is dependent on the
 * input structure (in this case byte[], int[] or long[]) from code that is independent of the input
 * structure. This also makes the code more readable and suitable for future extensions.
 * </p>
 *
 * <p>Note that even though this hash function produces 128 bits, the entropy of the resulting hash cannot
 * be greater than the entropy of the input. For example, if the input is only a single long of 64 bits,
 * the entropy of the resulting 128 bit hash is no greater than 64 bits.
 *
 * @author Lee Rhodes
 */
public final class MurmurHash3 implements Serializable {
  private static final long serialVersionUID = 0L;

  private MurmurHash3() {}

  //--Hash of long---------------------------------------------------------
  /**
   * Hash the given long.
   *
   * @param key The input long.
   * @param seed A long valued seed.
   * @return a 128-bit hash of the input as a long array of size 2.
   */
  public static long[] hash(final long key, final long seed) {
    final HashState hashState = new HashState(seed, seed);
    return hashState.finalMix128(key, 0, Long.BYTES);
  }

  //--Hash of long[]-------------------------------------------------------
  /**
   * Hash the given long[] array.
   *
   * @param key The input long[] array. It must be non-null and non-empty.
   * @param seed A long valued seed.
   * @return a 128-bit hash of the input as a long array of size 2.
   */
  public static long[] hash(final long[] key, final long seed) {
    return hash(key, 0, key.length, seed);
  }

  /**
   * Hash a portion of the given long[] array.
   *
   * @param key The input long[] array. It must be non-null and non-empty.
   * @param offsetLongs the starting offset in longs.
   * @param lengthLongs the length in longs of the portion of the array to be hashed.
   * @param seed A long valued seed.
   * @return a 128-bit hash of the input as a long array of size 2
   */
  public static long[] hash(final long[] key, final int offsetLongs, final int lengthLongs, final long seed) {
    Objects.requireNonNull(key);
    final int arrLen = key.length;
    checkPositive(arrLen);
    Util.checkBounds(offsetLongs, lengthLongs, arrLen);
    final HashState hashState = new HashState(seed, seed);

    // Number of full 128-bit blocks of 2 longs (the body).
    // Possible exclusion of a remainder of 1 long.
    final int nblocks = lengthLongs >>> 1; //longs / 2

    // Process the 128-bit blocks (the body) into the hash
    for (int i = 0; i < nblocks; i++ ) {
      final long k1 = key[offsetLongs + (i << 1)]; //offsetLongs + 0, 2, 4, ...
      final long k2 = key[offsetLongs + (i << 1) + 1]; //offsetLongs + 1, 3, 5, ...
      hashState.blockMix128(k1, k2);
    }

    // Get the tail index wrt hashed portion, remainder length
    final int tail = nblocks << 1; // 2 longs / block
    final int rem = lengthLongs - tail; // remainder longs: 0,1

    // Get the tail
    final long k1 = rem == 0 ? 0 : key[offsetLongs + tail]; //k2 -> 0
    // Mix the tail into the hash and return
    return hashState.finalMix128(k1, 0, lengthLongs << 3); //convert to bytes
  }

  //--Hash of int[]--------------------------------------------------------
  /**
   * Hash the given int[] array.
   *
   * @param key The input int[] array. It must be non-null and non-empty.
   * @param seed A long valued seed.
   * @return a 128-bit hash of the input as a long array of size 2.
   */
  public static long[] hash(final int[] key, final long seed) {
    return hash(key, 0, key.length, seed);
  }

  /**
   * Hash a portion of the given int[] array.
   *
   * @param key The input int[] array. It must be non-null and non-empty.
   * @param offsetInts the starting offset in ints.
   * @param lengthInts the length in ints of the portion of the array to be hashed.
   * @param seed A long valued seed.
   * @return a 128-bit hash of the input as a long array of size 2.
   */
  public static long[] hash(final int[] key, final int offsetInts, final int lengthInts, final long seed) {
    Objects.requireNonNull(key);
    final int arrLen = key.length;
    checkPositive(arrLen);
    Util.checkBounds(offsetInts, lengthInts, arrLen);
    final HashState hashState = new HashState(seed, seed);

    // Number of full 128-bit blocks of 4 ints.
    // Possible exclusion of a remainder of up to 3 ints.
    final int nblocks = lengthInts >>> 2; //ints / 4

    // Process the 128-bit blocks (the body) into the hash
    for (int i = 0; i < nblocks; i++ ) { //4 ints per block
      final long k1 = getLong(key, offsetInts + (i << 2), 2); //offsetInts + 0, 4, 8, ...
      final long k2 = getLong(key, offsetInts + (i << 2) + 2, 2); //offsetInts + 2, 6, 10, ...
      hashState.blockMix128(k1, k2);
    }

    // Get the tail index wrt hashed portion, remainder length
    final int tail = nblocks << 2; // 4 ints per block
    final int rem = lengthInts - tail; // remainder ints: 0,1,2,3

    // Get the tail
    final long k1;
    final long k2;
    if (rem > 2) { //k1 -> whole; k2 -> partial
      k1 = getLong(key, offsetInts + tail, 2);
      k2 = getLong(key, offsetInts + tail + 2, rem - 2);
    }
    else { //k1 -> whole(2), partial(1) or 0; k2 == 0
      k1 = rem == 0 ? 0 : getLong(key, offsetInts + tail, rem);
      k2 = 0;
    }
    // Mix the tail into the hash and return
    return hashState.finalMix128(k1, k2, lengthInts << 2); //convert to bytes
  }

  //--Hash of char[]-------------------------------------------------------
  /**
   * Hash the given char[] array.
   *
   * @param key The input char[] array. It must be non-null and non-empty.
   * @param seed A long valued seed.
   * @return a 128-bit hash of the input as a long array of size 2
   */
  public static long[] hash(final char[] key, final long seed) {
    return hash(key, 0, key.length, seed);
  }

  /**
   * Hash a portion of the given char[] array.
   *
   * @param key The input char[] array. It must be non-null and non-empty.
   * @param offsetChars the starting offset in chars.
   * @param lengthChars the length in chars of the portion of the array to be hashed.
   * @param seed A long valued seed.
   * @return a 128-bit hash of the input as a long array of size 2
   */
  public static long[] hash(final char[] key, final int offsetChars, final int lengthChars, final long seed) {
    Objects.requireNonNull(key);
    final int arrLen = key.length;
    checkPositive(arrLen);
    Util.checkBounds(offsetChars, lengthChars, arrLen);
    final HashState hashState = new HashState(seed, seed);

    // Number of full 128-bit blocks of 8 chars.
    // Possible exclusion of a remainder of up to 7 chars.
    final int nblocks = lengthChars >>> 3; //chars / 8

    // Process the 128-bit blocks (the body) into the hash
    for (int i = 0; i < nblocks; i++ ) { //8 chars per block
      final long k1 = getLong(key, offsetChars + (i << 3), 4); //offsetChars + 0, 8, 16, ...
      final long k2 = getLong(key, offsetChars + (i << 3) + 4, 4); //offsetChars + 4, 12, 20, ...
      hashState.blockMix128(k1, k2);
    }

    // Get the tail index wrt hashed portion, remainder length
    final int tail = nblocks << 3; // 8 chars per block
    final int rem = lengthChars - tail; // remainder chars: 0,1,2,3,4,5,6,7

    // Get the tail
    final long k1;
    final long k2;
    if (rem > 4) { //k1 -> whole; k2 -> partial
      k1 = getLong(key, offsetChars + tail, 4);
      k2 = getLong(key, offsetChars + tail + 4, rem - 4);
    }
    else { //k1 -> whole, partial or 0; k2 == 0
      k1 = rem == 0 ? 0 : getLong(key, offsetChars + tail, rem);
      k2 = 0;
    }
    // Mix the tail into the hash and return
    return hashState.finalMix128(k1, k2, lengthChars << 1); //convert to bytes
  }

  //--Hash of byte[]-------------------------------------------------------
  /**
   * Hash the given byte[] array.
   *
   * @param key The input byte[] array. It must be non-null and non-empty.
   * @param seed A long valued seed.
   * @return a 128-bit hash of the input as a long array of size 2.
   */
  public static long[] hash(final byte[] key, final long seed) {
    return hash(key, 0, key.length, seed);
  }

  /**
   * Hash a portion of the given byte[] array.
   *
   * @param key The input byte[] array. It must be non-null and non-empty.
   * @param offsetBytes the starting offset in bytes.
   * @param lengthBytes the length in bytes of the portion of the array to be hashed.
   * @param seed A long valued seed.
   * @return a 128-bit hash of the input as a long array of size 2.
   */
  public static long[] hash(final byte[] key, final int offsetBytes, final int lengthBytes, final long seed) {
    Objects.requireNonNull(key);
    final int arrLen = key.length;
    checkPositive(arrLen);
    Util.checkBounds(offsetBytes, lengthBytes, arrLen);
    final HashState hashState = new HashState(seed, seed);

    // Number of full 128-bit blocks of 16 bytes.
    // Possible exclusion of a remainder of up to 15 bytes.
    final int nblocks = lengthBytes >>> 4; //bytes / 16

    // Process the 128-bit blocks (the body) into the hash
    for (int i = 0; i < nblocks; i++ ) { //16 bytes per block
      final long k1 = getLong(key, offsetBytes + (i << 4), 8); //offsetBytes + 0, 16, 32, ...
      final long k2 = getLong(key, offsetBytes + (i << 4) + 8, 8); //offsetBytes + 8, 24, 40, ...
      hashState.blockMix128(k1, k2);
    }

    // Get the tail index wrt hashed portion, remainder length
    final int tail = nblocks << 4; //16 bytes per block
    final int rem = lengthBytes - tail; // remainder bytes: 0,1,...,15

    // Get the tail
    final long k1;
    final long k2;
    if (rem > 8) { //k1 -> whole; k2 -> partial
      k1 = getLong(key, offsetBytes + tail, 8);
      k2 = getLong(key, offsetBytes + tail + 8, rem - 8);
    }
    else { //k1 -> whole, partial or 0; k2 == 0
      k1 = rem == 0 ? 0 : getLong(key, offsetBytes + tail, rem);
      k2 = 0;
    }
    // Mix the tail into the hash and return
    return hashState.finalMix128(k1, k2, lengthBytes);
  }

  //--Hash of ByteBuffer---------------------------------------------------
  /**
   * Hash the remaining bytes of the given ByteBuffer starting at position().
   *
   * @param buf The input ByteBuffer. It must be non-null and non-empty.
   * @param seed A long valued seed.
   * @return a 128-bit hash of the input as a long array of size 2.
   */
  public static long[] hash(final ByteBuffer buf, final long seed) {
    Objects.requireNonNull(buf);
    final int pos = buf.position();
    final int rem = buf.remaining();
    checkPositive(rem);
    final Memory mem = Memory.wrap(buf, ByteOrder.LITTLE_ENDIAN).region(pos, rem);
    return hash(mem, seed);
  }

  //--Hash of Memory-------------------------------------------------------
  /**
   * Hash the given Memory.
   *
   * <p>Note: if you want to hash only a portion of Memory, convert it to the
   * appropriate Region first with ByteOrder = Little Endian. If it is not
   * Little Endian a new region view will be created as Little Endian.
   * This does not change the underlying data.
   *
   * @param mem The input Memory. It must be non-null and non-empty.
   * @param seed A long valued seed.
   * @return a 128-bit hash of the input as a long array of size 2.
   */
  public static long[] hash(final Memory mem, final long seed) {
    Objects.requireNonNull(mem);
    final long lengthBytes = mem.getCapacity();
    checkPositive(lengthBytes);

    final Memory memLE = mem.getTypeByteOrder() == ByteOrder.LITTLE_ENDIAN
        ? mem : mem.region(0, lengthBytes, ByteOrder.LITTLE_ENDIAN);

    final HashState hashState = new HashState(seed, seed);

    // Number of full 128-bit blocks of 16 bytes.
    // Possible exclusion of a remainder of up to 15 bytes.
    final long nblocks = lengthBytes >>> 4; //bytes / 16

    // Process the 128-bit blocks (the body) into the hash
    for (long i = 0; i < nblocks; i++ ) { //16 bytes per block
      final long k1 = memLE.getLong(i << 4);       //0, 16, 32, ...
      final long k2 = memLE.getLong((i << 4) + 8); //8, 24, 40, ...
      hashState.blockMix128(k1, k2);
    }

    // Get the tail index wrt hashed portion, remainder length
    final long tail = nblocks << 4; //16 bytes per block
    final int rem = (int)(lengthBytes - tail); // remainder bytes: 0,1,...,15

    // Get the tail
    final long k1;
    final long k2;
    if (rem > 8) { //k1 -> whole; k2 -> partial
      k1 = memLE.getLong(tail);
      k2 = getLong(memLE, tail + 8, rem - 8);
    }
    else { //k1 -> whole, partial or 0; k2 == 0
      k1 = rem == 0 ? 0 : getLong(memLE, tail, rem);
      k2 = 0;
    }
    // Mix the tail into the hash and return
    return hashState.finalMix128(k1, k2, lengthBytes);
  }

  //--HashState class------------------------------------------------------
  /**
   * Common processing of the 128-bit hash state independent of input type.
   */
  private static final class HashState {
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private long h1;
    private long h2;

    HashState(final long h1, final long h2) {
      this.h1 = h1;
      this.h2 = h2;
    }

    /**
     * Block mix (128-bit block) of input key to internal hash state.
     *
     * @param k1 intermediate mix value
     * @param k2 intermediate mix value
     */
    void blockMix128(final long k1, final long k2) {
      h1 ^= mixK1(k1);
      h1 = Long.rotateLeft(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729;

      h2 ^= mixK2(k2);
      h2 = Long.rotateLeft(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5;
    }

    long[] finalMix128(final long k1, final long k2, final long inputLengthBytes) {
      h1 ^= mixK1(k1);
      h2 ^= mixK2(k2);
      h1 ^= inputLengthBytes;
      h2 ^= inputLengthBytes;
      h1 += h2;
      h2 += h1;
      h1 = finalMix64(h1);
      h2 = finalMix64(h2);
      h1 += h2;
      h2 += h1;
      return new long[] { h1, h2 };
    }

    /**
     * Final self mix of h*.
     *
     * @param h input to final mix
     * @return mix
     */
    private static long finalMix64(long h) {
      h ^= h >>> 33;
      h *= 0xff51afd7ed558ccdL;
      h ^= h >>> 33;
      h *= 0xc4ceb9fe1a85ec53L;
      h ^= h >>> 33;
      return h;
    }

    /**
     * Self mix of k1
     *
     * @param k1 input argument
     * @return mix
     */
    private static long mixK1(long k1) {
      k1 *= C1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= C2;
      return k1;
    }

    /**
     * Self mix of k2
     *
     * @param k2 input argument
     * @return mix
     */
    private static long mixK2(long k2) {
      k2 *= C2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= C1;
      return k2;
    }
  }

  //--Helper methods-------------------------------------------------------

  /**
   * Gets a long from the given int array starting at the given int array index and continuing for
   * remainder (rem) integers. The integers are extracted in little-endian order. There is no limit
   * checking.
   *
   * @param intArr The given input int array.
   * @param index Zero-based index from the start of the int array.
   * @param rem Remainder integers. An integer in the range [1,2].
   * @return long
   */
  private static long getLong(final int[] intArr, final int index, final int rem) {
    long out = 0L;
    for (int i = rem; i-- > 0;) { //i= 1,0
      final int v = intArr[index + i];
      out ^= (v & 0xFFFFFFFFL) << i * 32; //equivalent to |=
    }
    return out;
  }

  /**
   * Gets a long from the given char array starting at the given char array index and continuing for
   * remainder (rem) chars. The chars are extracted in little-endian order. There is no limit
   * checking.
   *
   * @param charArr The given input char array.
   * @param index Zero-based index from the start of the char array.
   * @param rem Remainder chars. An integer in the range [1,4].
   * @return a long
   */
  private static long getLong(final char[] charArr, final int index, final int rem) {
    long out = 0L;
    for (int i = rem; i-- > 0;) { //i= 3,2,1,0
      final char c = charArr[index + i];
      out ^= (c & 0xFFFFL) << i * 16; //equivalent to |=
    }
    return out;
  }

  /**
   * Gets a long from the given byte array starting at the given byte array index and continuing for
   * remainder (rem) bytes. The bytes are extracted in little-endian order. There is no limit
   * checking.
   *
   * @param bArr The given input byte array.
   * @param index Zero-based index from the start of the byte array.
   * @param rem Remainder bytes. An integer in the range [1,8].
   * @return a long
   */
  private static long getLong(final byte[] bArr, final int index, final int rem) {
    long out = 0L;
    for (int i = rem; i-- > 0;) { //i= 7,6,5,4,3,2,1,0
      final byte b = bArr[index + i];
      out ^= (b & 0xFFL) << i * 8; //equivalent to |=
    }
    return out;
  }

  /**
   * Gets a long from the given Memory starting at the given offsetBytes and continuing for
   * remainder (rem) bytes. The bytes are extracted in little-endian order. There is no limit
   * checking.
   *
   * @param mem The given input Memory.
   * @param offsetBytes Zero-based offset in bytes from the start of the Memory.
   * @param rem Remainder bytes. An integer in the range [1,8].
   * @return a long
   */
  private static long getLong(final Memory mem, final long offsetBytes, final int rem) {
    long out = 0L;
    if (rem == 8) {
      return mem.getLong(offsetBytes);
    }
    for (int i = rem; i-- > 0; ) { //i= 7,6,5,4,3,2,1,0
      final byte b = mem.getByte(offsetBytes + i);
      out ^= (b & 0xFFL) << (i << 3); //equivalent to |=
    }
    return out;
  }

  private static void checkPositive(final long size) {
    if (size <= 0) {
      throw new SketchesArgumentException("Array size must not be negative or zero: " + size);
    }
  }
}
