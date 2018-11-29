/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hash;

import java.io.Serializable;


/**
 * <p>
 * The MurmurHash3 is a fast, non-cryptographic, 128-bit hash function that has
 * excellent avalanche and 2-way bit independence properties.
 * </p>
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
 * @author Lee Rhodes
 */
public final class MurmurHash3 implements Serializable {
  private static final long serialVersionUID = 0L;

  private MurmurHash3() {}

  //--Hash of long[]----------------------------------------------------
  /**
   * Returns a long array of size 2, which is a 128-bit hash of the input.
   *
   * @param key The input long[] array. Must be non-null and non-empty.
   * @param seed A long valued seed.
   * @return the hash.
   */
  public static long[] hash(final long[] key, final long seed) {
    final HashState hashState = new HashState(seed, seed);
    final int longs = key.length; //in longs

    // Number of full 128-bit blocks of 2 longs (the body).
    // Possible exclusion of a remainder of 1 long.
    final int nblocks = longs >>> 1; //longs / 2

    // Process the 128-bit blocks (the body) into the hash
    for (int i = 0; i < nblocks; i++ ) {
      final long k1 = key[i << 1]; //0, 2, 4, ...
      final long k2 = key[(i << 1) + 1]; //1, 3, 5, ...
      hashState.blockMix128(k1, k2);
    }

    // Get the tail index, remainder length
    final int tail = nblocks << 1; // 2 longs / block
    final int rem = longs - tail; // remainder longs: 0,1

    // Get the tail
    final long k1 = (rem == 0) ? 0 : key[tail]; //k2 -> 0
    // Mix the tail into the hash and return
    return hashState.finalMix128(k1, 0, longs << 3); //convert to bytes
  }

  //--Hash of int[]----------------------------------------------------
  /**
   * Returns a long array of size 2, which is a 128-bit hash of the input.
   *
   * @param key The input int[] array. Must be non-null and non-empty.
   * @param seed A long valued seed.
   * @return the hash.
   */
  public static long[] hash(final int[] key, final long seed) {
    final HashState hashState = new HashState(seed, seed);
    final int ints = key.length; //in ints

    // Number of full 128-bit blocks of 4 ints.
    // Possible exclusion of a remainder of up to 3 ints.
    final int nblocks = ints >>> 2; //ints / 4

    // Process the 128-bit blocks (the body) into the hash
    for (int i = 0; i < nblocks; i++ ) { //4 ints per block
      final long k1 = getLong(key, i << 2, 2); //0, 4, 8, ...
      final long k2 = getLong(key, (i << 2) + 2, 2); //2, 6, 10, ...
      hashState.blockMix128(k1, k2);
    }

    // Get the tail index, remainder length
    final int tail = nblocks << 2; // 4 ints per block
    final int rem = ints - tail; // remainder ints: 0,1,2,3

    // Get the tail
    final long k1;
    final long k2;
    if (rem > 2) { //k1 -> whole; k2 -> partial
      k1 = getLong(key, tail, 2);
      k2 = getLong(key, tail + 2, rem - 2);
    }
    else { //k1 -> whole, partial or 0; k2 == 0
      k1 = (rem == 0) ? 0 : getLong(key, tail, rem);
      k2 = 0;
    }
    // Mix the tail into the hash and return
    return hashState.finalMix128(k1, k2, ints << 2); //convert to bytes
  }

  //--Hash of char[]----------------------------------------------------
  /**
   * Returns a long array of size 2, which is a 128-bit hash of the input.
   *
   * @param key The input char[] array. Must be non-null and non-empty.
   * @param seed A long valued seed.
   * @return the hash.
   */
  public static long[] hash(final char[] key, final long seed) {
    final HashState hashState = new HashState(seed, seed);
    final int chars = key.length; //in chars

    // Number of full 128-bit blocks of 8 chars.
    // Possible exclusion of a remainder of up to 7 chars.
    final int nblocks = chars >>> 3; //chars / 8

    // Process the 128-bit blocks (the body) into the hash
    for (int i = 0; i < nblocks; i++ ) { //8 chars per block
      final long k1 = getLong(key, i << 3, 4); //0, 8, 16, ...
      final long k2 = getLong(key, (i << 3) + 4, 4); //4, 12, 20, ...
      hashState.blockMix128(k1, k2);
    }

    // Get the tail index, remainder length
    final int tail = nblocks << 3; // 8 chars per block
    final int rem = chars - tail; // remainder chars: 0,1,2,3,4,5,6,7

    // Get the tail
    final long k1;
    final long k2;
    if (rem > 4) { //k1 -> whole; k2 -> partial
      k1 = getLong(key, tail, 4);
      k2 = getLong(key, tail + 4, rem - 4);
    }
    else { //k1 -> whole, partial or 0; k2 == 0
      k1 = (rem == 0) ? 0 : getLong(key, tail, rem);
      k2 = 0;
    }
    // Mix the tail into the hash and return
    return hashState.finalMix128(k1, k2, chars << 1); //convert to bytes
  }

  //--Hash of byte[]----------------------------------------------------
  /**
   * Returns a long array of size 2, which is a 128-bit hash of the input.
   *
   * @param key The input byte[] array. Must be non-null and non-empty.
   * @param seed A long valued seed.
   * @return the hash.
   */
  public static long[] hash(final byte[] key, final long seed) {
    final HashState hashState = new HashState(seed, seed);
    final int bytes = key.length; //in bytes

    // Number of full 128-bit blocks of 16 bytes.
    // Possible exclusion of a remainder of up to 15 bytes.
    final int nblocks = bytes >>> 4; //bytes / 16

    // Process the 128-bit blocks (the body) into the hash
    for (int i = 0; i < nblocks; i++ ) { //16 bytes per block
      final long k1 = getLong(key, i << 4, 8); //0, 16, 32, ...
      final long k2 = getLong(key, (i << 4) + 8, 8); //8, 24, 40, ...
      hashState.blockMix128(k1, k2);
    }

    // Get the tail index, remainder length
    final int tail = nblocks << 4; //16 bytes per block
    final int rem = bytes - tail; // remainder bytes: 0,1,...,15

    // Get the tail
    final long k1;
    final long k2;
    if (rem > 8) { //k1 -> whole; k2 -> partial
      k1 = getLong(key, tail, 8);
      k2 = getLong(key, tail + 8, rem - 8);
    }
    else { //k1 -> whole, partial or 0; k2 == 0
      k1 = (rem == 0) ? 0 : getLong(key, tail, rem);
      k2 = 0;
    }
    // Mix the tail into the hash and return
    return hashState.finalMix128(k1, k2, bytes);
  }

  //--HashState class---------------------------------------------------
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
      h1 = (h1 * 5) + 0x52dce729;

      h2 ^= mixK2(k2);
      h2 = Long.rotateLeft(h2, 31);
      h2 += h1;
      h2 = (h2 * 5) + 0x38495ab5;
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

  //--Helper methods----------------------------------------------------
  /**
   * Gets a long from the given byte array starting at the given byte array index and continuing for
   * remainder (rem) bytes. The bytes are extracted in little-endian order. There is no limit
   * checking.
   *
   * @param bArr The given input byte array.
   * @param index Zero-based index from the start of the byte array.
   * @param rem Remainder bytes. An integer in the range [1,8].
   * @return long
   */
  private static long getLong(final byte[] bArr, final int index, final int rem) {
    long out = 0L;
    for (int i = rem; i-- > 0;) { //i= 7,6,5,4,3,2,1,0
      final byte b = bArr[index + i];
      out ^= (b & 0xFFL) << (i * 8); //equivalent to |=
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
   * @return long
   */
  private static long getLong(final char[] charArr, final int index, final int rem) {
    long out = 0L;
    for (int i = rem; i-- > 0;) { //i= 3,2,1,0
      final char c = charArr[index + i];
      out ^= (c & 0xFFFFL) << (i * 16); //equivalent to |=
    }
    return out;
  }

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
      out ^= (v & 0xFFFFFFFFL) << (i * 32); //equivalent to |=
    }
    return out;
  }

}
