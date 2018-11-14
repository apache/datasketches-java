/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hash;

import com.yahoo.memory.Memory;

/**
 * This implementation of the MurmurHash3 allows hashing of a block of Memory defined by an offset
 * and length. This implementation produces exactly the same hash result as the MurmurHash3 function.
 *
 * @author Lee Rhodes
 */
public final class MemoryMurmurHash3 {
  private static final long C1 = 0x87c37b91114253d5L;
  private static final long C2 = 0x4cf5ad432745937fL;

  /**
   * Returns a long array of size 2, which is a 128-bit hash of the input.
   *
   * @param mem The input Memory. Must be non-null and non-empty.
   * @param offsetBytes the starting point within Memory.
   * @param lengthBytes the total number of bytes to be hashed.
   * @param seed A long valued seed.
   * @param out the size 2 long array for the resulting 128-bit hash
   * @return the hash.
   */
  public static long[] hash(final Memory mem, final long offsetBytes, final long lengthBytes,
      final long seed, final long[] out) {
    final long[] h1h2 = out;
    h1h2[0] = seed;
    h1h2[1] = seed;

    // Number of full 128-bit blocks of 16 bytes.
    // Possible exclusion of a remainder of up to 15 bytes.
    final long nblocks = lengthBytes >>> 4; //bytes / 16

    // Process the 128-bit blocks (the body) into the hash
    for (long i = 0, j = offsetBytes; i < nblocks; i++, j += 16 ) { //16 bytes per block
      final long k1 = mem.getLong(j);     //0, 16, 32, ...
      final long k2 = mem.getLong(j + 8); //8, 24, 40, ...
      blockMix128(h1h2, k1, k2);
    }

    // Get the tail index, remainder length
    final long tailIdx = (nblocks << 4); //16 bytes per block
    final int rem = (int) (lengthBytes - tailIdx); // remainder bytes: 0,1,...,15

    // Get the tail
    final long k1;
    final long k2;
    if (rem > 8) { //k1 -> whole; k2 -> partial
      k1 = mem.getLong(offsetBytes + tailIdx);
      k2 = getLong(mem, offsetBytes + tailIdx + 8, rem - 8);
    }
    else { //k1 -> whole, partial or 0; k2 == 0
      k1 = (rem == 0) ? 0 : getLong(mem, offsetBytes + tailIdx, rem);
      k2 = 0;
    }
    // Mix the tail and length into the hash and return
    finalMix128(h1h2, k1, k2, lengthBytes);

    return h1h2;
  }

  /**
   * Returns a long array of size 2, which is a 128-bit hash of the input.
   *
   * <p>This alternative call allows performance comparison with the Memory version to assess if the
   * above Memory version has overhead compared with a simple long array. For short blocks less than
   * one KB, the Memory version is a few percent slower. For longer blocks, the JIT compiler
   * is very effective at removing this overhead so that there is virtually no difference in speed
   * compared to this long array version. Interestingly, for blocks less than one KB the
   * MurmurHash3 is still slightly faster than either the Memory based or long array based versions
   * here. For longer blocks, they all have a throughput of about 4GB per second (on my machine).
   *
   * @param longArr The input long array. Must be non-null and non-empty.
   * @param offsetLongs the starting point within the input array.
   * @param lengthLongs the total number of longsto be hashed.
   * @param seed A long valued seed.
   * @param out the size 2 long array for the resulting hash
   * @return the hash.
   */
  public static long[] hash(final long[] longArr, final int offsetLongs, final int lengthLongs,
      final long seed, final long[] out) {
    final long[] h1h2 = out;
    h1h2[0] = seed;
    h1h2[1] = seed;

    // Number of full 128-bit blocks of 2 longs.
    // Possible exclusion of a remainder of 1 long.
    final int nblocks = lengthLongs >>> 1; //lengthLongs / 2

    // Process the 128-bit blocks (the body) into the hash
    for (int i = 0, j = offsetLongs; i < nblocks; i++, j += 2 ) { //2 longs per block
      final long k1 = longArr[j];     //0, 2, 4, ...
      final long k2 = longArr[j + 1]; //1, 3, 5, ...
      blockMix128(h1h2, k1, k2);
    }

    // If lengthLongs is odd get the tail and length and mix into the hash
    if ((lengthLongs & 1) > 0) {
      finalMix128(h1h2, longArr[(offsetLongs + lengthLongs) - 1], 0L, lengthLongs << 3);
    } else {
      finalMix128(h1h2, 0L, 0L, lengthLongs << 3);
    }
    return h1h2;
  }

  /**
   * Gets a long from the given Memory starting at the given offsetBytes and continuing for
   * remBytes. The bytes are extracted in little-endian order. There is no limit
   * checking.
   *
   * @param bArr The given input byte array.
   * @param offsetBytes Zero-based index from the original offsetBytes.
   * @param remBytes Remainder bytes. An integer in the range [1,7].
   * @return partial long
   */
  private static long getLong(final Memory mem, final long offsetBytes, final int remBytes) {
    long out = 0L;
    for (int i = remBytes; i-- > 0;) { //i= 6,5,4,3,2,1,0
      final byte b = mem.getByte(offsetBytes + i);
      out |= (b & 0xFFL) << (i * 8); //equivalent to ^=
    }
    return out;
  }

  /**
   * Block mix (128-bit block) of input into the internal hash state.
   * @param h1h2 current hash state
   * @param k1 intermediate mix value
   * @param k2 intermediate mix value
   */
  private static void blockMix128(final long[] h1h2, final long k1, final long k2) {
    long h1 = h1h2[0];
    long h2 = h1h2[1];
    h1 ^= mixK1(k1);
    h1 = Long.rotateLeft(h1, 27);
    h1 += h2;
    h1 = (h1 * 5) + 0x52dce729;

    h2 ^= mixK2(k2);
    h2 = Long.rotateLeft(h2, 31);
    h2 += h1;
    h2 = (h2 * 5) + 0x38495ab5;
    h1h2[0] = h1;
    h1h2[1] = h2;
  }

  /**
   * Final mix of the remainder, if any, and the input length into the hash state.
   * @param h1h2 the input and output hash state
   * @param k1 up to 8 bytes of the remainder, if any
   * @param k2 up 7 bytes of the remainder, if any
   * @param inputLengthBytes the input length in bytes
   */
  private static void finalMix128(final long[] h1h2, final long k1, final long k2,
      final long inputLengthBytes) {
    long h1 = h1h2[0];
    long h2 = h1h2[1];
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
    h1h2[0] = h1;
    h1h2[1] = h2;
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
