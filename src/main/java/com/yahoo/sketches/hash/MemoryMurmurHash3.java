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

    long h1 = seed;
    long h2 = seed;

    // Number of full 128-bit blocks of 16 bytes.
    // Possible exclusion of a remainder of up to 15 bytes.
    final long nblocks = lengthBytes >>> 4; //bytes / 16

    // Process the 128-bit blocks (the body) into the hash
    for (long i = 0, j = offsetBytes; i < nblocks; i++, j += 16 ) { //16 bytes per block
      long k1 = mem.getLong(j);     //0, 16, 32, ...
      long k2 = mem.getLong(j + 8); //8, 24, 40, ...

      k1 *= C1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= C2;
      h1 ^= k1;

      h1 = Long.rotateLeft(h1, 27);
      h1 += h2;
      h1 = (h1 * 5) + 0x52dce729;

      k2 *= C2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= C1;
      h2 ^= k2;

      h2 = Long.rotateLeft(h2, 31);
      h2 += h1;
      h2 = (h2 * 5) + 0x38495ab5;
    }

    // Get the tail start, remainder length
    final long tailStart = (nblocks << 4); //16 bytes per block
    final int rem = (int) (lengthBytes - tailStart); // remainder bytes: 0,1,...,15

    // Get the tail
    long k1 = 0;
    long k2 = 0;
    final long offTail = offsetBytes + tailStart;

    switch (rem) {
      case 15: k2 ^= (mem.getByte(offTail + 14) & 0xFFL) << 48;
      //$FALL-THROUGH$
      case 14: k2 ^= (mem.getByte(offTail + 13) & 0xFFL) << 40;
      //$FALL-THROUGH$
      case 13: k2 ^= (mem.getByte(offTail + 12) & 0xFFL) << 32;
      //$FALL-THROUGH$
      case 12: k2 ^= (mem.getByte(offTail + 11) & 0xFFL) << 24;
      //$FALL-THROUGH$
      case 11: k2 ^= (mem.getByte(offTail + 10) & 0xFFL) << 16;
      //$FALL-THROUGH$
      case 10: k2 ^= (mem.getByte(offTail +  9) & 0xFFL) <<  8;
      //$FALL-THROUGH$
      case  9: k2 ^= (mem.getByte(offTail +  8) & 0xFFL);
               k2 *= C2;
               k2  = Long.rotateLeft(k2, 33);
               k2 *= C1;
               h2 ^= k2;
               //$FALL-THROUGH$
      case  8: k1 ^= (mem.getByte(offTail +  7) & 0xFFL) << 56;
      //$FALL-THROUGH$
      case  7: k1 ^= (mem.getByte(offTail +  6) & 0xFFL) << 48;
      //$FALL-THROUGH$
      case  6: k1 ^= (mem.getByte(offTail +  5) & 0xFFL) << 40;
      //$FALL-THROUGH$
      case  5: k1 ^= (mem.getByte(offTail +  4) & 0xFFL) << 32;
      //$FALL-THROUGH$
      case  4: k1 ^= (mem.getByte(offTail +  3) & 0xFFL) << 24;
      //$FALL-THROUGH$
      case  3: k1 ^= (mem.getByte(offTail +  2) & 0xFFL) << 16;
      //$FALL-THROUGH$
      case  2: k1 ^= (mem.getByte(offTail +  1) & 0xFFL) <<  8;
      //$FALL-THROUGH$
      case  1: k1 ^= (mem.getByte(offTail +  0) & 0xFFL);
               k1 *= C1;
               k1  = Long.rotateLeft(k1,31);
               k1 *= C2;
               h1 ^= k1;
               //$FALL-THROUGH$
      case  0:
    }

    //finalization: Add the length into the hash and mix

    h1 ^= lengthBytes;
    h2 ^= lengthBytes;

    h1 += h2;
    h2 += h1;

    h1 = finalMix64(h1);
    h2 = finalMix64(h2);

    h1 += h2;
    h2 += h1;

    out[0] = h1;
    out[1] = h2;
    return out;
  }

  /**
   * Returns a long array of size 2, which is a 128-bit hash of the input. Primarily for testing.
   *
   * <p>This alternative call allows performance comparison with the Memory version to assess if the
   * above Memory version has overhead compared with a simple long array. For short blocks less than
   * 2 KB, the Memory version is a few percent slower. For longer blocks, the JIT compiler
   * is very effective at removing this overhead so that there is virtually no difference in speed
   * compared to this long array version. Interestingly, for blocks less than 2 KB the original
   * MurmurHash3 in the library is still slightly faster than either the Memory based or long array
   * based versions here. For longer blocks, they all have a throughput of about 4GB per second
   * (on my machine).
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
    long h1 = seed;
    long h2 = seed;

    // Number of full 128-bit blocks of 2 longs.
    // Possible exclusion of a remainder of 1 long.
    final int nblocks = lengthLongs >>> 1; //lengthLongs / 2

    // Process the 128-bit blocks (the body) into the hash
    for (int i = 0, j = offsetLongs; i < nblocks; i++, j += 2 ) { //2 longs per block
      long k1 = longArr[j];     //0, 2, 4, ...
      long k2 = longArr[j + 1]; //1, 3, 5, ...

      k1 *= C1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= C2;
      h1 ^= k1;

      h1 = Long.rotateLeft(h1, 27);
      h1 += h2;
      h1 = (h1 * 5) + 0x52dce729;

      k2 *= C2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= C1;
      h2 ^= k2;

      h2 = Long.rotateLeft(h2, 31);
      h2 += h1;
      h2 = (h2 * 5) + 0x38495ab5;
    }

    // If lengthLongs is odd get the tail and mix into the hash
    if ((lengthLongs & 1) > 0) {
      long k1 = longArr[(offsetLongs + lengthLongs) - 1];
      k1 *= C1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= C2;
      h1 ^= k1;
    }

    //finalization: Add the length into the hash and mix
    h1 ^= lengthLongs << 3;
    h2 ^= lengthLongs << 3;

    h1 += h2;
    h2 += h1;

    h1 = finalMix64(h1);
    h2 = finalMix64(h2);

    h1 += h2;
    h2 += h1;

    out[0] = h1;
    out[1] = h2;
    return out;
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

}
