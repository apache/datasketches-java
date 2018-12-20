/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hash;

import static com.yahoo.memory.UnsafeUtil.unsafe;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * <p>The MurmurHash3 is a fast, non-cryptographic, 128-bit hash function that has
 * excellent avalanche and 2-way bit independence properties.</p>
 *
 * <p>Austin Appleby's C++
 * <a href="https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp">
 * MurmurHash3_x64_128(...), final revision 150</a>,
 * which is in the Public Domain, was the inspiration for this implementation in Java.</p>
 *
 * <p>This implementation of the MurmurHash3 allows hashing of a block of Memory defined by an offset
 * and length. The calling API also allows the user to supply the small output array of two longs,
 * so that the entire hash function is static and free of object allocations.</p>
 *
 * <p>This implementation produces exactly the same hash result as the
 * {@link MurmurHash3#hash} function given compatible inputs.</p>
 *
 * @author Lee Rhodes
 */
public final class MurmurHash3v2 {
  private static final long C1 = 0x87c37b91114253d5L;
  private static final long C2 = 0x4cf5ad432745937fL;

  //Provided for backward compatibility

  /**
   * Returns a 128-bit hash of the input.
   * Provided for compatibility with older version of MurmurHash3,
   * but empty or null input now returns a hash.
   * @param in long array
   * @param seed A long valued seed.
   * @return the hash
   */
  public static long[] hash(final long[] in, final long seed) {
    if ((in == null) || (in.length == 0)) {
      return emptyOrNull(seed, new long[2]);
    }
    return hash(Memory.wrap(in), 0L, in.length << 3, seed, new long[2]);
  }

  /**
   * Returns a 128-bit hash of the input.
   * Provided for compatibility with older version of MurmurHash3,
   * but empty or null input now returns a hash.
   * @param in int array
   * @param seed A long valued seed.
   * @return the hash
   */
  public static long[] hash(final int[] in, final long seed) {
    if ((in == null) || (in.length == 0)) {
      return emptyOrNull(seed, new long[2]);
    }
    return hash(Memory.wrap(in), 0L, in.length << 2, seed, new long[2]);
  }

  /**
   * Returns a 128-bit hash of the input.
   * Provided for compatibility with older version of MurmurHash3,
   * but empty or null input now returns a hash.
   * @param in char array
   * @param seed A long valued seed.
   * @return the hash
   */
  public static long[] hash(final char[] in, final long seed) {
    if ((in == null) || (in.length == 0)) {
      return emptyOrNull(seed, new long[2]);
    }
    return hash(Memory.wrap(in), 0L, in.length << 1, seed, new long[2]);
  }

  /**
   * Returns a 128-bit hash of the input.
   * Provided for compatibility with older version of MurmurHash3,
   * but empty or null input now returns a hash.
   * @param in byte array
   * @param seed A long valued seed.
   * @return the hash
   */
  public static long[] hash(final byte[] in, final long seed) {
    if ((in == null) || (in.length == 0)) {
      return emptyOrNull(seed, new long[2]);
    }
    return hash(Memory.wrap(in), 0L, in.length, seed, new long[2]);
  }

  //Single primitive inputs

  /**
   * Returns a 128-bit hash of the input.
   * Note the entropy of the resulting hash cannot be more than 64 bits.
   * @param in a long
   * @param seed A long valued seed.
   * @param hashOut A long array of size 2
   * @return the hash
   */
  public static long[] hash(final long in, final long seed, final long[] hashOut) {
    final long h1 = seed ^ mixK1(in);
    final long h2 = seed;
    return finalMix128(h1, h2, 8, hashOut);
  }

  /**
   * Returns a 128-bit hash of the input.
   * Note the entropy of the resulting hash cannot be more than 64 bits.
   * @param in a double
   * @param seed A long valued seed.
   * @param hashOut A long array of size 2
   * @return the hash
   */
  public static long[] hash(final double in, final long seed, final long[] hashOut) {
    final double d = (in == 0.0) ? 0.0 : in;    // canonicalize -0.0, 0.0
    final long k1 = Double.doubleToLongBits(d); // canonicalize all NaN forms
    final long h1 = seed ^ mixK1(k1);
    final long h2 = seed;
    return finalMix128(h1, h2, 8, hashOut);
  }

  /**
   * Returns a 128-bit hash of the input.
   * @param in a String
   * @param seed A long valued seed.
   * @param hashOut A long array of size 2
   * @return the hash
   */
  public static long[] hash(final String in, final long seed, final long[] hashOut) {
    if ((in == null) || (in.length() == 0)) {
      return emptyOrNull(seed, hashOut);
    }
    final byte[] byteArr = in.getBytes(UTF_8);
    return hash(Memory.wrap(byteArr), 0L, byteArr.length, seed, hashOut);
  }

  //The main API call

  /**
   * Returns a 128-bit hash of the input as a long array of size 2.
   *
   * @param mem The input Memory. Must be non-null and non-empty.
   * @param offsetBytes the starting point within Memory.
   * @param lengthBytes the total number of bytes to be hashed.
   * @param seed A long valued seed.
   * @param hashOut the size 2 long array for the resulting 128-bit hash
   * @return the hash.
   */
  @SuppressWarnings("restriction")
  public static long[] hash(final Memory mem, final long offsetBytes, final long lengthBytes,
      final long seed, final long[] hashOut) {
    if ((mem == null) || (mem.getCapacity() == 0L)) {
      return emptyOrNull(seed, hashOut);
    }
    final Object uObj = ((WritableMemory) mem).getArray(); //may be null
    long cumOff = mem.getCumulativeOffset() + offsetBytes;

    long h1 = seed;
    long h2 = seed;
    long rem = lengthBytes;

    // Process the 128-bit blocks (the body) into the hash
    while (rem >= 16L) {
      final long k1 = unsafe.getLong(uObj, cumOff);     //0, 16, 32, ...
      final long k2 = unsafe.getLong(uObj, cumOff + 8); //8, 24, 40, ...
      cumOff += 16L;
      rem -= 16L;

      h1 ^= mixK1(k1);
      h1 = Long.rotateLeft(h1, 27);
      h1 += h2;
      h1 = (h1 * 5) + 0x52dce729L;

      h2 ^= mixK2(k2);
      h2 = Long.rotateLeft(h2, 31);
      h2 += h1;
      h2 = (h2 * 5) + 0x38495ab5L;
    }

    // Get the tail (if any): 1 to 15 bytes
    if (rem > 0L) {
      long k1 = 0;
      long k2 = 0;
      switch ((int) rem) {
        case 15: {
          k2 ^= (unsafe.getByte(uObj, cumOff + 14) & 0xFFL) << 48;
        }
        //$FALL-THROUGH$
        case 14: {
          k2 ^= (unsafe.getShort(uObj, cumOff + 12) & 0xFFFFL) << 32;
          k2 ^= (unsafe.getInt(uObj, cumOff + 8) & 0xFFFFFFFFL);
          k1 = unsafe.getLong(uObj, cumOff);
          break;
        }

        case 13: {
          k2 ^= (unsafe.getByte(uObj, cumOff + 12) & 0xFFL) << 32;
        }
        //$FALL-THROUGH$
        case 12: {
          k2 ^= (unsafe.getInt(uObj, cumOff + 8) & 0xFFFFFFFFL);
          k1 = unsafe.getLong(uObj, cumOff);
          break;
        }

        case 11: {
          k2 ^= (unsafe.getByte(uObj, cumOff + 10) & 0xFFL) << 16;
        }
        //$FALL-THROUGH$
        case 10: {
          k2 ^= (unsafe.getShort(uObj, cumOff +  8) & 0xFFFFL);
          k1 = unsafe.getLong(uObj, cumOff);
          break;
        }

        case  9: {
          k2 ^= (unsafe.getByte(uObj, cumOff +  8) & 0xFFL);
        }
        //$FALL-THROUGH$
        case  8: {
          k1 = unsafe.getLong(uObj, cumOff);
          break;
        }

        case  7: {
          k1 ^= (unsafe.getByte(uObj, cumOff +  6) & 0xFFL) << 48;
        }
        //$FALL-THROUGH$
        case  6: {
          k1 ^= (unsafe.getShort(uObj, cumOff +  4) & 0xFFFFL) << 32;
          k1 ^= (unsafe.getInt(uObj, cumOff) & 0xFFFFFFFFL);
          break;
        }

        case  5: {
          k1 ^= (unsafe.getByte(uObj, cumOff +  4) & 0xFFL) << 32;
        }
        //$FALL-THROUGH$
        case  4: {
          k1 ^= (unsafe.getInt(uObj, cumOff) & 0xFFFFFFFFL);
          break;
        }

        case  3: {
          k1 ^= (unsafe.getByte(uObj, cumOff +  2) & 0xFFL) << 16;
        }
        //$FALL-THROUGH$
        case  2: {
          k1 ^= (unsafe.getShort(uObj, cumOff) & 0xFFFFL);
          break;
        }

        case  1: {
          k1 ^= (unsafe.getByte(uObj, cumOff) & 0xFFL);
          break;
        }
        //default: break; //can't happen
      }

      h1 ^= mixK1(k1);
      h2 ^= mixK2(k2);
    }
    return finalMix128(h1, h2, lengthBytes, hashOut);
  }

  //--Helper methods----------------------------------------------------

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
   * Finalization: Add the length into the hash and mix
   * @param h1 intermediate hash
   * @param h2 intermediate hash
   * @param lengthBytes the length in bytes
   * @param hashOut the output array of 2 longs
   * @return hashOut
   */
  private static long[] finalMix128(long h1, long h2, final long lengthBytes, final long[] hashOut) {
    h1 ^= lengthBytes;
    h2 ^= lengthBytes;

    h1 += h2;
    h2 += h1;

    h1 = finalMix64(h1);
    h2 = finalMix64(h2);

    h1 += h2;
    h2 += h1;

    hashOut[0] = h1;
    hashOut[1] = h2;
    return hashOut;
  }

  private static long[] emptyOrNull(final long seed, final long[] hashOut) {
    return finalMix128(seed, seed, 0, hashOut);
  }
}
