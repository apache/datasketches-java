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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

import org.apache.datasketches.memory.Memory;

/**
 * The MurmurHash3 is a fast, non-cryptographic, 128-bit hash function that has
 * excellent avalanche and 2-way bit independence properties.
 *
 * <p>Austin Appleby's C++
 * <a href="https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp">
 * MurmurHash3_x64_128(...), final revision 150</a>,
 * which is in the Public Domain, was the inspiration for this implementation in Java.</p>
 *
 * <p>This implementation of the MurmurHash3 allows hashing of a block of on-heap Memory defined by an offset
 * and length. The calling API also allows the user to supply the small output array of two longs,
 * so that the entire hash function is static and free of object allocations.</p>
 *
 * <p>This implementation produces exactly the same hash result as the
 * MurmurHash3 function in datasketches-java given compatible inputs.</p>
 *
 * <p>This FFM21 version of the implementation leverages the java.lang.foreign package (FFM) of JDK-21 in place of
 * the Unsafe class.
 *
 * @author Lee Rhodes
 */
public final class MurmurHash3FFM21 {
  private static final long C1 = 0x87c37b91114253d5L;
  private static final long C2 = 0x4cf5ad432745937fL;

  /**
   * Returns a 128-bit hash of the input.
   * Provided for compatibility with older version of MurmurHash3,
   * but empty or null input now throws IllegalArgumentException.
   * @param in long array
   * @param seed A long valued seed.
   * @return the hash
   * @throws IllegalArgumentException if input is empty or null
   */
  public static long[] hash(final long[] in, final long seed) {
    if ((in == null) || (in.length == 0)) {
      throw new IllegalArgumentException("Input in is empty or null.");
    }
    return hash(MemorySegment.ofArray(in), 0L, in.length << 3, seed, new long[2]);
  }

  /**
   * Returns a 128-bit hash of the input.
   * Provided for compatibility with older version of MurmurHash3,
   * but empty or null input now throws IllegalArgumentException.
   * @param in int array
   * @param seed A long valued seed.
   * @return the hash
   * @throws IllegalArgumentException if input is empty or null
   */
  public static long[] hash(final int[] in, final long seed) {
    if ((in == null) || (in.length == 0)) {
      throw new IllegalArgumentException("Input in is empty or null.");
    }
    return hash(MemorySegment.ofArray(in), 0L, in.length << 2, seed, new long[2]);
  }

  /**
   * Returns a 128-bit hash of the input.
   * Provided for compatibility with older version of MurmurHash3,
   * but empty or null input now throws IllegalArgumentException.
   * @param in char array
   * @param seed A long valued seed.
   * @return the hash
   * @throws IllegalArgumentException if input is empty or null
   */
  public static long[] hash(final char[] in, final long seed) {
    if ((in == null) || (in.length == 0)) {
      throw new IllegalArgumentException("Input in is empty or null.");
    }
    return hash(MemorySegment.ofArray(in), 0L, in.length << 1, seed, new long[2]);
  }

  /**
   * Returns a 128-bit hash of the input.
   * Provided for compatibility with older version of MurmurHash3,
   * but empty or null input now throws IllegalArgumentException.
   * @param in byte array
   * @param seed A long valued seed.
   * @return the hash
   * @throws IllegalArgumentException if input is empty or null
   */
  public static long[] hash(final byte[] in, final long seed) {
    if ((in == null) || (in.length == 0)) {
      throw new IllegalArgumentException("Input in is empty or null.");
    }
    return hash(MemorySegment.ofArray(in), 0L, in.length, seed, new long[2]);
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
   * An empty or null input throws IllegalArgumentException.
   * @param in a String
   * @param seed A long valued seed.
   * @param hashOut A long array of size 2
   * @return the hash
   * @throws IllegalArgumentException if input is empty or null
   */
  public static long[] hash(final String in, final long seed, final long[] hashOut) {
    if ((in == null) || (in.length() == 0)) {
      throw new IllegalArgumentException("Input in is empty or null.");
    }
    final byte[] byteArr = in.getBytes(UTF_8);
    return hash(MemorySegment.ofArray(byteArr), 0L, byteArr.length, seed, hashOut);
  }

  //The main API calls

  /**
   * Returns a 128-bit hash of the input as a long array of size 2.
   *
   * @param mem The input Memory. Must be non-null and non-empty,
   * otherwise throws IllegalArgumentException.
   * @param offsetBytes the starting point within Memory.
   * @param lengthBytes the total number of bytes to be hashed.
   * @param seed A long valued seed.
   * @param hashOut the size 2 long array for the resulting 128-bit hash
   * @return the hash.
   */
  public static long[] hash(final Memory mem, final long offsetBytes, final long lengthBytes,
      final long seed, final long[] hashOut) {
    Objects.requireNonNull(mem, "Input Memory is null");
    final MemorySegment seg = mem.getMemorySegment();
    return hash(seg, offsetBytes, lengthBytes, seed, hashOut);
  }

  /**
   * Returns a 128-bit hash of the input as a long array of size 2.
   *
   * @param seg The input MemorySegment. Must be non-null and non-empty,
   * otherwise throws IllegalArgumentException.
   * @param offsetBytes the starting point within Memory.
   * @param lengthBytes the total number of bytes to be hashed.
   * @param seed A long valued seed.
   * @param hashOut the size 2 long array for the resulting 128-bit hash
   * @return the hash.
   * @throws IllegalArgumentException if input MemorySegment is empty
   */
  public static long[] hash(final MemorySegment seg, final long offsetBytes, final long lengthBytes,
      final long seed, final long[] hashOut) {
    Objects.requireNonNull(seg, "Input MemorySegment is null");
    if (seg.byteSize() == 0L) { throw new IllegalArgumentException("Input MemorySegment is empty."); }

    long cumOff = offsetBytes;

    long h1 = seed;
    long h2 = seed;
    long rem = lengthBytes;

    // Process the 128-bit blocks (the body) into the hash
    while (rem >= 16L) {
      final long k1 = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, cumOff);     //0, 16, 32, ...
      final long k2 = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, cumOff + 8); //8, 24, 40, ...
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
          k2 ^= (seg.get(ValueLayout.JAVA_BYTE, cumOff + 14) & 0xFFL) << 48;
        }
        //$FALL-THROUGH$
        case 14: {
          k2 ^= (seg.get(ValueLayout.JAVA_SHORT_UNALIGNED, cumOff + 12) & 0xFFFFL) << 32;
          k2 ^= seg.get(ValueLayout.JAVA_INT_UNALIGNED, cumOff + 8) & 0xFFFFFFFFL;
          k1 = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, cumOff);
          break;
        }

        case 13: {
          k2 ^= (seg.get(ValueLayout.JAVA_BYTE, cumOff + 12) & 0xFFL) << 32;
        }
        //$FALL-THROUGH$
        case 12: {
          k2 ^= seg.get(ValueLayout.JAVA_INT_UNALIGNED, cumOff + 8) & 0xFFFFFFFFL;
          k1 = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, cumOff);
          break;
        }

        case 11: {
          k2 ^= (seg.get(ValueLayout.JAVA_BYTE, cumOff + 10) & 0xFFL) << 16;
        }
        //$FALL-THROUGH$
        case 10: {
          k2 ^= seg.get(ValueLayout.JAVA_SHORT_UNALIGNED, cumOff + 8) & 0xFFFFL;
          k1 = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, cumOff);
          break;
        }

        case  9: {
          k2 ^= seg.get(ValueLayout.JAVA_BYTE, cumOff + 8) & 0xFFL;
        }
        //$FALL-THROUGH$
        case  8: {
          k1 = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, cumOff);
          break;
        }

        case  7: {
          k1 ^= (seg.get(ValueLayout.JAVA_BYTE, cumOff + 6) & 0xFFL) << 48;
        }
        //$FALL-THROUGH$
        case  6: {
          k1 ^= (seg.get(ValueLayout.JAVA_SHORT_UNALIGNED, cumOff + 4) & 0xFFFFL) << 32;
          k1 ^= seg.get(ValueLayout.JAVA_INT_UNALIGNED, cumOff) & 0xFFFFFFFFL;
          break;
        }

        case  5: {
          k1 ^= (seg.get(ValueLayout.JAVA_BYTE, cumOff + 4) & 0xFFL) << 32;
        }
        //$FALL-THROUGH$
        case  4: {
          k1 ^= seg.get(ValueLayout.JAVA_INT_UNALIGNED, cumOff) & 0xFFFFFFFFL;
          break;
        }

        case  3: {
          k1 ^= (seg.get(ValueLayout.JAVA_BYTE, cumOff + 2) & 0xFFL) << 16;
        }
        //$FALL-THROUGH$
        case  2: {
          k1 ^= seg.get(ValueLayout.JAVA_SHORT_UNALIGNED, cumOff) & 0xFFFFL;
          break;
        }

        case  1: {
          k1 ^= seg.get(ValueLayout.JAVA_BYTE, cumOff) & 0xFFL;
          break;
        }
        default: break; //can't happen
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

  private MurmurHash3FFM21() { }

}
