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
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

import java.lang.foreign.MemorySegment;

/**
 * The XxHash is a fast, non-cryptographic, 64-bit hash function that has
 * excellent avalanche and 2-way bit independence properties.
 * This java version adapted from the C++ version and the OpenHFT/Zero-Allocation-Hashing implementation
 * referenced below as inspiration.
 *
 * <p>The C++ source repository:
 * <a href="https://github.com/Cyan4973/xxHash">
 * https://github.com/Cyan4973/xxHash</a>. It has a BSD 2-Clause License:
 * <a href="http://www.opensource.org/licenses/bsd-license.php">
 * http://www.opensource.org/licenses/bsd-license.php</a>.  See LICENSE.
 *
 * <p>Portions of this code were adapted from
 * <a href="https://github.com/OpenHFT/Zero-Allocation-Hashing/blob/master/src/main/java/net/openhft/hashing/XxHash.java">
 * OpenHFT/Zero-Allocation-Hashing</a>, which has an Apache 2 license as does this site. See LICENSE.
 *
 * @author Lee Rhodes
 */
public final class XxHash64 {
  // Unsigned, 64-bit primes
  private static final long P1 = -7046029288634856825L;
  private static final long P2 = -4417276706812531889L;
  private static final long P3 =  1609587929392839161L;
  private static final long P4 = -8796714831421723037L;
  private static final long P5 =  2870177450012600261L;
  //shift constants
  private static final byte SHORT_SHIFT     = 1;
  private static final byte CHAR_SHIFT      = 1;
  private static final byte INT_SHIFT       = 2;
  private static final byte LONG_SHIFT      = 3;
  private static final byte FLOAT_SHIFT     = 2;
  private static final byte DOUBLE_SHIFT    = 3;

  private XxHash64() { }

  /**
   * Returns the 64-bit hash of the sequence of bytes in the given MemorySegment
   *
   * @param seg A reference to the relevant MemorySegment.
   * @param offsetBytes offset in bytes in the given segment.
   * @param lengthBytes the length in bytes to be hashed
   * @param seed a given seed
   * @return the 64-bit hash of the sequence of bytes.
   */
  public static long hash(final MemorySegment seg, long offsetBytes, final long lengthBytes, final long seed) {
    long hash;
    long remaining = lengthBytes;

    if (remaining >= 32) {
      long v1 = seed + P1 + P2;
      long v2 = seed + P2;
      long v3 = seed;
      long v4 = seed - P1;

      do {
        v1 += seg.get(JAVA_LONG_UNALIGNED, offsetBytes) * P2;
        v1 = Long.rotateLeft(v1, 31);
        v1 *= P1;

        v2 += seg.get(JAVA_LONG_UNALIGNED, offsetBytes + 8L) * P2;
        v2 = Long.rotateLeft(v2, 31);
        v2 *= P1;

        v3 += seg.get(JAVA_LONG_UNALIGNED, offsetBytes + 16L) * P2;
        v3 = Long.rotateLeft(v3, 31);
        v3 *= P1;

        v4 += seg.get(JAVA_LONG_UNALIGNED, offsetBytes + 24L) * P2;
        v4 = Long.rotateLeft(v4, 31);
        v4 *= P1;

        offsetBytes += 32;
        remaining -= 32;
      } while (remaining >= 32);

      hash = Long.rotateLeft(v1, 1)
          + Long.rotateLeft(v2, 7)
          + Long.rotateLeft(v3, 12)
          + Long.rotateLeft(v4, 18);

      v1 *= P2;
      v1 = Long.rotateLeft(v1, 31);
      v1 *= P1;
      hash ^= v1;
      hash = (hash * P1) + P4;

      v2 *= P2;
      v2 = Long.rotateLeft(v2, 31);
      v2 *= P1;
      hash ^= v2;
      hash = (hash * P1) + P4;

      v3 *= P2;
      v3 = Long.rotateLeft(v3, 31);
      v3 *= P1;
      hash ^= v3;
      hash = (hash * P1) + P4;

      v4 *= P2;
      v4 = Long.rotateLeft(v4, 31);
      v4 *= P1;
      hash ^= v4;
      hash = (hash * P1) + P4;
    } //end remaining >= 32
    else {
      hash = seed + P5;
    }

    hash += lengthBytes;

    while (remaining >= 8) {
      long k1 = seg.get(JAVA_LONG_UNALIGNED, offsetBytes);
      k1 *= P2;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= P1;
      hash ^= k1;
      hash = (Long.rotateLeft(hash, 27) * P1) + P4;
      offsetBytes += 8;
      remaining -= 8;
    }

    if (remaining >= 4) { //treat as unsigned ints
      hash ^= (seg.get(JAVA_INT_UNALIGNED, offsetBytes) & 0XFFFF_FFFFL) * P1;
      hash = (Long.rotateLeft(hash, 23) * P2) + P3;
      offsetBytes += 4;
      remaining -= 4;
    }

    while (remaining != 0) { //treat as unsigned bytes
      hash ^= (seg.get(JAVA_BYTE, offsetBytes) & 0XFFL) * P5;
      hash = Long.rotateLeft(hash, 11) * P1;
      --remaining;
      ++offsetBytes;
    }

    return finalize(hash);
  }

  /**
   * Returns a 64-bit hash from a single long. This method has been optimized for speed when only
   * a single hash of a long is required.
   * @param in A long.
   * @param seed A long valued seed.
   * @return the hash.
   */
  public static long hash(final long in, final long seed) {
    long hash = seed + P5;
    hash += 8;
    long k1 = in;
    k1 *= P2;
    k1 = Long.rotateLeft(k1, 31);
    k1 *= P1;
    hash ^= k1;
    hash = (Long.rotateLeft(hash, 27) * P1) + P4;
    return finalize(hash);
  }

  private static long finalize(long hash) {
    hash ^= hash >>> 33;
    hash *= P2;
    hash ^= hash >>> 29;
    hash *= P3;
    hash ^= hash >>> 32;
    return hash;
  }

  /**
   * Hash the given arr starting at the given offset and continuing for the given length using the
   * given seed.
   * @param arr the given array
   * @param offsetBytes starting at this offset
   * @param lengthBytes continuing for this length
   * @param seed the given seed
   * @return the hash
   */
  public static long hashBytes(final byte[] arr, final int offsetBytes,
      final int lengthBytes, final long seed) {
    final MemorySegment seg = MemorySegment.ofArray(arr);
    return hash(seg, offsetBytes, lengthBytes, seed);
  }

  /**
   * Hash the given arr starting at the given offset and continuing for the given length using the
   * given seed.
   * @param arr the given array
   * @param offsetShorts starting at this offset
   * @param lengthShorts continuing for this length
   * @param seed the given seed
   * @return the hash
   */
  public static long hashShorts(final short[] arr, final int offsetShorts,
      final int lengthShorts, final long seed) {
    final MemorySegment seg = MemorySegment.ofArray(arr);
    return hash(seg, (offsetShorts << SHORT_SHIFT), lengthShorts << SHORT_SHIFT, seed);
  }

  /**
   * Hash the given arr starting at the given offset and continuing for the given length using the
   * given seed.
   * @param arr the given array
   * @param offsetChars starting at this offset
   * @param lengthChars continuing for this length
   * @param seed the given seed
   * @return the hash
   */
  public static long hashChars(final char[] arr, final int offsetChars,
      final int lengthChars, final long seed) {
    final MemorySegment seg = MemorySegment.ofArray(arr);
    return hash(seg, offsetChars << CHAR_SHIFT, lengthChars << CHAR_SHIFT, seed);
  }

  /**
   * Hash the given arr starting at the given offset and continuing for the given length using the
   * given seed.
   * @param arr the given array
   * @param offsetInts starting at this offset
   * @param lengthInts continuing for this length
   * @param seed the given seed
   * @return the hash
   */
  public static long hashInts(final int[] arr, final int offsetInts,
      final int lengthInts, final long seed) {
    final MemorySegment seg = MemorySegment.ofArray(arr);
    return hash(seg, offsetInts << INT_SHIFT, lengthInts << INT_SHIFT, seed);
  }

  /**
   * Hash the given arr starting at the given offset and continuing for the given length using the
   * given seed.
   * @param arr the given array
   * @param offsetLongs starting at this offset
   * @param lengthLongs continuing for this length
   * @param seed the given seed
   * @return the hash
   */
  public static long hashLongs(final long[] arr, final int offsetLongs,
      final int lengthLongs, final long seed) {
    final MemorySegment seg = MemorySegment.ofArray(arr);
    return hash(seg, offsetLongs << LONG_SHIFT, lengthLongs << LONG_SHIFT, seed);
  }

  /**
   * Hash the given arr starting at the given offset and continuing for the given length using the
   * given seed.
   * @param arr the given array
   * @param offsetFloats starting at this offset
   * @param lengthFloats continuing for this length
   * @param seed the given seed
   * @return the hash
   */
  public static long hashFloats(final float[] arr, final int offsetFloats,
      final int lengthFloats, final long seed) {
    final MemorySegment seg = MemorySegment.ofArray(arr);
    return hash(seg, offsetFloats << FLOAT_SHIFT, lengthFloats << FLOAT_SHIFT, seed);
  }

  /**
   * Hash the given arr starting at the given offset and continuing for the given length using the
   * given seed.
   * @param arr the given array
   * @param offsetDoubles starting at this offset
   * @param lengthDoubles continuing for this length
   * @param seed the given seed
   * @return the hash
   */
  public static long hashDoubles(final double[] arr, final int offsetDoubles,
      final int lengthDoubles, final long seed) {
    final MemorySegment seg = MemorySegment.ofArray(arr);
    return hash(seg, offsetDoubles << DOUBLE_SHIFT, lengthDoubles << DOUBLE_SHIFT, seed);
  }

  /**
   * Hash the given arr starting at the given offset and continuing for the given length using the
   * given seed.
   * @param str the given string
   * @param offsetChars starting at this offset
   * @param lengthChars continuing for this length
   * @param seed the given seed
   * @return the hash
   */
  public static long hashString(final String str, final int offsetChars,
      final int lengthChars, final long seed) {
    final MemorySegment seg = MemorySegment.ofArray(str.toCharArray());
    return hash(seg, offsetChars << CHAR_SHIFT, lengthChars << CHAR_SHIFT, seed);
  }

}
