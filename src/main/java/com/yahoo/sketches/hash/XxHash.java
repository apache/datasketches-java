/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hash;

import com.yahoo.memory.Memory;

/**
 * The XxHash is a fast, non-cryptographic, 64-bit hash function that has
 * excellent avalanche and 2-way bit independence properties. This java version
 * used the C++ version referenced below as inspiration.
 *
 * <p>The C++ source repository:
 * <a href="https://github.com/Cyan4973/xxHash">
 * https://github.com/Cyan4973/xxHash</a>.
 *
 * <p>It has a BSD 2-Clause License:
 * <a href="http://www.opensource.org/licenses/bsd-license.php">
 * http://www.opensource.org/licenses/bsd-license.php</a>
 *
 * @author Lee Rhodes
 */
public class XxHash {
  // Unsigned, 64-bit primes
  private static final long P1 = -7046029288634856825L;
  private static final long P2 = -4417276706812531889L;
  private static final long P3 =  1609587929392839161L;
  private static final long P4 = -8796714831421723037L;
  private static final long P5 =  2870177450012600261L;

  /**
   * Returns a 64-bit hash.
   * @param in a long
   * @param seed A long valued seed.
   * @return the hash
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

  /**
   * Returns a 64-bit hash.
   *
   * @param mem The source Memory
   * @param offsetBytes the offset in bytes
   * @param lengthBytes the length in bytes
   * @param seed a given seed
   * @return a 64-bit hash
   */
  public static long hash(final Memory mem, long offsetBytes, final long lengthBytes, final long seed) {
    long hash;
    long remaining = lengthBytes;

    if (remaining >= 32) {
      long v1 = seed + P1 + P2;
      long v2 = seed + P2;
      long v3 = seed;
      long v4 = seed - P1;

      do {
        v1 += mem.getLong(offsetBytes) * P2;
        v1 = Long.rotateLeft(v1, 31);
        v1 *= P1;

        v2 += mem.getLong(offsetBytes + 8L) * P2;
        v2 = Long.rotateLeft(v2, 31);
        v2 *= P1;

        v3 += mem.getLong(offsetBytes + 16L) * P2;
        v3 = Long.rotateLeft(v3, 31);
        v3 *= P1;

        v4 += mem.getLong(offsetBytes + 24L) * P2;
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
      long k1 = mem.getLong(offsetBytes);
      k1 *= P2;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= P1;
      hash ^= k1;
      hash = (Long.rotateLeft(hash, 27) * P1) + P4;
      offsetBytes += 8;
      remaining -= 8;
    }

    if (remaining >= 4) { //treat as unsigned ints
      hash ^= (mem.getInt(offsetBytes) & 0XFFFF_FFFFL) * P1;
      hash = (Long.rotateLeft(hash, 23) * P2) + P3;
      offsetBytes += 4;
      remaining -= 4;
    }

    while (remaining != 0) { //treat as unsigned bytes
      hash ^= (mem.getByte(offsetBytes) & 0XFFL) * P5;
      hash = Long.rotateLeft(hash, 11) * P1;
      --remaining;
      ++offsetBytes;
    }

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

}
