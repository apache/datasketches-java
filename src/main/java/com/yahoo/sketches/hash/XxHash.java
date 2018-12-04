/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hash;

import com.yahoo.memory.Memory;

/**
 * The XxHash is a fast, non-cryptographic, 64-bit hash function that has
 * excellent avalanche and 2-way bit independence properties.
 * This java version used the C++ version and the OpenHFT/Zero-Allocation-Hashing implementation
 * referenced below as inspiration.
 *
 * <p>The C++ source repository:
 * <a href="https://github.com/Cyan4973/xxHash">
 * https://github.com/Cyan4973/xxHash</a>. It has a BSD 2-Clause License:
 * <a href="http://www.opensource.org/licenses/bsd-license.php">
 * http://www.opensource.org/licenses/bsd-license.php</a>
 *
 * <p>Portions of this code were leveraged from
 * <a href="https://github.com/OpenHFT/Zero-Allocation-Hashing/blob/master/src/main/java/net/openhft/hashing/XxHash.java">
 * OpenHFT/Zero-Allocation-Hashing</a>, which has an Apache 2 license as does this site.
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


  public static long hash(final Memory mem, final long offsetBytes, final long lengthBytes,
      final long seed) {
    return mem.xxHash64(offsetBytes, lengthBytes, seed);
  }

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

  private static long finalize(long hash) {
    hash ^= hash >>> 33;
    hash *= P2;
    hash ^= hash >>> 29;
    hash *= P3;
    hash ^= hash >>> 32;
    return hash;
  }

}
