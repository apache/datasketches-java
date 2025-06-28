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

import static org.apache.datasketches.memory.internal.XxHash64.hash;
import static org.apache.datasketches.memory.internal.XxHash64.hashBytes;
import static org.apache.datasketches.memory.internal.XxHash64.hashChars;
import static org.apache.datasketches.memory.internal.XxHash64.hashDoubles;
import static org.apache.datasketches.memory.internal.XxHash64.hashFloats;
import static org.apache.datasketches.memory.internal.XxHash64.hashInts;
import static org.apache.datasketches.memory.internal.XxHash64.hashLongs;
import static org.apache.datasketches.memory.internal.XxHash64.hashShorts;

import org.apache.datasketches.memory.internal.XxHash64;

/**
 * The XxHash is a fast, non-cryptographic, 64-bit hash function that has
 * excellent avalanche and 2-way bit independence properties.
 * This java version adapted  the C++ version and the OpenHFT/Zero-Allocation-Hashing implementation
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
public final class XxHash {

  private XxHash() { /* singleton */ }

  /**
   * Hash the given arr starting at the given offset and continuing for the given length using the
   * given seed.
   * @param arr the given array
   * @param offsetBytes starting at this offset
   * @param lengthBytes continuing for this length
   * @param seed the given seed
   * @return the hash
   */
  public static long hashByteArr(
      final byte[] arr,
      final int offsetBytes,
      final int lengthBytes,
      final long seed) {
    return hashBytes(arr, offsetBytes, lengthBytes, seed);
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
  public static long hashShortArr(
      final short[] arr,
      final int offsetShorts,
      final int lengthShorts,
      final long seed) {
    return hashShorts(arr, offsetShorts, lengthShorts, seed);
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
  public static long hashCharArr(
      final char[] arr,
      final int offsetChars,
      final int lengthChars,
      final long seed) {
    return hashChars(arr, offsetChars, lengthChars, seed);
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
  public static long hashIntArr(
      final int[] arr,
      final int offsetInts,
      final int lengthInts,
      final long seed) {
    return hashInts(arr, offsetInts, lengthInts, seed);
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
  public static long hashLongArr(
      final long[] arr,
      final int offsetLongs,
      final int lengthLongs,
      final long seed) {
    return hashLongs(arr, offsetLongs, lengthLongs, seed);
  }

  /**
   * Returns a 64-bit hash from a single long. This method has been optimized for speed when only
   * a single hash of a long is required.
   * @param in A long.
   * @param seed A long valued seed.
   * @return the hash.
   */
  public static long hashLong(
      final long in,
      final long seed) {
    return hash(in, seed);
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
  public static long hashFloatArr(
      final float[] arr,
      final int offsetFloats,
      final int lengthFloats,
      final long seed) {
    return hashFloats(arr, offsetFloats, lengthFloats, seed);
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
  public static long hashDoubleArr(
      final double[] arr,
      final int offsetDoubles,
      final int lengthDoubles,
      final long seed) {
    return hashDoubles(arr, offsetDoubles, lengthDoubles, seed);
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
  public static long hashString(
      final String str,
      final int offsetChars,
      final int lengthChars,
      final long seed) {
    return XxHash64.hashString(str, offsetChars, lengthChars, seed);
  }

}

