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
import java.nio.ByteBuffer;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * The canonicalized 128-bit MurmurHash3 of one update item, together with the seed it was computed with.
 *
 * <p>Every hash-based sketch derives its internal value from this hash, so one instance can update
 * several sketches that share the seed, producing the same result as updating each with the item.
 * The factory methods return null for null or empty items, which sketches ignore.</p>
 */
public final class ItemHash {
  private final long h0;
  private final long h1;
  private final long seed;

  private ItemHash(final long[] hash, final long seed) {
    this.h0 = hash[0];
    this.h1 = hash[1];
    this.seed = seed;
  }

  /**
   * Hashes a long.
   * @param datum the item
   * @param seed the update seed
   * @return the item hash
   */
  public static ItemHash of(final long datum, final long seed) {
    return new ItemHash(MurmurHash3.hash(datum, seed), seed);
  }

  /**
   * Hashes a double, treating -0.0 as 0.0 and all NaN forms as one value.
   * @param datum the item
   * @param seed the update seed
   * @return the item hash
   */
  public static ItemHash of(final double datum, final long seed) {
    return of(Double.doubleToLongBits(datum == 0.0 ? 0.0 : datum), seed);
  }

  /**
   * Hashes the UTF-8 encoding of a String.
   * @param datum the item
   * @param seed the update seed
   * @return the item hash, or null if the item is null or empty
   */
  public static ItemHash of(final String datum, final long seed) {
    return datum == null || datum.isEmpty() ? null : new ItemHash(MurmurHash3.hash(datum.getBytes(UTF_8), seed), seed);
  }

  /**
   * Hashes a byte array.
   * @param data the item
   * @param seed the update seed
   * @return the item hash, or null if the item is null or empty
   */
  public static ItemHash of(final byte[] data, final long seed) {
    return data == null || data.length == 0 ? null : new ItemHash(MurmurHash3.hash(data, seed), seed);
  }

  /**
   * Hashes a char array.
   * @param data the item
   * @param seed the update seed
   * @return the item hash, or null if the item is null or empty
   */
  public static ItemHash of(final char[] data, final long seed) {
    return data == null || data.length == 0 ? null : new ItemHash(MurmurHash3.hash(data, seed), seed);
  }

  /**
   * Hashes an int array.
   * @param data the item
   * @param seed the update seed
   * @return the item hash, or null if the item is null or empty
   */
  public static ItemHash of(final int[] data, final long seed) {
    return data == null || data.length == 0 ? null : new ItemHash(MurmurHash3.hash(data, seed), seed);
  }

  /**
   * Hashes a long array.
   * @param data the item
   * @param seed the update seed
   * @return the item hash, or null if the item is null or empty
   */
  public static ItemHash of(final long[] data, final long seed) {
    return data == null || data.length == 0 ? null : new ItemHash(MurmurHash3.hash(data, seed), seed);
  }

  /**
   * Hashes the remaining bytes of a ByteBuffer.
   * @param data the item
   * @param seed the update seed
   * @return the item hash, or null if the item is null or has no remaining bytes
   */
  public static ItemHash of(final ByteBuffer data, final long seed) {
    return data == null || !data.hasRemaining() ? null : new ItemHash(MurmurHash3.hash(data, seed), seed);
  }

  /**
   * Hashes the bytes of a MemorySegment.
   * @param data the item
   * @param seed the update seed
   * @return the item hash, or null if the item is null or empty
   */
  public static ItemHash of(final MemorySegment data, final long seed) {
    return data == null || data.byteSize() == 0 ? null : new ItemHash(MurmurHash3.hash(data, seed), seed);
  }

  /**
   * Returns the low 64 bits of the hash.
   * @return the low 64 bits
   */
  public long h0() { return h0; }

  /**
   * Returns the high 64 bits of the hash.
   * @return the high 64 bits
   */
  public long h1() { return h1; }

  /**
   * Checks that this hash was computed with the given seed.
   * @param seed the seed of the sketch being updated
   * @throws SketchesArgumentException if the seeds differ
   */
  public void checkSeed(final long seed) {
    if (this.seed != seed) { throw new SketchesArgumentException("ItemHash seed does not match the sketch seed"); }
  }
}
