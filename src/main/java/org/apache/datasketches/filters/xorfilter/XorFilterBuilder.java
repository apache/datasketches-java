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

package org.apache.datasketches.filters.xorfilter;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.hash.XxHash;
import org.apache.datasketches.hash.XxHash64;

/**
 * This class accumulates items and builds an immutable XorFilter from them. Because the xor filter
 * construction algorithm needs the full set of items at once, items are added to the builder with
 * the update methods and the filter is produced with a single call to {@link #build()}.
 *
 * <p>Items are reduced to 64-bit keys as they are added, so the builder retains only one long per
 * item regardless of the item's type or size. Duplicate items are removed automatically when the
 * filter is built.</p>
 *
 * <p>The underlying algorithm is described in Graf and Lemire,
 * "Xor Filters: Faster and Smaller Than Bloom and Cuckoo Filters,"
 * ACM Journal of Experimental Algorithmics, 2020.</p>
 */
public final class XorFilterBuilder {
  private static final int DEFAULT_BITS_PER_FINGERPRINT = 8;
  private static final int MIN_INITIAL_CAPACITY = 16;

  private final int bitsPerFingerprint_;  // fingerprint width in bits, 8 or 16
  private final long seed_;               // base seed for the construction attempts
  private long[] keys_;                   // accumulated 64-bit keys
  private int count_;                     // number of keys accumulated

  /**
   * Creates a builder with 8-bit fingerprints and a random base seed.
   */
  public XorFilterBuilder() {
    this(DEFAULT_BITS_PER_FINGERPRINT, ThreadLocalRandom.current().nextLong());
  }

  /**
   * Creates a builder with the given fingerprint width and a random base seed.
   * @param bitsPerFingerprint The fingerprint width in bits, either 8 or 16
   */
  public XorFilterBuilder(final int bitsPerFingerprint) {
    this(bitsPerFingerprint, ThreadLocalRandom.current().nextLong());
  }

  /**
   * Creates a builder with the given fingerprint width and base seed.
   * @param bitsPerFingerprint The fingerprint width in bits, either 8 or 16
   * @param seed A base seed for the construction attempts
   */
  public XorFilterBuilder(final int bitsPerFingerprint, final long seed) {
    validateBitsPerFingerprint(bitsPerFingerprint);
    bitsPerFingerprint_ = bitsPerFingerprint;
    seed_ = seed;
    keys_ = new long[MIN_INITIAL_CAPACITY];
    count_ = 0;
  }

  /**
   * Returns the number of items added to this builder so far, including any duplicates.
   * @return The number of items added to this builder
   */
  public long getNumItems() { return count_; }

  // UPDATE METHODS
  /**
   * Updates the builder with the provided long value.
   * @param item an item with which to update the builder
   * @return this builder
   */
  public XorFilterBuilder update(final long item) {
    return appendKey(XxHash.hashLong(item, XorFilter.HASH_SEED));
  }

  /**
   * Updates the builder with the provided double value. The value is
   * canonicalized (NaN and infinities) prior to updating.
   * @param item an item with which to update the builder
   * @return this builder
   */
  public XorFilterBuilder update(final double item) {
    // canonicalize all NaN & +/- infinity forms
    final long[] data = { Double.doubleToLongBits(item) };
    return appendKey(XxHash.hashLongArr(data, 0, 1, XorFilter.HASH_SEED));
  }

  /**
   * Updates the builder with the provided String.
   * The string is converted to a byte array using UTF8 encoding.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(char[])}
   * method and will generally be a little slower depending on the complexity of the UTF8 encoding.
   * </p>
   *
   * @param item an item with which to update the builder
   * @return this builder
   */
  public XorFilterBuilder update(final String item) {
    if ((item == null) || item.isEmpty()) { return this; }
    final byte[] strBytes = item.getBytes(StandardCharsets.UTF_8);
    return appendKey(XxHash.hashByteArr(strBytes, 0, strBytes.length, XorFilter.HASH_SEED));
  }

  /**
   * Updates the builder with the provided byte[].
   * @param data an array with which to update the builder
   * @return this builder
   */
  public XorFilterBuilder update(final byte[] data) {
    if (data == null) { return this; }
    return appendKey(XxHash.hashByteArr(data, 0, data.length, XorFilter.HASH_SEED));
  }

  /**
   * Updates the builder with the provided char[].
   * @param data an array with which to update the builder
   * @return this builder
   */
  public XorFilterBuilder update(final char[] data) {
    if (data == null) { return this; }
    return appendKey(XxHash.hashCharArr(data, 0, data.length, XorFilter.HASH_SEED));
  }

  /**
   * Updates the builder with the provided short[].
   * @param data an array with which to update the builder
   * @return this builder
   */
  public XorFilterBuilder update(final short[] data) {
    if (data == null) { return this; }
    return appendKey(XxHash.hashShortArr(data, 0, data.length, XorFilter.HASH_SEED));
  }

  /**
   * Updates the builder with the provided int[].
   * @param data an array with which to update the builder
   * @return this builder
   */
  public XorFilterBuilder update(final int[] data) {
    if (data == null) { return this; }
    return appendKey(XxHash.hashIntArr(data, 0, data.length, XorFilter.HASH_SEED));
  }

  /**
   * Updates the builder with the provided long[].
   * @param data an array with which to update the builder
   * @return this builder
   */
  public XorFilterBuilder update(final long[] data) {
    if (data == null) { return this; }
    return appendKey(XxHash.hashLongArr(data, 0, data.length, XorFilter.HASH_SEED));
  }

  /**
   * Updates the builder with the data in the provided MemorySegment.
   * @param seg a MemorySegment object with which to update the builder
   * @return this builder
   */
  public XorFilterBuilder update(final MemorySegment seg) {
    if (seg == null) { return this; }
    return appendKey(XxHash64.hash(seg, 0, seg.byteSize(), XorFilter.HASH_SEED));
  }

  /**
   * Builds an immutable XorFilter from the items added to this builder. Duplicate items are
   * removed before construction.
   * @return A new XorFilter holding the accumulated items
   */
  public XorFilter build() {
    final long[] distinct = Arrays.copyOf(keys_, count_);
    Arrays.sort(distinct);
    int numDistinct = 0;
    for (int i = 0; i < distinct.length; ++i) {
      if ((i == 0) || (distinct[i] != distinct[i - 1])) {
        distinct[numDistinct++] = distinct[i];
      }
    }
    return new XorFilter(bitsPerFingerprint_, seed_, distinct, numDistinct);
  }

  /**
   * Creates a XorFilter with 8-bit fingerprints from the given items, using a random base seed.
   * Each element of the array is treated as a separate item.
   * @param items The items with which to build the filter
   * @return A new XorFilter holding the given items
   */
  public static XorFilter create(final long[] items) {
    return create(items, DEFAULT_BITS_PER_FINGERPRINT, ThreadLocalRandom.current().nextLong());
  }

  /**
   * Creates a XorFilter with the given fingerprint width from the given items, using a random base seed.
   * Each element of the array is treated as a separate item.
   * @param items The items with which to build the filter
   * @param bitsPerFingerprint The fingerprint width in bits, either 8 or 16
   * @return A new XorFilter holding the given items
   */
  public static XorFilter create(final long[] items, final int bitsPerFingerprint) {
    return create(items, bitsPerFingerprint, ThreadLocalRandom.current().nextLong());
  }

  /**
   * Creates a XorFilter with the given fingerprint width from the given items, using the provided base seed.
   * Each element of the array is treated as a separate item.
   * @param items The items with which to build the filter
   * @param bitsPerFingerprint The fingerprint width in bits, either 8 or 16
   * @param seed A base seed for the construction attempts
   * @return A new XorFilter holding the given items
   */
  public static XorFilter create(final long[] items, final int bitsPerFingerprint, final long seed) {
    if (items == null) {
      throw new SketchesArgumentException("Items array must not be null");
    }
    final XorFilterBuilder builder = new XorFilterBuilder(bitsPerFingerprint, seed);
    for (final long item : items) {
      builder.update(item);
    }
    return builder.build();
  }

  // Appends a 64-bit key to the accumulated keys, growing the backing array as needed.
  private XorFilterBuilder appendKey(final long key) {
    if (count_ == keys_.length) {
      keys_ = Arrays.copyOf(keys_, keys_.length << 1);
    }
    keys_[count_++] = key;
    return this;
  }

  private static void validateBitsPerFingerprint(final int bitsPerFingerprint) {
    if ((bitsPerFingerprint != 8) && (bitsPerFingerprint != 16)) {
      throw new SketchesArgumentException("Fingerprint width must be 8 or 16 bits. Requested: " + bitsPerFingerprint);
    }
  }
}
