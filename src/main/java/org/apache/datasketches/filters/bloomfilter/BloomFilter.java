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

package org.apache.datasketches.filters.bloomfilter;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.Util.LS;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.common.positional.PositionalSegment;
import org.apache.datasketches.hash.XxHash;
import org.apache.datasketches.hash.XxHash64;

/**
 * A Bloom filter is a data structure that can be used for probabilistic
 * set membership.
 *
 * <p>When querying a Bloom filter, there are no false positives. Specifically:
 * When querying an item that has already been inserted to the filter, the filter will
 * always indicate that the item is present. There is a chance of false positives, where
 * querying an item that has never been presented to the filter will indicate that the
 * item has already been seen. Consequently, any query should be interpreted as
 * "might have seen."</p>
 *
 * <p>A standard Bloom filter is unlike typical sketches in that it is not sub-linear
 * in size and does not resize itself. A Bloom filter will work up to a target number of
 * distinct items, beyond which it will saturate and the false positive rate will start to
 * increase. The size of a Bloom filter will be linear in the expected number of
 * distinct items.</p>
 *
 * <p>See the BloomFilterBuilder class for methods to create a filter, especially
 * one sized correctly for a target number of distinct elements and a target
 * false positive probability.</p>
 *
 * <p>This implementation uses xxHash64 and follows the approach in Kirsch and Mitzenmacher,
 * "Less Hashing, Same Performance: Building a Better Bloom Filter," Wiley Interscience, 2008, pp. 187-218.</p>
 */
public final class BloomFilter implements MemorySegmentStatus {
  /**
   * The maximum size of a bloom filter in bits.
   */
  public static final long MAX_SIZE_BITS = (Integer.MAX_VALUE - Family.BLOOMFILTER.getMaxPreLongs()) * (long) Long.SIZE;
  private static final int SER_VER = 1;
  private static final int EMPTY_FLAG_MASK = 4;
  private static final long BIT_ARRAY_OFFSET = 16;
  private static final int FLAGS_BYTE = 3;

  private final long seed_;            // hash seed
  private final short numHashes_;      // number of hash values
  private final BitArray bitArray_;    // the actual data bits
  private final MemorySegment wseg_;  // used only for direct mode BitArray

  /**
   * Creates a BloomFilter with given number of bits and number of hash functions,
   * and a user-specified seed.
   *
   * @param numBits The size of the BloomFilter, in bits
   * @param numHashes The number of hash functions to apply to items
   * @param seed The base hash seed
   */
  BloomFilter(final long numBits, final int numHashes, final long seed) {
    seed_ = seed;
    numHashes_ = (short) numHashes;
    bitArray_ = new HeapBitArray(numBits);
    wseg_ = null;
  }

  /**
   * Creates a BloomFilter with given number of bits and number of hash functions,
   * and a user-specified seed in the provided MemorySegment
   *
   * @param numBits The size of the BloomFilter, in bits
   * @param numHashes The number of hash functions to apply to items
   * @param seed The base hash seed
   * @param wseg A MemorySegment that will be initialized to hold the filter
   */
  BloomFilter(final long numBits, final int numHashes, final long seed, final MemorySegment wseg) {
    if (wseg.byteSize() < Family.BLOOMFILTER.getMaxPreLongs()) {
      throw new SketchesArgumentException("Provided MemorySegment capacity insufficient to initialize BloomFilter");
    }

    // we don't resize so initialize with non-empty preLongs value
    // and no empty flag
    final PositionalSegment posSeg = PositionalSegment.wrap(wseg);
    posSeg.setByte((byte) Family.BLOOMFILTER.getMaxPreLongs());
    posSeg.setByte((byte) SER_VER);
    posSeg.setByte((byte) Family.BLOOMFILTER.getID());
    posSeg.setByte((byte) 0); // instead of (bitArray_.isEmpty() ? EMPTY_FLAG_MASK : 0);
    posSeg.setShort((short) numHashes);
    posSeg.setShort((short) 0); // unused
    posSeg.setLong(seed);

    seed_ = seed;
    numHashes_ = (short) numHashes;
    bitArray_ = DirectBitArray.initialize(numBits, wseg.asSlice(BIT_ARRAY_OFFSET, wseg.byteSize() - BIT_ARRAY_OFFSET));
    wseg_ = wseg;
  }

  // Constructor used with internalHeapifyOrWrap()
  BloomFilter(final short numHashes, final long seed, final BitArray bitArray, final MemorySegment wseg) {
    seed_ = seed;
    numHashes_ = numHashes;
    bitArray_ = bitArray;
    wseg_ = wseg;
  }

  /**
   * Reads a serialized image of a BloomFilter from the provided MemorySegment
   * @param seg MemorySegment containing a previously serialized BloomFilter
   * @return a BloomFilter object
   */
  public static BloomFilter heapify(final MemorySegment seg) {
    // casting to writable, but heapify so only reading
    return internalHeapifyOrWrap(seg, false, false);
  }

  /**
   * Wraps the given MemorySegment into this filter class.  The class itself only contains a few metadata items and holds
   * a reference to the MemorySegment object, which contains all the data.
   * @param seg the given MemorySegment object
   * @return the wrapping BloomFilter class.
   */
  public static BloomFilter wrap(final MemorySegment seg) {
    // casting to writable, but tracking that the object is read-only
    return internalHeapifyOrWrap(seg, true, false);
  }

  /**
   * Wraps the given MemorySegment into this filter class.  The class itself only contains a few metadata items and holds
   * a reference to the MemorySegment object, which contains all the data.
   * @param wseg the given MemorySegment object
   * @return the wrapping BloomFilter class.
   */
  public static BloomFilter writableWrap(final MemorySegment wseg) {
    return internalHeapifyOrWrap(wseg, true, true);
  }

  private static BloomFilter internalHeapifyOrWrap(final MemorySegment wseg, final boolean isWrap, final boolean isWritable) {
    final PositionalSegment posSeg = PositionalSegment.wrap(wseg);
    final int preLongs = posSeg.getByte();
    final int serVer = posSeg.getByte();
    final int familyID = posSeg.getByte();
    final int flags = posSeg.getByte();

    checkArgument((preLongs < Family.BLOOMFILTER.getMinPreLongs()) || (preLongs > Family.BLOOMFILTER.getMaxPreLongs()),
      "Possible corruption: Incorrect number of preamble bytes specified in header");
    checkArgument(serVer != SER_VER, "Possible corruption: Unrecognized serialization version: " + serVer);
    checkArgument(familyID != Family.BLOOMFILTER.getID(), "Possible corruption: Incorrect FamilyID for bloom filter. Found: " + familyID);

    final short numHashes = posSeg.getShort();
    posSeg.getShort(); // unused
    checkArgument(numHashes < 1, "Possible corruption: Need strictly positive number of hash functions. Found: " + numHashes);

    final long seed = posSeg.getLong();

    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) != 0;

    final BitArray bitArray;
    if (isWrap) {
      if (isWritable) {
        bitArray = BitArray.writableWrap(wseg.asSlice(BIT_ARRAY_OFFSET, wseg.byteSize() - BIT_ARRAY_OFFSET), isEmpty);
      } else {
        bitArray = BitArray.wrap(wseg.asSlice(BIT_ARRAY_OFFSET, wseg.byteSize() - BIT_ARRAY_OFFSET), isEmpty);
      }
      return new BloomFilter(numHashes, seed, bitArray, wseg);
    } else { // if heapify
      bitArray = BitArray.heapify(posSeg, isEmpty);
      return new BloomFilter(numHashes, seed, bitArray, null);
    }
  }

  /**
   * Resets the BloomFilter to an empty state
   */
  public void reset() {
    bitArray_.reset();
  }

  /**
   * Checks if the BloomFilter has processed any items
   * @return True if the BloomFilter is empty, otherwise False
   */
  public boolean isEmpty() { return bitArray_.isEmpty(); }

  /**
   * Returns the number of bits in the BloomFilter that are set to 1.
   * @return The number of bits in use in this filter
   */
  public long getBitsUsed() { return bitArray_.getNumBitsSet(); }

  /**
   * Returns the total number of bits in the BloomFilter.
   * @return The total size of the BloomFilter
   */
  public long getCapacity() { return bitArray_.getCapacity(); }

  /**
   * Returns the configured number of hash functions for this BloomFilter
   * @return The number of hash functions to apply to inputs
   */
  public short getNumHashes() { return numHashes_; }

  /**
   * Returns the hash seed for this BloomFilter.
   * @return The hash seed for this filter
   */
  public long getSeed() { return seed_; }

  @Override
  public boolean hasMemorySegment() { return wseg_ != null; }

  /**
   * Returns whether the filter is in read-only mode. That is possible
   * only if there is a backing MemorySegment in read-only mode.
   * @return true if read-only, otherwise false
   */
  public boolean isReadOnly() {
    return (wseg_ != null) && bitArray_.isReadOnly();
  }

  @Override
  public boolean isOffHeap() {
    return hasMemorySegment() && bitArray_.isOffHeap();
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return MemorySegmentStatus.isSameResource(wseg_, that);
  }

  /**
   * Returns the percentage of all bits in the BloomFilter set to 1.
   * @return the percentage of bits in the filter set to 1
   */
  public double getFillPercentage() {
    return (double) bitArray_.getNumBitsSet() / bitArray_.getCapacity();
  }

  // UPDATE METHODS
  /**
   * Updates the filter with the provided long value.
   * @param item an item with which to update the filter
   */
  public void update(final long item) {
    final long h0 = XxHash.hashLong(item, seed_);
    final long h1 = XxHash.hashLong(item, h0);
    updateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided double value. The value is
   * canonicalized (NaN and infinities) prior to updating.
   * @param item an item with which to update the filter
   */
  public void update(final double item) {
    // canonicalize all NaN & +/- infinity forms
    final long[] data = { Double.doubleToLongBits(item) };
    final long h0 = XxHash.hashLongArr(data, 0, 1, seed_);
    final long h1 = XxHash.hashLongArr(data, 0, 1, h0);
    updateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided String.
   * The string is converted to a byte array using UTF8 encoding.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(char[])}
   * method and will generally be a little slower depending on the complexity of the UTF8 encoding.
   * </p>
   *
   * @param item an item with which to update the filter
   */
  public void update(final String item) {
    if ((item == null) || item.isEmpty()) { return; }
    final byte[] strBytes = item.getBytes(StandardCharsets.UTF_8);
    final long h0 = XxHash.hashByteArr(strBytes, 0, strBytes.length, seed_);
    final long h1 = XxHash.hashByteArr(strBytes, 0, strBytes.length, h0);
    updateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided byte[].
   * @param data an array with which to update the filter
   */
  public void update(final byte[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashByteArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashByteArr(data, 0, data.length, h0);
    updateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided char[].
   * @param data an array with which to update the filter
   */
  public void update(final char[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashCharArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashCharArr(data, 0, data.length, h0);
    updateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided short[].
   * @param data an array with which to update the filter
   */
  public void update(final short[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashShortArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashShortArr(data, 0, data.length, h0);
    updateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided int[].
   * @param data an array with which to update the filter
   */
  public void update(final int[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashIntArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashIntArr(data, 0, data.length, h0);
    updateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided long[].
   * @param data an array with which to update the filter
   */
  public void update(final long[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashLongArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashLongArr(data, 0, data.length, h0);
    updateInternal(h0, h1);
  }

  /**
   * Updates the filter with the data in the provided MemorySegment.
   * @param seg a MemorySegment object with which to update the filter
   */
  public void update(final MemorySegment seg) {
    if (seg == null) { return; }
    final long h0 = XxHash64.hash(seg, 0, seg.byteSize(), seed_);
    final long h1 = XxHash64.hash(seg, 0, seg.byteSize(), h0);
    updateInternal(h0, h1);
  }

  // Internal method to apply updates given pre-computed hashes
  private void updateInternal(final long h0, final long h1) {
    final long numBits = bitArray_.getCapacity();
    for (int i = 1; i <= numHashes_; ++i) {
      // right-shift to ensure non-negative value
      final long hashIndex = ((h0 + (i * h1)) >>> 1) % numBits;
      bitArray_.setBit(hashIndex);
    }
  }

  // QUERY-AND-UPDATE METHODS

  /**
   * Updates the filter with the provided long and
   * returns the result from querying that value prior to the update.
   * @param item an item with which to update the filter
   * @return The query result prior to applying the update
   */
  public boolean queryAndUpdate(final long item) {
    final long h0 = XxHash.hashLong(item, seed_);
    final long h1 = XxHash.hashLong(item, h0);
    return queryAndUpdateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided double and
   * returns the result from querying that value prior to the update.
   * The double is canonicalized (NaN and +/- infinity) in the call.
   * @param item an item with which to update the filter
   * @return The query result prior to applying the update
   */
  public boolean queryAndUpdate(final double item) {
    // canonicalize all NaN & +/- infinity forms
    final long[] data = { Double.doubleToLongBits(item) };
    final long h0 = XxHash.hashLongArr(data, 0, 1, seed_);
    final long h1 = XxHash.hashLongArr(data, 0, 1, h0);
    return queryAndUpdateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided String and
   * returns the result from querying that value prior to the update.
   * The string is converted to a byte array using UTF8 encoding.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #queryAndUpdate(char[])}
   * method and will generally be a little slower depending on the complexity of the UTF8 encoding.
   * </p>
   *
   * @param item an item with which to update the filter
   * @return The query result prior to applying the update, or false if item is null
   */
  public boolean queryAndUpdate(final String item) {
    if ((item == null) || item.isEmpty()) { return false; }
    final byte[] strBytes = item.getBytes(StandardCharsets.UTF_8);
    final long h0 = XxHash.hashByteArr(strBytes, 0, strBytes.length, seed_);
    final long h1 = XxHash.hashByteArr(strBytes, 0, strBytes.length, h0);
    return queryAndUpdateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided byte[] and
   * returns the result from querying that array prior to the update.
   * @param data an array with which to update the filter
   * @return The query result prior to applying the update, or false if data is null
   */
  public boolean queryAndUpdate(final byte[] data) {
    final long h0 = XxHash.hashByteArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashByteArr(data, 0, data.length, h0);
    return queryAndUpdateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided char[] and
   * returns the result from querying that array prior to the update.
   * @param data an array with which to update the filter
   * @return The query result prior to applying the update, or false if data is null
   */
  public boolean queryAndUpdate(final char[] data) {
    if (data == null) { return false; }
    final long h0 = XxHash.hashCharArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashCharArr(data, 0, data.length, h0);
    return queryAndUpdateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided short[] and
   * returns the result from querying that array prior to the update.
   * @param data an array with which to update the filter
   * @return The query result prior to applying the update, or false if data is null
   */
  public boolean queryAndUpdate(final short[] data) {
    if (data == null) { return false; }
    final long h0 = XxHash.hashShortArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashShortArr(data, 0, data.length, h0);
    return queryAndUpdateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided int[] and
   * returns the result from querying that array prior to the update.
   * @param data an array with which to update the filter
   * @return The query result prior to applying the update, or false if data is null
   */
  public boolean queryAndUpdate(final int[] data) {
    if (data == null) { return false; }
    final long h0 = XxHash.hashIntArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashIntArr(data, 0, data.length, h0);
    return queryAndUpdateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided long[] and
   * returns the result from querying that array prior to the update.
   * @param data an array with which to update the filter
   * @return The query result prior to applying the update, or false if data is null
   */
  public boolean queryAndUpdate(final long[] data) {
    if (data == null) { return false; }
    final long h0 = XxHash.hashLongArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashLongArr(data, 0, data.length, h0);
    return queryAndUpdateInternal(h0, h1);
  }

  /**
   * Updates the filter with the provided MemorySegment and
   * returns the result from querying that MemorySegment prior to the update.
   * @param seg an array with which to update the filter
   * @return The query result prior to applying the update, or false if MemorySegment is null
   */
  public boolean queryAndUpdate(final MemorySegment seg) {
    if (seg == null) { return false; }
    final long h0 = XxHash64.hash(seg, 0, seg.byteSize(), seed_);
    final long h1 = XxHash64.hash(seg, 0, seg.byteSize(), h0);
    return queryAndUpdateInternal(h0, h1);
  }

  // Internal query-and-update method given pre-computed hashes
  private boolean queryAndUpdateInternal(final long h0, final long h1) {
    final long numBits = bitArray_.getCapacity();
    boolean valueAlreadyExists = true;
    for (int i = 1; i <= numHashes_; ++i) {
      final long hashIndex = ((h0 + (i * h1)) >>> 1) % numBits;
      // returns old value of bit
      valueAlreadyExists &= bitArray_.getAndSetBit(hashIndex);
    }
    return valueAlreadyExists;
  }

  // QUERY METHODS
  /**
   * Queries the filter with the provided long and returns whether the
   * value <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param item an item with which to query the filter
   * @return The result of querying the filter with the given item
   */
  public boolean query(final long item) {
    final long h0 = XxHash.hashLong(item, seed_);
    final long h1 = XxHash.hashLong(item, h0);
    return queryInternal(h0, h1);
  }

  /**
   * Queries the filter with the provided double and returns whether the
   * value <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible. Double values are
   * canonicalized (NaN and +/- infinity) before querying.
   * @param item an item with which to query the filter
   * @return The result of querying the filter with the given item
   */
  public boolean query(final double item) {
    // canonicalize all NaN & +/- infinity forms
    final long[] data = { Double.doubleToLongBits(item) };
    final long h0 = XxHash.hashLongArr(data, 0, 1, seed_);
    final long h1 = XxHash.hashLongArr(data, 0, 1, h0);
    return queryInternal(h0, h1);
  }

  /**
   * Queries the filter with the provided double and returns whether the
   * value <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * The string is converted to a byte array using UTF8 encoding.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(char[])}
   * method and will generally be a little slower depending on the complexity of the UTF8 encoding.
   * </p>
   *
   * @param item an item with which to query the filter
   * @return The result of querying the filter with the given item, or false if item is null
   */
  public boolean query(final String item) {
    if ((item == null) || item.isEmpty()) { return false; }
    final byte[] strBytes = item.getBytes(StandardCharsets.UTF_8);
    final long h0 = XxHash.hashByteArr(strBytes, 0, strBytes.length, seed_);
    final long h1 = XxHash.hashByteArr(strBytes, 0, strBytes.length, h0);
    return queryInternal(h0, h1);
  }

  /**
   * Queries the filter with the provided byte[] and returns whether the
   * array <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param data an array with which to query the filter
   * @return The result of querying the filter with the given data, or false if data is null
   */
  public boolean query(final byte[] data) {
    if (data == null) { return false; }
    final long h0 = XxHash.hashByteArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashByteArr(data, 0, data.length, h0);
    return queryInternal(h0, h1);
  }

  /**
   * Queries the filter with the provided char[] and returns whether the
   * array <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param data an array with which to query the filter
   * @return The result of querying the filter with the given data, or false if data is null
   */
  public boolean query(final char[] data) {
    if (data == null) { return false; }
    final long h0 = XxHash.hashCharArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashCharArr(data, 0, data.length, h0);
    return queryInternal(h0, h1);
  }

  /**
   * Queries the filter with the provided short[] and returns whether the
   * array <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param data an array with which to query the filter
   * @return The result of querying the filter with the given data, or false if data is null
   */
  public boolean query(final short[] data) {
    if (data == null) { return false; }
    final long h0 = XxHash.hashShortArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashShortArr(data, 0, data.length, h0);
    return queryInternal(h0, h1);
  }

  /**
   * Queries the filter with the provided int[] and returns whether the
   * array <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param data an array with which to query the filter
   * @return The result of querying the filter with the given data, or false if data is null
   */
  public boolean query(final int[] data) {
    if (data == null) { return false; }
    final long h0 = XxHash.hashIntArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashIntArr(data, 0, data.length, h0);
    return queryInternal(h0, h1);
  }

  /**
   * Queries the filter with the provided long[] and returns whether the
   * array <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param data an array with which to query the filter
   * @return The result of querying the filter with the given data, or false if data is null
   */
  public boolean query(final long[] data) {
    if (data == null) { return false; }
    final long h0 = XxHash.hashLongArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashLongArr(data, 0, data.length, h0);
    return queryInternal(h0, h1);
  }

  /**
   * Queries the filter with the provided MemorySegment and returns whether the
   * data <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param seg a MemorySegment array with which to query the filter
   * @return The result of querying the filter with the given MemorySegment, or false if data is null
   */
  public boolean query(final MemorySegment seg) {
    if (seg == null) { return false; }
    final long h0 = XxHash64.hash(seg, 0, seg.byteSize(), seed_);
    final long h1 = XxHash64.hash(seg, 0, seg.byteSize(), h0);
    return queryInternal(h0, h1);
  }

  // Internal method to query the filter given pre-computed hashes
  private boolean queryInternal(final long h0, final long h1) {
    final long numBits = bitArray_.getCapacity();
    for (int i = 1; i <= numHashes_; ++i) {
      final long hashIndex = ((h0 + (i * h1)) >>> 1) % numBits;
      // returns old value of bit
      if (!bitArray_.getBit(hashIndex)) {
        return false;
      }
    }
    return true;
  }

  // OTHER OPERATIONS
  /**
   * Unions two BloomFilters by applying a logical OR. The result will recognized
   * any values seen by either filter (as well as false positives).
   * @param other A BloomFilter to union with this one
   */
  public void union(final BloomFilter other) {
    if (other == null) { return; }
    if (!isCompatible(other)) {
      throw new SketchesArgumentException("Cannot union sketches with different seeds, hash functions, or sizes");
    }

    bitArray_.union(other.bitArray_);
  }

  /**
   * Intersects two BloomFilters by applying a logical AND. The result will recognize
   * only values seen by both filters (as well as false positives).
   * @param other A BloomFilter to union with this one
   */
  public void intersect(final BloomFilter other) {
    if (other == null) { return; }
    if (!isCompatible(other)) {
      throw new SketchesArgumentException("Cannot union sketches with different seeds, hash functions, or sizes");
    }

    bitArray_.intersect(other.bitArray_);
  }

  /**
   * Inverts all the bits of the BloomFilter. Approximately inverts the notion of set-membership.
   */
  public void invert() {
    bitArray_.invert();
  }

  /**
   * Helps identify if two BloomFilters may be unioned or intersected.
   * @param other A BloomFilter to check for compatibility with this one
   * @return True if the filters are compatible, otherwise false
   */
  public boolean isCompatible(final BloomFilter other) {
    if ((other == null)
        || (seed_ != other.seed_)
        || (numHashes_ != other.numHashes_)
        || (bitArray_.getArrayLength() != other.bitArray_.getArrayLength())) {
          return false;
    }
    return true;
  }

  /**
   * Returns the length of this BloomFilter when serialized, in bytes
   * @return The length of this BloomFilter when serialized, in bytes
   */
  public long getSerializedSizeBytes() {
    long sizeBytes = 2L * Long.BYTES; // basic sketch info + baseSeed
    sizeBytes += bitArray_.getSerializedSizeBytes();
    return sizeBytes;
  }

  /**
   * Returns the serialized length of a non-empty BloomFilter of the given size, in bytes
   * @param numBits The number of bits of to use for size computation
   * @return The serialized length of a non-empty BloomFilter of the given size, in bytes
   */
  public static long getSerializedSize(final long numBits) {
    return (2L * Long.BYTES) + BitArray.getSerializedSizeBytes(numBits);
  }

/*
 * A Bloom Filter's serialized image always uses 3 longs of preamble when empty,
 * otherwise 4 longs:
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |----Num Hashes---|-----Unused------|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  1   ||---------------------------------Hash Seed-------------------------------------|
 *
 *      ||      16        |   17   |   18   |   19   |   20   |   21   |   22   |   23   |
 *  2   ||-------BitArray Length (in longs)----------|-----------Unused------------------|
 *
 *      ||      24        |   25   |   26   |   27   |   28   |   29   |   30   |   31   |
 *  3   ||---------------------------------NumBitsSet------------------------------------|
 *  </pre>
 *
 * The raw BitArray bits, if non-empty, start at byte 32.
 */

  /**
   * Serializes the current BloomFilter to an array of bytes.
   *
   * <p>Note: Method throws if the serialized size exceeds <code>Integer.MAX_VALUE</code>.</p>
   * @return A serialized image of the current BloomFilter as byte[]
   */
  public byte[] toByteArray() {
    final long sizeBytes = getSerializedSizeBytes();
    if (sizeBytes > Integer.MAX_VALUE) {
      throw new SketchesStateException("Cannot serialize a BloomFilter of this size using toByteArray(); use toLongArray() instead.");
    }

    final byte[] bytes = new byte[(int) sizeBytes];

    if (wseg_ == null) {
      final MemorySegment seg = MemorySegment.ofArray(bytes);
      final PositionalSegment posSeg = PositionalSegment.wrap(seg);

      final int numPreLongs = isEmpty() ? Family.BLOOMFILTER.getMinPreLongs() : Family.BLOOMFILTER.getMaxPreLongs();
      posSeg.setByte((byte) numPreLongs);
      posSeg.setByte((byte) SER_VER);
      posSeg.setByte((byte) Family.BLOOMFILTER.getID());
      posSeg.setByte((byte) (bitArray_.isEmpty() ? EMPTY_FLAG_MASK : 0));
      posSeg.setShort(numHashes_);
      posSeg.setShort((short) 0); // unused
      posSeg.setLong(seed_);

      ((HeapBitArray) bitArray_).writeToSegmentAsStream(posSeg); //option: posSeg.asSlice()
      System.out.println("HERE");
    } else {
      MemorySegment.copy(wseg_, JAVA_BYTE, 0, bytes, 0, (int)sizeBytes);

      if (isEmpty()) {
        bytes[FLAGS_BYTE] |= EMPTY_FLAG_MASK;
      }
    }
    return bytes;
  }

  /**
   * Serializes the current BloomFilter to an array of longs. Unlike {@link #toByteArray()},
   * this method can handle any size filter.
   *
   * @return A serialized image of the current BloomFilter as long[]
   */
  public long[] toLongArray() {
    final long sizeBytes = getSerializedSizeBytes();

    final long[] longs = new long[(int) (sizeBytes >> 3)];
    if (wseg_ == null) {
      final MemorySegment wseg = MemorySegment.ofArray(longs);
      final PositionalSegment posSeg = PositionalSegment.wrap(wseg);

      final int numPreLongs = isEmpty() ? Family.BLOOMFILTER.getMinPreLongs() : Family.BLOOMFILTER.getMaxPreLongs();
      posSeg.setByte((byte) numPreLongs);
      posSeg.setByte((byte) SER_VER); // to do: add constant
      posSeg.setByte((byte) Family.BLOOMFILTER.getID());
      posSeg.setByte((byte) (bitArray_.isEmpty() ? EMPTY_FLAG_MASK : 0));
      posSeg.setShort(numHashes_);
      posSeg.setShort((short) 0); // unused
      posSeg.setLong(seed_);

      ((HeapBitArray) bitArray_).writeToSegmentAsStream(posSeg); //option: posSeg.asSlice()
    } else {
      MemorySegment.copy(wseg_, JAVA_LONG_UNALIGNED, 0, longs, 0, (int) (sizeBytes >>> 3));
      if (isEmpty()) {
        longs[0] |= (EMPTY_FLAG_MASK << (FLAGS_BYTE << 3));
      }
    }
    return longs;
  }

  // Throws an exception with the provided message if the given condition is false
  private static void checkArgument(final boolean condition, final String message) {
    if (condition) { throw new SketchesArgumentException(message); }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    sb.append(LS);
    final String thisSimpleName = this.getClass().getSimpleName();
    sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("   numBits      : ").append(bitArray_.getCapacity()).append(LS);
    sb.append("   numHashes    : ").append(numHashes_).append(LS);
    sb.append("   seed         : ").append(seed_).append(LS);
    sb.append("   bitsUsed     : ").append(bitArray_.getNumBitsSet()).append(LS);
    sb.append("   fill %       : ").append(getFillPercentage()).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);

    return sb.toString();
  }
}
