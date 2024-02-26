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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.memory.XxHash;

/**
 * <p>A Bloom filter is a data structure that can be used for probabilistic
 * set membership.</p>
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
 * <p>See the BloomFilterBuilder class for methods to create a filter sized correctly
 * for a target number of distinct elements and a target false positive probability.</p>
 * 
 * <p>This implementaiton uses xxHash64 and follows the approach in Kirsch and Mitzenmacher,
 * "Less Hashing, Same Performance: Building a Better Bloom Filter," Wiley Interscience, 2008,
 * pp. 187-218.</p>
 */
public final class BloomFilter {
  // maximum number of longs in the array with space for a header at serialization
  private static final long MAX_SIZE = Integer.MAX_VALUE * (long) Long.SIZE - 3;
  private static final int SER_VER = 1;
  private static final int EMPTY_FLAG_MASK = 4;

  private long seed_;            // hash seed
  private short numHashes_;      // number of hash values
  private BitArray bitArray_;    // the actual data bits

  /**
   * Creates a BloomFilter with given number of bits and number of hash functions,
   * and a random seed.
   *
   * @param numBits The size of the BloomFilter, in bits
   * @param numHashes The number of hash functions to apply to items
   */
  public BloomFilter(final long numBits, final int numHashes) {
    this(numBits, numHashes, ThreadLocalRandom.current().nextLong());   
  }

  /**
   * Creates a BloomFilter with given number of bits and number of hash functions,
   * and a user-specified  seed.
   * 
   * @param numBits The size of the BloomFilter, in bits
   * @param numHashes The number of hash functions to apply to items
   * @param seed The base hash seed
   */
  public BloomFilter(final long numBits, final int numHashes, final long seed) {
    checkArgument(numBits > MAX_SIZE, "Size of BloomFilter must be <= " + MAX_SIZE
      + ". Requested: " + numBits);
    checkArgument(numHashes < 1, "Must specify a strictly positive number of hash functions. "
      + "Requested: " + numHashes);
    checkArgument(numHashes > Short.MAX_VALUE, "Number of hashes cannot exceed " + Short.MAX_VALUE
      + ". Requested: " + numHashes);
    
    seed_ = seed;
    numHashes_ = (short) numHashes;
    bitArray_ = new BitArray(numBits);
  }

  // Constructor used with heapify()
  BloomFilter(final short numHashes, final long seed, final BitArray bitArray) {
    seed_ = seed;
    numHashes_ = numHashes;
    bitArray_ = bitArray;
  }

  /**
   * Reads a serialized image of a BloomFilter from the provided Memory
   * @param mem Memory containing a previously serialized BloomFilter
   * @return a BloomFilter object
   */
  public static BloomFilter heapify(final Memory mem) {
    int offsetBytes = 0;
    final int preLongs = mem.getByte(offsetBytes++);
    final int serVer = mem.getByte(offsetBytes++);
    final int familyID = mem.getByte(offsetBytes++);
    final int flags = mem.getByte(offsetBytes++);

    checkArgument(preLongs < Family.BLOOMFILTER.getMinPreLongs() || preLongs > Family.BLOOMFILTER.getMaxPreLongs(),
      "Possible corruption: Incorrect number of preamble bytes specified in header");
    checkArgument(serVer != SER_VER, "Possible corruption: Unrecognized serialization version: " + serVer);
    checkArgument(familyID != Family.BLOOMFILTER.getID(), "Possible corruption: Incorrect FamilyID for bloom filter. Found: " + familyID);
    
    final short numHashes = mem.getShort(offsetBytes);
    offsetBytes += Integer.BYTES; // increment by 4 even after reading 2
    checkArgument(numHashes < 1, "Possible corruption: Need strictly positive number of hash functions. Found: " + numHashes);

    final long seed = mem.getLong(offsetBytes);
    offsetBytes += Long.BYTES;

    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) != 0;

    final BitArray bitArray = BitArray.heapify(mem.region(offsetBytes, mem.getCapacity() - offsetBytes), isEmpty);

    return new BloomFilter(numHashes, seed, bitArray);
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
   * @param item an item with which to update the filter
   */
  public void update(final String item) {
    if (item == null || item.isEmpty()) { return; }
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
   * Updates the filter with the data in the provided Memory.
   * @param mem a Memory object with which to update the filter
   */
  public void update(final Memory mem) {
    if (mem == null) { return; }
    final long h0 = mem.xxHash64(0, mem.getCapacity(), seed_);
    final long h1 = mem.xxHash64(0, mem.getCapacity(), h0);
    updateInternal(h0, h1);
  }

  // Internal method to apply updates given pre-computed hashes
  private void updateInternal(final long h0, final long h1) {
    final long numBits = bitArray_.getCapacity();
    for (int i = 1; i <= numHashes_; ++i) {
      // right-shift to ensure non-negative value
      final long hashIndex = ((h0 + i * h1) >>> 1) % numBits;
      bitArray_.setBit(hashIndex);
    }
  }

  // QUERY-AND-UPDATE METHODS 
  public boolean queryAndUpdate(final long item) {
    final long h0 = XxHash.hashLong(item, seed_);
    final long h1 = XxHash.hashLong(item, h0);
    return queryAndUpdateInternal(h0, h1);
  }
  
  public boolean queryAndUpdate(final double item) {
    // canonicalize all NaN & +/- infinity forms    
    final long[] data = { Double.doubleToLongBits(item) };
    final long h0 = XxHash.hashLongArr(data, 0, 1, seed_);
    final long h1 = XxHash.hashLongArr(data, 0, 1, h0);
    return queryAndUpdateInternal(h0, h1);
  }

  public boolean queryAndUpdate(final String item) {
    if (item == null || item.isEmpty()) { return false; }
    final byte[] strBytes = item.getBytes(StandardCharsets.UTF_8);
    final long h0 = XxHash.hashByteArr(strBytes, 0, strBytes.length, seed_);
    final long h1 = XxHash.hashByteArr(strBytes, 0, strBytes.length, h0);
    return queryAndUpdateInternal(h0, h1);
  }

  public boolean queryAndUpdate(final byte[] data) {
    final long h0 = XxHash.hashByteArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashByteArr(data, 0, data.length, h0);
    return queryAndUpdateInternal(h0, h1);
  }
  
  public void queryAndUpdate(final char[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashCharArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashCharArr(data, 0, data.length, h0);
    queryAndUpdateInternal(h0, h1);
  }

  public void queryAndUpdate(final short[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashShortArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashShortArr(data, 0, data.length, h0);
    queryAndUpdateInternal(h0, h1);
  }

  public void queryAndUpdate(final int[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashIntArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashIntArr(data, 0, data.length, h0);
    queryAndUpdateInternal(h0, h1);
  }

  public void queryAndUpdate(final long[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashLongArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashLongArr(data, 0, data.length, h0);
    queryAndUpdateInternal(h0, h1);
  }

  public void queryAndUpdate(final Memory mem) {
    if (mem == null) { return; }
    final long h0 = mem.xxHash64(0, mem.getCapacity(), seed_);
    final long h1 = mem.xxHash64(0, mem.getCapacity(), h0);
    queryAndUpdateInternal(h0, h1);
  }

  private boolean queryAndUpdateInternal(final long h0, final long h1) {
    final long numBits = bitArray_.getCapacity();
    boolean valueAlreadyExists = true;
    for (int i = 1; i <= numHashes_; ++i) {
      final long hashIndex = ((h0 + i * h1) >>> 1) % numBits;
      // returns old value of bit
      valueAlreadyExists &= bitArray_.getAndSetBit(hashIndex);
    }
    return valueAlreadyExists;
  }

  // QUERY METHODS
  public boolean query(final long item) {
    final long h0 = XxHash.hashLong(item, seed_);
    final long h1 = XxHash.hashLong(item, h0);
    return queryInternal(h0, h1);
  }

  public boolean query(final double item) {
    // canonicalize all NaN & +/- infinity forms    
    final long[] data = { Double.doubleToLongBits(item) };
    final long h0 = XxHash.hashLongArr(data, 0, 1, seed_);
    final long h1 = XxHash.hashLongArr(data, 0, 1, h0);
    return queryInternal(h0, h1);
  }

  public boolean query(final String item) {
    if (item == null || item.isEmpty()) { return false; }    
    final byte[] strBytes = item.getBytes(StandardCharsets.UTF_8);
    final long h0 = XxHash.hashByteArr(strBytes, 0, strBytes.length, seed_);
    final long h1 = XxHash.hashByteArr(strBytes, 0, strBytes.length, h0);
    return queryInternal(h0, h1);
  }

  public boolean query(final byte[] data) {
    final long h0 = XxHash.hashByteArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashByteArr(data, 0, data.length, h0);
    return queryInternal(h0, h1);
  }

  public void query(final char[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashCharArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashCharArr(data, 0, data.length, h0);
    queryInternal(h0, h1);
  }

  public void query(final short[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashShortArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashShortArr(data, 0, data.length, h0);
    queryInternal(h0, h1);
  }

  public void query(final int[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashIntArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashIntArr(data, 0, data.length, h0);
    queryInternal(h0, h1);
  }

  public void query(final long[] data) {
    if (data == null) { return; }
    final long h0 = XxHash.hashLongArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashLongArr(data, 0, data.length, h0);
    queryInternal(h0, h1);
  }

  public void query(final Memory mem) {
    if (mem == null) { return; }
    final long h0 = mem.xxHash64(0, mem.getCapacity(), seed_);
    final long h1 = mem.xxHash64(0, mem.getCapacity(), h0);
    queryInternal(h0, h1);
  }

  private boolean queryInternal(final long h0, final long h1) {
    final long numBits = bitArray_.getCapacity();
    for (int i = 1; i <= numHashes_; ++i) {
      final long hashIndex = ((h0 + i * h1) >>> 1) % numBits;
      // returns old value of bit
      if (!bitArray_.getBit(hashIndex)) {
        return false;
      }
    }
    return true;
  }

  // OTHER OPERATIONS
  public void union(final BloomFilter other) {
    if (!isCompatible(other)) {
      throw new SketchesArgumentException("Cannot union sketches with different seeds, hash functions, or sizes");
    }

    bitArray_.union(other.bitArray_);
  }

  public void intersect(final BloomFilter other) {
    if (!isCompatible(other)) {
      throw new SketchesArgumentException("Cannot union sketches with different seeds, hash functions, or sizes");
    }

    bitArray_.intersect(other.bitArray_);
  }

  public void invert() {
    bitArray_.invert();
  }

  public boolean isCompatible(final BloomFilter other) {
    if (seed_ != other.seed_
        || numHashes_ != other.numHashes_
        || bitArray_.getArrayLength() != other.bitArray_.getArrayLength()) {
          return false;
    }
    return true;
  }

  public long getSerializedSizeBytes() {
    long sizeBytes = 2 * Long.BYTES; // basic sketch info + baseSeed
    sizeBytes += bitArray_.getSerializedSizeBytes();
    return sizeBytes;
  }

  public byte[] toByteArray() {
    final long sizeBytes = getSerializedSizeBytes();
    if (sizeBytes > Integer.MAX_VALUE) {
      throw new SketchesStateException("Cannot serialize a Bloom Filter of this size using toByteArray(); use toLongArray() instead.");
    }

    final byte[] bytes = new byte[(int) sizeBytes];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);

    int offsetBytes = 0;
    wmem.putByte(offsetBytes++, (byte) Family.BLOOMFILTER.getMinPreLongs());
    wmem.putByte(offsetBytes++, (byte) SER_VER); // to do: add constant
    wmem.putByte(offsetBytes++, (byte) Family.BLOOMFILTER.getID());
    wmem.putByte(offsetBytes++, (byte) (bitArray_.isEmpty() ? EMPTY_FLAG_MASK : 0));
    wmem.putShort(offsetBytes, numHashes_);
    offsetBytes += Integer.BYTES; // wrote a short and skipping the next 2 bytes
    wmem.putLong(offsetBytes, seed_);
    offsetBytes += Long.BYTES;

    bitArray_.writeToMemory(wmem.writableRegion(offsetBytes, sizeBytes - offsetBytes));

    return bytes;
  }

  public long[] toLongArray() {
    final long sizeBytes = getSerializedSizeBytes();

    final long[] longs = new long[(int) (sizeBytes >> 3)];
    final WritableMemory wmem = WritableMemory.writableWrap(longs);

    int offsetBytes = 0;
    wmem.putByte(offsetBytes++, (byte) Family.BLOOMFILTER.getMinPreLongs());
    wmem.putByte(offsetBytes++, (byte) SER_VER); // to do: add constant
    wmem.putByte(offsetBytes++, (byte) Family.BLOOMFILTER.getID());
    wmem.putByte(offsetBytes++, (byte) (bitArray_.isEmpty() ? EMPTY_FLAG_MASK : 0));
    wmem.putShort(offsetBytes, numHashes_);
    offsetBytes += Integer.BYTES; // wrote a short and skipping the next 2 bytes
    wmem.putLong(offsetBytes, seed_);
    offsetBytes += Long.BYTES;

    bitArray_.writeToMemory(wmem.writableRegion(offsetBytes, sizeBytes - offsetBytes));

    return longs;
  }

  private static void checkArgument(final boolean val, final String message) {
    if (val) { throw new SketchesArgumentException(message); }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(bitArray_.toString());
    return sb.toString();
  }
}
