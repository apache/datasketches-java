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

public final class BloomFilter {
  // maximum number of longs in the array with space for a header at serialization
  private static final long MAX_SIZE = Integer.MAX_VALUE * (long) Long.SIZE - 3;
  private static final int SER_VER = 1;
  private static final int EMPTY_FLAG_MASK = 4;

  private long seed_;            // hash seed
  private short numHashes_;      // number of hash values
  private BitArray bitArray_;    // the actual data bits

  // Creates a BloomFilter with a random base seed
  public BloomFilter(final long numBits, final short numHashes) {
    this(numBits, numHashes, ThreadLocalRandom.current().nextLong());   
  }

  // Creates a BloomFilter with the given base seed
  public BloomFilter(final long numBits, final short numHashes, final long seed) {
    checkArgument(numBits > MAX_SIZE, "Size of BloomFilter must be <= " + MAX_SIZE
      + ". Requested: " + numBits);
    checkArgument(numHashes < 1, "Must specify a strictly positive number of hash functions. "
      + "Requested: " + numHashes);

    seed_ = seed;
    bitArray_ = new BitArray(numBits);
  }

  BloomFilter(final int numHashes, final long seed, final BitArray bitArray) {
    seed_ = seed;
    bitArray_ = bitArray;
  }

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

  public boolean isEmpty() { return bitArray_.isEmpty(); }
  
  public long getBitsUsed() { return bitArray_.getNumBitsSet(); }

  public long getCapacity() { return bitArray_.getCapacity(); }

  public short getNumHashes() { return numHashes_; }

  public long getSeed() { return seed_; }

  public double getFillPercentage() {
    return (double) bitArray_.getNumBitsSet() / bitArray_.getCapacity();
  }

  // UPDATE METHODS
  public void update(final long item) {
    final long h0 = XxHash.hashLong(item, seed_);
    final long h1 = XxHash.hashLong(item, h0);
    updateInternal(h0, h1);
  }

  public void update(final double item) {
    final double val[] = { item };
    final long h0 = XxHash.hashDoubleArr(val, 0, 1, seed_);
    final long h1 = XxHash.hashDoubleArr(val, 0, 1, h0);
    updateInternal(h0, h1);
  }

  public void update(final byte[] data) {
    final long h0 = XxHash.hashByteArr(data, 0, data.length, seed_);
    final long h1 = XxHash.hashByteArr(data, 0, data.length, h0);
    updateInternal(h0, h1);
  }

  public void update(final String item) {
    final byte[] strBytes = item.getBytes(StandardCharsets.UTF_8);
    final long h0 = XxHash.hashByteArr(strBytes, 0, strBytes.length, seed_);
    final long h1 = XxHash.hashByteArr(strBytes, 0, strBytes.length, h0);
    updateInternal(h0, h1);
  }

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
    final double val[] = { item };
    final long h0 = XxHash.hashDoubleArr(val, 0, 1, seed_);
    final long h1 = XxHash.hashDoubleArr(val, 0, 1, h0);
    return queryAndUpdateInternal(h0, h1);
  }

  public boolean queryAndUpdate(final String item) {
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
    final double val[] = { item };
    final long h0 = XxHash.hashDoubleArr(val, 0, 1, seed_);
    final long h1 = XxHash.hashDoubleArr(val, 0, 1, h0);
    return queryInternal(h0, h1);
  }

  public boolean query(final String item) {
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
