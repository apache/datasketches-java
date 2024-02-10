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

import static org.apache.datasketches.common.Util.INVERSE_GOLDEN_U64;
import static org.apache.datasketches.common.Util.bitAt;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.memory.XxHash;

public class BloomFilter {
  // maximum number of longs in the array with space for a header at serialization
  private static final long MAX_SIZE = Integer.MAX_VALUE * (long) Long.SIZE - 3;
  private static final int SER_VER = 1;
  private static final int EMPTY_FLAG_MASK = 4;

  private final long baseSeed_;  // base seed for hashes
  private long seeds_[];         // array of seeds
  private BitArray bitArray_;    // the actual data bits

  // Creates a BloomFilter with a random base seed
  public BloomFilter(final long numBits, final int numHashes) {
    this(numBits, numHashes, ThreadLocalRandom.current().nextLong());   
  }

  // Creates a BloomFilter with the given base seed
  public BloomFilter(final long numBits, final int numHashes, final long baseSeed) {
    if (numBits > MAX_SIZE)
      throw new SketchesArgumentException("Size of BloomFilter must be <= " + MAX_SIZE
        + ". Requested: " + numBits);

    baseSeed_ = baseSeed;
    seeds_ = generateSeeds(numHashes, baseSeed);

    bitArray_ = new BitArray(numBits);
  }

  BloomFilter(final int numHashes, final long baseSeed, final BitArray bitArray) {
    baseSeed_ = baseSeed;
    seeds_ = generateSeeds(numHashes, baseSeed);
    bitArray_ = bitArray;
  }

  public static BloomFilter heapify(final Memory mem) {
    // TODO: check values
    int offsetBytes = 0;
    final int preLongs = mem.getByte(offsetBytes++);
    final int serVer = mem.getByte(offsetBytes++);
    final int familyID = mem.getByte(offsetBytes++);
    final int flags = mem.getByte(offsetBytes++);
    final int numHashes = mem.getShort(offsetBytes);
    offsetBytes += Integer.BYTES; // increment by 4 even after reading 2
    final long baseSeed = mem.getLong(offsetBytes);
    offsetBytes += Long.BYTES;

    boolean isEmpty = (flags & EMPTY_FLAG_MASK) != 0;

    final BitArray bitArray = BitArray.heapify(mem.region(offsetBytes, mem.getCapacity() - offsetBytes), isEmpty);

    return new BloomFilter(numHashes, baseSeed, bitArray);
  }

  public boolean isEmpty() { return bitArray_.isEmpty(); }
  
  public long getBitsUsed() { return bitArray_.getNumBitsSet(); }

  public long getCapacity() { return bitArray_.getCapacity(); }

  public int getNumHashes() { return seeds_.length; }

  public long getSeed() { return baseSeed_; }

  public double getFillPercentage() {
    return (double) bitArray_.getNumBitsSet() / bitArray_.getCapacity();
  }

  public boolean checkAndUpdate(final long item) {
    boolean valueAlreadyExists = true;
    final long numBits = bitArray_.getCapacity();
    for (long seed : seeds_) {
      // right shift to ensure positive
      long hashIndex = (XxHash.hashLong(item, seed) >>> 1) % numBits;
      // returns old value of bit
      valueAlreadyExists &= bitArray_.getAndSetBit(hashIndex);
    }
    return valueAlreadyExists;
  }

  public boolean checkAndUpdate(final double item) {
    boolean valueAlreadyExists = true;
    final long numBits = bitArray_.getCapacity();
    final double val[] = { item };
    for (long seed : seeds_) {
      // right shift to ensure positive
      long hashIndex = (XxHash.hashDoubleArr(val, 0, 1, seed) >>> 1) % numBits;
      // returns old value of bit
      valueAlreadyExists &= bitArray_.getAndSetBit(hashIndex);
    }
    return valueAlreadyExists;
  }

  public boolean checkAndUpdate(final String item) {
    boolean valueAlreadyExists = true;
    final long numBits = bitArray_.getCapacity();
    for (long seed : seeds_) {
      // right shift to ensure positive
      long hashIndex = (XxHash.hashString(item, 0, item.length(), seed) >>> 1) % numBits;
      // returns old value of bit
      valueAlreadyExists &= bitArray_.getAndSetBit(hashIndex);
    }
    return valueAlreadyExists;
  }

  public boolean checkAndUpdate(final byte[] data) {
    boolean valueAlreadyExists = true;
    final long numBits = bitArray_.getCapacity();
    for (long seed : seeds_) {
      // right shift to ensure positive
      long hashIndex = (XxHash.hashByteArr(data, 0, data.length, seed) >>> 1) % numBits;
      // returns old value of bit
      valueAlreadyExists &= bitArray_.getAndSetBit(hashIndex);
    }
    return valueAlreadyExists;
  }
  
  public boolean check(final long item) {
    boolean valueExists = true;
    final long numBits = bitArray_.getCapacity();
    for (long seed : seeds_) {
      // right shift to ensure positive
      long hashIndex = (XxHash.hashLong(item, seed) >>> 1) % numBits;
      valueExists &= bitArray_.getBit(hashIndex);
    }
    return valueExists;
  }

  public boolean check(final double item) {
    boolean valueExists = true;
    final long numBits = bitArray_.getCapacity();
    final double[] val = { item };
    for (long seed : seeds_) {
      // right shift to ensure positive
      long hashIndex = (XxHash.hashDoubleArr(val, 0, 1, seed) >>> 1) % numBits;
      valueExists &= bitArray_.getBit(hashIndex);
    }
    return valueExists;
  }

  public boolean check(final String item) {
    boolean valueExists = true;
    final long numBits = bitArray_.getCapacity();
    for (long seed : seeds_) {
      // right shift to ensure positive
      long hashIndex = (XxHash.hashString(item, 0, item.length(), seed) >>> 1) % numBits;
      valueExists &= bitArray_.getBit(hashIndex);
    }
    return valueExists;
  }

  public boolean check(final byte[] data) {
    boolean valueExists = true;
    final long numBits = bitArray_.getCapacity();
    for (long seed : seeds_) {
      // right shift to ensure positive
      long hashIndex = (XxHash.hashByteArr(data, 0, data.length, seed) >>> 1) % numBits;
      valueExists &= bitArray_.getBit(hashIndex);
    }
    return valueExists;
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
    wmem.putShort(offsetBytes, (short) seeds_.length);
    offsetBytes += Integer.BYTES; // wrote a short and skipping the next 2 bytes
    wmem.putLong(offsetBytes, seeds_[0]);
    offsetBytes += Long.BYTES;

    bitArray_.writeToMemory(wmem.writableRegion(offsetBytes, sizeBytes - (Family.BLOOMFILTER.getMaxPreLongs() * Long.BYTES)));

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
    wmem.putShort(offsetBytes, (short) seeds_.length);
    offsetBytes += Integer.BYTES; // wrote a short and skipping the next 2 bytes
    wmem.putLong(offsetBytes, seeds_[0]);
    offsetBytes += Long.BYTES;

    bitArray_.writeToMemory(wmem.writableRegion(offsetBytes, sizeBytes - (Family.BLOOMFILTER.getMinPreLongs() * Long.BYTES)));

    return longs;
  }

  private long[] generateSeeds(final int numSeeds, final long baseSeed) {
    final long[] seeds = new long[numSeeds];
    seeds[0] = baseSeed;
    for (int i = 1; i < numSeeds; ++i) {
      seeds_[i] = XxHash.hashLong((seeds_[i-1] + INVERSE_GOLDEN_U64) | 1L, 0);
    }
    return seeds;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(bitArray_.toString());
    return sb.toString();
  }

}
