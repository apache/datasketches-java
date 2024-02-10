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

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class holds an array of bits suitable for use in a Bloom Filter
 * 
 * Rounds the number of bits up to the smallest multiple of 64 (one long)
 * that is not smaller than the specified number.
 */
class BitArray {
  // MAX_BITS using longs, based on array indices being capped at Integer.MAX_VALUE
  private static final long MAX_BITS = Integer.MAX_VALUE * (long) Long.SIZE; 
  private static final long DATA_OFFSET = 8; // offset into memory for start of data array

  private long numBitsSet_;
  private long[] data_;

  BitArray(final long numBits) {
    if (numBits <= 0)
      throw new SketchesArgumentException("Number of bits must be strictly positive. Found: " + numBits);
    if (numBits > MAX_BITS)
      throw new SketchesArgumentException("Number of bits may not exceed " + MAX_BITS + ". Found: " + numBits);

    final int numLongs = (int) Math.ceil(numBits / 64.0);
    numBitsSet_ = 0;
    data_ = new long[numLongs];
  }

  BitArray(final long[] data) {
    data_ = data;
    numBitsSet_ = 0;
    for (long val : data)
      numBitsSet_ += Long.bitCount(val);
  }

  static BitArray heapify(final Memory mem, final boolean isEmpty) {
    final int numLongs = mem.getInt(0);
    if (isEmpty)
      return new BitArray(numLongs * Long.SIZE);

    final long[] data = new long[numLongs];
    mem.getLongArray(DATA_OFFSET, data, 0, numLongs);
    return new BitArray(data);
  }

  boolean isEmpty() {
    return numBitsSet_ == 0;
  }

  boolean getBit(final long index) {
    return (data_[(int) index >>> 6] & (1L << index)) != 0 ? true : false;
  }

  // returns existing value of bit
  boolean getAndSetBit(final long index) {
    final int offset = (int) index >>> 6;
    final long mask = 1L << index;
    if ((data_[offset] & mask) != 0) {
      return true; // already seen
    } else {
      data_[offset] |= mask;
      ++numBitsSet_;
      return false; // new set
    }
  }

  long getNumBitsSet() { return numBitsSet_; }

  long getCapacity() { return (long) data_.length * Long.SIZE; }

  int getArrayLength() { return data_.length; }

  // applies logical OR
  void union(final BitArray other) {
    if (data_.length != other.data_.length)
      throw new SketchesArgumentException("Cannot union bit arrays with unequal lengths");

    long numBitsSet = 0;
    for (int i = 0; i < data_.length; ++i) {
      data_[i] |= other.data_[i];
      numBitsSet += Long.bitCount(data_[i]);
    }
    numBitsSet_ = numBitsSet;
  }

  // applies logical AND
  void intersect(final BitArray other) {
    if (data_.length != other.data_.length)
      throw new SketchesArgumentException("Cannot intersect bit arrays with unequal lengths");

    long numBitsSet = 0;
    for (int i = 0; i < data_.length; ++i) {
      data_[i] &= other.data_[i];
      numBitsSet += Long.bitCount(data_[i]);
    }
    numBitsSet_ = numBitsSet;
  }

  long getSerializedSizeBytes() {
    // We only really need an int for array length but this will keep everything
    // aligned to 8 bytes.
    // Always write array length, even if empty
    return isEmpty() ? Long.BYTES : Long.BYTES * (1 + data_.length);
  }

  void writeToMemory(final WritableMemory wmem) {
    if (wmem.getCapacity() < getSerializedSizeBytes())
      throw new SketchesStateException("Attempt to serialize BitArray into WritableMemory with insufficient capacity");
    
    wmem.putInt(0, data_.length);
    
    if (!isEmpty())
      wmem.putLongArray(DATA_OFFSET, data_, 0, data_.length);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < data_.length; ++i) {
      sb.append(i + ": ")
        .append(printLong(data_[i]))
        .append("\n");
    }
    return sb.toString();
  }

  String printLong(long val) {
    StringBuilder sb = new StringBuilder();
    for (int j = 0; j < Long.SIZE; ++j) {
      sb.append((val & (1L << j)) != 0 ? "1" : "0");
      if (j % 8 == 7) sb.append(" ");
    }
    return sb.toString();
  }

  void reset() {
    final int numLongs = data_.length;
    data_ = new long[numLongs];
    numBitsSet_ = 0;
  }

}
