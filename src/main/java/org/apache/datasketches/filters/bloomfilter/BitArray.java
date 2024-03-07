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

import java.util.Arrays;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.WritableBuffer;

/**
 * This class holds an array of bits suitable for use in a Bloom Filter
 *
 * <p>Rounds the number of bits up to the smallest multiple of 64 (one long)
 * that is not smaller than the specified number.
 */
final class BitArray {
  // MAX_BITS using longs, based on array indices being capped at Integer.MAX_VALUE
  private static final long MAX_BITS = Integer.MAX_VALUE * (long) Long.SIZE;

  private long numBitsSet_;  // if -1, need to recompute value
  private boolean isDirty_;
  private long[] data_;

  // creates an array of a given size
  BitArray(final long numBits) {
    if (numBits <= 0) {
      throw new SketchesArgumentException("Number of bits must be strictly positive. Found: " + numBits);
    }
    if (numBits > MAX_BITS) {
      throw new SketchesArgumentException("Number of bits may not exceed " + MAX_BITS + ". Found: " + numBits);
    }

    final int numLongs = (int) Math.ceil(numBits / 64.0);
    numBitsSet_ = 0;
    isDirty_ = false;
    data_ = new long[numLongs];
  }

  // uses the provided array
  BitArray(final long numBitsSet, final long[] data) {
    data_ = data;
    isDirty_ = numBitsSet < 0;
    numBitsSet_ = numBitsSet;
  }

  // reads a serialized image, but the BitArray is not fully self-describing so requires
  // a flag to indicate whether the array is empty
  static BitArray heapify(final Buffer buffer, final boolean isEmpty) {
    final int numLongs = buffer.getInt();
    if (numLongs < 0) {
      throw new SketchesArgumentException("Possible corruption: Must have strictly positive array size. Found: " + numLongs);
    }

    if (isEmpty) {
      return new BitArray((long) numLongs * Long.SIZE);
    }

    buffer.getInt(); // unused

    // will be -1 if dirty
    final long numBitsSet = buffer.getLong();

    final long[] data = new long[numLongs];
    buffer.getLongArray(data, 0, numLongs);
    return new BitArray(numBitsSet, data);
  }

  boolean isEmpty() {
    return getNumBitsSet() == 0 && !isDirty_;
  }

  // queries a single bit in the array
  boolean getBit(final long index) {
    return (data_[(int) index >>> 6] & (1L << index)) != 0 ? true : false;
  }

  // sets a single bit in the array without querying, meaning the method
  // cannot properly track the number of bits set so set isDirty = true
  void setBit(final long index) {
    data_[(int) index >>> 6] |= 1L << index;
    isDirty_ = true;
  }

  // returns existing value of bit
  boolean getAndSetBit(final long index) {
    final int offset = (int) index >>> 6;
    final long mask = 1L << index;
    if ((data_[offset] & mask) != 0) {
      return true; // already seen
    } else {
      data_[offset] |= mask;
      ++numBitsSet_; // increment regardless of isDirty_
      return false; // new set
    }
  }

  // may need to recompute value:
  // O(1) if only getAndSetBit() has been used
  // O(data_.length) if setBit() has ever been used
  long getNumBitsSet() {
    if (isDirty_) {
      numBitsSet_ = 0;
      for (final long val : data_) {
        numBitsSet_ += Long.bitCount(val);
      }
    }
    return numBitsSet_;
  }

  long getCapacity() { return (long) data_.length * Long.SIZE; }

  int getArrayLength() { return data_.length; }

  // applies logical OR
  void union(final BitArray other) {
    if (data_.length != other.data_.length) {
      throw new SketchesArgumentException("Cannot union bit arrays with unequal lengths");
    }

    numBitsSet_ = 0;
    for (int i = 0; i < data_.length; ++i) {
      data_[i] |= other.data_[i];
      numBitsSet_ += Long.bitCount(data_[i]);
    }
    isDirty_ = false;
  }

  // applies logical AND
  void intersect(final BitArray other) {
    if (data_.length != other.data_.length) {
      throw new SketchesArgumentException("Cannot intersect bit arrays with unequal lengths");
    }

    numBitsSet_ = 0;
    for (int i = 0; i < data_.length; ++i) {
      data_[i] &= other.data_[i];
      numBitsSet_ += Long.bitCount(data_[i]);
    }
    isDirty_ = false;
  }

  // applies bitwise inversion
  void invert() {
    if (isDirty_) {
      numBitsSet_ = 0;
      for (int i = 0; i < data_.length; ++i) {
        data_[i] = ~data_[i];
        numBitsSet_ += Long.bitCount(data_[i]);
      }
      isDirty_ = false;
    } else {
      for (int i = 0; i < data_.length; ++i) {
        data_[i] = ~data_[i];
      }
      numBitsSet_ = getCapacity() - numBitsSet_;
    }
  }

  long getSerializedSizeBytes() {
    // We only really need an int for array length but this will keep everything
    // aligned to 8 bytes.
    // Always write array length and numBitsSet, even if empty
    return isEmpty() ? Long.BYTES : Long.BYTES * (2L + data_.length);
  }

  void writeToBuffer(final WritableBuffer wbuf) {
    wbuf.putInt(data_.length);
    wbuf.putInt(0); // unused

    if (!isEmpty()) {
      wbuf.putLong(isDirty_ ? -1 : numBitsSet_);
      wbuf.putLongArray(data_, 0, data_.length);
    }
  }

  // prints the raw BitArray as 0s and 1s, one long per row
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < data_.length; ++i) {
      sb.append(i + ": ")
        .append(printLong(data_[i]))
        .append("\n");
    }
    return sb.toString();
  }

  // prints a long as a series of 0s and 1s as little endian
  private static String printLong(final long val) {
    final StringBuilder sb = new StringBuilder();
    for (int j = 0; j < Long.SIZE; ++j) {
      sb.append((val & (1L << j)) != 0 ? "1" : "0");
      if (j % 8 == 7) { sb.append(" "); }
    }
    return sb.toString();
  }

  // clears the array
  void reset() {
    Arrays.fill(data_, 0);
    numBitsSet_ = 0;
    isDirty_ = false;
  }
}
