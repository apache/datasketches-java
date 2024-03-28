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
final class HeapBitArray extends BitArray {
  private long numBitsSet_;  // if -1, need to recompute value
  private boolean isDirty_;
  final private long[] data_;

  // creates an array of a given size
  HeapBitArray(final long numBits) {
    super();

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
  HeapBitArray(final long numBitsSet, final long[] data) {
    super();

    data_ = data;
    isDirty_ = numBitsSet < 0;
    numBitsSet_ = numBitsSet;
  }

  // reads a serialized image, but the BitArray is not fully self-describing so requires
  // a flag to indicate whether the array is empty
  static HeapBitArray heapify(final Buffer buffer, final boolean isEmpty) {
    final int numLongs = buffer.getInt();
    if (numLongs < 0) {
      throw new SketchesArgumentException("Possible corruption: Must have strictly positive array size. Found: " + numLongs);
    }

    if (isEmpty) {
      return new HeapBitArray((long) numLongs * Long.SIZE);
    }

    buffer.getInt(); // unused

    // will be -1 if dirty
    final long numBitsSet = buffer.getLong();

    final long[] data = new long[numLongs];
    buffer.getLongArray(data, 0, numLongs);
    return new HeapBitArray(numBitsSet, data);
  }

  @Override
  protected boolean isDirty() {
    return isDirty_;
  }

  @Override
  boolean hasMemory() {
    return false;
  }

  @Override
  boolean isDirect() {
    return false;
  }

  // queries a single bit in the array
  @Override
  boolean getBit(final long index) {
    return (data_[(int) index >>> 6] & (1L << index)) != 0 ? true : false;
  }

  // sets a single bit in the array without querying, meaning the method
  // cannot properly track the number of bits set so set isDirty = true
  @Override
  void setBit(final long index) {
    data_[(int) index >>> 6] |= 1L << index;
    isDirty_ = true;
  }

  // returns existing value of bit
  @Override
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
  @Override
  long getNumBitsSet() {
    if (isDirty_) {
      numBitsSet_ = 0;
      for (final long val : data_) {
        numBitsSet_ += Long.bitCount(val);
      }
    }
    return numBitsSet_;
  }

  @Override
  long getCapacity() { return (long) data_.length * Long.SIZE; }

  @Override
  int getArrayLength() { return data_.length; }

  // applies logical OR
  @Override
  void union(final BitArray other) {
    if (getCapacity() != other.getCapacity()) {
      throw new SketchesArgumentException("Cannot union bit arrays with unequal lengths");
    }

    numBitsSet_ = 0;
    for (int i = 0; i < data_.length; ++i) {
      final long val = data_[i] | other.getLong(i);
      numBitsSet_ += Long.bitCount(val);
      data_[i] = val;
    }
    isDirty_ = false;
  }

  // applies logical AND
  @Override
  void intersect(final BitArray other) {
    if (getCapacity() != other.getCapacity()) {
      throw new SketchesArgumentException("Cannot intersect bit arrays with unequal lengths");
    }

    numBitsSet_ = 0;
    for (int i = 0; i < data_.length; ++i) {
      final long val = data_[i] & other.getLong(i);
      numBitsSet_ += Long.bitCount(val);
      data_[i] = val;
    }
    isDirty_ = false;
  }

  // applies bitwise inversion
  @Override
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

  void writeToBuffer(final WritableBuffer wbuf) {
    wbuf.putInt(data_.length);
    wbuf.putInt(0); // unused

    if (!isEmpty()) {
      wbuf.putLong(isDirty_ ? -1 : numBitsSet_);
      wbuf.putLongArray(data_, 0, data_.length);
    }
  }

  @Override
  protected long getLong(final int arrayIndex) {
    return data_[arrayIndex];
  }

  @Override
  protected void setLong(final int arrayIndex, final long value) {
    data_[arrayIndex] = value;
  }

  // clears the array
  @Override
  void reset() {
    Arrays.fill(data_, 0);
    numBitsSet_ = 0;
    isDirty_ = false;
  }
}
