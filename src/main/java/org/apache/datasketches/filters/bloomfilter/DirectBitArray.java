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
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.Util.clear;
import static org.apache.datasketches.common.Util.setBits;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;

final class DirectBitArray extends DirectBitArrayR {

  DirectBitArray(final int dataLength, final long storedNumBitsSet, final MemorySegment wseg) {
    super(dataLength, 0, wseg); // we'll set numBitsSet_ ourselves so pass 0

    // can recompute later if needed
    numBitsSet_ = storedNumBitsSet;
  }

  DirectBitArray(final int dataLength, final MemorySegment wseg) {
    super(dataLength, 0, wseg);

    wseg_.set(JAVA_INT_UNALIGNED, 0, dataLength_);
    setNumBitsSet(0);
    clear(wseg_, DATA_OFFSET, (long) dataLength_ * Long.BYTES);
  }

  static DirectBitArray initialize(final long numBits, final MemorySegment wseg) {
    if (numBits <= 0) {
      throw new SketchesArgumentException("Number of bits must be strictly positive. Found: " + numBits);
    }
    if (numBits > MAX_BITS) {
      throw new SketchesArgumentException("Maximum size of a single filter is " + MAX_BITS + " + bits. "
              + "Requested: " + numBits);
    }

    final int arrayLength = (int) Math.ceil(numBits / 64.0); // we know it'll fit in an int based on above checks
    final long requiredBytes = (2L + arrayLength) * Long.BYTES;
    if (wseg.byteSize() < requiredBytes) {
      throw new SketchesArgumentException("Provided MemorySegment too small for requested array length. "
        + "Requited: " + requiredBytes + ", provided capcity: " + wseg.byteSize());
    }

    return new DirectBitArray(arrayLength, wseg);
  }

  static DirectBitArray writableWrap(final MemorySegment seg, final boolean isEmpty) {
    final int arrayLength = seg.get(JAVA_INT_UNALIGNED, 0);
    final long storedNumBitsSet = isEmpty ? 0L : seg.get(JAVA_LONG_UNALIGNED, NUM_BITS_OFFSET);

    if ((arrayLength * (long) Long.SIZE) > MAX_BITS) {
      throw new SketchesArgumentException("Possible corruption: Serialized image indicates array beyond maximum filter capacity");
    }

    // if empty cannot wrap as writable
    if (isEmpty) {
      throw new SketchesArgumentException("Cannot wrap an empty filter for writing as there is no backing data array");
    }

    // required capacity is arrayLength plus room for
    // arrayLength (in longs) and numBitsSet
    if ((storedNumBitsSet != 0) && (seg.byteSize() < (arrayLength + 2))) {
      throw new SketchesArgumentException("MemorySegment capacity is insufficient for Bloom Filter. Needs: "
        + (arrayLength + 2) + " , found: " + seg.byteSize());
    }
    return new DirectBitArray(arrayLength, storedNumBitsSet, seg);
  }

  @Override
  long getNumBitsSet() {
    // update numBitsSet and store in array
    if (isDirty()) {
      numBitsSet_ = 0;
      for (int i = 0; i < dataLength_; ++i) {
        numBitsSet_ += Long.bitCount(getLong(i));
      }
      wseg_.set(JAVA_LONG_UNALIGNED, NUM_BITS_OFFSET, numBitsSet_);
    }

    return numBitsSet_;
  }

  @Override
  protected boolean isDirty() {
    return numBitsSet_ == -1;
  }

  @Override
  boolean getBit(final long index) {
    return (wseg_.get(JAVA_BYTE, DATA_OFFSET + ((int) index >>> 3)) & (1 << (index & 0x7))) != 0;
  }

  @Override
  protected long getLong(final int arrayIndex) {
    return wseg_.get(JAVA_LONG_UNALIGNED, DATA_OFFSET + (arrayIndex << 3));
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  void reset() {
    setNumBitsSet(0);
    clear(wseg_, DATA_OFFSET, (long) dataLength_ * Long.BYTES);
  }

  @Override
  void setBit(final long index) {
    final long segmentOffset = DATA_OFFSET + ((int) index >>> 3);
    final byte val = wseg_.get(JAVA_BYTE, segmentOffset);
    setBits(wseg_, segmentOffset, (byte) (val | (1 << (index & 0x07))));
    setNumBitsSet(-1); // mark dirty
  }

  @Override
  boolean getAndSetBit(final long index) {
    final long segmentOffset = DATA_OFFSET + ((int) index >>> 3);
    final byte mask = (byte) (1 << (index & 0x07));
    final byte val = wseg_.get(JAVA_BYTE, segmentOffset);
    if ((val & mask) != 0) {
      return true; // already seen
    } else {
      setBits(wseg_, segmentOffset, (byte) (val | mask));
      if (!isDirty()) { setNumBitsSet(numBitsSet_ + 1); }
      return false; // new set
    }
  }

  @Override
  void intersect(final BitArray other) {
    if (getCapacity() != other.getCapacity()) {
      throw new SketchesArgumentException("Cannot intersect bit arrays with unequal lengths");
    }

    numBitsSet_ = 0;
    for (int i = 0; i < dataLength_; ++i) {
      final long val = getLong(i) & other.getLong(i);
      numBitsSet_ += Long.bitCount(val);
      setLong(i, val);
    }
    wseg_.set(JAVA_LONG_UNALIGNED, NUM_BITS_OFFSET, numBitsSet_);
  }

  @Override
  void union(final BitArray other) {
    if (getCapacity() != other.getCapacity()) {
      throw new SketchesArgumentException("Cannot intersect bit arrays with unequal lengths");
    }

    numBitsSet_ = 0;
    for (int i = 0; i < dataLength_; ++i) {
      final long val = getLong(i) | other.getLong(i);
      numBitsSet_ += Long.bitCount(val);
      setLong(i, val);
    }
    wseg_.set(JAVA_LONG_UNALIGNED, NUM_BITS_OFFSET, numBitsSet_);
  }

  @Override
  void invert() {
    if (isDirty()) {
      numBitsSet_ = 0;
      for (int i = 0; i < dataLength_; ++i) {
        final long val = ~getLong(i);
        setLong(i, val);
        numBitsSet_ += Long.bitCount(val);
      }
    } else {
      for (int i = 0; i < dataLength_; ++i) {
        setLong(i, ~getLong(i));
      }
      numBitsSet_ = getCapacity() - numBitsSet_;
    }
    wseg_.set(JAVA_LONG_UNALIGNED, NUM_BITS_OFFSET, numBitsSet_);
  }

  @Override
  protected void setLong(final int arrayIndex, final long value) {
    wseg_.set(JAVA_LONG_UNALIGNED, DATA_OFFSET + (arrayIndex << 3), value);
  }

  private void setNumBitsSet(final long numBitsSet) {
    numBitsSet_ = numBitsSet;
    wseg_.set(JAVA_LONG_UNALIGNED, NUM_BITS_OFFSET, numBitsSet);
  }
}
