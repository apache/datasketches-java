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

package org.apache.datasketches.filters.common;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.WritableMemory;

public final class DirectBitArray extends DirectBitArrayR {

  public DirectBitArray(final int dataLength, final long storedNumBitsSet, final WritableMemory wmem) {
    super(dataLength, 0, wmem); // we'll set numBitsSet_ ourselves so pass 0

    // can recompute later if needed
    numBitsSet_ = storedNumBitsSet;
  }

  public DirectBitArray(final int dataLength, final WritableMemory wmem) {
    super(dataLength, 0, wmem);

    wmem_.putInt(0, dataLength_);
    setNumBitsSet(0);
    wmem_.clear(DATA_OFFSET, (long) dataLength_ * Long.BYTES);
  }

  public static DirectBitArray initialize(final long numBits, final WritableMemory wmem) {
    if (numBits <= 0) {
      throw new SketchesArgumentException("Number of bits must be strictly positive. Found: " + numBits);
    }
    if (numBits > MAX_BITS) {
      throw new SketchesArgumentException("Maximum size of a single filter is " + MAX_BITS + " + bits. "
              + "Requested: " + numBits);
    }

    final int arrayLength = (int) Math.ceil(numBits / 64.0); // we know it'll fit in an int based on above checks
    final long requiredBytes = (2L + arrayLength) * Long.BYTES;
    if (wmem.getCapacity() < requiredBytes) {
      throw new SketchesArgumentException("Provided WritableMemory too small for requested array length. "
        + "Requited: " + requiredBytes + ", provided capcity: " + wmem.getCapacity());
    }

    return new DirectBitArray(arrayLength, wmem);
  }

  public static DirectBitArray writableWrap(final WritableMemory mem, final boolean isEmpty) {
    final int arrayLength = mem.getInt(0);
    final long storedNumBitsSet = isEmpty ? 0L : mem.getLong(NUM_BITS_OFFSET);

    if (arrayLength * (long) Long.SIZE > MAX_BITS) {
      throw new SketchesArgumentException("Possible corruption: Serialized image indicates array beyond maximum filter capacity");
    }

    // if empty cannot wrap as writable
    if (isEmpty) {
      throw new SketchesArgumentException("Cannot wrap an empty filter for writing as there is no backing data array");
    }

    // required capacity is arrayLength plus room for
    // arrayLength (in longs) and numBitsSet
    if (storedNumBitsSet != 0 && mem.getCapacity() < arrayLength + 2) {
      throw new SketchesArgumentException("Memory capacity insufficient for Bloom Filter. Needed: "
        + (arrayLength + 2) + " , found: " + mem.getCapacity());
    }
    return new DirectBitArray(arrayLength, storedNumBitsSet, mem);
  }

  @Override
  public long getNumBitsSet() {
    // update numBitsSet and store in array
    if (isDirty()) {
      numBitsSet_ = 0;
      for (int i = 0; i < dataLength_; ++i) {
        numBitsSet_ += Long.bitCount(getLong(i));
      }
      wmem_.putLong(NUM_BITS_OFFSET, numBitsSet_);
    }

    return numBitsSet_;
  }

  @Override
  public boolean isDirty() {
    return numBitsSet_ == -1;
  }

  @Override
  public boolean getBit(final long index) {
    return (wmem_.getByte(DATA_OFFSET + ((int) index >>> 3)) & (1 << (index & 0x7))) != 0;
  }

  @Override
  public long getLong(final int arrayIndex) {
    return wmem_.getLong(DATA_OFFSET + (arrayIndex << 3));
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public void reset() {
    setNumBitsSet(0);
    wmem_.clear(DATA_OFFSET, (long) dataLength_ * Long.BYTES);
  }

  @Override
  public void setBit(final long index) {
    final long memoryOffset = DATA_OFFSET + ((int) index >>> 3);
    final byte val = wmem_.getByte(memoryOffset);
    wmem_.putByte(memoryOffset, (byte) (val | (1 << (index & 0x07))));
    setNumBitsSet(-1); // mark dirty
  }

  @Override
  public void clearBit(final long index) {
    final long memoryOffset = DATA_OFFSET + ((int) index >>> 3);
    final byte val = wmem_.getByte(memoryOffset);
    wmem_.putByte(memoryOffset, (byte) (val & ~(1 << (index & 0x07))));
    setNumBitsSet(-1); // mark dirty
  }

  @Override
  public void assignBit(final long index, final boolean value) {
    if (value) {
      setBit(index);
    } else {
      clearBit(index);
    }
  }

  @Override
  public void setBits(final long index, final int numBits, final long bits) {
    if (numBits < 0 || numBits > 64) {
      throw new SketchesArgumentException("numBits must be between 0 and 64 (inclusive)");
    } else if (index + numBits > getCapacity()) {
      throw new SketchesArgumentException("End of range exceeds capacity");
    }

    // TODO: since Memory provides byte offsets even when reading a long, we can be sure
    // that the result always fits in a single long. We can potentially optimize this, but
    // need to handle cases where a long would read beyond the end of the Memory.

    final long endBit = index + numBits - 1;

    // these are indices into a long[] array, need to adjust to byte offsets
    // when calling wmem_.getLong()
    final int fromIndex = (int) index >>> 6;
    final int toIndex = (int) endBit >>> 6;

    setNumBitsSet(-1); // mark dirty
    final long fromOffset = index & 0x3F;
    final long toOffset = endBit & 0x3F;

    // within a single long
    if (fromIndex == toIndex) {
      final long toMask = (toOffset == 63) ? -1L : (1L << (toOffset + 1)) - 1L;
      final long fromMask = (1L << fromOffset) - 1L;
      final long mask = toMask - fromMask;
      final long maskedVal = wmem_.getLong(DATA_OFFSET + (fromIndex << 3)) & ~mask;
      wmem_.putLong(DATA_OFFSET + (fromIndex << 3), maskedVal | ((bits << fromOffset) & mask));
      return;
    }

    // spans longs, need to set bits in two longs
    final long splitBit = Long.SIZE - (fromOffset);
    final long fromMask = -1L - ((1L << fromOffset) - 1);
    final long toMask = (1L << (toOffset + 1)) - 1;

    final long maskedFromVal = wmem_.getLong(DATA_OFFSET + (fromIndex << 3)) & ~fromMask;
    final long maskedToVal   = wmem_.getLong(DATA_OFFSET + (toIndex << 3))   & ~toMask;

    wmem_.putLong(DATA_OFFSET + (fromIndex << 3), maskedFromVal | ((bits << fromOffset) & fromMask));
    wmem_.putLong(DATA_OFFSET + (toIndex << 3),   maskedToVal   | ((bits >>> splitBit) & toMask));
  }

  @Override
  public boolean getAndSetBit(final long index) {
    final long memoryOffset = DATA_OFFSET + ((int) index >>> 3);
    final byte mask = (byte) (1 << (index & 0x07));
    final byte val = wmem_.getByte(memoryOffset);
    if ((val & mask) != 0) {
      return true; // already seen
    } else {
      wmem_.setBits(memoryOffset, (byte) (val | mask));
      if (!isDirty()) { setNumBitsSet(numBitsSet_ + 1); }
      return false; // new set
    }
  }

  @Override
  public void intersect(final BitArray other) {
    if (getCapacity() != other.getCapacity()) {
      throw new SketchesArgumentException("Cannot intersect bit arrays with unequal lengths");
    }

    numBitsSet_ = 0;
    for (int i = 0; i < dataLength_; ++i) {
      final long val = getLong(i) & other.getLong(i);
      numBitsSet_ += Long.bitCount(val);
      setLong(i, val);
    }
    wmem_.putLong(NUM_BITS_OFFSET, numBitsSet_);
  }

  @Override
  public void union(final BitArray other) {
    if (getCapacity() != other.getCapacity()) {
      throw new SketchesArgumentException("Cannot intersect bit arrays with unequal lengths");
    }

    numBitsSet_ = 0;
    for (int i = 0; i < dataLength_; ++i) {
      final long val = getLong(i) | other.getLong(i);
      numBitsSet_ += Long.bitCount(val);
      setLong(i, val);
    }
    wmem_.putLong(NUM_BITS_OFFSET, numBitsSet_);
  }

  @Override
  public void invert() {
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
    wmem_.putLong(NUM_BITS_OFFSET, numBitsSet_);
  }

  @Override
  void setLong(final int arrayIndex, final long value) {
    wmem_.putLong(DATA_OFFSET + (arrayIndex << 3), value);
  }

  private final void setNumBitsSet(final long numBitsSet) {
    numBitsSet_ = numBitsSet;
    wmem_.putLong(NUM_BITS_OFFSET, numBitsSet);
  }
}
