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
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

public class DirectBitArrayR extends BitArray {
  final static protected long NUM_BITS_OFFSET = Long.BYTES;
  final static protected long DATA_OFFSET = 2L * Long.BYTES;

  final protected int dataLength_;
  final protected WritableMemory wmem_; // for inheritance; we won't write to it
  protected long numBitsSet_; // could be final here but writable direct will update it

  protected DirectBitArrayR(final int dataLength, final long storedNumBitsSet, final Memory mem) {
    super();

    dataLength_ = dataLength;
    wmem_ = (WritableMemory) mem;

    if (storedNumBitsSet == -1) {
      numBitsSet_ = 0;
      for (int i = 0; i < dataLength_; ++i) {
        numBitsSet_ += Long.bitCount(wmem_.getLong(DATA_OFFSET + (i << 3)));
      }
    } else {
      numBitsSet_ = storedNumBitsSet;
    }
  }

  // assumes we have a region with only the portion of Memory
  // the BitArray cares about
  static DirectBitArrayR wrap(final Memory mem, final boolean isEmpty) {
    final int arrayLength = mem.getInt(0);
    final long storedNumBitsSet = isEmpty ? 0L : mem.getLong(NUM_BITS_OFFSET);

    if (arrayLength < 0) {
      throw new SketchesArgumentException("Possible corruption: Serialized image indicates non-positive array length");
    }

    // required capacity is arrayLength plus room for
    // arrayLength (in longs) and numBitsSet
    if (storedNumBitsSet != 0 && mem.getCapacity() < arrayLength + 2) {
      throw new SketchesArgumentException("Memory capacity insufficient for Bloom Filter. Needed: "
        + (arrayLength + 2) + " , found: " + mem.getCapacity());
    }
    return new DirectBitArrayR(arrayLength, storedNumBitsSet, mem);
  }

  @Override
  long getCapacity() {
    return (long) dataLength_ * Long.SIZE;
  }

  @Override
  long getNumBitsSet() {
    return numBitsSet_;
  }

  @Override
  protected boolean isDirty() {
    // read-only so necessarily false
    return false;
  }

  @Override
  int getArrayLength() {
    return dataLength_;
  }

  @Override
  boolean getBit(final long index) {
    if (isEmpty()) { return false; }
    return (wmem_.getByte(DATA_OFFSET + ((int) index >>> 3)) & (1 << (index & 0x7))) != 0;
  }

  @Override
  protected long getLong(final int arrayIndex) {
    if (isEmpty()) { return 0L; }
    return wmem_.getLong(DATA_OFFSET + (arrayIndex << 3));
  }

  @Override
  public boolean hasMemory() {
    return (wmem_ != null);
  }

  @Override
  public boolean isDirect() {
    return (wmem_ != null) ? wmem_.isDirect() : false;
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  void reset() {
    throw new SketchesReadOnlyException("Attempt to call reset() on read-only memory");
  }

  @Override
  void setBit(final long index) {
    throw new SketchesReadOnlyException("Attempt to call setBit() on read-only memory");
  }

  @Override
  boolean getAndSetBit(final long index) {
    throw new SketchesReadOnlyException("Attempt to call getAndSetBit() on read-only memory");
  }

  @Override
  void intersect(final BitArray other) {
    throw new SketchesReadOnlyException("Attempt to call intersect() on read-only memory");
  }

  @Override
  void union(final BitArray other) {
    throw new SketchesReadOnlyException("Attempt to call union() on read-only memory");
  }

  @Override
  void invert() {
    throw new SketchesReadOnlyException("Attempt to call invert() on read-only memory");
  }

  @Override
  protected void setLong(final int arrayIndex, final long value) {
    throw new SketchesReadOnlyException("Attempt to call setLong() on read-only memory");
  }
}
