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

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;

/**
 * This class can maintain the BitArray object off-heap.
 */
public class DirectBitArrayR extends BitArray {
  final static protected long NUM_BITS_OFFSET = Long.BYTES;
  final static protected long DATA_OFFSET = 2L * Long.BYTES;

  final protected int dataLength_;
  final protected MemorySegment wseg_; // for inheritance; we won't write to it
  protected long numBitsSet_; // could be final here but DirectBitArray will update it

  protected DirectBitArrayR(final int dataLength, final long storedNumBitsSet, final MemorySegment seg) {
    dataLength_ = dataLength;
    wseg_ = seg;

    if (storedNumBitsSet == -1) {
      numBitsSet_ = 0;
      for (int i = 0; i < dataLength_; ++i) {
        numBitsSet_ += Long.bitCount(wseg_.get(JAVA_LONG_UNALIGNED, DATA_OFFSET + (i << 3)));
      }
    } else {
      numBitsSet_ = storedNumBitsSet;
    }
  }

  // assumes we have a slice with only the portion of the MemorySegment the BitArray cares about
  static DirectBitArrayR wrap(final MemorySegment seg, final boolean isEmpty) {
    final int arrayLength = seg.get(JAVA_INT_UNALIGNED, 0L);
    final long storedNumBitsSet = isEmpty ? 0L : seg.get(JAVA_LONG_UNALIGNED, NUM_BITS_OFFSET);

    if (arrayLength < 0) {
      throw new SketchesArgumentException("Possible corruption: Serialized image indicates non-positive array length");
    }

    // required capacity is arrayLength plus room for
    // arrayLength (in longs) and numBitsSet
    if ((storedNumBitsSet != 0) && (seg.byteSize() < (arrayLength + 2))) {
      throw new SketchesArgumentException("MemorySegment capacity is insufficient for Bloom Filter. Needs: "
        + (arrayLength + 2) + " , found: " + seg.byteSize());
    }
    return new DirectBitArrayR(arrayLength, storedNumBitsSet, seg);
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
  boolean getBit(final long index) { //index into an array of bytes
    if (isEmpty()) { return false; }
    return (wseg_.get(JAVA_BYTE, DATA_OFFSET + ((int) index >>> 3)) & (1 << (index & 0x7))) != 0;
  }

  @Override
  protected long getLong(final int arrayIndex) {
    if (isEmpty()) { return 0L; }
    return wseg_.get(JAVA_LONG_UNALIGNED, DATA_OFFSET + (arrayIndex << 3));
  }

  @Override
  public boolean hasMemorySegment() {
    return (wseg_ != null);
  }

  @Override
  public boolean isOffHeap() {
    return hasMemorySegment() && wseg_.isNative();
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return MemorySegmentStatus.isSameResource(wseg_, that);
  }

  @Override
  void reset() {
    throw new SketchesReadOnlyException("Attempt to call reset() on read-only MemorySegment");
  }

  @Override
  void setBit(final long index) {
    throw new SketchesReadOnlyException("Attempt to call setBit() on read-only MemorySegment");
  }

  @Override
  boolean getAndSetBit(final long index) {
    throw new SketchesReadOnlyException("Attempt to call getAndSetBit() on read-only MemorySegment");
  }

  @Override
  void intersect(final BitArray other) {
    throw new SketchesReadOnlyException("Attempt to call intersect() on read-only MemorySegment");
  }

  @Override
  void union(final BitArray other) {
    throw new SketchesReadOnlyException("Attempt to call union() on read-only MemorySegment");
  }

  @Override
  void invert() {
    throw new SketchesReadOnlyException("Attempt to call invert() on read-only MemorySegment");
  }

  @Override
  protected void setLong(final int arrayIndex, final long value) {
    throw new SketchesReadOnlyException("Attempt to call setLong() on read-only MemorySegment");
  }
}
