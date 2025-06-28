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

package org.apache.datasketches.tuple.arrayofdoubles;

import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

import java.lang.foreign.MemorySegment;

/**
 * Iterator over the off-heap, Direct tuple sketch of type ArrayOfDoubles (compact or hash table).
 *
 * <p>This implementation uses data in a given MemorySegment that is owned and managed by the caller.
 * This MemorySegment can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
final class DirectArrayOfDoublesSketchIterator implements ArrayOfDoublesSketchIterator {

  private MemorySegment seg_;
  private int offset_;
  private int numEntries_;
  private int numValues_;
  private int i_;
  private static final int SIZE_OF_KEY_BYTES = 8;
  private static final int SIZE_OF_VALUE_BYTES = 8;

  DirectArrayOfDoublesSketchIterator(final MemorySegment seg, final int offset, final int numEntries,
      final int numValues) {
    seg_ = seg;
    offset_ = offset;
    numEntries_ = numEntries;
    numValues_ = numValues;
    i_ = -1;
  }

  @Override
  public boolean next() {
    i_++;
    while (i_ < numEntries_) {
      final long off = offset_ + ((long) SIZE_OF_KEY_BYTES * i_);
      if (seg_.get(JAVA_LONG_UNALIGNED, off) != 0) { return true; }
      i_++;
    }
    return false;
  }

  @Override
  public long getKey() {
    final long off = offset_ + ((long) SIZE_OF_KEY_BYTES * i_);
    return seg_.get(JAVA_LONG_UNALIGNED, off);
  }

  @Override
  public double[] getValues() {
    long off;
    if (numValues_ == 1) {
      off = offset_ + ((long) SIZE_OF_KEY_BYTES * numEntries_) + ((long) SIZE_OF_VALUE_BYTES * i_);
      return new double[] { seg_.get(JAVA_DOUBLE_UNALIGNED, off) };
    }
    final double[] array = new double[numValues_];
    off = offset_ + ((long) SIZE_OF_KEY_BYTES * numEntries_) + ((long) SIZE_OF_VALUE_BYTES * i_ * numValues_);
    MemorySegment.copy(seg_, JAVA_DOUBLE_UNALIGNED, off, array, 0, numValues_);
    return array;
  }

}
