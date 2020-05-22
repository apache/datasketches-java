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

import org.apache.datasketches.memory.Memory;

/**
 * Iterator over the off-heap, Direct tuple sketch of type ArrayOfDoubles (compact or hash table).
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
final class DirectArrayOfDoublesSketchIterator implements ArrayOfDoublesSketchIterator {

  private Memory mem_;
  private int offset_;
  private int numEntries_;
  private int numValues_;
  private int i_;
  private static final int SIZE_OF_KEY_BYTES = 8;
  private static final int SIZE_OF_VALUE_BYTES = 8;

  DirectArrayOfDoublesSketchIterator(final Memory mem, final int offset, final int numEntries,
      final int numValues) {
    mem_ = mem;
    offset_ = offset;
    numEntries_ = numEntries;
    numValues_ = numValues;
    i_ = -1;
  }

  @Override
  public boolean next() {
    i_++;
    while (i_ < numEntries_) {
      if (mem_.getLong(offset_ + ((long) SIZE_OF_KEY_BYTES * i_)) != 0) { return true; }
      i_++;
    }
    return false;
  }

  @Override
  public long getKey() {
    return mem_.getLong(offset_ + ((long) SIZE_OF_KEY_BYTES * i_));
  }

  @Override
  public double[] getValues() {
    if (numValues_ == 1) {
      return new double[] {
        mem_.getDouble(offset_ + ((long) SIZE_OF_KEY_BYTES * numEntries_)
            + ((long) SIZE_OF_VALUE_BYTES * i_)) };
    }
    final double[] array = new double[numValues_];
    mem_.getDoubleArray(offset_ + ((long) SIZE_OF_KEY_BYTES * numEntries_)
        + ((long) SIZE_OF_VALUE_BYTES * i_ * numValues_), array, 0, numValues_);
    return array;
  }

}
