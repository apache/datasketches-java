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

 /**
  * This class holds an array of bits suitable for use in a Bloom Filter
  *
  * <p>Rounds the number of bits up to the smallest multiple of 64 (one long)
  * that is not smaller than the specified number.
  */
abstract class BitArray {
  // MAX_BITS using longs, based on array indices being capped at Integer.MAX_VALUE
  protected static final long MAX_BITS = Integer.MAX_VALUE * (long) Long.SIZE;

  protected BitArray() {}

  boolean isEmpty() {
    return !isDirty() && getNumBitsSet() == 0;
  }

  abstract boolean hasMemory();

  abstract boolean isDirect();

  boolean isReadOnly() {
    return false;
  }

  abstract boolean getBit(final long index);

  abstract boolean getAndSetBit(final long index);

  abstract void setBit(final long index);

  abstract long getNumBitsSet();

  abstract void reset();

  abstract long getCapacity();

  abstract int getArrayLength();

  abstract void union(final BitArray other);

  abstract void intersect(final BitArray other);

  abstract void invert();

  // prints the raw BitArray as 0s and 1s, one long per row
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getArrayLength(); ++i) {
      sb.append(i + ": ")
        .append(printLong(getLong(i)))
        .append("\n");
    }
    return sb.toString();
  }

  long getSerializedSizeBytes() {
    // We only really need an int for array length but this will keep everything
    // aligned to 8 bytes.
    // Always write array length and numBitsSet, even if empty
    return Long.BYTES * (isEmpty() ? 2L : (2L + getArrayLength()));
  }

  abstract protected boolean isDirty();

  // used to get a long from the array regardless of underlying storage
  // NOT used to query individual bits
  abstract protected long getLong(final int arrayIndex);

  // used to set a long in the array regardless of underlying storage
  // NOT used to set individual bits
  abstract protected void setLong(final int arrayIndex, final long value);

  // prints a long as a series of 0s and 1s as little endian
  protected static String printLong(final long val) {
    final StringBuilder sb = new StringBuilder();
    for (int j = 0; j < Long.SIZE; ++j) {
      sb.append((val & (1L << j)) != 0 ? "1" : "0");
      if (j % 8 == 7) { sb.append(" "); }
    }
    return sb.toString();
  }

}
