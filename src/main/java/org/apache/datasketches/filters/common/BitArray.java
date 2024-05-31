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

import static org.apache.datasketches.common.Util.LS;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
  * This class holds an array of bits and should be suitable for use in
  * the various membership filters. The representation is not compressed and
  * is designed to fit in a single array, meaning that the maximum number
  * of bits is limited by the maximize size of an array of longs in Java.
  *
  * <p>Rounds the number of bits up to the smallest multiple of 64 (one long)
  * that is not smaller than the specified number.
  */
public abstract class BitArray {

  /**
   * The maximum number of bits that can be represented using longs,
   * based on array indices being capped at Integer.MAX_VALUE
   * and allowing room for encoding both the size and the number of bits set.
   */
  protected static final long MAX_BITS = (Integer.MAX_VALUE - 1) * (long) Long.SIZE;

  /**
   * Constructs a new BitArray.
   */
  BitArray() {}

  /**
   * Creates a BitArray from a given Buffer.
   *
   * @param mem The Buffer to heapify.
   * @param isEmpty Indicates whether the BitArray is empty.
   * @return The heapified BitArray.
   */
  public static BitArray heapify(final Buffer mem, final boolean isEmpty) {
    return HeapBitArray.heapify(mem, isEmpty);
  }

  /**
   * Creates a BitArray from a given Memory.
   *
   * @param mem The Memory to wrap.
   * @param isEmpty Indicates whether the BitArray is empty.
   * @return The wrapped BitArray.
   */
  public static BitArray wrap(final Memory mem, final boolean isEmpty) {
    return DirectBitArrayR.wrap(mem, isEmpty);
  }

  /**
   * Creates a writable BitArray from a given WritableMemory.
   *
   * @param wmem The WritableMemory to wrap.
   * @param isEmpty Indicates whether the BitArray is empty.
   * @return The writable wrapped BitArray.
   */
  public static BitArray writableWrap(final WritableMemory wmem, final boolean isEmpty) {
    return DirectBitArray.writableWrap(wmem, isEmpty);
  }

  /**
   * Checks if the BitArray is empty.
   *
   * @return True if the BitArray is empty, false otherwise.
   */
  public boolean isEmpty() {
    return !isDirty() && getNumBitsSet() == 0;
  }

  /**
   * Checks if the BitArray has a backing Memory.
   *
   * @return True if the BitArray has a backing Memory, false otherwise.
   */
  public abstract boolean hasMemory();

  /**
   * Checks if the BitArray is direct.
   *
   * @return True if the BitArray is direct, false otherwise.
   */
  public abstract boolean isDirect();

  /**
   * Checks if the BitArray is read-only.
   *
   * @return True if the BitArray is read-only, false otherwise.
   */
  public abstract boolean isReadOnly();

  /**
   * Gets the value of a bit at the specified index.
   *
   * @param index The index of the bit.
   * @return The value of the bit at the specified index.
   */
  public abstract boolean getBit(final long index);

  /**
   * Gets the a specified number of bits starting at the given index. Limited
   * to a single long (64 bits).
   *
   * @param index The starting index.
   * @param numBits The number of bits to return.
   * @return The value of the requested bits, starting at bit 0 of the result.
   */
  public abstract long getBits(final long index, final int numBits);

  /**
   * Gets the value of a bit at the specified index and sets it to true.
   *
   * @param index The index of the bit.
   * @return The previous value of the bit at the specified index.
   */
  public abstract boolean getAndSetBit(final long index);

  /**
   * Assigns the value of a bit at the specified index to true.
   *
   * @param index The index of the bit.
   */
  public abstract void setBit(final long index);

  /**
   * Assigns the value of a bit at the specified index to false.
   *
   * @param index The index of the bit.
   */
  public abstract void clearBit(final long index);

  /**
   * Assigns the given value of a bit at the specified index.
   *
   * @param index The index of the bit.
   * @param value The value to set the bit to.
   */
  public abstract void assignBit(final long index, final boolean value);

  /**
  /**
   * Sets {@code numBits} starting from {@code index} to the specified value.
   * Limited to a single long (64 bits).
   *
   * @param index the starting index of the range (inclusive)
   * @param numBits the number of bits to write
   * @param bits the value to set the bits to, starting with bit 0
   */
  public abstract void setBits(final long index, final int numBits, final long bits);

  /**
   * Gets the number of bits that are set to true in the BitArray.
   *
   * @return The number of bits set to true.
   */
  public abstract long getNumBitsSet();

  /**
   * Resets the BitArray, setting all bits to false.
   */
  public abstract void reset();

  /**
   * Gets the capacity of the BitArray in bits.
   *
   * @return The capacity of the BitArray in bits
   */
  public abstract long getCapacity();

  /**
   * Gets the length of the underlying array in longs.
   *
   * @return The length of the underlying array in longs.
   */
  public abstract int getArrayLength();

  /**
   * Performs a union operation with another BitArray.
   *
   * @param other The other BitArray to perform the union with.
   */
  public abstract void union(final BitArray other);

  /**
   * Performs an intersection operation with another BitArray.
   *
   * @param other The other BitArray to perform the intersection with.
   */
  public abstract void intersect(final BitArray other);

  /**
   * Inverts the BitArray, flipping all bits.
   */
  public abstract void invert();

  /**
   * Returns a string representation of the BitArray.
   *
   * @return A string representation of the BitArray.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getArrayLength(); ++i) {
      sb.append(i + ": ")
        .append(printLong(getLong(i)))
        .append(LS);
    }
    return sb.toString();
  }

  /**
   * Gets the serialized size of the BitArray in bytes.
   *
   * @return The serialized size of the BitArray in bytes.
   */
  public long getSerializedSizeBytes() {
    // We only really need an int for array length but this will keep everything
    // aligned to 8 bytes.
    // Always write array length, but write numBitsSet only if empty
    return Long.BYTES * (isEmpty() ? 1L : (2L + getArrayLength()));
  }

  /**
   * Gets the serialized size of a non-empty BitArray of the specified size in bytes.
   *
   * @param numBits The number of bits in the BitArray.
   * @return The serialized size of the BitArray in bytes.
   * @throws SketchesArgumentException If the requested number of bits is not strictly positive
   *                                   or exceeds the maximum allowed.
   */
  public static long getSerializedSizeBytes(final long numBits) {
    if (numBits <= 0) {
      throw new SketchesArgumentException("Requested number of bits must be strictly positive");
    }
    if (numBits > MAX_BITS) {
      throw new SketchesArgumentException("Requested number of bits exceeds maximum allowed. "
        + "Requested: " + numBits + ", maximum: " + MAX_BITS);
    }
    final int numLongs = (int) Math.ceil(numBits / 64.0);
    return Long.BYTES * (numLongs + 2L);
  }

  /**
   * Checks if the BitArray has changes not reflected in state variables.
   *
   * @return True if the BitArray is dirty, false otherwise.
   */
  abstract boolean isDirty();

  /**
   * Gets the long value at the specified array index.
   *
   * @param arrayIndex The index of the long value in the array.
   * @return The long value at the specified array index.
   */
  abstract long getLong(final int arrayIndex);

  /**
   * Sets the long value at the specified array index.
   *
   * @param arrayIndex The index of the long value in the array.
   * @param value The value to set the long to.
   */
  abstract void setLong(final int arrayIndex, final long value);

  /**
   * Returns a string representation of a long value as a series of 0s and 1s (little endian).
   *
   * @param val The long value to print.
   * @return A string representation of the long value.
   */
  public static String printLong(final long val) {
    final StringBuilder sb = new StringBuilder();
    for (int j = 0; j < Long.SIZE; ++j) {
      sb.append((val & (1L << j)) != 0 ? "1" : "0");
      if (j % 8 == 7) { sb.append(" "); }
    }
    return sb.toString();
  }

}
