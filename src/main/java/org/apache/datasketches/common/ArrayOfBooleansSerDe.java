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

package org.apache.datasketches.common;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * Methods of serializing and deserializing arrays of Boolean as a bit array.
 *
 * @author Jon Malkin
 */
public class ArrayOfBooleansSerDe extends ArrayOfItemsSerDe<Boolean> {

  /**
   * No argument constructor.
   */
  public ArrayOfBooleansSerDe() { }

  /**
   * Computes number of bytes needed for packed bit encoding of the array of booleans. Rounds
   * partial bytes up to return a whole number of bytes.
   *
   * @param arrayLength Number of items in the array to serialize
   * @return Number of bytes needed to encode the array
   */
  public static int computeBytesNeeded(final int arrayLength) {
    return (arrayLength >>> 3) + ((arrayLength & 0x7) > 0 ? 1 : 0);
  }

  @Override
  public byte[] serializeToByteArray(final Boolean item) {
    Objects.requireNonNull(item, "Item must not be null");
    final byte[] bytes = new byte[1];
    bytes[0] = (item) ? (byte)1 : 0;
    return bytes;
  }

  @Override
  public byte[] serializeToByteArray(final Boolean[] items) {
    Objects.requireNonNull(items, "Items must not be null");
    final int bytesNeeded = computeBytesNeeded(items.length);
    final byte[] bytes = new byte[bytesNeeded];
    final MemorySegment seg = MemorySegment.ofArray(bytes);

    byte val = 0;
    for (int i = 0; i < items.length; ++i) {
      if (items[i]) {
        val |= 0x1 << (i & 0x7);
      }
      if ((i & 0x7) == 0x7) {
        seg.set(JAVA_BYTE, i >>> 3, val);
        val = 0;
      }
    }
    // write out any remaining values (if val=0, still good to be explicit)
    if ((items.length & 0x7) > 0) {
      seg.set(JAVA_BYTE, bytesNeeded - 1, val);
    }
    return bytes;
  }

  @Override
  public Boolean[] deserializeFromMemorySegment(final MemorySegment seg, final int numItems) {
    return deserializeFromMemorySegment(seg, 0, numItems);
  }

  @Override
  public Boolean[] deserializeFromMemorySegment(final MemorySegment seg, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(seg, "MemorySegment must not be null");
    if (numItems <= 0) { return new Boolean[0]; }
    final int numBytes = computeBytesNeeded(numItems);
    Util.checkBounds(offsetBytes, numBytes, seg.byteSize());
    final Boolean[] array = new Boolean[numItems];

    byte srcVal = 0;
    for (int i = 0, b = 0; i < numItems; ++i) {
      if ((i & 0x7) == 0x0) { // should trigger on first iteration
        srcVal = seg.get(JAVA_BYTE, offsetBytes + b++);
      }
      array[i] = ((srcVal >>> (i & 0x7)) & 0x1) == 1;
    }
    return array;
  }

  @Override
  public int sizeOf(final Boolean item) {
    Objects.requireNonNull(item, "Item must not be null");
    return computeBytesNeeded(1);
  }

  @Override //needs to override default due to the bit packing, which must be computed.
  public int sizeOf(final Boolean[] items) {
    Objects.requireNonNull(items, "Item must not be null");
    return computeBytesNeeded(items.length);
  }

  @Override
  public int sizeOf(final MemorySegment seg, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(seg, "MemorySegment must not be null");
    return computeBytesNeeded(numItems);
  }

  @Override
  public String toString(final Boolean item) {
    if (item == null) { return "null"; }
    return item ? "true" : "false";
  }

  @Override
  public Class<Boolean> getClassOfT() { return Boolean.class; }
}
