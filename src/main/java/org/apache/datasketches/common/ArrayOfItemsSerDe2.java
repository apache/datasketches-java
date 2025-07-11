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

import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * Base class for serializing and deserializing custom types.
 * @param <T> Type of item
 *
 * @author Alexander Saydakov
 */
public abstract class ArrayOfItemsSerDe2<T> {

  /**
   * Serialize a single unserialized item to a byte array.
   *
   * @param item the item to be serialized
   * @return serialized representation of the given item
   */
  public abstract byte[] serializeToByteArray(T item);

  /**
   * Serialize an array of unserialized items to a byte array of contiguous serialized items.
   *
   * @param items array of items to be serialized
   * @return contiguous, serialized representation of the given array of unserialized items
   */
  public abstract byte[] serializeToByteArray(T[] items);

  /**
   * Deserialize a contiguous sequence of serialized items from the given MemorySegment
   * starting at a MemorySegment offset of zero and extending numItems.
   *
   * @param seg MemorySegment containing a contiguous sequence of serialized items
   * @param numItems number of items in the contiguous serialized sequence.
   * @return array of deserialized items
   * @see #deserializeFromMemorySegment(MemorySegment, long, int)
   */
  public T[] deserializeFromMemorySegment(final MemorySegment seg, final int numItems) {
    return deserializeFromMemorySegment(seg, 0, numItems);
  }

  /**
   * Deserialize a contiguous sequence of serialized items from the given MemorySegment
   * starting at the given MemorySegment <i>offsetBytes</i> and extending numItems.
   *
   * @param seg MemorySegment containing a contiguous sequence of serialized items
   * @param offsetBytes the starting offset in the given MemorySegment.
   * @param numItems number of items in the contiguous serialized sequence.
   * @return array of deserialized items
   */
  public abstract T[] deserializeFromMemorySegment(MemorySegment seg, long offsetBytes, int numItems);

  /**
   * Returns the serialized size in bytes of a single unserialized item.
   * @param item a specific item
   * @return the serialized size in bytes of a single unserialized item.
   */
  public abstract int sizeOf(T item);

  /**
   * Returns the serialized size in bytes of the array of items.
   * @param items an array of items.
   * @return the serialized size in bytes of the array of items.
   */
  public int sizeOf(final T[] items) {
    Objects.requireNonNull(items, "Items must not be null");
    int totalBytes = 0;
    for (int i = 0; i < items.length; i++) {
      totalBytes += sizeOf(items[i]);
    }
    return totalBytes;
  }

  /**
   * Returns the serialized size in bytes of the number of contiguous serialized items in MemorySegment.
   * The capacity of the given MemorySegment can be much larger that the required size of the items.
   * @param seg the given MemorySegment.
   * @param offsetBytes the starting offset in the given MemorySegment.
   * @param numItems the number of serialized items contained in the MemorySegment
   * @return the serialized size in bytes of the given number of items.
   */
  public abstract int sizeOf(MemorySegment seg, long offsetBytes, int numItems);

  /**
   * Returns a human readable string of an item.
   * @param item a specific item
   * @return a human readable string of an item.
   */
  public abstract String toString(T item);

  /**
   * Returns the concrete class of type T
   * @return the concrete class of type T
   */
  public abstract Class<T> getClassOfT();
}
