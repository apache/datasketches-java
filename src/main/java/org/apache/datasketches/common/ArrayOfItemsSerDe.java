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

import org.apache.datasketches.memory.Memory;

/**
 * Base class for serializing and deserializing custom types.
 * @param <T> Type of item
 *
 * @author Alexander Saydakov
 */
public abstract class ArrayOfItemsSerDe<T> {

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
   * Deserialize a single serialized item from a given Memory object.
   *
   * @param mem Memory containing the serialized item
   * @param offsetBytes the starting offset in the given Memory.
   * @return deserialized item
   */
  public abstract T deserializeOneFromMemory(Memory mem, long offsetBytes);

  /**
   * Deserialize a contiguous sequence of serialized items from a given Memory.
   *
   * @param mem Memory containing a contiguous sequence of serialized items
   * @param numItems number of items in the contiguous serialized sequence.
   * @return array of deserialized items
   * @deprecated use
   * {@link #deserializeFromMemory(Memory, long, int) deserializeFromMemory(mem, offset, numItems)}
   */
  @Deprecated
  public abstract T[] deserializeFromMemory(Memory mem, int numItems);

  /**
   * Deserialize a contiguous sequence of serialized items from a given Memory.
   *
   * @param mem Memory containing a contiguous sequence of serialized items
   * @param offsetBytes the starting offset in the given Memory.
   * @param numItems number of items in the contiguous serialized sequence.
   * @return array of deserialized items
   */
  public abstract T[] deserializeFromMemory(Memory mem, long offsetBytes, int numItems);

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
    int totalBytes = 0;
    for (int i = 0; i < items.length; i++) {
      totalBytes += sizeOf(items[i]);
    }
    return totalBytes;
  }

  /**
   * Returns the serialized size in bytes of the number of contiguous serialized items in Memory.
   * The capacity of the given Memory can be much larger that the required size of the items.
   * @param mem the given Memory.
   * @param offsetBytes the starting offset in the given Memory.
   * @param numItems the number of serialized items contained in the Memory
   * @return the serialized size in bytes of the number of items.
   */
  public abstract int sizeOf(Memory mem, long offsetBytes, int numItems);

}
