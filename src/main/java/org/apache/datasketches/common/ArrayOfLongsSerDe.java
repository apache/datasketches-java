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

import static org.apache.datasketches.common.ByteArrayUtil.putLongLE;

import java.util.Objects;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Methods of serializing and deserializing arrays of Long.
 *
 * @author Alexander Saydakov
 */
public class ArrayOfLongsSerDe extends ArrayOfItemsSerDe<Long> {

  @Override
  public byte[] serializeToByteArray(final Long item) {
    Objects.requireNonNull(item, "Item must not be null");
    final byte[] byteArr = new byte[Long.BYTES];
    putLongLE(byteArr, 0, item.longValue());
    return byteArr;
  }

  @Override
  public byte[] serializeToByteArray(final Long[] items) {
    Objects.requireNonNull(items, "Items must not be null");
    if (items.length == 0) { return new byte[0]; }
    final byte[] bytes = new byte[Long.BYTES * items.length];
    final WritableMemory mem = WritableMemory.writableWrap(bytes);
    long offset = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putLong(offset, items[i]);
      offset += Long.BYTES;
    }
    return bytes;
  }

  @Override
  public Long[] deserializeFromMemory(final Memory mem, final int numItems) {
    return deserializeFromMemory(mem, 0, numItems);
  }

  @Override
  public Long[] deserializeFromMemory(final Memory mem, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(mem, "Memory must not be null");
    if (numItems <= 0) { return new Long[0]; }
    long offset = offsetBytes;
    Util.checkBounds(offset, Long.BYTES * (long)numItems, mem.getCapacity());
    final Long[] array = new Long[numItems];
    for (int i = 0; i < numItems; i++) {
      array[i] = mem.getLong(offset);
      offset += Long.BYTES;
    }
    return array;
  }

  @Override
  public int sizeOf(final Long item) {
    Objects.requireNonNull(item, "Item must not be null");
    return Long.BYTES;
  }

  @Override //override because this is simpler
  public int sizeOf(final Long[] items) {
    Objects.requireNonNull(items, "Items must not be null");
    return items.length * Long.BYTES;
  }

  @Override
  public int sizeOf(final Memory mem, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(mem, "Memory must not be null");
    return numItems * Long.BYTES;
  }

  @Override
  public String toString(final Long item) {
    if (item == null) { return "null"; }
    return item.toString();
  }

  @Override
  public Class<Long> getClassOfT() { return Long.class; }
}
