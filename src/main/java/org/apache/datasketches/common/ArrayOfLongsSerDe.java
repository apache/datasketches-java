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

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.ByteArrayUtil.putLongLE;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * Methods of serializing and deserializing arrays of Long.
 *
 * @author Alexander Saydakov
 */
public class ArrayOfLongsSerDe extends ArrayOfItemsSerDe<Long> {

  /**
   * No argument constructor.
   */
  public ArrayOfLongsSerDe() { }

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
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    long offset = 0;
    for (int i = 0; i < items.length; i++) {
      seg.set(JAVA_LONG_UNALIGNED, offset, items[i]);
      offset += Long.BYTES;
    }
    return bytes;
  }

  @Override
  public Long[] deserializeFromMemorySegment(final MemorySegment seg, final int numItems) {
    return deserializeFromMemorySegment(seg, 0, numItems);
  }

  @Override
  public Long[] deserializeFromMemorySegment(final MemorySegment seg, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(seg, "MemorySegment must not be null");
    if (numItems <= 0) { return new Long[0]; }
    long offset = offsetBytes;
    Util.checkBounds(offset, Long.BYTES * (long)numItems, seg.byteSize());
    final Long[] array = new Long[numItems];
    for (int i = 0; i < numItems; i++) {
      array[i] = seg.get(JAVA_LONG_UNALIGNED, offset);
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
  public int sizeOf(final MemorySegment seg, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(seg, "MemorySegment must not be null");
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
