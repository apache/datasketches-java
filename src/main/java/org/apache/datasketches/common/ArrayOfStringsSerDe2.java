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
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.common.ByteArrayUtil.copyBytes;
import static org.apache.datasketches.common.ByteArrayUtil.putIntLE;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Methods of serializing and deserializing arrays of String.
 * This class serializes strings in UTF-8 format, which is more compact compared to
 * {@link ArrayOfUtf16StringsSerDe}. In an extreme case when all strings are in ASCII,
 * this method is 2 times more compact, but it takes more time to encode and decode
 * by a factor of 1.5 to 2.
 *
 * <p>The serialization
 *
 * @author Alexander Saydakov
 */
public class ArrayOfStringsSerDe2 extends ArrayOfItemsSerDe2<String> {

  @Override
  public byte[] serializeToByteArray(final String item) {
    Objects.requireNonNull(item, "Item must not be null");
    if (item.isEmpty()) { return new byte[] { 0, 0, 0, 0 }; }
    final byte[] utf8ByteArr = item.getBytes(StandardCharsets.UTF_8);
    final int numBytes = utf8ByteArr.length;
    final byte[] out = new byte[numBytes + Integer.BYTES];
    copyBytes(utf8ByteArr, 0, out, 4, numBytes);
    putIntLE(out, 0, numBytes);
    return out;
  }

  @Override
  public byte[] serializeToByteArray(final String[] items) {
    Objects.requireNonNull(items, "Items must not be null");
    if (items.length == 0) { return new byte[0]; }
    int totalBytes = 0;
    final int numItems = items.length;
    final byte[][] serialized2DArray = new byte[numItems][];
    for (int i = 0; i < numItems; i++) {
      serialized2DArray[i] = items[i].getBytes(StandardCharsets.UTF_8);
      totalBytes += serialized2DArray[i].length + Integer.BYTES;
    }
    final byte[] bytesOut = new byte[totalBytes];
    int offset = 0;
    for (int i = 0; i < numItems; i++) {
      final int utf8len = serialized2DArray[i].length;
      putIntLE(bytesOut, offset, utf8len);
      offset += Integer.BYTES;
      copyBytes(serialized2DArray[i], 0, bytesOut, offset, utf8len);
      offset += utf8len;
    }
    return bytesOut;
  }

  @Override
  public String[] deserializeFromMemorySegment(final MemorySegment seg, final int numItems) {
    return deserializeFromMemorySegment(seg, 0, numItems);
  }

  @Override
  public String[] deserializeFromMemorySegment(final MemorySegment seg, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(seg, "MemorySegment must not be null");
    if (numItems <= 0) { return new String[0]; }
    final String[] array = new String[numItems];
    long offset = offsetBytes;
    for (int i = 0; i < numItems; i++) {
      Util.checkBounds(offset, Integer.BYTES, seg.byteSize());
      final int strLength = seg.get(JAVA_INT_UNALIGNED, offset);
      offset += Integer.BYTES;
      final byte[] utf8Bytes = new byte[strLength];
      Util.checkBounds(offset, strLength, seg.byteSize());
      MemorySegment.copy(seg, JAVA_BYTE, offset, utf8Bytes, 0, strLength);
      offset += strLength;
      array[i] = new String(utf8Bytes, StandardCharsets.UTF_8);
    }
    return array;
  }

  @Override
  public int sizeOf(final String item) {
    Objects.requireNonNull(item, "Item must not be null");
    if (item.isEmpty()) { return Integer.BYTES; }
    return item.getBytes(StandardCharsets.UTF_8).length + Integer.BYTES;
  }

  @Override
  public int sizeOf(final MemorySegment seg, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(seg, "MemorySegment must not be null");
    if (numItems <= 0) { return 0; }
    long offset = offsetBytes;
    final long segCap = seg.byteSize();
    for (int i = 0; i < numItems; i++) {
      Util.checkBounds(offset, Integer.BYTES, segCap);
      final int itemLenBytes = seg.get(JAVA_INT_UNALIGNED, offset);
      offset += Integer.BYTES;
      Util.checkBounds(offset, itemLenBytes, segCap);
      offset += itemLenBytes;
    }
    return (int)(offset - offsetBytes);
  }

  @Override
  public String toString(final String item) {
    if (item == null) { return "null"; }
    return item;
  }

  @Override
  public Class<String> getClassOfT() { return String.class; }
}
