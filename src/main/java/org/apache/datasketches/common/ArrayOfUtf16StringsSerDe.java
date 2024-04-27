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

import static org.apache.datasketches.common.ByteArrayUtil.copyBytes;
import static org.apache.datasketches.common.ByteArrayUtil.putIntLE;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.datasketches.memory.Memory;

/**
 * Methods of serializing and deserializing arrays of String.
 * This class serializes strings using internal Java representation as char[], where each char
 * is a 16-bit code. The result is larger than one from {@link ArrayOfStringsSerDe}.
 * In an extreme case when all strings are in ASCII, the size is doubled. However it takes
 * less time to serialize and deserialize by a factor of 1.5 to 2.
 *
 * @author Alexander Saydakov
 */
public class ArrayOfUtf16StringsSerDe extends ArrayOfItemsSerDe<String> {

  @Override
  public byte[] serializeToByteArray(final String item) {
    Objects.requireNonNull(item, "Item must not be null");
    final byte[] utf16ByteArr = item.getBytes(StandardCharsets.UTF_16); //includes BOM
    final int numBytes = utf16ByteArr.length;
    final byte[] out = new byte[numBytes + Integer.BYTES];
    copyBytes(utf16ByteArr, 0, out, 4, numBytes);
    putIntLE(out, 0, numBytes);
    return out;
  }

  @Override
  public byte[] serializeToByteArray(final String[] items) {
    Objects.requireNonNull(items, "Items must not be null");
    int totalBytes = 0;
    final int numItems = items.length;
    final byte[][] serialized2DArray = new byte[numItems][];
    for (int i = 0; i < numItems; i++) {
      serialized2DArray[i] = items[i].getBytes(StandardCharsets.UTF_16);
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
  public String[] deserializeFromMemory(final Memory mem, final int numItems) {
    return deserializeFromMemory(mem, 0, numItems);
  }

  @Override
  public String[] deserializeFromMemory(final Memory mem, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(mem, "Memory must not be null");
    if (numItems <= 0) { return new String[0]; }
    final String[] array = new String[numItems];
    long offset = offsetBytes;
    for (int i = 0; i < numItems; i++) {
      Util.checkBounds(offset, Integer.BYTES, mem.getCapacity());
      final int strLength = mem.getInt(offset);
      offset += Integer.BYTES;
      final byte[] utf16Bytes = new byte[strLength];
      Util.checkBounds(offset, strLength, mem.getCapacity());
      mem.getByteArray(offset, utf16Bytes, 0, strLength);
      offset += strLength;
      array[i] = new String(utf16Bytes, StandardCharsets.UTF_16);
    }
    return array;
  }

  @Override
  public int sizeOf(final String item) {
    Objects.requireNonNull(item, "Item must not be null");
    return item.getBytes(StandardCharsets.UTF_16).length + Integer.BYTES;
  }

  @Override
  public int sizeOf(final Memory mem, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(mem, "Memory must not be null");
    long offset = offsetBytes;
    final long memCap = mem.getCapacity();
    for (int i = 0; i < numItems; i++) {
      Util.checkBounds(offset, Integer.BYTES, memCap);
      final int itemLenBytes = mem.getInt(offset);
      offset += Integer.BYTES;
      Util.checkBounds(offset, itemLenBytes, memCap);
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

  @Override
  public boolean isFixedWidth()
  {
    return false;
  }
}
