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

package org.apache.datasketches;

import java.nio.charset.StandardCharsets;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Methods of serializing and deserializing arrays of String.
 * This class serializes strings in UTF-8 format, which is more compact compared to
 * {@link ArrayOfUtf16StringsSerDe}. In an extreme case when all strings are in ASCII,
 * this method is 2 times more compact, but it takes more time to encode and decode
 * by a factor of 1.5 to 2.
 *
 * @author Alexander Saydakov
 */
public class ArrayOfStringsSerDe extends ArrayOfItemsSerDe<String> {

  private static final int NULL_STRING_LENGTH = -1;

  private final boolean nullSafe;

  /**
   * Creates an instance of {@code ArrayOfStringsSerDe} which does not handle
   * null values.
   */
  public ArrayOfStringsSerDe() {
    this(false);
  }

  /**
   * Creates an instance of {@code ArrayOfStringsSerDe}.
   *
   * @param nullSafe true if null values should be serialized/deserialized safely.
   */
  public ArrayOfStringsSerDe(boolean nullSafe) {
    this.nullSafe = nullSafe;
  }

  @Override
  public byte[] serializeToByteArray(final String[] items) {
    int length = 0;
    final byte[][] itemsBytes = new byte[items.length][];
    for (int i = 0; i < items.length; i++) {
      length += Integer.BYTES;
      if (items[i] != null) {
        itemsBytes[i] = items[i].getBytes(StandardCharsets.UTF_8);
        length += itemsBytes[i].length;
      } else if (!nullSafe) {
        throw new SketchesArgumentException(
            "All Strings must be non-null in non null-safe mode.");
      }
    }
    final byte[] bytes = new byte[length];
    final WritableMemory mem = WritableMemory.writableWrap(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      if (itemsBytes[i] != null) {
        mem.putInt(offsetBytes, itemsBytes[i].length);
        offsetBytes += Integer.BYTES;
        mem.putByteArray(offsetBytes, itemsBytes[i], 0, itemsBytes[i].length);
        offsetBytes += itemsBytes[i].length;
      } else if (nullSafe) {
        mem.putInt(offsetBytes, NULL_STRING_LENGTH);
        offsetBytes += Integer.BYTES;
      }
    }
    return bytes;
  }

  @Override
  public String[] deserializeFromMemory(final Memory mem, final int numItems) {
    final String[] array = new String[numItems];
    long offsetBytes = 0;
    for (int i = 0; i < numItems; i++) {
      Util.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
      final int strLength = mem.getInt(offsetBytes);
      offsetBytes += Integer.BYTES;
      if (strLength >= 0) {
        final byte[] bytes = new byte[strLength];
        Util.checkBounds(offsetBytes, strLength, mem.getCapacity());
        mem.getByteArray(offsetBytes, bytes, 0, strLength);
        offsetBytes += strLength;
        array[i] = new String(bytes, StandardCharsets.UTF_8);
      } else if (strLength != NULL_STRING_LENGTH || !nullSafe) {
        throw new SketchesArgumentException(
            "Unrecognized String length reading entry " + i + ": " + strLength);
      }
    }
    return array;
  }

}
