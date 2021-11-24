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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

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

  private static final int NULL_STRING_LENGTH = -1;

  private final boolean nullSafe;

  /**
   * Creates an instance of {@code ArrayOfUtf16StringsSerDe} which does not
   * handle null values.
   */
  public ArrayOfUtf16StringsSerDe() {
    this(false);
  }

  /**
   * Creates an instance of {@code ArrayOfUtf16StringsSerDe}.
   *
   * @param nullSafe true if null values should be serialized/deserialized safely.
   */
  public ArrayOfUtf16StringsSerDe(boolean nullSafe) {
    this.nullSafe = nullSafe;
  }

  @Override
  public byte[] serializeToByteArray(final String[] items) {
    int length = 0;
    for (String item : items) {
      length += Integer.BYTES;
      if (item != null) {
        length += (item.length() * Character.BYTES);
      } else if (!nullSafe) {
        throw new SketchesArgumentException(
            "All Strings must be non-null in non null-safe mode.");
      }
    }
    final byte[] bytes = new byte[length];
    final WritableMemory mem = WritableMemory.writableWrap(bytes);
    long offsetBytes = 0;
    for (String item : items) {
      if (item != null) {
        mem.putInt(offsetBytes, item.length());
        offsetBytes += Integer.BYTES;
        mem.putCharArray(offsetBytes, item.toCharArray(), 0, item.length());
        offsetBytes += (long) (item.length()) * Character.BYTES;
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
        final char[] chars = new char[strLength];
        Util.checkBounds(offsetBytes, (long) strLength * Character.BYTES, mem.getCapacity());
        mem.getCharArray(offsetBytes, chars, 0, strLength);
        array[i] = new String(chars);
        offsetBytes += (long) strLength * Character.BYTES;
      } else if (strLength != NULL_STRING_LENGTH || !nullSafe) {
        throw new SketchesArgumentException(
            "Unrecognized String length reading entry " + i + ": " + strLength);
      }
    }
    return array;
  }

}
