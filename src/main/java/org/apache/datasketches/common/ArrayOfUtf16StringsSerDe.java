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

  @Override
  public byte[] serializeToByteArray(final String[] items) {
    int length = 0;
    for (int i = 0; i < items.length; i++) {
      length += (items[i].length() * Character.BYTES) + Integer.BYTES;
    }
    final byte[] bytes = new byte[length];
    final WritableMemory mem = WritableMemory.writableWrap(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putInt(offsetBytes, items[i].length());
      offsetBytes += Integer.BYTES;
      mem.putCharArray(offsetBytes, items[i].toCharArray(), 0, items[i].length());
      offsetBytes += (long) (items[i].length()) * Character.BYTES;
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
      final char[] chars = new char[strLength];
      Util.checkBounds(offsetBytes, (long) strLength * Character.BYTES, mem.getCapacity());
      mem.getCharArray(offsetBytes, chars, 0, strLength);
      array[i] = new String(chars);
      offsetBytes += (long) strLength * Character.BYTES;
    }
    return array;
  }

  @Override
  public int sizeOf(final String item) {
    return item.length() * Character.BYTES + Integer.BYTES;
  }

  @Override
  public int sizeOf(final Memory mem, final long offset, final int numItems) {
    long offsetBytes = 0;
    final long memCap = mem.getCapacity();
    for (int i = 0; i < numItems; i++) {
      Util.checkBounds(offsetBytes, Integer.BYTES, memCap);
      final int strLength = mem.getInt(offsetBytes);
      offsetBytes += Integer.BYTES;
      final int charBytes = strLength * Character.BYTES;
      Util.checkBounds(offsetBytes, charBytes, memCap);
      offsetBytes += charBytes;
    }
    return (int)offsetBytes;
  }

  @Override
  public byte[] serializeToByteArray(String item) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String deserializeOneFromMemory(Memory mem, long offset) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] deserializeFromMemory(Memory mem, long offset, int numItems) {
    // TODO Auto-generated method stub
    return null;
  }

}
