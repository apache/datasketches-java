/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.memory.Memory;
import com.yahoo.memory.UnsafeUtil;
import com.yahoo.memory.WritableMemory;

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
    final WritableMemory mem = WritableMemory.wrap(bytes);
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
      UnsafeUtil.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
      final int strLength = mem.getInt(offsetBytes);
      offsetBytes += Integer.BYTES;
      final char[] chars = new char[strLength];
      UnsafeUtil.checkBounds(offsetBytes, (long) strLength * Character.BYTES, mem.getCapacity());
      mem.getCharArray(offsetBytes, chars, 0, strLength);
      array[i] = new String(chars);
      offsetBytes += (long) strLength * Character.BYTES;
    }
    return array;
  }

}
