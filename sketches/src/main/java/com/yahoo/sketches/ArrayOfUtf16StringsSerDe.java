/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

/**
 * Methods of serializing and deserializing arrays of String.
 * This class serializes strings using internal Java representation as char[], where each char
 * is a 16-bit code. The result is larger than one from {@link ArrayOfStringsSerDe}.
 * In an extreme case when all strings are in ASCII, the size is doubled. However it takes
 * less time to serialize and deserialize by a factor of 1.5 to 2.
 */
public class ArrayOfUtf16StringsSerDe extends ArrayOfItemsSerDe<String> {

  @Override
  public byte[] serializeToByteArray(String[] items) {
    int length = 0;
    for (int i = 0; i < items.length; i++) {
      length += items[i].length() * Character.BYTES + Integer.BYTES;
    }
    final byte[] bytes = new byte[length];
    final Memory mem = new NativeMemory(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putInt(offsetBytes, items[i].length());
      offsetBytes += Integer.BYTES;
      mem.putCharArray(offsetBytes, items[i].toCharArray(), 0, items[i].length());
      offsetBytes += items[i].length() * Character.BYTES;
    }
    return bytes;
  }

  @Override
  public String[] deserializeFromMemory(Memory mem, int numItems) {
    final String[] array = new String[numItems];
    long offsetBytes = 0;
    for (int i = 0; i < numItems; i++) {
      final int strLength = mem.getInt(offsetBytes);
      offsetBytes += Integer.BYTES;
      final char[] chars = new char[strLength];
      mem.getCharArray(offsetBytes, chars, 0, strLength);
      array[i] = new String(chars);
      offsetBytes += strLength * Character.BYTES;
    }
    return array;
  }
}
