/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import java.nio.charset.StandardCharsets;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

/**
 * Methods of serializing and deserializing arrays of String.
 * This class serializes strings in UTF-8 format, which is more compact compared to
 * {@link ArrayOfUtf16StringsSerDe}. In an extreme case when all strings are in ASCII,
 * this method is 2 times more compact, but it takes more time to encode and decode
 * by a factor of 1.5 to 2.
 */
public class ArrayOfStringsSerDe extends ArrayOfItemsSerDe<String> {

  @Override
  public byte[] serializeToByteArray(String[] items) {
    int length = 0;
    byte[][] itemsBytes = new byte[items.length][];
    for (int i = 0; i < items.length; i++) {
      itemsBytes[i] = items[i].getBytes(StandardCharsets.UTF_8);
      length += itemsBytes[i].length + Integer.BYTES;
    }
    final byte[] bytes = new byte[length];
    final Memory mem = new NativeMemory(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putInt(offsetBytes, itemsBytes[i].length);
      offsetBytes += Integer.BYTES;
      mem.putByteArray(offsetBytes, itemsBytes[i], 0, itemsBytes[i].length);
      offsetBytes += itemsBytes[i].length;
    }
    return bytes;
  }

  @Override
  public String[] deserializeFromMemory(Memory mem, int length) {
    final String[] array = new String[length];
    long offsetBytes = 0;
    for (int i = 0; i < length; i++) {
      final int strLength = mem.getInt(offsetBytes);
      offsetBytes += Integer.BYTES;
      final byte[] bytes = new byte[strLength];
      mem.getByteArray(offsetBytes, bytes, 0, strLength);
      offsetBytes += strLength;
      array[i] = new String(bytes, StandardCharsets.UTF_8);
    }
    return array;
  }

}
