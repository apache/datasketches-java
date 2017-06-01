/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import java.nio.charset.StandardCharsets;

import com.yahoo.memory.Memory;
import com.yahoo.memory.UnsafeUtil;
import com.yahoo.memory.WritableMemory;

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

  @Override
  public byte[] serializeToByteArray(final String[] items) {
    int length = 0;
    final byte[][] itemsBytes = new byte[items.length][];
    for (int i = 0; i < items.length; i++) {
      itemsBytes[i] = items[i].getBytes(StandardCharsets.UTF_8);
      length += itemsBytes[i].length + Integer.BYTES;
    }
    final byte[] bytes = new byte[length];
    final WritableMemory mem = WritableMemory.wrap(bytes);
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
  public String[] deserializeFromMemory(final Memory mem, final int numItems) {
    final String[] array = new String[numItems];
    long offsetBytes = 0;
    for (int i = 0; i < numItems; i++) {
      UnsafeUtil.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
      final int strLength = mem.getInt(offsetBytes);
      offsetBytes += Integer.BYTES;
      final byte[] bytes = new byte[strLength];
      UnsafeUtil.checkBounds(offsetBytes, strLength, mem.getCapacity());
      mem.getByteArray(offsetBytes, bytes, 0, strLength);
      offsetBytes += strLength;
      array[i] = new String(bytes, StandardCharsets.UTF_8);
    }
    return array;
  }

}
