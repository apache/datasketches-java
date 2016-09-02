/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

/**
 * Methods of serializing and deserializing arrays of Long.
 */
public class ArrayOfLongsSerDe extends ArrayOfItemsSerDe<Long> {

  @Override
  public byte[] serializeToByteArray(Long[] items) {
    final byte[] bytes = new byte[Long.BYTES * items.length];
    final Memory mem = new NativeMemory(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putLong(offsetBytes, items[i]);
      offsetBytes += Long.BYTES;
    }
    return bytes;
  }

  @Override
  public Long[] deserializeFromMemory(Memory mem, int length) {
    final Long[] array = new Long[length];
    long offsetBytes = 0;
    for (int i = 0; i < length; i++) {
      array[i] = mem.getLong(offsetBytes);
      offsetBytes += Long.BYTES;
    }
    return array;
  }

}
