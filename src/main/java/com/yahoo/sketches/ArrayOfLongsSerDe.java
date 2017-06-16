/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.memory.Memory;
import com.yahoo.memory.UnsafeUtil;
import com.yahoo.memory.WritableMemory;

/**
 * Methods of serializing and deserializing arrays of Long.
 *
 * @author Alexander Saydakov
 */
public class ArrayOfLongsSerDe extends ArrayOfItemsSerDe<Long> {

  @Override
  public byte[] serializeToByteArray(final Long[] items) {
    final byte[] bytes = new byte[Long.BYTES * items.length];
    final WritableMemory mem = WritableMemory.wrap(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putLong(offsetBytes, items[i]);
      offsetBytes += Long.BYTES;
    }
    return bytes;
  }

  @Override
  public Long[] deserializeFromMemory(final Memory mem, final int length) {
    UnsafeUtil.checkBounds(0, (long)length * Long.BYTES, mem.getCapacity());
    final Long[] array = new Long[length];
    long offsetBytes = 0;
    for (int i = 0; i < length; i++) {
      array[i] = mem.getLong(offsetBytes);
      offsetBytes += Long.BYTES;
    }
    return array;
  }

}
