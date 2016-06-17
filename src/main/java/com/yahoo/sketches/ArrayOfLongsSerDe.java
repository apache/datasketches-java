package com.yahoo.sketches;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * Methods of serializing and deserializing arrays of Long.
 */
public class ArrayOfLongsSerDe implements ArrayOfItemsSerDe<Long> {

  private static final byte TYPE = 3;

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

  @Override
  public byte getType() {
    return TYPE;
  }

}
