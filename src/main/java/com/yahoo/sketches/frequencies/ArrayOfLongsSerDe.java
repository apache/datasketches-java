package com.yahoo.sketches.frequencies;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * Methods of serializing and deserializing arrays of Long for use in FrequentItemsSketch
 */
public class ArrayOfLongsSerDe implements ArrayOfItemsSerDe<Long> {

  private static final byte TYPE = 3;

  @Override
  public byte[] serializeToByteArray(Long[] items) {
    final byte[] bytes = new byte[8 * items.length];
    final Memory mem = new NativeMemory(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putLong(offsetBytes, items[i]);
      offsetBytes += 8;
    }
    return bytes;
  }

  @Override
  public Long[] deserializeFromMemory(Memory mem, int length) {
    final Long[] array = new Long[length];
    long offsetBytes = 0;
    for (int i = 0; i < length; i++) {
      array[i] = mem.getLong(offsetBytes);
      offsetBytes += 8;
    }
    return array;
  }

  @Override
  public byte getType() {
    return TYPE;
  }

}
