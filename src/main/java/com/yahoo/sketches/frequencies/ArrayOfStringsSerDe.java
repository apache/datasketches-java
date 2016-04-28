package com.yahoo.sketches.frequencies;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * Methods of serializing and deserializing arrays of String for use in FrequentItemsSketch
 */
public class ArrayOfStringsSerDe implements ArrayOfItemsSerDe<String> {

  private static final byte TYPE = 2;

  @Override
  public byte[] serializeToByteArray(String[] items) {
    int length = 0;
    for (int i = 0; i < items.length; i++) length += items[i].length() + 4;
    final byte[] bytes = new byte[length];
    final Memory mem = new NativeMemory(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putInt(offsetBytes, items[i].length());
      offsetBytes += 4;
      mem.putByteArray(offsetBytes, items[i].getBytes(), 0, items[i].length());
      offsetBytes += items[i].length();
    }
    return bytes;
  }

  @Override
  public String[] deserializeFromMemory(Memory mem, int length) {
    final String[] array = new String[length];
    long offsetBytes = 0;
    for (int i = 0; i < length; i++) {
      final int strLength = mem.getInt(offsetBytes);
      offsetBytes += 4;
      final byte[] bytes = new byte[strLength];
      mem.getByteArray(offsetBytes, bytes, 0, strLength);
      offsetBytes += strLength;
      array[i] = new String(bytes);
    }
    return array;
  }

  @Override
  public byte getType() {
    return TYPE;
  }

}
