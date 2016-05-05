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
    byte[][] itemsBytes = new byte[items.length][];
    for (int i = 0; i < items.length; i++) {
      itemsBytes[i] = items[i].getBytes();
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
      array[i] = new String(bytes);
    }
    return array;
  }

  @Override
  public byte getType() {
    return TYPE;
  }

}
