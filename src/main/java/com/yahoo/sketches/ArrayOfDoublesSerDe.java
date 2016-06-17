package com.yahoo.sketches;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * Methods of serializing and deserializing arrays of Double.
 */
public class ArrayOfDoublesSerDe implements ArrayOfItemsSerDe<Double> {

  private static final byte TYPE = 3;

  @Override
  public byte[] serializeToByteArray(Double[] items) {
    final byte[] bytes = new byte[Double.BYTES * items.length];
    final Memory mem = new NativeMemory(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putDouble(offsetBytes, items[i]);
      offsetBytes += Double.BYTES;
    }
    return bytes;
  }

  @Override
  public Double[] deserializeFromMemory(Memory mem, int length) {
    final Double[] array = new Double[length];
    long offsetBytes = 0;
    for (int i = 0; i < length; i++) {
      array[i] = mem.getDouble(offsetBytes);
      offsetBytes += Double.BYTES;
    }
    return array;
  }

  @Override
  public byte getType() {
    return TYPE;
  }

}
