package com.yahoo.sketches.frequencies;

import com.yahoo.sketches.memory.Memory;

public interface ArrayOfItemsSerDe<T> {

  byte[] serializeToByteArray(T[] items);

  T[] deserializeFromMemory(Memory mem, int numItems);

  byte getType();
}
