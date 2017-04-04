package com.yahoo.sketches.hll;

import com.yahoo.memory.NativeMemory;

/**
 */
public class Serde
{
  public static int putInt(NativeMemory mem, int offset, int value) {
    mem.putInt(offset, value);
    return offset + 4;
  }
}
