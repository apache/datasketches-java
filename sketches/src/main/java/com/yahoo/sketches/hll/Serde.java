package com.yahoo.sketches.hll;

import com.yahoo.memory.NativeMemory;

/**
 */
public class Serde {
  public static int putInt(final NativeMemory mem, final int offset, final int value) {
    mem.putInt(offset, value);
    return offset + 4;
  }
}
