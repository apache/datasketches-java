package com.yahoo.sketches.hll;

import com.yahoo.memory.NativeMemory;

/**
 * Positional putInt.  Puts the value at the current offset and returns the offset for the next int.
 */
public class Serde {
  public static int putInt(final NativeMemory mem, final int offset, final int value) {
    mem.putInt(offset, value);
    return offset + 4;
  }
}
