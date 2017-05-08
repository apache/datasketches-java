/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.memory.UnsafeUtil;

/**
 * Methods of serializing and deserializing arrays of Boolean as a bit array.
 *
 * @author Jon Malkin
 */
public class ArrayOfBooleansSerDe extends ArrayOfItemsSerDe<Boolean> {
  @Override
  public byte[] serializeToByteArray(final Boolean[] items) {
    final int bytesNeeded = (items.length >> 3) + ((items.length & 0x7) > 0 ? 1 : 0);
    final byte[] bytes = new byte[bytesNeeded];
    final Memory mem = new NativeMemory(bytes);

    byte val = 0;
    for (int i = 0; i < items.length; ++i) {
      if (items[i]) {
        val |= 0x1 << (i & 0x7);
      }

      if ((i & 0x7) == 0x7) {
        mem.putByte(i >> 3, val);
        val = 0;
      }
    }

    // write out any remaining values
    if (val != 0) {
      mem.putByte(bytesNeeded - 1, val);
    }

    return bytes;
  }

  @Override
  public Boolean[] deserializeFromMemory(final Memory mem, final int length) {
    final int numBytes = (length >> 3) + ((length & 0x7) > 0 ? 1 : 0);
    UnsafeUtil.checkBounds(0, numBytes, mem.getCapacity());
    final Boolean[] array = new Boolean[length];

    byte srcVal = 0;
    for (int i = 0, b = 0; i < length; ++i) {
      if ((i & 0x7) == 0x0) { // should trigger on first iteration
        srcVal = mem.getByte(b++);
      }
      array[i] = (srcVal >> (i & 0x7) & 0x1) == 1;
    }

    return array;
  }
}
