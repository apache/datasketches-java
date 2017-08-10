/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.memory.Memory;
import com.yahoo.memory.UnsafeUtil;
import com.yahoo.memory.WritableMemory;

/**
 * Methods of serializing and deserializing arrays of Boolean as a bit array.
 *
 * @author Jon Malkin
 */
public class ArrayOfBooleansSerDe extends ArrayOfItemsSerDe<Boolean> {
  /**
   * Computes number of bytes needed for packed bit encoding of the array of booleans. Rounds
   * partial bytes up to return a whole number of bytes.
   *
   * @param arrayLength Number of items in the array to serialize
   * @return Number of bytes needed to encode the array
   */
  public static int computeBytesNeeded(final int arrayLength) {
    return (arrayLength >>> 3) + ((arrayLength & 0x7) > 0 ? 1 : 0);
  }

  @Override
  public byte[] serializeToByteArray(final Boolean[] items) {
    final int bytesNeeded = computeBytesNeeded(items.length);
    final byte[] bytes = new byte[bytesNeeded];
    final WritableMemory mem = WritableMemory.wrap(bytes);

    byte val = 0;
    for (int i = 0; i < items.length; ++i) {
      if (items[i]) {
        val |= 0x1 << (i & 0x7);
      }

      if ((i & 0x7) == 0x7) {
        mem.putByte(i >>> 3, val);
        val = 0;
      }
    }

    // write out any remaining values (if val=0, still good to be explicit)
    if ((items.length & 0x7) > 0) {
      mem.putByte(bytesNeeded - 1, val);
    }

    return bytes;
  }

  @Override
  public Boolean[] deserializeFromMemory(final Memory mem, final int length) {
    final int numBytes = computeBytesNeeded(length);
    UnsafeUtil.checkBounds(0, numBytes, mem.getCapacity());
    final Boolean[] array = new Boolean[length];

    byte srcVal = 0;
    for (int i = 0, b = 0; i < length; ++i) {
      if ((i & 0x7) == 0x0) { // should trigger on first iteration
        srcVal = mem.getByte(b++);
      }
      array[i] = ((srcVal >>> (i & 0x7)) & 0x1) == 1;
    }

    return array;
  }
}
