/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

/**
 * Methods of serializing and deserializing arrays of Double.
 *
 * @author Alexander Saydakov
 */
public class ArrayOfDoublesSerDe extends ArrayOfItemsSerDe<Double> {

  @Override
  public byte[] serializeToByteArray(final Double[] items) {
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
  public Double[] deserializeFromMemory(final Memory mem, final int length) {
    checkMemorySize(mem, length * Double.BYTES);
    final Double[] array = new Double[length];
    long offsetBytes = 0;
    for (int i = 0; i < length; i++) {
      array[i] = mem.getDouble(offsetBytes);
      offsetBytes += Double.BYTES;
    }
    return array;
  }

}
