/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.memory.UnsafeUtil.unsafe;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * Iterates over pairs given a Memory, byte offset and length in pairs.
 * @author Lee Rhodes
 */
class MemoryPairIterator implements PairIterator {
  private final Object memObj;
  private final long memAdd;
  private final long offsetBytes;
  private final int lengthPairs;
  private int index;
  private int pair;

  MemoryPairIterator(final Memory mem, final long offsetBytes, final int lengthPairs) {
    memObj = ((WritableMemory) mem).getArray();
    memAdd = mem.getCumulativeOffset(0L);
    this.offsetBytes = offsetBytes;
    this.lengthPairs = lengthPairs;
    index = -1;
  }

  @Override
  public boolean nextValid() {
    while (++index < lengthPairs) {
      final int pair = unsafe.getInt(memObj, memAdd + offsetBytes + (index << 2));
      if (pair != EMPTY) {
        this.pair = pair;
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean nextAll() {
    if (++index < lengthPairs) {
      pair = unsafe.getInt(memObj, memAdd + offsetBytes + (index << 2));
      return true;
    }
    return false;
  }

  @Override
  public int getPair() {
    return pair;
  }

  @Override
  public int getKey() {
    return HllUtil.getLow26(pair);
  }

  @Override
  public int getValue() {
    return HllUtil.getValue(pair);
  }

  @Override
  public int getIndex() {
    return index;
  }

}
