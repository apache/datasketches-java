/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.pair;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * Iterates over an HLL Memory producing pairs of index, value.
 *
 * @author Lee Rhodes
 */
abstract class HllMemoryPairIterator implements PairIterator {
  final Object memObj;
  final long memAdd;
  final long offsetBytes;
  final int lengthPairs;
  int index;
  int value;

  HllMemoryPairIterator(final Memory mem, final long offsetBytes, final int lengthPairs) {
    memObj = ((WritableMemory) mem).getArray();
    memAdd = mem.getCumulativeOffset(0L);
    this.offsetBytes = offsetBytes;
    this.lengthPairs = lengthPairs;
    index = -1;
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public int getKey() {
    return index;
  }

  @Override
  public int getPair() {
    return pair(index, value);
  }

  @Override
  public int getSlot() {
    return index;
  }

  @Override
  public int getValue() {
    return value;
  }

  @Override
  public boolean nextAll() {
    if (++index < lengthPairs) {
      value = value();
      return true;
    }
    return false;
  }

  @Override
  public boolean nextValid() {
    while (++index < lengthPairs) {
      value = value();
      if (value != EMPTY) {
        return true;
      }
    }
    return false;
  }

  abstract int value();

}
