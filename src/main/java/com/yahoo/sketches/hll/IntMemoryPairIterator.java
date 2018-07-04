/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;

import com.yahoo.memory.Memory;

/**
 * Iterates within a given Memory extracting integer pairs.
 *
 * @author Lee Rhodes
 */
class IntMemoryPairIterator implements PairIterator {
  final Memory mem;
  final long offsetBytes;
  final int lengthPairs;
  final int slotMask;
  int index;
  int pair;

  IntMemoryPairIterator(final Memory mem, final long offsetBytes, final int lengthPairs,
      final int lgConfigK) {
    this.mem = mem;
    this.offsetBytes = offsetBytes;
    this.lengthPairs = lengthPairs;
    slotMask = (1 << lgConfigK) - 1;
    index = -1;
  }

  IntMemoryPairIterator(final byte[] byteArr, final long offsetBytes, final int lengthPairs,
      final int lgConfigK) {
    this(Memory.wrap(byteArr), offsetBytes, lengthPairs, lgConfigK);
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public int getKey() {
    return HllUtil.getLow26(pair);
  }

  @Override
  public int getPair() {
    return pair;
  }

  @Override
  public int getSlot() {
    return getKey() & slotMask;
  }

  @Override
  public int getValue() {
    return HllUtil.getValue(pair);
  }

  @Override
  public boolean nextAll() {
    if (++index < lengthPairs) {
      pair = pair();
      return true;
    }
    return false;
  }

  @Override
  public boolean nextValid() {
    while (++index < lengthPairs) {
      final int pair = pair();
      if (pair != EMPTY) {
        this.pair = pair;
        return true;
      }
    }
    return false;
  }

  int pair() {
    return mem.getInt(offsetBytes + (index << 2));
  }

}
