/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.pair;

/**
 * Iterates over an on-heap HLL byte array producing pairs of index, value.
 *
 * @author Lee Rhodes
 */
abstract class HllPairIterator implements PairIterator {
  final int lengthPairs;
  int index;
  int value;

  HllPairIterator(final int lengthPairs) {
    this.lengthPairs = lengthPairs;
    index = - 1;
  }

  @Override
  public String getHeader() {
    return String.format("%10s%6s", "Slot", "Value");
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
  public String getString() {
    final int slot = getSlot();
    final int value = getValue();
    return String.format("%10d%6d", slot, value);
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
