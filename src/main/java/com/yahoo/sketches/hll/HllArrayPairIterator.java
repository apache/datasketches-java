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
abstract class HllArrayPairIterator implements PairIterator {
  final byte[] array;
  final int lengthPairs;
  int index;
  int value;

  HllArrayPairIterator(final byte[] array, final int lengthPairs) {
    this.array = array;
    this.lengthPairs = lengthPairs;
    index = - 1;
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

  @Override
  public boolean nextAll() {
    if (++index < lengthPairs) {
      value = value();
      return true;
    }
    return false;
  }

  abstract int value();

  @Override
  public int getPair() {
    return pair(index, value);
  }

  @Override
  public int getKey() {
    return index;
  }

  @Override
  public int getValue() {
    return value;
  }

  @Override
  public int getIndex() {
    return index;
  }
}
