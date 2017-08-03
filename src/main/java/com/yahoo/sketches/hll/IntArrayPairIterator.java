/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;

/**
 * Iterates over pairs given an on-heap integer array.
 *
 * @author Lee Rhodes
 */
class IntArrayPairIterator implements PairIterator {
  private final int[] array;
  private final int lengthPairs;
  private int index;
  private int pair;


  IntArrayPairIterator(final int[] array) {
    this.array = array;
    lengthPairs = array.length;
    index = - 1;
  }

  @Override
  public boolean nextValid() {
    while (++index < lengthPairs) {
      final int pair = array[index];
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
      pair = array[index];
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
