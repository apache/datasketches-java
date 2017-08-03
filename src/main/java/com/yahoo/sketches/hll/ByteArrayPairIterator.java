/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.pair;

/**
 * Iterates over pairs given an on-heap byte array.
 *
 * @author Lee Rhodes
 */
abstract class ByteArrayPairIterator implements PairIterator {
  final byte[] array;
  final int lengthPairs;
  int index;
  int value;

  ByteArrayPairIterator(final byte[] array, final int lengthPairs) {
    this.array = array;
    this.lengthPairs = lengthPairs;
    index = - 1;
  }

  @Override
  abstract public boolean nextValid();

  @Override
  abstract public boolean nextAll();

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
