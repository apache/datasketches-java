/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

/**
 * @author Lee Rhodes
 */
class HeapHashIterator implements HashIterator {
  private long[] cache;
  private int arrLongs;
  private long thetaLong;
  private int index;
  private long hash;

  HeapHashIterator(final long[] cache, final int arrLongs, final long thetaLong) {
    this.cache = cache;
    this.arrLongs = arrLongs;
    this.thetaLong = thetaLong;
    index = -1;
    hash = 0;
  }

  @Override
  public long get() {
    return hash;
  }

  @Override
  public boolean next() {
    while (++index < arrLongs) {
      hash = cache[index];
      if ((hash != 0) && (hash < thetaLong)) {
        return true;
      }
    }
    return false;
  }

}
