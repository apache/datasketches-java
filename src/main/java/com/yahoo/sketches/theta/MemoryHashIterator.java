/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import com.yahoo.memory.Memory;

/**
 * @author Lee Rhodes
 */
class MemoryHashIterator implements HashIterator {
  private Memory mem;
  private int arrLongs;
  private long thetaLong;
  private long offsetBytes;
  private int index;
  private long hash;

  MemoryHashIterator(final Memory mem, final int arrLongs, final long thetaLong) {
    this.mem = mem;
    this.arrLongs = arrLongs;
    this.thetaLong = thetaLong;
    offsetBytes = PreambleUtil.extractPreLongs(mem) << 3;
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
      hash = mem.getLong(offsetBytes + (index << 3));
      if ((hash != 0) && (hash < thetaLong)) {
        return true;
      }
    }
    return false;
  }

}
