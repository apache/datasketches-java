/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import com.yahoo.memory.Memory;

/**
 * Parent class of the Heap Compact Sketches.
 *
 * @author Lee Rhodes
 */
abstract class HeapCompactSketch extends CompactSketch {
  private final short seedHash_;
  private final boolean empty_;
  private final int curCount_;
  private final long thetaLong_;
  private final long[] cache_;

  HeapCompactSketch(final long[] cache, final boolean empty, final short seedHash,
      final int curCount, final long thetaLong) {
    empty_ = empty;
    seedHash_ = seedHash;
    curCount_ = curCount;
    thetaLong_ = thetaLong;
    cache_ = cache;
  }

  //Sketch

  @Override
  public int getCurrentBytes(final boolean compact) { //compact is ignored
    final int preLongs = getCurrentPreambleLongs();
    return (preLongs + curCount_) << 3;
  }

  @Override
  public int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public boolean isEmpty() {
    return empty_;
  }

  //restricted methods

  @Override
  long[] getCache() {
    return cache_;
  }

  @Override
  int getCurrentPreambleLongs() {
    return (thetaLong_ < Long.MAX_VALUE) ? 3 : empty_ ? 1 : (curCount_ > 1) ? 2 : 1;
  }

  @Override
  Memory getMemory() {
    return null;
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

  @Override
  long getThetaLong() {
    return thetaLong_;
  }

}
