/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

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
  private final int preLongs_;

  /**
   * Constructs this sketch from correct, valid components.
   * @param cache in compact form
   * @param empty The correct <a href="{@docRoot}/resources/dictionary.html#empty">Empty</a>.
   * @param seedHash The correct
   * <a href="{@docRoot}/resources/dictionary.html#seedHash">Seed Hash</a>.
   * @param curCount correct value
   * @param thetaLong The correct
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   */
  HeapCompactSketch(final long[] cache, final boolean empty, final short seedHash,
      final int curCount, final long thetaLong) {
    empty_ = empty;
    seedHash_ = seedHash;
    curCount_ = empty ? 0 : curCount;
    thetaLong_ = empty ? Long.MAX_VALUE : thetaLong;
    cache_ = cache;
    preLongs_ = computeCompactPreLongs(thetaLong, empty, curCount);
  }

  //Sketch

  @Override
  public int getCurrentBytes(final boolean compact) { //already compact; ignored
    return (preLongs_ + curCount_) << 3;
  }

  @Override
  public HashIterator iterator() {
    return new HeapHashIterator(cache_, cache_.length, thetaLong_);
  }

  @Override
  public int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  public long getThetaLong() {
    return thetaLong_;
  }

  @Override
  public boolean hasMemory() {
    return false;
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
  int getCurrentPreambleLongs(final boolean compact) { //already compact; ignored
    return preLongs_;
  }

  @Override
  Memory getMemory() {
    return null;
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

  byte[] toByteArray(final boolean ordered) {
    final int bytes = getCurrentBytes(true);
    final byte[] byteArray = new byte[bytes];
    final WritableMemory dstMem = WritableMemory.wrap(byteArray);
    final int emptyBit = isEmpty() ? (byte) EMPTY_FLAG_MASK : 0;
    final int orderedBit = ordered ? (byte) ORDERED_FLAG_MASK : 0;
    final byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK | orderedBit);
    final int preLongs = getCurrentPreambleLongs(true);
    loadCompactMemory(getCache(), getSeedHash(), getRetainedEntries(true), getThetaLong(),
        dstMem, flags, preLongs);
    return byteArray;
  }

}
