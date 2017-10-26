/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * Parent class of the Direct Compact Sketches.
 *
 * @author Lee Rhodes
 */
abstract class DirectCompactSketch extends CompactSketch {
  final Memory mem_;
  final Object memObj_;
  final long memAdd_;

  DirectCompactSketch(final Memory mem) {
    mem_ = mem;
    memObj_ = ((WritableMemory)mem).getArray();
    memAdd_ = mem.getCumulativeOffset(0L);
  }

  //Sketch

  @Override
  public int getCurrentBytes(final boolean compact) { //compact is ignored here
    final int preLongs = getCurrentPreambleLongs(true);
    final boolean empty = PreambleUtil.isEmpty(memObj_, memAdd_);
    if (preLongs == 1) {
      return (empty) ? 8 : 16; //empty or singleItem
    }
    //preLongs > 1
    final int curCount = extractCurCount(memObj_, memAdd_);
    return (preLongs + curCount) << 3;
  }

  @Override
  public int getRetainedEntries(final boolean valid) { //compact is always valid
    final int preLongs = getCurrentPreambleLongs(true);
    final boolean empty = PreambleUtil.isEmpty(memObj_, memAdd_);
    if (preLongs == 1) {
      return (empty) ? 0 : 1;
    }
    //preLongs > 1
    final int curCount = extractCurCount(memObj_, memAdd_);
    return curCount;
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public boolean isEmpty() {
    return PreambleUtil.isEmpty(memObj_, memAdd_);
  }

  @Override
  public boolean isSameResource(final Memory mem) {
    return mem_.isSameResource(mem);
  }

  @Override
  public byte[] toByteArray() {
    return
        compactMemoryToByteArray(mem_, getCurrentPreambleLongs(true), getRetainedEntries(true));
  }

  //restricted methods

  @Override
  long[] getCache() {
    final int curCount = getRetainedEntries(true);
    if (curCount > 0) {
      final long[] cache = new long[curCount];
      final int preLongs = getCurrentPreambleLongs(true);
      mem_.getLongArray(preLongs << 3, cache, 0, curCount);
      return cache;
    }
    return new long[0];
  }

  @Override
  int getCurrentPreambleLongs(final boolean compact) { //already compact; ignore
    return extractPreLongs(memObj_, memAdd_);
  }

  @Override
  Memory getMemory() {
    return mem_;
  }

  @Override
  short getSeedHash() {
    return (short) extractSeedHash(memObj_, memAdd_);
  }

  @Override
  long getThetaLong() {
    final int preLongs = extractPreLongs(memObj_, memAdd_);
    return (preLongs > 2) ? extractThetaLong(memObj_, memAdd_) : Long.MAX_VALUE;
  }

  /**
   * Serializes a Memory based compact sketch to a byte array
   * @param srcMem the source Memory
   * @param preLongs current preamble longs
   * @param curCount the current valid count
   * @return this Direct, Compact sketch as a byte array
   */
  static byte[] compactMemoryToByteArray(final Memory srcMem, final int preLongs,
      final int curCount) {
    final int outBytes = (curCount + preLongs) << 3;
    final byte[] byteArrOut = new byte[outBytes];
    srcMem.getByteArray(0, byteArrOut, 0, outBytes); //copies the whole thing
    return byteArrOut;
  }

}
