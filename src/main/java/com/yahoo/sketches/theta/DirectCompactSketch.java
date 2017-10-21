/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.checkSeedHashes;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
abstract class DirectCompactSketch extends CompactSketch {
  final Memory mem_;
  final Object memObj_;
  final long memAdd_;


  DirectCompactSketch(final Memory mem, final long seed) {
    mem_ = mem;
    memObj_ = ((WritableMemory)mem).getArray();
    memAdd_ = mem.getCumulativeOffset(0L);

    final short memSeedHash = (short) extractSeedHash(memObj_, memAdd_);
    final short computedSeedHash = computeSeedHash(seed);
    checkSeedHashes(memSeedHash, computedSeedHash);
  }

  //Sketch

  @Override
  public int getCurrentBytes(final boolean compact) { //compact is ignored here
    final boolean empty = PreambleUtil.isEmpty(memObj_, memAdd_);
    if (empty) { return 8; }
    //not empty
    final int preLongs = extractPreLongs(memObj_, memAdd_);
    if (preLongs == 1) { return 16; } //Singleton
    //preLongs > 1
    final int curCount = extractCurCount(memObj_, memAdd_);
    final int dataBytes = curCount << 3;
    return dataBytes + ((preLongs == 2) ? 16 : 24);
  }

  @Override
  public int getRetainedEntries(final boolean valid) {
    final boolean empty = PreambleUtil.isEmpty(memObj_, memAdd_);
    if (empty) { return 0; }
    //not empty
    final int preLongs = extractPreLongs(memObj_, memAdd_);
    if (preLongs == 1) { return 1; } //Singleton
    //preLongs > 1
    return extractCurCount(memObj_, memAdd_);
  }

  @Override
  public boolean isEmpty() {
    return PreambleUtil.isEmpty(memObj_, memAdd_);
  }

  //restricted methods

  @Override
  int getCurrentPreambleLongs() {
    return extractPreLongs(memObj_, memAdd_);
  }

  @Override
  short getSeedHash() {
    return (short) extractSeedHash(memObj_, memAdd_);
  }

  @Override
  long getThetaLong() {

    return extractThetaLong(memObj_, memAdd_);
  }

}
