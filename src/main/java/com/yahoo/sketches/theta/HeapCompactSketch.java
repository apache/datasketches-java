/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.checkSeedHashes;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * An on-heap, compact, read-only sketch.
 *
 * @author Lee Rhodes
 */
final class HeapCompactSketch extends CompactSketch {
  private final long[] cache_;

  private HeapCompactSketch(final boolean empty, final short seedHash, final int curCount,
      final long thetaLong, final long[] cache) {
    super(empty, seedHash, curCount, thetaLong);
    cache_ = cache;
  }

  /**
   * Heapifies the given source Memory with seed
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return this sketch
   */
  static HeapCompactSketch heapifyInstance(final Memory srcMem, final long seed) {
    final Object memObj = ((WritableMemory)srcMem).getArray(); //may be null
    final long memAdd = srcMem.getCumulativeOffset(0L);

    final int preambleLongs = extractPreLongs(memObj, memAdd);
    final short memSeedHash = (short) extractSeedHash(memObj, memAdd);
    final int curCount = (preambleLongs > 1) ? extractCurCount(memObj, memAdd) : 0;
    final long thetaLong = (preambleLongs > 2) ? extractThetaLong(memObj, memAdd) : Long.MAX_VALUE;

    final short computedSeedHash = computeSeedHash(seed);
    checkSeedHashes(memSeedHash, computedSeedHash);

    final boolean empty = PreambleUtil.isEmpty(memObj, memAdd);
    final long[] cacheArr = new long[curCount];
    if (curCount > 0) {
      srcMem.getLongArray(preambleLongs << 3, cacheArr, 0, curCount);
    }
    final HeapCompactSketch hcs =
        new HeapCompactSketch(empty, memSeedHash, curCount, thetaLong, cacheArr);
    return hcs;
  }

  /**
   * Converts the given UpdateSketch to this compact form.
   * @param sketch the given UpdateSketch
   */
  HeapCompactSketch(final UpdateSketch sketch) {
    super(sketch.isEmpty(),
        sketch.getSeedHash(),
        sketch.getRetainedEntries(true), //curCount_  set here
        sketch.getThetaLong()            //thetaLong_ set here
        );
    final boolean ordered = false;
    cache_ = CompactSketch.compactCache(sketch.getCache(), getRetainedEntries(false),
        getThetaLong(), ordered);
  }

  /**
   * Constructs this sketch from correct, valid components.
   * @param compactCache in compact form
   * @param empty The correct <a href="{@docRoot}/resources/dictionary.html#empty">Empty</a>.
   * @param seedHash The correct
   * <a href="{@docRoot}/resources/dictionary.html#seedHash">Seed Hash</a>.
   * @param curCount correct value
   * @param thetaLong The correct
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   */
  HeapCompactSketch(final long[] compactCache, final boolean empty, final short seedHash,
      final int curCount, final long thetaLong) {
    super(empty, seedHash, curCount, thetaLong);
    assert compactCache != null;
    cache_ = (curCount == 0) ? new long[0] : compactCache;
  }

  //Sketch interface

  @Override
  public byte[] toByteArray() {
    final int bytes = getCurrentBytes(true);
    final byte[] byteArray = new byte[bytes];
    final WritableMemory dstMem = WritableMemory.wrap(byteArray);
    final int emptyBit = isEmpty() ? (byte) EMPTY_FLAG_MASK : 0;
    final byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK);
    loadCompactMemory(getCache(), isEmpty(), getSeedHash(), getRetainedEntries(true),
        getThetaLong(), dstMem, flags);
    return byteArray;
  }

  //restricted methods

  @Override
  public boolean isDirect() {
    return false;
  }

  //SetArgument "interface"

  @Override
  long[] getCache() {
    return cache_;
  }

  @Override
  Memory getMemory() {
    return null;
  }

  @Override
  public boolean isOrdered() {
    return false;
  }

}
