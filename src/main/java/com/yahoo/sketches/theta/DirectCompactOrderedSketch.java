/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.checkSeedHashes;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * An off-heap (Direct), compact, ordered, read-only sketch. This sketch may be associated
 * with Serialization Version 3 format binary image.
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 */
final class DirectCompactOrderedSketch extends DirectCompactSketch {

  private DirectCompactOrderedSketch(final Memory mem) {
    super(mem);
  }

  /**
   * Wraps the given Memory, which must be a SerVer 3, ordered, Compact Sketch image.
   * Must check the validity of the Memory before calling.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed the update seed
   * @return this sketch
   */
  static DirectCompactOrderedSketch wrapInstance(final Memory srcMem, final long seed) {
    final short memSeedHash = srcMem.getShort(SEED_HASH_SHORT);
    final short computedSeedHash = computeSeedHash(seed);
    checkSeedHashes(memSeedHash, computedSeedHash);
    return new DirectCompactOrderedSketch(srcMem);
  }

  /**
   * Converts the given UpdateSketch to this compact form.
   * @param sketch the given UpdateSketch
   * @param dstMem the given destination Memory. This clears it before use.
   * @return a DirectCompactOrderedSketch.
   */
  static DirectCompactOrderedSketch compact(final UpdateSketch sketch, final WritableMemory dstMem) {
    final long thetaLong = sketch.getThetaLong();
    final boolean empty = sketch.isEmpty();
    final int curCount = sketch.getRetainedEntries(true);
    //checkEmptyState(empty, curCount, thetaLong);
    final int preLongs = computeCompactPreLongs(thetaLong, empty, curCount);
    final short seedHash = sketch.getSeedHash();
    final long[] cache = sketch.getCache();
    final int requiredFlags = READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK | ORDERED_FLAG_MASK;
    final byte flags = (byte) (requiredFlags | (empty ? EMPTY_FLAG_MASK : 0));
    final boolean ordered = true;
    final long[] compactCache = CompactSketch.compactCache(cache, curCount, thetaLong, ordered);
    loadCompactMemory(compactCache, seedHash, curCount, thetaLong, dstMem, flags, preLongs);
    return new DirectCompactOrderedSketch(dstMem);
  }

  /**
   * Constructs this sketch from correct, valid components.
   * @param cache in compact, ordered form
   * @param empty The correct <a href="{@docRoot}/resources/dictionary.html#empty">Empty</a>.
   * @param seedHash The correct
   * <a href="{@docRoot}/resources/dictionary.html#seedHash">Seed Hash</a>.
   * @param curCount correct value
   * @param thetaLong The correct
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @param dstMem the given destination Memory. This clears it before use.
   * @return a DirectCompactOrderedSketch
   */
  static DirectCompactOrderedSketch compact(final long[] cache, final boolean empty,
      final short seedHash, final int curCount, final long thetaLong, final WritableMemory dstMem) {
    final int preLongs = computeCompactPreLongs(thetaLong, empty, curCount);
    final int requiredFlags = READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK | ORDERED_FLAG_MASK;
    final byte flags = (byte) (requiredFlags | (empty ? EMPTY_FLAG_MASK : 0));
    loadCompactMemory(cache, seedHash, curCount, thetaLong, dstMem, flags, preLongs);
    return new DirectCompactOrderedSketch(dstMem);
  }

  //restricted methods

  @Override
  public boolean isOrdered() {
    return true;
  }

}
