/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.checkSeedHashes;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;

import com.yahoo.memory.Memory;

/**
 * An off-heap (Direct), compact, ordered, read-only sketch. This sketch may be associated
 * with Serial Versions 1, 2, or 3.
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 */
final class DirectCompactOrderedSketch extends CompactSketch {
  private Memory mem_;
  private int preLongs_; //1, 2, or 3.

  private DirectCompactOrderedSketch(final boolean empty, final short seedHash, final int curCount,
      final long thetaLong) {
    super(empty, seedHash, curCount, thetaLong);
  }

  /**
   * Wraps the given Memory, which may be a SerVer 1, 2, or 3 sketch.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param pre0 the first 8 bytes of the preamble
   * @param seed the update seed
   * @return this sketch
   */
  static DirectCompactOrderedSketch wrapInstance(final Memory srcMem, final long pre0,
      final long seed) {
    final Object memObj = srcMem.array(); //may be null
    final long memAdd = srcMem.getCumulativeOffset(0L);

    final int preLongs = extractPreLongs(memObj, memAdd);
    final int flags = extractFlags(memObj, memAdd);
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    final short memSeedHash = (short) extractSeedHash(memObj, memAdd);
    final short computedSeedHash = computeSeedHash(seed);
    checkSeedHashes(memSeedHash, computedSeedHash);
    final int curCount = (preLongs > 1) ? extractCurCount(memObj, memAdd) : 0;
    final long thetaLong = (preLongs > 2) ? extractThetaLong(memObj, memAdd) : Long.MAX_VALUE;
    final DirectCompactOrderedSketch dcos =
        new DirectCompactOrderedSketch(empty, memSeedHash, curCount, thetaLong);
    dcos.preLongs_ = preLongs;
    dcos.mem_ = srcMem;
    return dcos;
  }

  /**   //TODO convert to factory
   * Converts the given UpdateSketch to this compact ordered form.
   * @param sketch the given UpdateSketch
   * @param dstMem the given destination Memory. This clears it before use.
   */
  DirectCompactOrderedSketch(final UpdateSketch sketch, final Memory dstMem) {
    super(sketch.isEmpty(),
        sketch.getSeedHash(),
        sketch.getRetainedEntries(true), //curCount_  set here
        sketch.getThetaLong()            //thetaLong_ set here
        );
    final int emptyBit = isEmpty() ? (byte) EMPTY_FLAG_MASK : 0;
    final byte flags =
        (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK | ORDERED_FLAG_MASK);
    final boolean ordered = true;
    final long[] compactOrderedCache =
        CompactSketch.compactCache(
            sketch.getCache(), getRetainedEntries(false), getThetaLong(), ordered);

    mem_ = loadCompactMemory(compactOrderedCache, isEmpty(), getSeedHash(),
        getRetainedEntries(false), getThetaLong(), dstMem, flags);
    preLongs_ = mem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
  }


  /**  //TODO convert to factory
   * Constructs this sketch from correct, valid components.
   * @param compactOrderedCache in compact, ordered form
   * @param empty The correct <a href="{@docRoot}/resources/dictionary.html#empty">Empty</a>.
   * @param seedHash The correct
   * <a href="{@docRoot}/resources/dictionary.html#seedHash">Seed Hash</a>.
   * @param curCount correct value
   * @param thetaLong The correct
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @param dstMem the destination Memory.  This clears it before use.
   */
  DirectCompactOrderedSketch(final long[] compactOrderedCache, final boolean empty,
      final short seedHash, final int curCount, final long thetaLong, final Memory dstMem) {
    super(empty, seedHash, curCount, thetaLong);
    final int emptyBit = isEmpty() ? (byte) EMPTY_FLAG_MASK : 0;
    final byte flags =
        (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK | ORDERED_FLAG_MASK);
    mem_ =
        loadCompactMemory(compactOrderedCache, empty, seedHash, curCount, thetaLong, dstMem, flags);
  }

  //Sketch interface

  @Override
  public byte[] toByteArray() {
    return DirectCompactSketch.compactMemoryToByteArray(mem_, getRetainedEntries(false));
  }

  //restricted methods

  @Override
  public boolean isDirect() {
    return true;
  }

  //SetArgument "interface"

  @Override
  long[] getCache() {
    final long[] cache = new long[getRetainedEntries(false)];
    mem_.getLongArray(preLongs_ << 3, cache, 0, getRetainedEntries(false));
    return cache;
  }

  @Override
  Memory getMemory() {
    return mem_;
  }

  @Override
  public boolean isOrdered() {
    return true;
  }

}
