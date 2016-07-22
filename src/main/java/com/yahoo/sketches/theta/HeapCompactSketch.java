/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.checkSeedHashes;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * An on-heap, compact, read-only sketch.
 * 
 * @author Lee Rhodes
 */
final class HeapCompactSketch extends CompactSketch {
  private final long[] cache_;
  
  private HeapCompactSketch(boolean empty, short seedHash, int curCount, long thetaLong, 
      long[] cache) {
    super(empty, seedHash, curCount, thetaLong);
    cache_ = cache;
  }
  
  /**
   * Heapifies the given source Memory with seed
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>. 
   * @return this sketch
   */
  static HeapCompactSketch heapifyInstance(Memory srcMem, long seed) {
    long pre0 = srcMem.getLong(PREAMBLE_LONGS_BYTE);
    int preLongs = extractPreLongs(pre0);
    int flags = extractFlags(pre0);
    boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    short memSeedHash = (short) extractSeedHash(pre0);
    short computedSeedHash = computeSeedHash(seed);
    checkSeedHashes(memSeedHash, computedSeedHash);
    int curCount = (preLongs > 1) ? srcMem.getInt(RETAINED_ENTRIES_INT) : 0;
    long thetaLong = (preLongs > 2) ? srcMem.getLong(THETA_LONG) : Long.MAX_VALUE;
    long[] cacheArr = new long[curCount];
    if (curCount > 0) {
      srcMem.getLongArray(preLongs << 3, cacheArr, 0, curCount);
    }
    HeapCompactSketch hcs = 
        new HeapCompactSketch(empty, memSeedHash, curCount, thetaLong, cacheArr);
    return hcs;
  }
  
  /**
   * Converts the given UpdateSketch to this compact form.
   * @param sketch the given UpdateSketch
   */
  HeapCompactSketch(UpdateSketch sketch) {
    super(sketch.isEmpty(),
        sketch.getSeedHash(),
        sketch.getRetainedEntries(true), //curCount_  set here
        sketch.getThetaLong()            //thetaLong_ set here
        );
    boolean ordered = false;
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
  HeapCompactSketch(long[] compactCache, boolean empty, short seedHash, int curCount, 
      long thetaLong) {
    super(empty, seedHash, curCount, thetaLong);
    assert compactCache != null;
    cache_ = (curCount == 0) ? new long[0] : compactCache;
  }
  
  //Sketch interface
  
  @Override
  public byte[] toByteArray() {
    int bytes = getCurrentBytes(true);
    byte[] byteArray = new byte[bytes];
    Memory dstMem = new NativeMemory(byteArray);
    int emptyBit = isEmpty() ? (byte) EMPTY_FLAG_MASK : 0;
    byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK);
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
