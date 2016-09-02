/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import com.yahoo.memory.Memory;

import static com.yahoo.sketches.Util.checkSeedHashes;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.*;

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
  
  private DirectCompactOrderedSketch(boolean empty, short seedHash, int curCount, long thetaLong) {
    super(empty, seedHash, curCount, thetaLong);
  }
  
  /**
   * Wraps the given Memory, which may be a SerVer 1, 2, or 3 sketch.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param pre0 the first 8 bytes of the preamble
   * @param seed the update seed
   * @return this sketch
   */
  static DirectCompactOrderedSketch wrapInstance(Memory srcMem, long pre0, long seed) {
    int preLongs = extractPreLongs(pre0);
    int flags = extractFlags(pre0);
    boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    short memSeedHash = (short) extractSeedHash(pre0);
    short computedSeedHash = computeSeedHash(seed);
    checkSeedHashes(memSeedHash, computedSeedHash);
    int curCount = (preLongs > 1) ? srcMem.getInt(RETAINED_ENTRIES_INT) : 0;
    long thetaLong = (preLongs > 2) ? srcMem.getLong(THETA_LONG) : Long.MAX_VALUE;
    DirectCompactOrderedSketch dcos = 
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
  DirectCompactOrderedSketch(UpdateSketch sketch, Memory dstMem) {
    super(sketch.isEmpty(), 
        sketch.getSeedHash(), 
        sketch.getRetainedEntries(true), //curCount_  set here 
        sketch.getThetaLong()            //thetaLong_ set here
        );
    int emptyBit = isEmpty() ? (byte) EMPTY_FLAG_MASK : 0;
    byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK | ORDERED_FLAG_MASK);
    boolean ordered = true;
    long[] compactOrderedCache = 
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
  DirectCompactOrderedSketch(long[] compactOrderedCache, boolean empty, short seedHash, 
      int curCount, long thetaLong, Memory dstMem) {
    super(empty, seedHash, curCount, thetaLong);
    int emptyBit = isEmpty() ? (byte) EMPTY_FLAG_MASK : 0;
    byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK | ORDERED_FLAG_MASK);
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
    long[] cache = new long[getRetainedEntries(false)];
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
