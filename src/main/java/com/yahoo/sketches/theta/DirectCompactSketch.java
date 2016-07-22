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

/**
 * An off-heap (Direct), compact, unordered, read-only sketch.  This sketch can only be associated
 * with a Serialization Version 3 format binary image.
 * 
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 * 
 * @author Lee Rhodes
 */
final class DirectCompactSketch extends CompactSketch {
  private Memory mem_;
  private int preLongs_; //1, 2, or 3
  
  private DirectCompactSketch(boolean empty, short seedHash, int curCount, long thetaLong) {
    super(empty, seedHash, curCount, thetaLong);
  }
  
  /**
   * Wraps the given Memory, which must be a SerVer 3, unordered, Compact Sketch
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param pre0 the first 8 bytes of the preamble
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>. 
   * @return this sketch
   */
  static DirectCompactSketch wrapInstance(Memory srcMem, long pre0, long seed) {
    int preLongs = extractPreLongs(pre0);
    int flags = extractFlags(pre0);
    boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    short memSeedHash = (short) extractSeedHash(pre0);
    short computedSeedHash = computeSeedHash(seed);
    checkSeedHashes(memSeedHash, computedSeedHash);
    int curCount = (preLongs > 1) ? srcMem.getInt(RETAINED_ENTRIES_INT) : 0;
    long thetaLong = (preLongs > 2) ? srcMem.getLong(THETA_LONG) : Long.MAX_VALUE;
    DirectCompactSketch dcs = new DirectCompactSketch(empty, memSeedHash, curCount, thetaLong);
    dcs.preLongs_ = extractPreLongs(pre0);
    dcs.mem_ = srcMem;
    return dcs;
  }
  
  /**  //TODO convert to factory
   * Converts the given UpdateSketch to this compact form.
   * @param sketch the given UpdateSketch
   * @param dstMem the given destination Memory.  This clears it before use.
   */
  DirectCompactSketch(UpdateSketch sketch, Memory dstMem) {
    super(sketch.isEmpty(), 
        sketch.getSeedHash(), 
        sketch.getRetainedEntries(true), //curCount_  set here
        sketch.getThetaLong()            //thetaLong_ set here
        );
    int emptyBit = isEmpty() ? (byte) EMPTY_FLAG_MASK : 0;
    byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK);
    boolean ordered = false;
    long[] compactCache = 
        CompactSketch.compactCache(sketch.getCache(), getRetainedEntries(false), getThetaLong(), ordered);
    mem_ = loadCompactMemory(compactCache, isEmpty(), getSeedHash(), getRetainedEntries(false), 
        getThetaLong(), dstMem, flags);
  }
  
  /**  //TODO convert to factory
   * Constructs this sketch from correct, valid components.
   * @param compactCache in compact form
   * @param empty The correct <a href="{@docRoot}/resources/dictionary.html#empty">Empty</a>.
   * @param seedHash The correct <a href="{@docRoot}/resources/dictionary.html#seedHash">Seed Hash</a>.
   * @param curCount correct value
   * @param thetaLong The correct <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @param dstMem the destination Memory. This clears it before use.
   */
  DirectCompactSketch(long[] compactCache, boolean empty, short seedHash, int curCount, 
      long thetaLong, Memory dstMem) {
    super(empty, seedHash, curCount, thetaLong);
    int emptyBit = empty ? (byte) EMPTY_FLAG_MASK : 0;
    byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK);
    mem_ = loadCompactMemory(compactCache, empty, seedHash, curCount, thetaLong, dstMem, flags);
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
    return false;
  }

  /**
   * Serializes a Memory based compact sketch to a byte array
   * @param srcMem the source Memory 
   * @param curCount the current valid count
   * @return this Direct, Compact sketch as a byte array
   */  
  static byte[] compactMemoryToByteArray(final Memory srcMem, int curCount) {
    int preLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    int outBytes = (curCount << 3) + (preLongs << 3);
    byte[] byteArrOut = new byte[outBytes];
    srcMem.getByteArray(0, byteArrOut, 0, outBytes);
    return byteArrOut;
  }
  
}
