/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;

/**
 * An off-heap (Direct), compact, read-only sketch
 * 
 * @author Lee Rhodes
 */
class DirectCompactSketch extends CompactSketch {
  private static final Family MY_FAMILY = Family.COMPACT;
  private Memory mem_;
    
  /**
   * Wraps the given Memory.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  DirectCompactSketch(Memory srcMem) {
    super(srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK), 
        srcMem.getShort(SEED_HASH_SHORT),
        getCurCount(srcMem), 
        getThetaLong(srcMem)
        );
    MY_FAMILY.checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    mem_ = srcMem;
  }
  
  /**
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
    int emptyBit = empty_? (byte) EMPTY_FLAG_MASK : 0;
    byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK);
    boolean ordered = false;
    long[] compactCache = 
        CompactSketch.compactCache(sketch.getCache(), curCount_, thetaLong_, ordered);
    mem_ = loadCompactMemory(compactCache, empty_, seedHash_, curCount_, thetaLong_, dstMem, flags);
  }
  
  /**
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
    int emptyBit = empty? (byte) EMPTY_FLAG_MASK : 0;
    byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK);
    mem_ = loadCompactMemory(compactCache, empty, seedHash, curCount, thetaLong, dstMem, flags);
  }
  
  //Sketch interface
  
  @Override
  public byte[] toByteArray() {
    return DirectCompactSketch.compactMemoryToByteArray(mem_, curCount_);
  }

  //restricted methods
  
  @Override
  public boolean isDirect() {
    return true; 
  }
  
  //SetArgument "interface"
  
  @Override
  long[] getCache() {
    long[] cache = new long[curCount_];
    int preLongs = mem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    mem_.getLongArray(preLongs << 3, cache, 0, curCount_);
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