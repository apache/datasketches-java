/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * An on-heap, compact, ordered, read-only sketch.
 * 
 * @author Lee Rhodes
 */
class HeapCompactOrderedSketch extends CompactSketch {
  private final long[] cache_;
  
  /**
   * Heapifies the given Memory.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  HeapCompactOrderedSketch(Memory srcMem) {
    super(srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK), 
        srcMem.getShort(SEED_HASH_SHORT),
        getCurCount(srcMem), 
        getThetaLong(srcMem)
        );
    getFamily().checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    cache_ = new long[getRetainedEntries(false)];
    int preLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    int preBytes = compactPreambleLongs(getThetaLong(), isEmpty()) << 3;
    assert (preLongs << 3) == preBytes;
    srcMem.getLongArray(preBytes, cache_, 0, getRetainedEntries(false));
  }
  
  /**
   * Converts the given UpdateSketch to this compact form.
   * @param sketch the given UpdateSketch
   */
  HeapCompactOrderedSketch(UpdateSketch sketch) {
    super(sketch.isEmpty(), 
        sketch.getSeedHash(), 
        sketch.getRetainedEntries(true), //curCount_  set here
        sketch.getThetaLong()            //thetaLong_ set here
        );
    boolean ordered = true;
    cache_ = CompactSketch.compactCache(sketch.getCache(), getRetainedEntries(false), getThetaLong(), ordered);
  }
  
  /**
   * Constructs this sketch from correct, valid components.
   * @param compactOrderedCache in compact, ordered form
   * @param empty The correct <a href="{@docRoot}/resources/dictionary.html#empty">Empty</a>.
   * @param seedHash The correct <a href="{@docRoot}/resources/dictionary.html#seedHash">Seed Hash</a>.
   * @param curCount correct value
   * @param thetaLong The correct <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   */
  HeapCompactOrderedSketch(long[] compactOrderedCache, boolean empty, short seedHash, int curCount, 
      long thetaLong) {
    super(empty, seedHash, curCount, thetaLong);
    assert compactOrderedCache != null;
    cache_ = (curCount == 0)? new long[0] : compactOrderedCache;
  }
  
  //Sketch interface
   
  @Override
  public byte[] toByteArray() {
    byte[] byteArray = new byte[getCurrentBytes(true)];
    Memory dstMem = new NativeMemory(byteArray);
    int emptyBit = isEmpty()? (byte) EMPTY_FLAG_MASK : 0;
    byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK | ORDERED_FLAG_MASK);
    dstMem = loadCompactMemory(getCache(), isEmpty(), getSeedHash(), getRetainedEntries(true),
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
    return true;
  }
  
}
