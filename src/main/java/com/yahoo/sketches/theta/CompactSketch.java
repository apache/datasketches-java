/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.stringToFamily;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;

import java.util.Arrays;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;

/**
 * The parent class of all the CompactSketches. CompactSketches are never created directly. 
 * They are created as a result of the compact() method of an UpdateSketch or as a result of a 
 * getResult() of a SetOperation.
 * 
 * <p>A CompactSketch is the simplist form of a Theta Sketch. It consists of a compact list 
 * (i.e., no intervening spaces) of hash values, which may be ordered or not, a value for theta 
 * and a seed hash.  A CompactSketch is read-only,
 * and the space required when stored is only the space required for the hash values and 8 to 24
 * bytes of preamble. An empty CompactSketch consumes only 8 bytes.</p>
 * 
 * @author Lee Rhodes
 */
public abstract class CompactSketch extends Sketch {
  static final Family MY_FAMILY = Family.COMPACT;
  final short seedHash_;
  final boolean empty_;
  final int curCount_;
  final long thetaLong_;
  
  CompactSketch(boolean empty, short seedHash, int curCount, long thetaLong) {
    empty_ = empty;
    seedHash_ = seedHash;
    curCount_ = curCount;
    thetaLong_ = thetaLong;
  }
  
  //Sketch
  
  @Override
  public int getRetainedEntries(boolean valid) {
    return curCount_;
  }
  
  @Override
  public boolean isEmpty() {
    return empty_;
  }
  
  @Override
  public boolean isEstimationMode() {
    return Sketch.estMode(getThetaLong(), isEmpty());
  }
  
  //SetArgument
  
  @Override
  short getSeedHash() {
    return seedHash_;
  }

  @Override
  long getThetaLong() {
    return thetaLong_;
  }
  
  //restricted methods
  
  @Override
  int getPreambleLongs() {
    return compactPreambleLongs(getThetaLong(), isEmpty());
  }
  
  @Override
  public boolean isCompact() {
    return true;
  }
  
  /**
   * Compact the given array.
   * @param srcCache anything
   * @param curCount must be correct
   * @param thetaLong The correct <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @param dstOrdered true if output array must be sorted
   * @return the compacted array
   */
  static final long[] compactCache(final long[] srcCache, int curCount, long thetaLong, boolean dstOrdered) {
    if (curCount == 0) {
      return new long[0];
    }
    long[] cacheOut = new long[curCount];
    int len = srcCache.length;
    int j=0;
    for (int i=0; i<len; i++) {
      long v = srcCache[i];
      if ((v <= 0L) || (v >= thetaLong) ) continue;
      cacheOut[j++] = v;
    }
    assert curCount == j;
    if (dstOrdered) {
      Arrays.sort(cacheOut);
    }
    return cacheOut;
  }
  
  /**
   * Compact first 2^lgArrLongs of given array
   * @param srcCache anything
   * @param lgArrLongs The correct <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">lgArrLongs</a>.
   * @param curCount must be correct
   * @param thetaLong The correct <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @param dstOrdered true if output array must be sorted
   * @return the compacted array
   */
  static final long[] compactCachePart(final long[] srcCache, int lgArrLongs, int curCount, 
      long thetaLong, boolean dstOrdered) {
    if (curCount == 0) {
      return new long[0];
    }
    long[] cacheOut = new long[curCount];
    int len = 1 << lgArrLongs;
    int j=0;
    for (int i=0; i<len; i++) {
      long v = srcCache[i];
      if ((v <= 0L) || (v >= thetaLong) ) continue;
      cacheOut[j++] = v;
    }
    assert curCount == j;
    if (dstOrdered) {
      Arrays.sort(cacheOut);
    }
    return cacheOut;
  }
  
  static final CompactSketch createCompactSketch(
      long[] compactCache, boolean empty, short seedHash, int curCount, long thetaLong, 
      boolean dstOrdered, Memory dstMem) {
    CompactSketch sketchOut = null;
    int sw = (dstOrdered? 2:0) | ((dstMem != null)? 1:0);
    switch (sw) {
      case 0: { //dst not ordered, dstMem == null 
        sketchOut = new HeapCompactSketch(compactCache, empty, seedHash, curCount, thetaLong);
        break;
      }
      case 1: { //dst not ordered, dstMem == valid
        sketchOut = new DirectCompactSketch(compactCache, empty, seedHash, curCount, thetaLong, dstMem);
        break;
      }
      case 2: { //dst ordered, dstMem == null
        sketchOut = new HeapCompactOrderedSketch(compactCache, empty, seedHash, curCount, thetaLong);
        break;
      }
      case 3: { //dst ordered, dstMem == valid        
        sketchOut = new DirectCompactOrderedSketch(compactCache, empty, seedHash, curCount, thetaLong, dstMem);
        break;
      }
    }
    return sketchOut;
  }
  
  static final Memory loadCompactMemory(
      long[] compactCache, boolean empty, short seedHash, int curCount, 
      long thetaLong, Memory dstMem, byte flags) {
    int preLongs = compactPreambleLongs(thetaLong, empty);
    int preBytes = preLongs << 3;
    int outBytes = (curCount << 3) + preBytes;
    int dstBytes = (int) dstMem.getCapacity();
    if (outBytes > dstBytes) {
      throw new IllegalArgumentException("Insufficient Memory: "+dstBytes+", Need: "+outBytes);
    }
    byte fam = (byte) stringToFamily("Compact").getID();
    
    dstMem.clear(0, outBytes);
    dstMem.putByte(PREAMBLE_LONGS_BYTE, (byte) preLongs); //RF not used = 0
    dstMem.putByte(SER_VER_BYTE, (byte) SER_VER);
    dstMem.putByte(FAMILY_BYTE, fam);
    //ignore lgNomLongs, lgArrLongs bytes for compact sketches
    dstMem.putByte(FLAGS_BYTE, flags);
    dstMem.putShort(SEED_HASH_SHORT, seedHash);
    if (preLongs > 1) {
      dstMem.putInt(RETAINED_ENTRIES_INT, curCount);
      dstMem.putFloat(P_FLOAT, (float)1.0);
    }
    if (preLongs > 2) {
      dstMem.putLong(THETA_LONG, thetaLong);
    }
    if ((compactCache != null) && (curCount > 0)) {
      dstMem.putLongArray(preBytes, compactCache, 0, compactCache.length);
    }
    return dstMem;
  }
  
  static final int getCurCount(Memory srcMem) {
    int preLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    int curCount = (preLongs > 1)? srcMem.getInt(RETAINED_ENTRIES_INT) : 0;
    return curCount;
  }
  
  static final long getThetaLong(Memory srcMem) {
    int preLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    long thetaLong = (preLongs > 2)? srcMem.getLong(THETA_LONG): Long.MAX_VALUE;
    return thetaLong;
  }
  
}
