/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.stringToFamily;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.insertCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.insertFlags;
import static com.yahoo.sketches.theta.PreambleUtil.insertP;
import static com.yahoo.sketches.theta.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.insertSerVer;

import java.util.Arrays;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
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
  private final short seedHash_;
  private final boolean empty_;
  private final int curCount_;
  private final long thetaLong_;
  
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
  
  @Override
  public Family getFamily() {
    return Family.COMPACT;
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
      //default: //This cannot happen and cannot be tested
    }
    return sketchOut;
  }
  
  static final Memory loadCompactMemory(
      long[] compactCache, boolean empty, short seedHash, int curCount, 
      long thetaLong, Memory dstMem, byte flags) {
    int preLongs = compactPreambleLongs(thetaLong, empty);
    int outLongs = preLongs + curCount;
    int outBytes = outLongs << 3;
    int dstBytes = (int) dstMem.getCapacity();
    if (outBytes > dstBytes) {
      throw new SketchesArgumentException("Insufficient Memory: "+dstBytes+", Need: "+outBytes);
    }
    byte famID = (byte) stringToFamily("Compact").getID();
    
    long[] outArr = new long[outLongs];
    long pre0 = 0;
    pre0 = insertPreLongs(preLongs, pre0); //RF not used = 0
    pre0 = insertSerVer(SER_VER, pre0);
    pre0 = insertFamilyID(famID, pre0);
    //ignore lgNomLongs, lgArrLongs bytes for compact sketches
    pre0 = insertFlags(flags, pre0);
    pre0 = insertSeedHash(seedHash, pre0);
    outArr[0] = pre0;
    
    if (preLongs > 1) {
      long pre1 = 0;
      pre1 = insertCurCount(curCount, pre1);
      pre1 = insertP((float) 1.0, pre1);
      outArr[1] = pre1;
    }
    if (preLongs > 2) {
      outArr[2] = thetaLong;
    }
    if ((compactCache != null) && (curCount > 0)) {
      System.arraycopy(compactCache, 0, outArr, preLongs, curCount);
    }
    dstMem.putLongArray(0, outArr, 0, outLongs);
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
