/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.isMultipleOf8AndGT0;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;

import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;

/**
 * Used to convert older serialization versions 1 and 2 to version 3.  The Serialization
 * Version is the version of the sketch binary image format and should not be confused with the
 * version number of the Open Source DataSketches Library.
 * 
 * @author Lee Rhodes
 */
class ForwardCompatibility {
  
  /**
   * Convert a serialization version (SerVer) 1 sketch to a SerVer 3 HeapCompactOrderedSketch.
   * Note: SerVer 1 sketches always have metadata-longs of 3 and are always stored
   * in a compact ordered form, but with 3 different sketch types.  All SerVer 1 sketches will
   * be converted to a SerVer 3, HeapCompactOrderedSketch.
   * 
   * @param srcMem the image of a SerVer 1 sketch
   * 
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * The seed used for building the sketch image in srcMem.  
   * Note: SerVer 1 sketches do not have the concept of the SeedHash, so the seed provided here 
   * MUST be the actual seed that was used when the SerVer 1 sketches were built. 
   * @return a SerVer 3 HeapCompactOrderedSketch.
   */
  static final CompactSketch heapify1to3(Memory srcMem, long seed) {
    int memCap = (int) srcMem.getCapacity();
    assert(isMultipleOf8AndGT0(memCap));
    
    short seedHash = Util.computeSeedHash(seed);
    
    if (memCap <= 24) { //return empty
      return new HeapCompactOrderedSketch(new long[0], true, seedHash, 0, Long.MAX_VALUE);
    }
    
    int curCount = srcMem.getInt(RETAINED_ENTRIES_INT);
    
    int mdLongs = 3;
    int reqBytesIn = (curCount + mdLongs) << 3;
    validateInputSize(reqBytesIn, memCap);
    
    long thetaLong = srcMem.getLong(THETA_LONG);
    
    long[] compactOrderedCache = new long[curCount];
    srcMem.getLongArray(24, compactOrderedCache, 0, curCount);
    
    return new HeapCompactOrderedSketch(compactOrderedCache, false, seedHash, curCount, thetaLong);
  }
  
  /**
   * Convert a serialization version (SerVer) 2 sketch to a SerVer 3 HeapCompactOrderedSketch.
   * Note: SerVer 2 sketches can have metadata-longs of 1,2 or 3 and are always stored
   * in a compact ordered form, but with 4 different sketch types.  
   * @param srcMem the image of a SerVer 1 sketch
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * The seed used for building the sketch image in srcMem
   * @return a SerVer 3 HeapCompactOrderedSketch
   */
  static final CompactSketch heapify2to3(Memory srcMem, long seed) {
    int memCap = (int) srcMem.getCapacity();
    assert(isMultipleOf8AndGT0(memCap));
    
    short seedHash = Util.computeSeedHash(seed);
    short memSeedHash = srcMem.getShort(SEED_HASH_SHORT);
    Util.checkSeedHashes(seedHash, memSeedHash);
    
    if (memCap == 8) { //return empty
      return new HeapCompactOrderedSketch(new long[0], true, seedHash, 0, Long.MAX_VALUE);
    }
    
    int curCount = srcMem.getInt(RETAINED_ENTRIES_INT);
    //Note: curCount could be zero and theta < 1.0 and be non-empty.
    
    int mdLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F; //either 2 or 3
    int reqBytesIn = (curCount + mdLongs) << 3;
    validateInputSize(reqBytesIn, memCap);
    
    long thetaLong = (mdLongs < 3)? Long.MAX_VALUE : srcMem.getLong(THETA_LONG);
    boolean empty = srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    
    long[] compactOrderedCache = new long[curCount];
    srcMem.getLongArray(mdLongs << 3, compactOrderedCache, 0, curCount);
    
    return new HeapCompactOrderedSketch(compactOrderedCache, empty, seedHash, curCount, thetaLong);
  }
  
  private static final void validateInputSize(int reqBytesIn, int memCap) {
    if (reqBytesIn > memCap) {
      throw new IllegalArgumentException(
        "Input Memory or byte[] size is too small: Required Bytes: "+reqBytesIn+
        ", bytesIn: "+memCap);
    }
  }
  
}
