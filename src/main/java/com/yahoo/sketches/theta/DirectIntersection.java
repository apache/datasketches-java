/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.CompactSketch.compactCachePart;
import static com.yahoo.sketches.theta.PreambleUtil.*;
import static com.yahoo.sketches.Util.*;
import static java.lang.Math.min;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectIntersection extends SetOperation implements Intersection {
  private final short seedHash;
  //Note: Intersection does not use lgNomLongs or k, per se.
  private int lgArrLongs; //current size of hash table
  private int curCount; //curCount of HT, if < 0 means Universal Set (US) is true
  private long thetaLong;
  private boolean empty;
  
  private final int maxLgArrLongs; //max size of hash table
  private final Memory mem;
  
  /**
   * Construct a new Intersection target direct to the given destination Memory.
   * Called by SetOperation.Builder.
   * 
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @param dstMem destination Memory.  
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  DirectIntersection(long seed, Memory dstMem) {
    int preLongs = CONST_PREAMBLE_LONGS;
    maxLgArrLongs = checkMaxLgArrLongs(dstMem);
    
    //build preamble and cache together in single Memory, insert fields into memory in one step
    long[] preArr = new long[preLongs]; //becomes the preamble
    
    long pre0 = 0;
    pre0 = insertPreLongs(preLongs, pre0); //RF not used = 0
    pre0 = insertSerVer(SER_VER, pre0);
    pre0 = insertFamilyID(Family.INTERSECTION.getID(), pre0);
    //Note: Intersection does not use lgNomLongs or k, per se.
    lgArrLongs = MIN_LG_ARR_LONGS; //set initially to minimum, but don't clear cache in mem
    pre0 = insertLgArrLongs(MIN_LG_ARR_LONGS, pre0);
    //flags: bigEndian = readOnly = compact = ordered = false;
    empty = false;
    pre0 = insertFlags(0, pre0);
    seedHash = computeSeedHash(seed);
    pre0 = insertSeedHash(seedHash, pre0);
    preArr[0] = pre0;
    
    long pre1 = 0;
    curCount = -1; //set in mem below
    pre1 = insertCurCount(-1, pre1);
    pre1 = insertP((float) 1.0, pre1);
    preArr[1] = pre1;
    
    thetaLong = Long.MAX_VALUE;
    preArr[2] = thetaLong;
    dstMem.putLongArray(0, preArr, 0, preLongs); //put into mem
    mem = dstMem;
  }
  
  /**
   * Wrap an Intersection target around the given source Memory containing intersection data. 
   * @param srcMem The source Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  DirectIntersection(Memory srcMem, long seed) {
    int preLongs = CONST_PREAMBLE_LONGS;
    long[] preArr = new long[preLongs];
    srcMem.getLongArray(0, preArr, 0, preLongs);
    
    long pre0 = preArr[0];
    int preLongsMem = extractPreLongs(pre0);
    if (preLongsMem != CONST_PREAMBLE_LONGS) {
      throw new IllegalArgumentException("PreambleLongs must = 3.");
    }
    int serVer = extractSerVer(pre0);
    if (serVer != 3) {
      throw new IllegalArgumentException("Ser Version must = 3");
    }
    int famID = extractFamilyID(pre0);
    Family.INTERSECTION.checkFamilyID(famID);
    //Note: Intersection does not use lgNomLongs or k, per se.
    lgArrLongs = extractLgArrLongs(pre0); //current hash table size
    maxLgArrLongs = checkMaxLgArrLongs(srcMem);
    int flags = extractFlags(pre0);
    empty = (flags & EMPTY_FLAG_MASK) > 0;
    
    seedHash = computeSeedHash(seed);
    short seedHashMem = srcMem.getShort(SEED_HASH_SHORT);
    Util.checkSeedHashes(seedHashMem, seedHash); //check for seed hash conflict
    
    curCount = srcMem.getInt(RETAINED_ENTRIES_INT);
    thetaLong = srcMem.getLong(THETA_LONG);
    
    if (empty && curCount != 0) {
      throw new IllegalArgumentException(
          "srcMem empty state inconsistent with curCount: "+ empty +","+ curCount);
      //empty = true AND curCount = 0: OK
    } //else empty = false, curCount could be anything
    mem = srcMem;
  }
  
  @Override
  public void update(Sketch sketchIn) {
    
    if (sketchIn == null) { //null := Th = 1.0, count = 0, empty = true
      //Can't check the seedHash
      if (curCount < 0) { //1st Call
        curCount = setCurCount(0);
        empty = setEmpty(true); //The Empty rule is OR
        thetaLong = setThetaLong(Long.MAX_VALUE);
      } else { //Nth Call
        curCount = setCurCount(0);
        empty = setEmpty(true);
        //theta stays the same
      }
      return;
    }
    
    //The Intersection State Machine
    int sketchInEntries = sketchIn.getRetainedEntries(true);
    
    if ((curCount == 0) || (sketchInEntries == 0)) {
      //The 1st Call (curCount  < 0) and sketchInEntries == 0.
      //The Nth Call (curCount == 0) and sketchInEntries == 0.
      //The Nth Call (curCount == 0) and sketchInEntries  > 0.
      //The Nth Call (curCount  > 0) and sketchInEntries == 0.
      //All future intersections result in zero data, but theta can still be reduced.
      
      Util.checkSeedHashes(seedHash, sketchIn.getSeedHash());
      thetaLong = minThetaLong(sketchIn.getThetaLong());
      empty = setEmpty(empty || sketchIn.isEmpty());  //Empty rule
      curCount = setCurCount(0);
      //No need for a HT.
    }
    else if (curCount < 0) { //virgin
      //The 1st Call (curCount  < 0) and sketchInEntries  > 0.
      //Clone the incoming sketch
      Util.checkSeedHashes(seedHash, sketchIn.getSeedHash());
      thetaLong = minThetaLong(sketchIn.getThetaLong());
      empty = setEmpty(empty || sketchIn.isEmpty());  //Empty rule
      
      curCount = setCurCount(sketchIn.getRetainedEntries(true));
      
      //Allocate a HT, checks lgArrLongs, then moves data to HT
      int requiredLgArrLongs = computeMinLgArrLongsFromCount(curCount);
      if (requiredLgArrLongs <= maxLgArrLongs) { //OK
        lgArrLongs = setLgArrLongs(requiredLgArrLongs);
        mem.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs);
      }
      else { //not enough space in dstMem //TODO move to request model
        throw new IllegalArgumentException(
            "Insufficient dstMem hash table space: "+(1<<requiredLgArrLongs)+" > "+(1<< lgArrLongs));
      }
      moveDataToHT(sketchIn.getCache(), curCount);
    }
    else { //curCount > 0
      //The Nth Call (curCount  > 0) and sketchInEntries  > 0.
      //Must perform full intersect
      Util.checkSeedHashes(seedHash, sketchIn.getSeedHash());
      thetaLong = minThetaLong(sketchIn.getThetaLong());
      empty = setEmpty(empty || sketchIn.isEmpty());
      
      // sets resulting hashTable, curCount and adjusts lgArrLongs
      performIntersect(sketchIn);
    }
  }

  @Override
  public CompactSketch getResult(boolean dstOrdered, Memory dstMem) {
    if (curCount < 0) {
      throw new IllegalStateException(
          "Calling getResult() with no intervening intersections is not a legal result.");
    }
    long[] compactCacheR;
    
    if (curCount == 0) {
      compactCacheR = new long[0];
      return CompactSketch.createCompactSketch(
          compactCacheR, empty, seedHash, curCount, thetaLong, dstOrdered, dstMem);
    } 
    //else curCount > 0
    int htLen = 1 << lgArrLongs;
    long[] hashTable = new long[htLen];
    mem.getLongArray(CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen);
    compactCacheR = compactCachePart(hashTable, lgArrLongs, curCount, thetaLong, dstOrdered);
    
    //Create the CompactSketch
    return CompactSketch.createCompactSketch(
        compactCacheR, empty, seedHash, curCount, thetaLong, dstOrdered, dstMem);
  }
  
  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }
  
  @Override
  public boolean hasResult() {
    return mem.getInt(RETAINED_ENTRIES_INT) >= 0;
  }
  
  @Override
  public byte[] toByteArray() {
    int preBytes = CONST_PREAMBLE_LONGS << 3;
    int dataBytes = (curCount > 0)? 8 << lgArrLongs : 0;
    int totalBytes = preBytes + dataBytes;
    byte[] byteArrOut = new byte[totalBytes];
    mem.getByteArray(0, byteArrOut, 0, totalBytes);
    return byteArrOut;
  }
  
  @Override
  public void reset() {
    lgArrLongs = 0;
    mem.putByte(LG_ARR_LONGS_BYTE, (byte) (lgArrLongs));
    curCount = setCurCount(-1); //Universal Set is true
    thetaLong = setThetaLong(Long.MAX_VALUE);
    empty = setEmpty(false);
  }
  
  @Override
  public Family getFamily() {
    return Family.INTERSECTION;
  }
  
  private void performIntersect(Sketch sketchIn) {
    // HT and input data are nonzero, match against HT
    assert (curCount > 0) && !empty;
    long[] cacheIn = sketchIn.getCache();
    int arrLongsIn = cacheIn.length;
    
    int htLen = 1 << lgArrLongs;
    long[] hashTable = new long[htLen];
    mem.getLongArray(CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen);
    
    //allocate space for matching
    long[] matchSet = new long[ min(curCount, sketchIn.getRetainedEntries(true)) ];

    int matchSetCount = 0;
    if (sketchIn.isOrdered()) {
      //ordered compact, which enables early stop
      for (int i = 0; i < arrLongsIn; i++ ) {
        long hashIn = cacheIn[i];
        if (hashIn <= 0L) {
          continue;
        }
        if (hashIn >= thetaLong) {
          break; //early stop assumes that hashes in input sketch are ordered!
        }
        int foundIdx = HashOperations.hashSearch(hashTable, lgArrLongs, hashIn);
        if (foundIdx == -1) {
          continue;
        }
        matchSet[matchSetCount++] = hashIn;
      }

    } 
    else {
      //either unordered compact or hash table
      for (int i = 0; i < arrLongsIn; i++ ) {
        long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= thetaLong)) {
          continue;
        }
        int foundIdx = HashOperations.hashSearch(hashTable, lgArrLongs, hashIn);
        if (foundIdx == -1) {
          continue;
        }
        matchSet[matchSetCount++] = hashIn;
      }
    }
    //reduce effective array size to minimum
    lgArrLongs = setLgArrLongs(computeMinLgArrLongsFromCount(curCount));
    curCount = setCurCount(matchSetCount);
    mem.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs); //clear for rebuild
    //move matchSet to hash table
    moveDataToHT(matchSet, matchSetCount);
  }
  
  private void moveDataToHT(long[] arr, int count) {
    int arrLongsIn = arr.length;
    int tmpCnt = 0;
    int preBytes = CONST_PREAMBLE_LONGS << 3;
    for (int i = 0; i < arrLongsIn; i++ ) {
      long hashIn = arr[i];
      if (HashOperations.continueCondition(thetaLong, hashIn)) {
        continue;
      }
      // opportunity to use faster unconditional insert
      tmpCnt += 
          HashOperations.hashSearchOrInsert(mem, lgArrLongs, hashIn, preBytes) < 0 ? 1 : 0;
    }
    if (tmpCnt != count) {
      throw new IllegalArgumentException("Count Check Exception: got: "+tmpCnt+", expected: "+count);
    }
  }
  
  //special handlers
  
  private static final int checkMaxLgArrLongs(Memory dstMem) {
    int preBytes = CONST_PREAMBLE_LONGS << 3;
    long cap = dstMem.getCapacity();
    int maxLgArrLongs = Integer.numberOfTrailingZeros(floorPowerOf2((int)(cap - preBytes)) >>> 3);
    if (maxLgArrLongs < MIN_LG_ARR_LONGS) {
      throw new IllegalArgumentException(
        "dstMem not large enough for minimum sized hash table: "+ cap);
    }
    return maxLgArrLongs;
  }
  
  private final boolean setEmpty(boolean empty) {
    if (empty) {
      mem.setBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    } 
    else {
      mem.clearBits(FLAGS_BYTE, (byte)EMPTY_FLAG_MASK);
    }
    return empty;
  }
  
  private final int setLgArrLongs(int lgArrLongs) {
    mem.putByte(LG_ARR_LONGS_BYTE, (byte) lgArrLongs);
    return lgArrLongs;
  }
  
  private final long setThetaLong(long thetaLong) {
    mem.putLong(THETA_LONG, thetaLong);
    return thetaLong;
  }
  
  private final long minThetaLong(long skThetaLong) {
    if (skThetaLong < thetaLong) {
      mem.putLong(THETA_LONG, skThetaLong);
      return skThetaLong;
    }
    return thetaLong;
  }
  
  private final int setCurCount(int curCount) {
    mem.putInt(RETAINED_ENTRIES_INT, curCount);
    return curCount;
  }
  
}
