/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.objectToFamily;
import static com.yahoo.sketches.theta.CompactSketch.compactCachePart;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSerVer;
import static java.lang.Math.min;

import java.util.Arrays;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class HeapIntersection extends SetOperation implements Intersection{
  private final short seedHash_;
  //Note: Intersection does not use lgNomLongs or k, per se.
  private int lgArrLongs_;
  private int curCount_; //curCount of HT, if < 0 means Universal Set (US) is true
  private long thetaLong_;
  private boolean empty_;
  
  private long[] hashTable_;  //HT => Data
  
  /**
   * Construct a new Intersection target on the java heap.
   * 
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   */
  HeapIntersection(long seed) {
    seedHash_ = computeSeedHash(seed);
    empty_ = false;
    curCount_ = -1;  //Universal Set is true
    thetaLong_ = Long.MAX_VALUE;
    lgArrLongs_ = 0;
    hashTable_ = null;
  }
  
  /**
   * Heapify an intersection target from a Memory image containing data. 
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  HeapIntersection(Memory srcMem, long seed) {
    int preLongs = CONST_PREAMBLE_LONGS;
    long[] preArr = new long[preLongs];
    srcMem.getLongArray(0, preArr, 0, preLongs);
    
    long pre0 = preArr[0];
    int preambleLongs = extractPreLongs(pre0);
    if (preambleLongs != CONST_PREAMBLE_LONGS) {
      throw new SketchesArgumentException("PreambleLongs must = 3.");
    }
    int serVer = extractSerVer(pre0);
    if (serVer != 3) throw new SketchesArgumentException("Ser Version must = 3");
    int famID = extractFamilyID(pre0);
    Family.INTERSECTION.checkFamilyID(famID);
    //Note: Intersection does not use lgNomLongs or k, per se.
    lgArrLongs_ = extractLgArrLongs(pre0); //current hash table size
    
    int flags = extractFlags(pre0);
    empty_ = (flags & EMPTY_FLAG_MASK) > 0;
    
    seedHash_ = computeSeedHash(seed);
    short seedHashMem = srcMem.getShort(SEED_HASH_SHORT);
    Util.checkSeedHashes(seedHashMem, seedHash_); //check for seed hash conflict
    
    curCount_ = srcMem.getInt(RETAINED_ENTRIES_INT);
    thetaLong_ = srcMem.getLong(THETA_LONG);
    
    if (empty_) {
      if (curCount_ != 0) {
        throw new SketchesArgumentException(
            "srcMem empty state inconsistent with curCount: "+empty_+","+curCount_);
      }
      //empty = true AND curCount_ = 0: OK
    }
    else { //empty = false, curCount could be anything
      if (curCount_ > 0) { //can't be virgin, empty, or curCount == 0
        hashTable_ = new long[1 << lgArrLongs_];
        srcMem.getLongArray(CONST_PREAMBLE_LONGS << 3, hashTable_, 0, 1 << lgArrLongs_);
      }
    }
  }
  
  @Override
  public void update(Sketch sketchIn) {
    
    if (sketchIn == null) { //null := Th = 1.0, count = 0, empty = true
      //Can't check the seedHash
      if (curCount_ < 0) { //1st Call
        curCount_ = 0;
        empty_ |= true;
        thetaLong_ = Long.MAX_VALUE;
      } else { //Nth Call
        curCount_ = 0;
        empty_ |= true;
        //theta stays the same
      }
      return;
    }
    
    //The Intersection State Machine
    int sketchInEntries = sketchIn.getRetainedEntries(true);
    
    if ((curCount_ == 0) || (sketchInEntries == 0)) {
      //The 1st Call (curCount  < 0) and sketchInEntries == 0.
      //The Nth Call (curCount == 0) and sketchInEntries == 0.
      //The Nth Call (curCount == 0) and sketchInEntries  > 0.
      //The Nth Call (curCount  > 0) and sketchInEntries == 0.
      //All future intersections result in zero data, but theta can still be reduced.

      Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
      thetaLong_ = min(thetaLong_, sketchIn.getThetaLong()); //Theta rule
      empty_ |= sketchIn.isEmpty();  //Empty rule
      curCount_ = 0;
      hashTable_ = null; //No need for HT.
    }
    else if (curCount_ < 0) { //virgin
      //The 1st Call (curCount  < 0) and sketchInEntries  > 0.
      //Clone the incoming sketch
      Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
      thetaLong_ = min(thetaLong_, sketchIn.getThetaLong());
      empty_ |= sketchIn.isEmpty(); //Empty rule
      
      curCount_ = sketchIn.getRetainedEntries(true);
      
      //Allocate a HT, checks lgArrLongs, then moves data to HT
      lgArrLongs_ = computeMinLgArrLongsFromCount(curCount_);
      hashTable_ = new long[1 << lgArrLongs_];
      //Then move data into HT
      moveDataToHT(sketchIn.getCache(), curCount_);
    }
    else { //curCount > 0
      //The Nth Call (curCount  > 0) and sketchInEntries  > 0.
      //Must perform full intersect
      Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
      thetaLong_ = min(thetaLong_, sketchIn.getThetaLong());
      empty_ |= sketchIn.isEmpty();
      
      //Sets resulting hashTable, curCount and adjusts lgArrLongs
      performIntersect(sketchIn);
    }
  }
  
  @Override
  public CompactSketch getResult(boolean dstOrdered, Memory dstMem) { 
    if (curCount_ < 0) {
      throw new SketchesStateException(
          "Calling getResult() with no intervening intersections is not a legal result.");
    }
    long[] compactCacheR;
    
    if (curCount_ == 0) {
      compactCacheR = new long[0];
      return CompactSketch.createCompactSketch(
          compactCacheR, empty_, seedHash_, curCount_, thetaLong_, dstOrdered, dstMem);
    } 
    //else curCount > 0
    compactCacheR = compactCachePart(hashTable_, lgArrLongs_, curCount_, thetaLong_, dstOrdered);
    
    //Create the CompactSketch
    return CompactSketch.createCompactSketch(
        compactCacheR, empty_, seedHash_, curCount_, thetaLong_, dstOrdered, dstMem);
  }
  
  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }
  
  @Override
  public boolean hasResult() {
    return curCount_ >= 0;
  }
  
  @Override
  public byte[] toByteArray() {
    int preBytes = CONST_PREAMBLE_LONGS << 3;
    int dataBytes = (curCount_ > 0)? 8 << lgArrLongs_ : 0;
    byte[] byteArrOut = new byte[preBytes + dataBytes];
    NativeMemory memOut = new NativeMemory(byteArrOut);
    
    //preamble
    memOut.putByte(PREAMBLE_LONGS_BYTE, (byte) CONST_PREAMBLE_LONGS); //RF not used = 0
    memOut.putByte(SER_VER_BYTE, (byte) SER_VER);
    memOut.putByte(FAMILY_BYTE, (byte) objectToFamily(this).getID());
    memOut.putByte(LG_NOM_LONGS_BYTE, (byte) 0); //not used
    memOut.putByte(LG_ARR_LONGS_BYTE, (byte) lgArrLongs_);
    if (empty_) {
      memOut.setBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    } 
    else {
      memOut.clearBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    }
    memOut.putShort(SEED_HASH_SHORT, seedHash_);
    memOut.putInt(RETAINED_ENTRIES_INT, curCount_);
    memOut.putFloat(P_FLOAT, (float) 1.0);
    memOut.putLong(THETA_LONG, thetaLong_);
    
    //data
    if (curCount_ > 0) {
      memOut.putLongArray(preBytes, hashTable_, 0, 1 << lgArrLongs_);
    }
    return byteArrOut;
  }
  
  @Override
  public void reset() { //retains the hashSeed.
    lgArrLongs_ = 0;
    hashTable_ = null;
    curCount_ = -1; //Universal Set is true
    thetaLong_ = Long.MAX_VALUE;
    empty_ = false;
  }
  
  @Override
  public Family getFamily() {
    return Family.INTERSECTION;
  }
  
  //restricted
  
  private void performIntersect(Sketch sketchIn) {
    // curCount and input data are nonzero, match against HT
    assert ((curCount_ > 0) && (!empty_));
    long[] cacheIn = sketchIn.getCache();
    int arrLongsIn = cacheIn.length;
    
    //allocate space for matching
    long[] matchSet = new long[ min(curCount_, sketchIn.getRetainedEntries(true)) ];

    int matchSetCount = 0;
    if (sketchIn.isOrdered()) {
      //ordered compact, which enables early stop
      for (int i = 0; i < arrLongsIn; i++ ) {
        long hashIn = cacheIn[i];
        if (hashIn <= 0L) continue;
        if (hashIn >= thetaLong_) {
          break; //early stop assumes that hashes in input sketch are ordered!
        }
        int foundIdx = HashOperations.hashSearch(hashTable_, lgArrLongs_, hashIn);
        if (foundIdx == -1) continue;
        matchSet[matchSetCount++] = hashIn;
      }

    } 
    else {
      //either unordered compact or hash table
      for (int i = 0; i < arrLongsIn; i++ ) {
        long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= thetaLong_)) continue;
        int foundIdx = HashOperations.hashSearch(hashTable_, lgArrLongs_, hashIn);
        if (foundIdx == -1) continue;
        matchSet[matchSetCount++] = hashIn;
      }
    }
    //reduce effective array size to minimum
    lgArrLongs_ = computeMinLgArrLongsFromCount(matchSetCount);
    curCount_ = matchSetCount;
    Arrays.fill(hashTable_, 0, 1 << lgArrLongs_, 0L); //clear for rebuild //TODO
    //move matchSet to hash table
    moveDataToHT(matchSet, matchSetCount);
  }
  
  //Assumes HT exists and is large enough
  private void moveDataToHT(long[] arr, int count) {
    int arrLongsIn = arr.length;
    int tmpCnt = 0;
    for (int i = 0; i < arrLongsIn; i++ ) {
      long hashIn = arr[i];
      if (HashOperations.continueCondition(thetaLong_, hashIn)) continue;
      // opportunity to use faster unconditional insert
      tmpCnt += 
          HashOperations.hashSearchOrInsert(hashTable_, lgArrLongs_, hashIn) < 0 ? 1 : 0;
    }
    if (tmpCnt != count) {
      throw new SketchesArgumentException("Count Check Exception: got: "+tmpCnt+", expected: "+count);
    }
  }
  
}
