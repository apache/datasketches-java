/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.objectToFamily;
import static com.yahoo.sketches.theta.CompactSketch.compactCachePart;
import static com.yahoo.sketches.theta.HashOperations.hashInsert;
import static com.yahoo.sketches.theta.HashOperations.hashSearch;
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
import static com.yahoo.sketches.theta.PreambleUtil.checkSeedHashes;
import static java.lang.Math.min;

import java.util.Arrays;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class HeapIntersection extends SetOperation implements Intersection{
  private static final Family MY_FAMILY = Family.INTERSECTION;
  private final short seedHash_;
  private int lgArrLongs_ = 0;
  private int curCount_; //curCount of HT, if < 0 means Universal Set (US) is true
  private long thetaLong_;
  private boolean empty_;
  
  private long[] hashTable_ = null;  //HT => Data
  
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
  }
  
  /**
   * Heapify an intersection target from a Memory image containing data. 
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  HeapIntersection(Memory srcMem, long seed) {
    int preambleLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    if (preambleLongs != CONST_PREAMBLE_LONGS) {
        throw new IllegalArgumentException("PreambleLongs must = 3.");
    }
    
    int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer != 3) throw new IllegalArgumentException("Ser Version must = 3");
    
    MY_FAMILY.checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    
    lgArrLongs_ = srcMem.getByte(LG_ARR_LONGS_BYTE);
    
    empty_ = srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    
    seedHash_ = computeSeedHash(seed);
    short seedHashMem = srcMem.getShort(SEED_HASH_SHORT);
    checkSeedHashes(seedHashMem, seedHash_); //check for seed hash conflict
    
    curCount_ = srcMem.getInt(RETAINED_ENTRIES_INT);
    thetaLong_ = srcMem.getLong(THETA_LONG);
    
    if (empty_) {
      if (curCount_ != 0) {
        throw new IllegalArgumentException(
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
  @SuppressWarnings("null") //due to the state machine construction
  public void update(Sketch sketchIn) {
    
    //The Intersection State Machine
    boolean skInIsValidAndNonZero = ((sketchIn != null) && (sketchIn.getRetainedEntries(true) > 0));
    
    if ((curCount_ == 0) || !skInIsValidAndNonZero) {
      //The 1st Call (curCount  < 0) and sketchIn was either null or had zero entries.
      //The Nth Call (curCount == 0) and sketchIn was either null or had zero entries.
      //The Nth Call (curCount == 0) and sketchIn was valid with cnt > 0.
      //The Nth Call (curCount  > 0) and sketchIn was either null or had zero entries.
      //All future intersections result in zero data, but theta can still be reduced.
      //set curCount == 0
      if (sketchIn != null) {
        checkSeedHashes(seedHash_, sketchIn.getSeedHash());
        thetaLong_ = min(thetaLong_, sketchIn.getThetaLong());
        empty_ |= sketchIn.isEmpty();  //Empty rule
      } 
      else { //null
        //Don't change Theta
        empty_ = true; //effectively the Empty rule
      }
      curCount_ = 0;
      hashTable_ = null; //No need for HT.
    }
    else if (curCount_ < 0) {
      //The 1st Call and sketchIn was a valid with cnt > 0.
      //Clone the incoming sketch
      checkSeedHashes(seedHash_, sketchIn.getSeedHash());
      thetaLong_ = min(thetaLong_, sketchIn.getThetaLong());
      empty_ |= sketchIn.isEmpty();
      
      curCount_ = sketchIn.getRetainedEntries(true);
      //Allocate a HT, checks lgArrLongs
      lgArrLongs_ = computeMinLgArrLongsFromCount(curCount_);
      hashTable_ = new long[1 << lgArrLongs_];
      //Then move data into HT
      moveDataToHT(sketchIn.getCache(), curCount_);
    }
    else { //curCount > 0
      //Nth Call: and and sketchIn was valid with cnt > 0.
      //Perform full intersect
      checkSeedHashes(seedHash_, sketchIn.getSeedHash());
      thetaLong_ = min(thetaLong_, sketchIn.getThetaLong());
      empty_ |= sketchIn.isEmpty();
      
      //Must perform full intersection
      // sets resulting hashTable, curCount and adjusts lgArrLongs
      performIntersect(sketchIn);
    }
  }
  
  @Override
  public CompactSketch getResult(boolean dstOrdered, Memory dstMem) { 
    if (curCount_ < 0) {
      throw new IllegalStateException(
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
  public void reset() {
    //lgArrLongs_ //based on whatever it was
    hashTable_ = null;
    curCount_ = -1; //Universal Set is true
    thetaLong_ = Long.MAX_VALUE;
    empty_ = false;
  }
  
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
        int foundIdx = hashSearch(hashTable_, lgArrLongs_, hashIn);
        if (foundIdx == -1) continue;
        matchSet[matchSetCount++] = hashIn;
      }

    } 
    else {
      //either unordered compact or hash table
      for (int i = 0; i < arrLongsIn; i++ ) {
        long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= thetaLong_)) continue;
        int foundIdx = hashSearch(hashTable_, lgArrLongs_, hashIn);
        if (foundIdx == -1) continue;
        matchSet[matchSetCount++] = hashIn;
      }
    }
    //reduce effective array size to minimum
    lgArrLongs_ = computeMinLgArrLongsFromCount(matchSetCount);
    curCount_ = matchSetCount;
    Arrays.fill(hashTable_, 0, 1 << lgArrLongs_, 0L); //clear for rebuild
    //move matchSet to hash table
    moveDataToHT(matchSet, matchSetCount);
  }
  
  //Assumes HT exists and is large enough
  private void moveDataToHT(long[] arr, int count) { //could use hashArrayInsert
    int arrLongsIn = arr.length;
    int tmpCnt = 0;
    for (int i = 0; i < arrLongsIn; i++ ) {
      long hashIn = arr[i];
      if (HashOperations.continueCondition(thetaLong_, hashIn)) continue;
      tmpCnt += hashInsert(hashTable_, lgArrLongs_, hashIn)? 1 : 0;
    }
    assert (tmpCnt == count);
  }
  
}
