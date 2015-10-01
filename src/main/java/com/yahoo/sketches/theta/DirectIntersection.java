/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.objectToFamily;
import static com.yahoo.sketches.Family.stringToFamily;
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

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryUtil;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectIntersection extends SetOperation implements Intersection {
  private static final Family MY_FAMILY = Family.INTERSECTION;
  private final short seedHash_;
  private final int lgNomLongs_;
  private final Memory mem_;
 
  private int lgArrLongs_;
  private int hashTableThreshold_; //only on heap, never serialized.
  private int curCount_;
  private long thetaLong_;
  private boolean empty_;
  
  /**
   * Construct a new Intersection target direct to the given destination Memory.
   * Called by SetOperation.Builder.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @param dstMem destination Memory 
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  DirectIntersection(int lgNomLongs, long seed, Memory dstMem) {
    
    lgNomLongs_ = lgNomLongs;
    if (lgNomLongs_ < MIN_LG_NOM_LONGS) throw new IllegalArgumentException(
        "This intersection requires a minimum nominal entries of "+(1 << MIN_LG_NOM_LONGS));
    
    //build preamble and cache together in single Memory
    mem_ = dstMem;

    long memCapacityBytes = mem_.getCapacity();
    int reqCapacityBytes = (16 << lgNomLongs_) + (CONST_PREAMBLE_LONGS << 3);
    if (memCapacityBytes < reqCapacityBytes) throw new IllegalArgumentException(
        "Not sufficient Memory capacity for targeted intersection.");
    
    mem_.clear(0, reqCapacityBytes); 
    
    //load preamble into mem
    mem_.putByte(PREAMBLE_LONGS_BYTE, (byte) CONST_PREAMBLE_LONGS); //RF not used = 0
    mem_.putByte(SER_VER_BYTE, (byte) SER_VER);
    mem_.putByte(FAMILY_BYTE, (byte) stringToFamily("Intersection").getID());
    mem_.putByte(LG_NOM_LONGS_BYTE, (byte) lgNomLongs_);
    
    lgArrLongs_ = setLgArrLongs(lgNomLongs+1);
    hashTableThreshold_ = setHashTableThreshold(lgArrLongs_);
    
    //flags: bigEndian = readOnly = compact = ordered = false;
    empty_ = setEmpty(false);
    
    seedHash_ = computeSeedHash(seed);
    mem_.putShort(SEED_HASH_SHORT, seedHash_);
    
    curCount_ = setCurCount(-1);
    
    mem_.putFloat(P_FLOAT, (float) 1.0);
    thetaLong_ = setThetaLong(Long.MAX_VALUE);
  }
  
  /**
   * Wrap an Intersection target around the given source Memory containing intersection data. 
   * @param srcMem The source Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  DirectIntersection(Memory srcMem, long seed) {
    mem_ = srcMem;
    seedHash_ = computeSeedHash(seed);
    short seedHashMem = mem_.getShort(SEED_HASH_SHORT);
    checkSeedHashes(seedHashMem, seedHash_); //check for seed hash conflict
    
    int preambleLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    if (preambleLongs != CONST_PREAMBLE_LONGS) {
      throw new IllegalArgumentException("PreambleLongs must = 3.");
    }
    
    MY_FAMILY.checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    lgNomLongs_ = srcMem.getByte(LG_NOM_LONGS_BYTE);
    thetaLong_ = srcMem.getLong(THETA_LONG);
    empty_ = srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    curCount_ = srcMem.getInt(RETAINED_ENTRIES_INT);
    lgArrLongs_ = srcMem.getByte(LG_ARR_LONGS_BYTE);
    hashTableThreshold_ = setHashTableThreshold(lgArrLongs_);
    if (curCount_ < 0) { //virgin, empty or no HT
      empty_ = false; //don't change curCount = -1, it is still virgin!
    } 
    else if (empty_) { //empty or no HT, curCount must be 0
      curCount_ = 0; //no longer virgin
    }
  }
  
  @Override
  @SuppressWarnings("null")
  public void update(Sketch sketchIn) {
    
    //The Intersection State Machine
    curCount_ = mem_.getInt(RETAINED_ENTRIES_INT);
    int skInState = ((sketchIn != null) && (sketchIn.getRetainedEntries(true) > 0))? 1 : 0;
    int sw = ((curCount_ < 0)? 0 : 4) | ((curCount_ <= 0)? 0: 2) | skInState;
    switch (sw) {
      case 0: {
        //The 1st Call was either null or a sketch with zero entries.
        //All future intersections result in zero data, but theta can still be reduced.
        // curCount = -1 : 0; curCount = -1 : 0; 0; :: set curCount = 0
        if (sketchIn != null) {
          checkSeedHashes(seedHash_, sketchIn.getSeedHash());
          thetaLong_ = minThetaLong(sketchIn.getThetaLong());
          empty_ = setEmpty(empty_ | sketchIn.isEmpty());
        } 
        else {
          //don't change theta
          empty_ = setEmpty(true);
        }
        curCount_ = setCurCount(0); //curCount was -1, must set to >= 0
        //No need for a HT.
        break;
      }
      case 1: {
        //The 1st Call was a valid sketch with > 0 entries.
        checkSeedHashes(seedHash_, sketchIn.getSeedHash());
        thetaLong_ = minThetaLong(sketchIn.getThetaLong());
        empty_ = setEmpty(empty_ | sketchIn.isEmpty());
        
        //curCount was -1, must set to >= 0
        curCount_ = setCurCount(sketchIn.getRetainedEntries(true));
        //HT already empty, no need to clear. Reduce effective array size to minimum
        lgArrLongs_ = setLgArrLongs(computeMinLgArrLongsFromCount(curCount_, lgArrLongs_));
        hashTableThreshold_ = setHashTableThreshold(lgArrLongs_);
        //Then move data into HT
        moveToHT(sketchIn.getCache(), curCount_);
        break;
      }
      case 4: {
        //Nth Call: curCount == 0. Incoming sketch was not valid or had cnt == 0
        //All future intersections result in zero data, but theta can still be reduced.
        if (sketchIn != null) {
          checkSeedHashes(seedHash_, sketchIn.getSeedHash());
          thetaLong_ = minThetaLong(sketchIn.getThetaLong());
          empty_ = setEmpty(empty_ | sketchIn.isEmpty());
        } 
        else {
          //don't change theta
          empty_ = setEmpty(true);
        }
        
        ////No need for HT. Reduce effective array to minimum
        lgArrLongs_ = setLgArrLongs(computeMinLgArrLongsFromCount(curCount_, lgArrLongs_));
        hashTableThreshold_ = setHashTableThreshold(lgArrLongs_);
        break;
      }
      case 5: {
        //Nth Call: curCount == 0. Incoming sketch was valid with cnt > 0.
        //All future intersections result in zero data, but theta can still be reduced.
        checkSeedHashes(seedHash_, sketchIn.getSeedHash());
        thetaLong_ = minThetaLong(sketchIn.getThetaLong());
        empty_ = setEmpty(empty_ | sketchIn.isEmpty());
        
        //No need for HT. Reduce effective array size to minimum
        lgArrLongs_ = setLgArrLongs(computeMinLgArrLongsFromCount(curCount_, lgArrLongs_));
        hashTableThreshold_ = setHashTableThreshold(lgArrLongs_);
        break;
      }
      case 6: {
        //Nth Call: curCount > 0. Incoming sketch was not valid or had cnt == 0
        //All future intersections result in zero data, but theta can still be reduced.
        if (sketchIn != null) {
          checkSeedHashes(seedHash_, sketchIn.getSeedHash());
          thetaLong_ = minThetaLong(sketchIn.getThetaLong());
          empty_ = setEmpty(empty_ | sketchIn.isEmpty());
        } 
        else {
          //don't change theta
          empty_ = setEmpty(true);
        }
        curCount_ = setCurCount(0);
        //No need for HT. Reduce effective array size to minimum
        lgArrLongs_ = setLgArrLongs(computeMinLgArrLongsFromCount(curCount_, lgArrLongs_));
        hashTableThreshold_ = setHashTableThreshold(lgArrLongs_);
        mem_.fill(CONST_PREAMBLE_LONGS<<3, 8 << lgArrLongs_, (byte) 0); //clears garbage in low array
        break;
      }
      case 7: {
        //Nth Call: curCount >0.  Incoming sketch was valid with cnt > 0.
        checkSeedHashes(seedHash_, sketchIn.getSeedHash());
        thetaLong_ = minThetaLong(sketchIn.getThetaLong());
        empty_ = setEmpty(empty_ | sketchIn.isEmpty());
        
        //Must perform full intersection
        // sets resulting hashTable, curCount and adjusts lgArrLongs
        performIntersect(sketchIn);
        break;
      }
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
    int htLen = 1 << lgArrLongs_;
    long[] hashTable = new long[htLen];
    mem_.getLongArray(CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen);
    compactCacheR = compactCachePart(hashTable, lgArrLongs_, curCount_, thetaLong_, dstOrdered);
    
    //Create the CompactSketch
    return CompactSketch.createCompactSketch(
        compactCacheR, empty_, seedHash_, curCount_, thetaLong_, dstOrdered, dstMem);
  }

  @Override
  public boolean hasResult() {
    return mem_.getInt(RETAINED_ENTRIES_INT) >= 0;
  }
  
  @Override
  public byte[] toByteArray() {
    int preBytes = CONST_PREAMBLE_LONGS << 3;
    int dataBytes = 8 << lgArrLongs_;
    byte[] byteArrOut = new byte[preBytes + dataBytes];
    NativeMemory memOut = new NativeMemory(byteArrOut);
    
    //preamble
    memOut.putByte(PREAMBLE_LONGS_BYTE, (byte) CONST_PREAMBLE_LONGS); //RF not used = 0
    memOut.putByte(SER_VER_BYTE, (byte) SER_VER);
    memOut.putByte(FAMILY_BYTE, (byte) objectToFamily(this).getID());
    memOut.putByte(LG_NOM_LONGS_BYTE, (byte) lgNomLongs_);
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
      MemoryUtil.copy(mem_, preBytes, memOut, preBytes, dataBytes);
    }
    return byteArrOut;
  }
  
  @Override
  public void reset() {
    lgArrLongs_ = lgNomLongs_ + 1;
    mem_.putByte(LG_ARR_LONGS_BYTE, (byte) (lgNomLongs_ + 1));
    curCount_ = -1; //Universal Set is true
    mem_.putInt(RETAINED_ENTRIES_INT, -1);
    thetaLong_ = Long.MAX_VALUE;
    mem_.putLong(THETA_LONG, Long.MAX_VALUE);
    empty_ = false;
    mem_.clearBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    mem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_);
  }
  
  private void performIntersect(Sketch sketchIn) {
    // HT and input data are nonzero, match against HT
    assert ((curCount_ > 0) && (!empty_));
    long[] cacheIn = sketchIn.getCache();
    int htLen = 1 << lgArrLongs_;
    long[] hashTable = new long[htLen];
    mem_.getLongArray(CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen);
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
        int foundIdx = hashSearch(hashTable, lgArrLongs_, hashIn);
        if (foundIdx == -1) continue;
        matchSet[matchSetCount++] = hashIn;
      }

    } 
    else {
      //either unordered compact or hash table
      for (int i = 0; i < arrLongsIn; i++ ) {
        long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= thetaLong_)) continue;
        int foundIdx = hashSearch(hashTable, lgArrLongs_, hashIn);
        if (foundIdx == -1) continue;
        matchSet[matchSetCount++] = hashIn;
      }
    }
    //reduce effective array size to minimum
    lgArrLongs_ = setLgArrLongs(computeMinLgArrLongsFromCount(curCount_, lgArrLongs_));
    curCount_ = setCurCount(matchSetCount);
    mem_.fill(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_, (byte) 0); //clear for rebuild
    //move matchSet to hash table
    moveToHT(matchSet, matchSetCount);
  }
  
  /**
   * Returns the cardinality limit given the current size of the hash table array.
   * 
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  private static final int setHashTableThreshold(final int lgArrLongs) {
    double fraction = RESIZE_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }
  
  private void moveToHT(long[] arr, int count) { //could use hashArrayInsert
    int arrLongsIn = arr.length;
    if (count > hashTableThreshold_) {
      throw new IllegalArgumentException("Intersection was not sized large enough: "+count);
    }
    int tmpCnt = 0;
    for (int i = 0; i < arrLongsIn; i++ ) {
      long hashIn = arr[i];
      if ((hashIn <= 0L) || (hashIn >= thetaLong_)) continue;
      tmpCnt += hashInsert(mem_, lgArrLongs_, hashIn, CONST_PREAMBLE_LONGS << 3)? 1 : 0;
      
    }
    assert (tmpCnt == count) : "tmp: "+tmpCnt+", count: "+count;
  }
  
  //special reset methods
  
  private final boolean setEmpty(boolean empty) {
    if (empty) {
      mem_.setBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    } 
    else {
      mem_.clearBits(FLAGS_BYTE, (byte)EMPTY_FLAG_MASK);
    }
    return empty;
  }
  
  private final int setLgArrLongs(int lgArrLongs) {
    mem_.putByte(LG_ARR_LONGS_BYTE, (byte) lgArrLongs);
    return lgArrLongs;
  }
  
  private final long setThetaLong(long thetaLong) {
    mem_.putLong(THETA_LONG, thetaLong);
    return thetaLong;
  }
  
  private final long minThetaLong(long skThetaLong) {
    if (skThetaLong < thetaLong_) {
      mem_.putLong(THETA_LONG, skThetaLong);
      return skThetaLong;
    }
    return thetaLong_;
  }
  
  private final int setCurCount(int curCount) {
    mem_.putInt(RETAINED_ENTRIES_INT, curCount);
    return curCount;
  }
  
}