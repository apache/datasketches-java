/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.CompactSketch.compactCache;
import static com.yahoo.sketches.theta.HashOperations.hashArrayInsert;
import static com.yahoo.sketches.theta.HashOperations.hashSearch;
import static com.yahoo.sketches.theta.PreambleUtil.checkSeedHashes;
import static com.yahoo.sketches.theta.SetOperation.SetReturnState.Success;
import static java.lang.Math.min;

import java.util.Arrays;

import com.yahoo.sketches.memory.Memory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class HeapAnotB extends SetOperation implements AnotB {
  //private static final Family MY_FAMILY = Family.A_NOT_B;
  private final short seedHash_;
  private Sketch a_;
  private Sketch b_;
  private long thetaLong_;
  private boolean empty_; 
  private long[] cache_; // no match set
  private int curCount_ = 0; // this will catch an attempt to get result before call to aNOTb.
  
  private int lgArrLongsHT_; //for Hash Table only
  private long[] bHashTable_; 
  
  /**
   * Construct a new Union SetOperation on the java heap.  Called by SetOperation.Builder.
   * 
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  HeapAnotB(long seed) {
    seedHash_ = computeSeedHash(seed);
  }
  
  @Override
  @SuppressWarnings("null")
  public SetReturnState update(Sketch a, Sketch b) { 
    a_ = a;
    b_ = b;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = true;
    cache_ = null;
    curCount_ = 0;
    lgArrLongsHT_ = 5;
    bHashTable_ = null;
    return compute();
  }
  
  @Override
  @SuppressWarnings("null")
  public CompactSketch getResult(boolean dstOrdered, Memory dstMem) {
    long[] compactCache = (curCount_ <= 0)? new long[0] : Arrays.copyOfRange(cache_, 0, curCount_);
    if (dstOrdered && (curCount_ > 1)) {
      Arrays.sort(compactCache);
    }
    //Create the CompactSketch
    CompactSketch comp = CompactSketch.createCompactSketch(compactCache, empty_, seedHash_, curCount_, 
        thetaLong_, dstOrdered, dstMem);
    reset();
    return comp;
  }
  
  @SuppressWarnings("null")
  SetReturnState compute() {
    int swA = (a_ == null)? 0 : (a_.isEmpty())? 1: (a_ instanceof UpdateSketch)? 4 : (a_.isOrdered())? 3 : 2;
    int swB = (b_ == null)? 0 : (b_.isEmpty())? 1: (b_ instanceof UpdateSketch)? 4 : (b_.isOrdered())? 3 : 2;
    int sw = (swA * 8) | swB;
    
    //  NOTES:
    //    In the table below, A and B refer to the two input sketches in the order A-not-B.
    //    The Theta rule: min( ThetaA, ThetaB)
    //    The Empty rule: Whatever A is.
    //    The Return triple is defined as: (Theta, Count, EmptyFlag).
    //    bHashTable temporarily stores the values of B.
    //    A sketch in stored form can be in one of 5 states
    //    Null is not actually a state, but is included for completeness.
    //    Null is interpreted as {Theta = 1.0, count = 0, empty = true}.
    //    The empty state may have Theta < 1.0, but count must be zero.
    //    State:
    //      0 null
    //      1 Empty
    //      2 Compact, not ordered
    //      3 Compact Ordered
    //      4 Hash-Table
    //
    //A    B    swA  swB  Case  Action
    //N    N    0    0    0     Return (1.0, 0, T)
    //N    E    0    1    1     Return (ThB, 0, T)
    //N    C    0    2    2     Return (ThB, 0, T)  ?
    //N    O    0    3    3     Return (ThB, 0, T)
    //N    H    0    4    4     Return (ThB, 0, T)
    //E    N    1    0    8     Return (ThA, 0, T)
    //E    E    1    1    9     Return (min, 0, T)
    //E    C    1    2    10    Return (min, 0, T)  ?
    //E    O    1    3    11    Return (min, 0, T)  ?
    //E    H    1    4    12    Return (min, 0, T)
    //C    N    2    0    16    Return (ThA, |A|, Ea)  ?
    //C    E    2    1    17    Return (ThA, |A|, Ea)  ?
    //C    C    2    2    18    B -> H; => CH
    //C    O    2    3    19    B -> H; => CH
    //C    H    2    4    20    scan all A, search B, on nomatch -> list (same as HH)
    //O    N    3    0    24    Return (ThA, |A|, Ea)
    //O    E    3    1    25    Return (ThA, |A|, Ea)  ?
    //O    C    3    2    26    B -> H; => OH
    //O    O    3    3    27    B -> H; => OH
    //O    H    3    4    28    scan A early stop, search B, on nomatch -> list
    //H    N    4    0    32    Return (ThA, |A|, Ea)
    //H    E    4    1    33    Return (ThA, |A|, Ea)
    //H    C    4    2    34    C -> H; => HH
    //H    O    4    3    35    O -> H; => HH
    //H    H    4    4    36    scan all A, search B, on nomatch -> list
    //Notes: Null is interpreted as {Theta = 1.0, count = 0, empty = true}.
    // However, the empty state may have Theta < 1.0, but count must be zero.
    
    switch (sw) {
      case 0 : { //A and B are null.  
        thetaLong_ = Long.MAX_VALUE;
        empty_ = true;
        break; //{1.0, 0, T}
      }
      case 1: 
      case 2: 
      case 3: 
      case 4:  { //A is null, B is valid
        checkSeedHashes(seedHash_, b_.getSeedHash());
        thetaLong_ = b_.getThetaLong();
        empty_ = true;
        break; //{ThB, 0, T}
      }
      case 8: { //A is empty, B is null
        checkSeedHashes(seedHash_, a_.getSeedHash());
        thetaLong_ = a_.getThetaLong();
        empty_ = true;
        break; //{ThA, 0, T}
      }
      case 9: 
      case 10: 
      case 11: 
      case 12: { //A empty, B valid
        checkSeedHashes(seedHash_, a_.getSeedHash());
        checkSeedHashes(seedHash_, b_.getSeedHash());
        thetaLong_ = min(a_.getThetaLong(), b_.getThetaLong());
        empty_ = true;
        break; //{min, 0, T}
      }
      case 16: 
      case 24: 
      case 32: { //A valid, B null
        checkSeedHashes(seedHash_, a_.getSeedHash());
        thetaLong_ = a_.getThetaLong();
        empty_ = a_.isEmpty();
        //move A to cache
        curCount_ = a_.getRetainedEntries(true);
        cache_ = compactCache(a_.getCache(), curCount_, thetaLong_, false);
        break; //(min, 0, Ea)
      }
      case 17: 
      case 25: 
      case 33: { //A valid, B empty
        checkSeedHashes(seedHash_, a_.getSeedHash());
        checkSeedHashes(seedHash_, b_.getSeedHash());
        thetaLong_ = min(a_.getThetaLong(), b_.getThetaLong());
        empty_ = a_.isEmpty();
        //move A to cache
        curCount_ = a_.getRetainedEntries(true);
        cache_ = compactCache(a_.getCache(), curCount_, thetaLong_, false); 
        break; //(min, 0, Ea)
      }
      case 18: 
      case 19: 
      case 34: 
      case 35: {//A compact or HT, B compact or ordered 
        checkSeedHashes(seedHash_, a_.getSeedHash());
        checkSeedHashes(seedHash_, b_.getSeedHash());
        thetaLong_ = min(a_.getThetaLong(), b_.getThetaLong());
        empty_ = a_.isEmpty();
        //must convert B to HT
        convertBtoHT(); //builds HT from B
        scanAllAsearchB(); //builds cache, curCount from A, HT
        break; //(min, n, Ea)
      }
      case 26: 
      case 27: { //A ordered early stop, B compact or ordered 
        checkSeedHashes(seedHash_, a_.getSeedHash());
        checkSeedHashes(seedHash_, b_.getSeedHash());
        thetaLong_ = min(a_.getThetaLong(), b_.getThetaLong());
        empty_ = a_.isEmpty();
        convertBtoHT(); //builds HT from B
        scanEarlyStopAsearchB();
        break; //(min, n, Ea)
      }
      case 20: 
      case 36: { //A compact or HT, B is already HT
        checkSeedHashes(seedHash_, a_.getSeedHash());
        checkSeedHashes(seedHash_, b_.getSeedHash());
        thetaLong_ = min(a_.getThetaLong(), b_.getThetaLong());
        empty_ = a_.isEmpty();
        //b is already HT
        lgArrLongsHT_ = ((UpdateSketch)b_).getLgArrLongs();
        bHashTable_ = b_.getCache(); //safe as bHashTable is read-only
        scanAllAsearchB(); //builds cache, curCount from A, HT
        break; //(min, n, Ea)
      }
      case 28: { //A ordered early stop, B is already hashtable
        checkSeedHashes(seedHash_, a_.getSeedHash());
        checkSeedHashes(seedHash_, b_.getSeedHash());
        thetaLong_ = min(a_.getThetaLong(), b_.getThetaLong());
        empty_ = a_.isEmpty();
        //b is already HT
        lgArrLongsHT_ = ((UpdateSketch)b_).getLgArrLongs();
        bHashTable_ = b_.getCache(); //safe as bHashTable is read-only
        scanEarlyStopAsearchB();
        break; //(min, n, Ea)
      }
    }
    return Success;
  }
  
  private void convertBtoHT() {
    int curCountB = b_.getRetainedEntries(true);
    lgArrLongsHT_ = computeLgArrLongsFromCount(curCountB);
    bHashTable_ = new long[1 << lgArrLongsHT_];
    int count = hashArrayInsert(b_.getCache(), bHashTable_, lgArrLongsHT_, thetaLong_);
    assert (count == curCountB);
  }
  
  //Sketch A is either unordered compact or hash table
  private void scanAllAsearchB() {
    long[] scanAArr = a_.getCache();
    int arrLongsIn = scanAArr.length;
    cache_ = new long[arrLongsIn];
    for (int i = 0; i < arrLongsIn; i++ ) {
      long hashIn = scanAArr[i];
      if ((hashIn <= 0L) || (hashIn >= thetaLong_)) continue;
      int foundIdx = hashSearch(bHashTable_, lgArrLongsHT_, hashIn);
      if (foundIdx > -1) continue;
      cache_[curCount_++] = hashIn;
    }
  }
  
  //Sketch A is ordered compact, which enables early stop
  private void scanEarlyStopAsearchB() {
    long[] scanAArr = a_.getCache();
    int arrLongsIn = scanAArr.length;
    cache_ = new long[arrLongsIn];
    for (int i = 0; i < arrLongsIn; i++ ) {
      long hashIn = scanAArr[i];
      if (hashIn <= 0L) continue;
      if (hashIn >= thetaLong_) {
        break; //early stop assumes that hashes in input sketch are ordered!
      }
      int foundIdx = hashSearch(bHashTable_, lgArrLongsHT_, hashIn);
      if (foundIdx > -1) continue;
      cache_[curCount_++] = hashIn;
    }
  }
  
  private void reset() {
    a_ = null;
    b_ = null;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = true;
    cache_ = null;
    curCount_ = 0;
    lgArrLongsHT_ = 5;
    bHashTable_ = null;
  }
  
}