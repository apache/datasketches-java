/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.CompactSketch.compactCache;
import static com.yahoo.sketches.theta.CompactSketch.createCompactSketch;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.UNION_THETA_LONG;
import static com.yahoo.sketches.theta.SetOperation.SetReturnState.Success;
import static java.lang.Math.min;

import java.util.Arrays;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;


/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class HeapUnion extends SetOperation implements Union {
  private static final Family MY_FAMILY = Family.UNION;
  private final short seedHash_;
  private final HeapQuickSelectSketch gadget_;
  private long unionThetaLong_;
  private boolean unionEmpty_;
  
  
  /**
   * Construct a new Union SetOperation on the java heap.  Called by SetOperation.Builder.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   */
  HeapUnion(int lgNomLongs, long seed, float p, ResizeFactor rf) {
    seedHash_ = computeSeedHash(seed);
    gadget_ = new HeapQuickSelectSketch(lgNomLongs, seed, p, rf, true);
    unionThetaLong_ = gadget_.getThetaLong();
    unionEmpty_ = true;
  }
  
  /**
   * Heapify a Union SetOperation from a Memory object containing data. 
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  HeapUnion(Memory srcMem, long seed) {
    seedHash_ = computeSeedHash(seed);
    MY_FAMILY.checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    
    gadget_ = new HeapQuickSelectSketch(srcMem, seed);
    unionThetaLong_ = srcMem.getLong(UNION_THETA_LONG);
    unionEmpty_ = (unionThetaLong_ < gadget_.getThetaLong())? false : gadget_.isEmpty();
  }
  
  @Override
  public SetReturnState update(Sketch sketchIn) { 
    //UNION Rule: AND the empty states
    
    if ((sketchIn == null)  || sketchIn.isEmpty()) {
      //null/empty is interpreted as (1.0, 0, T).  Nothing changes
      return SetReturnState.Success;
    }
    unionEmpty_ = false; //Empty rule: AND the empty states
    
    PreambleUtil.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    long thetaLongIn = sketchIn.getThetaLong();
    
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule
    
    long[] cacheIn = sketchIn.getCache(); //TODO consider optimizing sketchIn with mem
    int finalIndex = cacheIn.length;
    
    if(sketchIn.isOrdered()) {
      //Assume CompactOrdered, use early stop.  no zeros.
      finalIndex = Arrays.binarySearch(cacheIn, unionThetaLong_);
      finalIndex = finalIndex < 0 ? (-finalIndex) - 1 : finalIndex;
      for (int i = 0; i < finalIndex; i++ ) { //This is an "early" stop loop
        gadget_.hashUpdate(cacheIn[i]); //backdoor update, hash function is bypassed
      }
    } 
    else {
      //either not-ordered compact or Hash Table.
      int arrLongs = cacheIn.length;
      for (int i = 0; i < arrLongs; i++ ) {
        long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) continue;
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
    return Success;
  }
  
  @Override
  public CompactSketch getResult(boolean dstOrdered, Memory dstMem) {
    int gadgetCurCount = gadget_.getRetainedEntries(true);
    int k = 1 << gadget_.lgNomLongs_;
    
    if (gadgetCurCount > k) {
      gadget_.rebuild();
    } 
    //curCount <= k; gadget theta could be p < 1.0, but cannot do a quick select
    long thetaLongR = min(gadget_.getThetaLong(), unionThetaLong_);
    long[] gadgetCache = gadget_.getCache();
    int curCountR = HashOperations.count(gadgetCache, thetaLongR);
    long[] compactCacheR = compactCache(gadgetCache, curCountR, thetaLongR, dstOrdered);    
    return createCompactSketch(compactCacheR, unionEmpty_, seedHash_, curCountR, thetaLongR, 
        dstOrdered, dstMem);
  } //TODO auto reset?
  
  @Override
  public byte[] toByteArray() {
    byte[] gadgetByteArr = gadget_.toByteArray();
    Memory mem = new NativeMemory(gadgetByteArr);
    mem.putLong(UNION_THETA_LONG, unionThetaLong_); // union theta
    if (!unionEmpty_ && gadget_.isEmpty()) {
      mem.setBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    }
    return gadgetByteArr;
  }
  
  @Override
  public void reset() {
    gadget_.reset();
    unionThetaLong_ = gadget_.getThetaLong();
    unionEmpty_ = true;
  }
  
}