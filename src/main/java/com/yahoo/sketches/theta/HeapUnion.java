/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.CompactSketch.compactCache;
import static com.yahoo.sketches.theta.CompactSketch.createCompactSketch;
import static com.yahoo.sketches.theta.PreambleUtil.*;
import static java.lang.Math.min;

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
  public void update(Sketch sketchIn) {
    //UNION Rule: AND the empty states
    
    if ((sketchIn == null)  || sketchIn.isEmpty()) {
      //null/empty is interpreted as (1.0, 0, T).  Nothing changes
      return;
    }
    unionEmpty_ = false; //Empty rule: AND the empty states
    
    PreambleUtil.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    long thetaLongIn = sketchIn.getThetaLong();
    
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule
    
    if(sketchIn.isOrdered()) { //Use early stop
      int curCount = sketchIn.getRetainedEntries(false);
      
      if(sketchIn.isDirect()) {
        Memory skMem = sketchIn.getMemory();
        int preLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
        for (int i = 0; i < curCount; i++ ) {
          int offsetBytes = (preLongs +i) << 3;
          long hashIn = skMem.getLong(offsetBytes);
          if (hashIn >= unionThetaLong_) break; // "early stop"
          gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
        }
      } 
      else { //on Heap
        long[] cacheIn = sketchIn.getCache(); //not a copy!
        for (int i = 0; i < curCount; i++ ) {
          long hashIn = cacheIn[i];
          if (hashIn >= unionThetaLong_) break; // "early stop"
          gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
        }
      }
    } 
    else {
      //either not-ordered compact or Hash Table.
      long[] cacheIn = sketchIn.getCache(); //if off-heap this will be a copy
      int arrLongs = cacheIn.length;
      for (int i = 0; i < arrLongs; i++ ) {
        long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) continue;
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
  }
  
  @Override
  public void update(Memory skMem) {
    //UNION Rule: AND the empty states
    if (skMem == null) return;
    int cap = (int)skMem.getCapacity();
    int f;
    assert ((f=skMem.getByte(FAMILY_BYTE)) == 3) : "Illegal Family/SketchType byte: "+f;
    int serVer = skMem.getByte(SER_VER_BYTE);
    if (serVer == 1) {
      if (cap <= 24) return; //empty
      processVer1(skMem);
    }
    else if (serVer == 2) { 
      if (cap <= 8) return; //empty
      processVer2(skMem);
    }
    else if (serVer == 3) {
      if (cap <= 8) return; //empty
      processVer3(skMem);
    }
    else throw new IllegalArgumentException("SerVer is unknown: "+serVer);
  }
  
  //must trust seed, no seedhash. No p, can't be empty, can only be compact, ordered, size > 24
  private void processVer1(Memory skMem) {
    unionEmpty_ = false; //Empty rule: AND the empty states
    long thetaLongIn = skMem.getLong(THETA_LONG);
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule
    int curCount = skMem.getInt(RETAINED_ENTRIES_INT);
    int preLongs = 3;
    for (int i = 0; i < curCount; i++ ) {
      int offsetBytes = (preLongs +i) << 3;
      long hashIn = skMem.getLong(offsetBytes);
      if (hashIn >= unionThetaLong_) break; // "early stop"
      gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
  }
  
  //has seedhash, p, could have 0 entries & theta, can only be compact, ordered, size >= 24
  private void processVer2(Memory skMem) {
    PreambleUtil.checkSeedHashes(seedHash_, skMem.getShort(SEED_HASH_SHORT));
    
    int preLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    int curCount = skMem.getInt(RETAINED_ENTRIES_INT);
    long thetaLongIn;
    
    if (preLongs == 1) {
      return;
    }
    if (preLongs == 2) {
      assert curCount > 0;
      unionEmpty_ = false; //Empty rule: AND the empty states
      thetaLongIn = Long.MAX_VALUE;
    } else { //prelongs == 3, curCount may be 0 (e.g., from intersection)
      thetaLongIn = skMem.getLong(THETA_LONG);
    }
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule
    for (int i = 0; i < curCount; i++ ) {
      int offsetBytes = (preLongs +i) << 3;
      long hashIn = skMem.getLong(offsetBytes);
      if (hashIn >= unionThetaLong_) break; // "early stop"
      gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
  }
  
  //has seedhash, p, could have 0 entries & theta, could be unorderd, compact, size >= 24
  private void processVer3(Memory skMem) {
    PreambleUtil.checkSeedHashes(seedHash_, skMem.getShort(SEED_HASH_SHORT));
    unionEmpty_ = false; //Empty rule: AND the empty states
    int preLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    int curCount = skMem.getInt(RETAINED_ENTRIES_INT);
    long thetaLongIn;
    if (preLongs == 1) {
      return;
    }
    if (preLongs == 2) { //curCount has to be > 0 and exact
      assert curCount > 0;
      unionEmpty_ = false; //Empty rule: AND the empty states
      thetaLongIn = Long.MAX_VALUE;
    } else { //prelongs == 3, curCount may be 0 (e.g., from intersection)
      thetaLongIn = skMem.getLong(THETA_LONG);
    }
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule
    boolean ordered = skMem.isAnyBitsSet(FLAGS_BYTE, (byte) ORDERED_FLAG_MASK);
    if (ordered) {
      for (int i = 0; i < curCount; i++ ) {
        int offsetBytes = (preLongs +i) << 3;
        long hashIn = skMem.getLong(offsetBytes);
        if (hashIn >= unionThetaLong_) break; // "early stop"
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    else { //unordered
      for (int i = 0; i < curCount; i++ ) {
        int offsetBytes = (preLongs +i) << 3;
        long hashIn = skMem.getLong(offsetBytes);
        if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) continue;
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
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
    //CurCount must be recounted with a scan using the new theta
    int curCountR = HashOperations.count(gadgetCache, thetaLongR);
    long[] compactCacheR = compactCache(gadgetCache, curCountR, thetaLongR, dstOrdered);
    return createCompactSketch(compactCacheR, unionEmpty_, seedHash_, curCountR, thetaLongR, 
        dstOrdered, dstMem);
  }
  
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