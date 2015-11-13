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
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.UNION_THETA_LONG;
import static java.lang.Math.min;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectUnion extends SetOperation implements Union{
  private static final Family MY_FAMILY = Family.UNION;
  private final short seedHash_;
  private final DirectQuickSelectSketch gadget_;
  private final Memory mem_;
  private long unionThetaLong_;
  //unionEmpty_ flag is merged with the gadget's direct Memory
  
  /**
   * Construct a new Union SetOperation in off-heap Memory. 
   * Called by SetOperation.Builder.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param dstMem the given Memory object destination. It will be cleared prior to use.
   * @param memReq the callback function
   * 
   *  <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  DirectUnion(int lgNomLongs, long seed, float p, ResizeFactor rf, Memory dstMem) {
    mem_ = dstMem;
    seedHash_ = computeSeedHash(seed);
    
    gadget_ = new DirectQuickSelectSketch(lgNomLongs, seed, p, rf, dstMem, true); //clears mem
    dstMem.putByte(FAMILY_BYTE, (byte) Family.UNION.getID());
    unionThetaLong_ = gadget_.getThetaLong();
    dstMem.putLong(UNION_THETA_LONG, unionThetaLong_);
  }
  
  /**
   * Wrap a Union around the given source Memory containing union data. 
   * @param srcMem The source Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  DirectUnion(Memory srcMem, long seed) {
    mem_ = srcMem;
    seedHash_ = computeSeedHash(seed);
    
    MY_FAMILY.checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    
    gadget_ = new DirectQuickSelectSketch(srcMem, seed);
    unionThetaLong_ = srcMem.getLong(UNION_THETA_LONG);
  }
  
  @Override
  public void update(Sketch sketchIn) { 
    //UNION Empty Rule: AND the empty states
    
    if ((sketchIn == null)  || sketchIn.isEmpty()) {
      //null/empty is interpreted as (1.0, 0, T).  Nothing changes
      mem_.putLong(UNION_THETA_LONG, unionThetaLong_);
      return;
    }
    //unionEmpty_ flag is merged with the gadget
    mem_.clearBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    
    PreambleUtil.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    long thetaLongIn = sketchIn.getThetaLong();
    
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule
    
    if(sketchIn.isOrdered()) { //Use early stop
      int finalIndex = sketchIn.getRetainedEntries(false);
      
      if (sketchIn.isDirect()) {
        Memory skMem = sketchIn.getMemory();
        int preambleLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
        for (int i = 0; i < finalIndex; i++ ) {
          int offsetBytes = (preambleLongs +i) << 3;
          long hashIn = skMem.getLong(offsetBytes);
          if (hashIn >= unionThetaLong_) break; // "early stop"
          gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
        }
      } 
      else { //on Heap
        long[] cacheIn = sketchIn.getCache(); //not a copy!
        for (int i = 0; i < finalIndex; i++ ) {
          long hashIn = cacheIn[i];
          if (hashIn >= unionThetaLong_) break; // "early stop"
          gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
        }
      }
    } 
    else { //either not-ordered compact or Hash Table.
      long[] cacheIn = sketchIn.getCache();
      int arrLongs = cacheIn.length;
      for (int i = 0; i < arrLongs; i++ ) {
        long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) continue;
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
    mem_.putLong(UNION_THETA_LONG, unionThetaLong_);
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
  
  @Override
  public void update(long datum) {
    gadget_.update(datum);
  }
  
  @Override
  public void update(double datum) {
    gadget_.update(datum);
  }
  
  @Override
  public void update(String datum) {
    gadget_.update(datum);
  }
  
  @Override
  public void update(byte[] data) {
    gadget_.update(data);
  }
  
  @Override
  public void update(int[] data) {
    gadget_.update(data);
  }
  
  @Override
  public void update(long[] data) {
    gadget_.update(data);
  }
  
  //must trust seed, no seedhash. No p, can't be empty, can only be compact, ordered, size > 24
  private void processVer1(Memory skMem) {
    //unionEmpty_ flag is merged with the gadget
    mem_.clearBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); //set NOT empty
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
      //unionEmpty_ flag is merged with the gadget
      mem_.clearBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); //set NOT empty
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
    int preLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    int curCount = skMem.getInt(RETAINED_ENTRIES_INT);
    long thetaLongIn;
    if (preLongs == 1) {
      return;
    }
    if (preLongs == 2) {
      assert curCount > 0;
      //unionEmpty_ flag is merged with the gadget
      mem_.clearBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); //set NOT empty
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
    long[] gadgetCache = gadget_.getCache(); //this is a copy
    //CurCount must be recounted with a scan using the new theta
    int curCountR = HashOperations.count(gadgetCache, thetaLongR);
    long[] compactCacheR = compactCache(gadgetCache, curCountR, thetaLongR, dstOrdered);
    boolean emptyR = mem_.isAnyBitsSet(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    return createCompactSketch(compactCacheR, emptyR, seedHash_, curCountR, thetaLongR, 
        dstOrdered, dstMem);
  }
  
  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }
  
  @Override
  public byte[] toByteArray() {
    byte[] gadgetByteArr = gadget_.toByteArray();
    Memory mem = new NativeMemory(gadgetByteArr);
    mem.putLong(UNION_THETA_LONG, unionThetaLong_); // union theta
    //empty flag is already merged
    return gadgetByteArr;
  }
  
  @Override
  public void reset() {
    gadget_.reset();
    unionThetaLong_ = gadget_.getThetaLong();
  }
  
}