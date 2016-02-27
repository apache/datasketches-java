/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.CompactSketch.compactCache;
import static com.yahoo.sketches.theta.CompactSketch.createCompactSketch;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
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
import com.yahoo.sketches.HashOperations;

/**
 * Shared code for the HeapUnion and DirectUnion implementations.
 * 
 * @author Lee Rhodes
 * @author Kevin Lang
 */
abstract class UnionImpl extends SetOperation implements Union {
  protected final short seedHash_;
  protected final UpdateSketch gadget_;
  protected long unionThetaLong_;
  
  /**
   * Construct a new Union that can be on-heap or off-heap
   * 
   * @param gadget Configured instance of UpdateSketch.
   */
  UnionImpl(UpdateSketch gadget) {
    gadget_ = gadget;
    seedHash_ = computeSeedHash(gadget_.getSeed());
    unionThetaLong_ = gadget_.getThetaLong();
  }
  
  /**
   * Heapify or Wrap a Union that can be on-heap or off-heap 
   * from a Memory object containing data. 
   * 
   * @param gadget Configured instance of UpdateSketch.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  UnionImpl(UpdateSketch gadget, Memory srcMem, long seed) {
    gadget_ = gadget;
    seedHash_ = computeSeedHash(gadget_.getSeed());
    Family.UNION.checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    unionThetaLong_ = srcMem.getLong(UNION_THETA_LONG);
  }
  
  @Override
  public CompactSketch getResult(boolean dstOrdered, Memory dstMem) {
    int gadgetCurCount = gadget_.getRetainedEntries(true);
    int k = 1 << gadget_.getLgNomLongs();
    
    if (gadgetCurCount > k) {
      gadget_.rebuild();
    } 
    //curCount <= k; gadget theta could be p < 1.0, but cannot do a quick select
    long thetaLongR = min(gadget_.getThetaLong(), unionThetaLong_);
    double p = gadget_.getP();
    double thetaR = thetaLongR/MAX_THETA_LONG_AS_DOUBLE;
    long[] gadgetCache = gadget_.getCache(); //if Direct, always a copy
    //CurCount must be recounted with a scan using the new theta
    int curCountR = HashOperations.count(gadgetCache, thetaLongR);
    long[] compactCacheR = compactCache(gadgetCache, curCountR, thetaLongR, dstOrdered);
    boolean emptyR = (gadget_.isEmpty() && (p >= thetaR) && (curCountR == 0));
    return createCompactSketch(compactCacheR, emptyR, seedHash_, curCountR, thetaLongR, 
        dstOrdered, dstMem);
  }
  
  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }
  
  @Override
  public void reset() {
    gadget_.reset();
    unionThetaLong_ = gadget_.getThetaLong();
  }
  
  @Override
  public byte[] toByteArray() {
    byte[] gadgetByteArr = gadget_.toByteArray();
    Memory mem = new NativeMemory(gadgetByteArr);
    mem.putLong(UNION_THETA_LONG, unionThetaLong_); // union theta
    return gadgetByteArr;
  }
  
  @Override
  public void update(Sketch sketchIn) { //Only valid for theta Sketches using SerVer = 3
    //UNION Empty Rule: AND the empty states
    
    if ((sketchIn == null)  || sketchIn.isEmpty()) {
      //null/empty is interpreted as (Theta = 1.0, count = 0, empty = T).  Nothing changes
      return;
    }
    
    PreambleUtil.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    long thetaLongIn = sketchIn.getThetaLong();
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule with incoming
    int curCountIn = sketchIn.getRetainedEntries(true);
    
    if(sketchIn.isOrdered()) { //Only true if Compact. Use early stop
      
      if(sketchIn.isDirect()) { //ordered, direct thus compact
        Memory skMem = sketchIn.getMemory();
        int preambleLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
        for (int i = 0; i < curCountIn; i++ ) {
          int offsetBytes = (preambleLongs +i) << 3;
          long hashIn = skMem.getLong(offsetBytes);
          if (hashIn >= unionThetaLong_) break; // "early stop"
          gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
        }
      } 
      else { //on Java Heap, ordered, thus compact
        long[] cacheIn = sketchIn.getCache(); //not a copy!
        for (int i = 0; i < curCountIn; i++ ) {
          long hashIn = cacheIn[i];
          if (hashIn >= unionThetaLong_) break; // "early stop"
          gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
        }
      }
    } //End ordered, compact
    else { //either not-ordered compact or Hash Table form. A HT may have dirty values.
      long[] cacheIn = sketchIn.getCache(); //if off-heap this will be a copy
      int arrLongs = cacheIn.length;
      for (int i = 0, c=0; (i < arrLongs) && (c < curCountIn); i++ ) {
        long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) continue; //rejects dirty values
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
        c++; //insures against invalid state inside the incoming sketch
        
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong()); //Theta rule with gadget
  }
  
  @Override
  public void update(Memory skMem) {
    //UNION Empty Rule: AND the empty states
    if (skMem == null) return;
    int cap = (int)skMem.getCapacity();
    int f = skMem.getByte(FAMILY_BYTE);
    if (f != 3) { //
      throw new IllegalArgumentException("Family must be COMPACT or SET_SKETCH (old): "+f);
    }
    int serVer = skMem.getByte(SER_VER_BYTE);
    if (serVer == 1) { //older SetSketch, which is compact and ordered
      if (cap <= 24) return; //empty
      processVer1(skMem);
    }
    else if (serVer == 2) {//older SetSketch, which is compact and ordered
      if (cap <= 8) return; //empty
      processVer2(skMem);
    }
    else if (serVer == 3) { //Only the OpenSource sketches
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
  
  //no seedhash, must trust seed. No p, no empty flag, 
  // can only be compact, ordered, size > 24
  private void processVer1(Memory skMem) {
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
  
  //has seedhash and p, could have 0 entries & theta, 
  // can only be compact, ordered, size >= 8
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
  
  //has seedhash, p, could have 0 entries & theta, 
  // could be unordered, ordered, compact, or not, size >= 8
  private void processVer3(Memory skMem) {
    PreambleUtil.checkSeedHashes(seedHash_, skMem.getShort(SEED_HASH_SHORT));
    int preLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    int curCount = skMem.getInt(RETAINED_ENTRIES_INT);
    long thetaLongIn;
    if (preLongs == 1) {
      return;
    }
    if (preLongs == 2) { //curCount has to be > 0 and exact mode. Cannot be from intersection.
      assert curCount > 0;
      thetaLongIn = Long.MAX_VALUE;
    } 
    else { //prelongs == 3, curCount may be 0 (e.g., from intersection).
      thetaLongIn = skMem.getLong(THETA_LONG);
    }
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule
    boolean ordered = skMem.isAnyBitsSet(FLAGS_BYTE, (byte) ORDERED_FLAG_MASK);
    if (ordered) { //must be compact
      for (int i = 0; i < curCount; i++ ) {
        int offsetBytes = (preLongs +i) << 3;
        long hashIn = skMem.getLong(offsetBytes);
        if (hashIn >= unionThetaLong_) break; // "early stop"
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    else { //not-ordered, could be compact or hash-table form
      boolean compact = skMem.isAnyBitsSet(FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
      int size = (compact)? curCount : 1 << skMem.getByte(LG_ARR_LONGS_BYTE);
      for (int i = 0; i < size; i++ ) {
        int offsetBytes = (preLongs +i) << 3;
        long hashIn = skMem.getLong(offsetBytes);
        if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) continue;
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
  }

}
