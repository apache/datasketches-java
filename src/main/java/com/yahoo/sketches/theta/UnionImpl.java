/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.QuickSelect.selectExcludingZeros;
import static com.yahoo.sketches.theta.CompactSketch.compactCache;
import static com.yahoo.sketches.theta.CompactSketch.createCompactSketch;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.UNION_THETA_LONG;
import static java.lang.Math.min;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;


/**
 * Shared code for the HeapUnion and DirectUnion implementations.
 * 
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class UnionImpl extends SetOperation implements Union {
  private UpdateSketch gadget_;
  private long unionThetaLong_;
  private short seedHash_;
  private Memory unionMem_;
  
  
  private UnionImpl(UpdateSketch gadget, long seed) {
    gadget_ = gadget;
    unionThetaLong_ = gadget.getThetaLong();
    seedHash_ = computeSeedHash(seed);
  }
  
  /**
   * Construct a new Union SetOperation on the java heap. 
   * Called by SetOperation.Builder.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @return instance of this sketch
   */
  static UnionImpl initNewHeapInstance(int lgNomLongs, long seed, float p, ResizeFactor rf) {
    UpdateSketch gadget = HeapQuickSelectSketch.getInstance(lgNomLongs, seed, p, rf, true);
    UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionMem_ = null;
    return unionImpl;
  }
  
  /**
   * Heapify a Union from a Memory object containing data. 
   * Called by SetOperation.Builder.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl heapifyInstance(Memory srcMem, long seed) {
    Family.UNION.checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    UpdateSketch gadget = HeapQuickSelectSketch.getInstance(srcMem, seed);
    UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionMem_ = null;
    return unionImpl;
  }
  
  /**
   * Construct a new Direct Union in the off-heap destination Memory. 
   * Called by SetOperation.Builder.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param dstMem the given Memory object destination. It will be cleared prior to use.
   * @return this class
   */
  static UnionImpl 
      initNewDirectInstance(int lgNomLongs, long seed, float p, ResizeFactor rf, Memory dstMem) {
    UpdateSketch gadget = 
        DirectQuickSelectSketch.getInstance(lgNomLongs, seed, p, rf, dstMem, true);
    UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionMem_ = dstMem;
    return unionImpl;
  }
  
  /**
   * Wrap a Union object around a Union Memory object containing data. 
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl wrapInstance(Memory srcMem, long seed) {
    Family.UNION.checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    UpdateSketch gadget = DirectQuickSelectSketch.getInstance(srcMem, seed);
    UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionMem_ = srcMem;
    return unionImpl;
  }
  
  @Override
  public CompactSketch getResult(boolean dstOrdered, Memory dstMem) {
    int gadgetCurCount = gadget_.getRetainedEntries(true);
    long gadgetThetaLong = gadget_.getThetaLong();
    int arrLongs = 1 << gadget_.getLgArrLongs();
    int k = 1 << gadget_.getLgNomLongs();
    
    long[] gadgetCache = gadget_.getCache(); //if direct a copy, otherwise a reference
    long gNewThetaLong = gadgetThetaLong;
    
    if (gadgetCurCount > k) {
      if (!gadget_.isDirect()) {
        gadgetCache = new long[arrLongs];
        System.arraycopy(gadget_.getCache(), 0, gadgetCache, 0, arrLongs);
      }
      gNewThetaLong = selectExcludingZeros(gadgetCache, gadgetCurCount, k + 1);//messes the cache_ 
    }
    
    long thetaLongR = min(gNewThetaLong, unionThetaLong_);
    int curCountR = (thetaLongR < gadget_.getThetaLong()) 
        ? HashOperations.count(gadgetCache, thetaLongR)
        : gadgetCurCount;
    long[] compactCacheR = compactCache(gadgetCache, curCountR, thetaLongR, dstOrdered);
    boolean emptyR = (gadget_.isEmpty() 
        && (gadget_.getP() >= thetaLongR / MAX_THETA_LONG_AS_DOUBLE) && (curCountR == 0));
    
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
  public Family getFamily() {
    return Family.UNION;
  }
  
  @Override
  public void update(Sketch sketchIn) { //Only valid for theta Sketches using SerVer = 3
    //UNION Empty Rule: AND the empty states
    
    if ((sketchIn == null)  || sketchIn.isEmpty()) {
      //null/empty is interpreted as (Theta = 1.0, count = 0, empty = T).  Nothing changes
      return;
    }
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    long thetaLongIn = sketchIn.getThetaLong();
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule with incoming
    int curCountIn = sketchIn.getRetainedEntries(true);
    
    if (sketchIn.isOrdered()) { //Only true if Compact. Use early stop
      
      if (sketchIn.isDirect()) { //ordered, direct thus compact
        Memory skMem = sketchIn.getMemory();
        int preambleLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
        for (int i = 0; i < curCountIn; i++ ) {
          int offsetBytes = (preambleLongs + i) << 3;
          long hashIn = skMem.getLong(offsetBytes);
          if (hashIn >= unionThetaLong_) break; // "early stop"
          gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
        }
      } 
      else { //sketchIn is on the Java Heap, ordered, thus compact
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
      for (int i = 0, c = 0; (i < arrLongs) && (c < curCountIn); i++ ) {
        long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) continue; //rejects dirty values
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
        c++; //insures against invalid state inside the incoming sketch
        
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong()); //Theta rule with gadget
    if (unionMem_ != null) unionMem_.putLong(UNION_THETA_LONG, unionThetaLong_);
  }
  
  @Override
  public void update(Memory skMem) {
    //UNION Empty Rule: AND the empty states
    if (skMem == null) return;
    int cap = (int)skMem.getCapacity();
    int fam = skMem.getByte(FAMILY_BYTE);
    if (fam != 3) { //
      throw new SketchesArgumentException("Family must be COMPACT or SET_SKETCH (old): " + fam);
    }
    int serVer = skMem.getByte(SER_VER_BYTE);
    if (serVer == 1) { //older SetSketch, which is compact and ordered
      if (cap <= 24) return; //empty
      processVer1(skMem);
    }
    else if (serVer == 2) { //older SetSketch, which is compact and ordered
      if (cap <= 8) return; //empty
      processVer2(skMem);
    }
    else if (serVer == 3) { //Only the OpenSource sketches
      if (cap <= 8) return; //empty
      processVer3(skMem);
    }
    else {
      throw new SketchesArgumentException("SerVer is unknown: " + serVer);
    }
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
  public void update(char[] data) {
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
  
  //no seedhash, assumes given seed is correct. No p, no empty flag, 
  // can only be compact, ordered, size > 24
  private void processVer1(Memory skMem) {
    long thetaLongIn = skMem.getLong(THETA_LONG);
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule
    int curCount = skMem.getInt(RETAINED_ENTRIES_INT);
    int preLongs = 3;
    for (int i = 0; i < curCount; i++ ) {
      int offsetBytes = (preLongs + i) << 3;
      long hashIn = skMem.getLong(offsetBytes);
      if (hashIn >= unionThetaLong_) break; // "early stop"
      gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
    if (unionMem_ != null) unionMem_.putLong(UNION_THETA_LONG, unionThetaLong_);
  }
  
  //has seedhash and p, could have 0 entries & theta, 
  // can only be compact, ordered, size >= 8
  private void processVer2(Memory skMem) {
    Util.checkSeedHashes(seedHash_, skMem.getShort(SEED_HASH_SHORT));
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
      int offsetBytes = (preLongs + i) << 3;
      long hashIn = skMem.getLong(offsetBytes);
      if (hashIn >= unionThetaLong_) break; // "early stop"
      gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
    if (unionMem_ != null) unionMem_.putLong(UNION_THETA_LONG, unionThetaLong_);
  }
  
  //has seedhash, p, could have 0 entries & theta, 
  // could be unordered, ordered, compact, or not, size >= 8
  private void processVer3(Memory skMem) {
    Util.checkSeedHashes(seedHash_, skMem.getShort(SEED_HASH_SHORT));
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
        int offsetBytes = (preLongs + i) << 3;
        long hashIn = skMem.getLong(offsetBytes);
        if (hashIn >= unionThetaLong_) break; // "early stop"
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    else { //not-ordered, could be compact or hash-table form
      boolean compact = skMem.isAnyBitsSet(FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
      int size = (compact) ? curCount : 1 << skMem.getByte(LG_ARR_LONGS_BYTE);
      for (int i = 0; i < size; i++ ) {
        int offsetBytes = (preLongs + i) << 3;
        long hashIn = skMem.getLong(offsetBytes);
        if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) continue;
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
    if (unionMem_ != null) unionMem_.putLong(UNION_THETA_LONG, unionThetaLong_);
  }

}
