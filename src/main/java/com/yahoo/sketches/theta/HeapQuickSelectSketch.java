/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.QuickSelect.selectExcludingZeros;
import static com.yahoo.sketches.theta.PreambleUtil.BIG_ENDIAN_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractP;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.getMemBytes;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedDuplicate;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;
import static com.yahoo.sketches.Util.*;
import static java.lang.Math.max;
import static java.lang.Math.min;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class HeapQuickSelectSketch extends HeapUpdateSketch { //UpdateSketch implements UpdateInternal, SetArgument {
  private final Family MY_FAMILY;
  
  private final int preambleLongs_;
  private int lgArrLongs_;
  private int hashTableThreshold_;  //never serialized
  private int curCount_;
  private long thetaLong_;
  private boolean empty_;
  
  private long[] cache_;
  
  private HeapQuickSelectSketch(int lgNomLongs, long seed, float p, ResizeFactor rf, 
      int preambleLongs, Family family) {
    super(lgNomLongs, 
    seed, 
    p, 
    rf);
    preambleLongs_ = preambleLongs;
    MY_FAMILY = family;
  }
  
  /**
   * Get a new sketch instance on the java heap.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param unionGadget true if this sketch is implementing the Union gadget function. 
   * Otherwise, it is behaving as a normal QuickSelectSketch.
   * @return instance of this sketch
   */
  static HeapQuickSelectSketch getInstance(int lgNomLongs, long seed, float p, ResizeFactor rf, 
      boolean unionGadget) {
    
    //Choose family, preambleLongs
    Family family;
    int preambleLongs;
    if (unionGadget) {
      preambleLongs = Family.UNION.getMinPreLongs();
      family = Family.UNION;
    } 
    else {
      preambleLongs = Family.QUICKSELECT.getMinPreLongs();
      family = Family.QUICKSELECT;
    }
    
    HeapQuickSelectSketch hqss = new HeapQuickSelectSketch(lgNomLongs, seed, p, rf, 
        preambleLongs, family);
    int lgArrLongs = startingSubMultiple(lgNomLongs+1, rf, MIN_LG_ARR_LONGS);
    hqss.lgArrLongs_ = lgArrLongs;
    hqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    hqss.curCount_ = 0;
    hqss.thetaLong_ = (long)(p * MAX_THETA_LONG_AS_DOUBLE);
    hqss.empty_ = true; //other flags: bigEndian = readOnly = compact = ordered = false; 
    hqss.cache_ = new long[1 << lgArrLongs];
    return hqss;
  }
  
  /**
   * Heapify a sketch from a Memory UpdateSketch or Union object 
   * containing sketch data. 
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return instance of this sketch
   */
  static HeapQuickSelectSketch getInstance(Memory srcMem, long seed) {
    long[] preArr = new long[3];
    srcMem.getLongArray(0, preArr, 0, 3); //extract the preamble
    long long0 = preArr[0];
    int preambleLongs = extractPreLongs(long0);                           //byte 0
    ResizeFactor myRF = ResizeFactor.getRF(extractResizeFactor(long0));   //byte 0
    int serVer = extractSerVer(long0);                                    //byte 1
    int familyID = extractFamilyID(long0);                                //byte 2
    int lgNomLongs = extractLgNomLongs(long0);                            //byte 3
    int lgArrLongs = extractLgArrLongs(long0);                            //byte 4
    int flags = extractFlags(long0);                                      //byte 5
    short seedHash = (short)extractSeedHash(long0);                       //byte 6,7
    long long1 = preArr[1];
    int curCount = extractCurCount(long1);                                //bytes 8-11
    float p = extractP(long1);                                            //bytes 12-15
    long thetaLong = preArr[2];                                           //bytes 16-23
    
    if (serVer != SER_VER) {
      throw new IllegalArgumentException(
          "Possible corruption: Invalid Serialization Version: "+serVer);
    }
    
    Family family = Family.idToFamily(familyID);
    if (family.equals(Family.UNION)) {
      if (preambleLongs != Family.UNION.getMinPreLongs()) {
        throw new IllegalArgumentException(
            "Possible corruption: Invalid PreambleLongs value for UNION: " +preambleLongs);
      }
    }
    else if (family.equals(Family.QUICKSELECT)) {
      if (preambleLongs != Family.QUICKSELECT.getMinPreLongs()) {
        throw new IllegalArgumentException(
            "Possible corruption: Invalid PreambleLongs value for QUICKSELECT: " +preambleLongs);
      }
    } 
    else {
      throw new IllegalArgumentException(
          "Possible corruption: Invalid Family: " + family.toString());
    }
    
    if (lgNomLongs < MIN_LG_NOM_LONGS) {
      throw new IllegalArgumentException(
          "Possible corruption: Current Memory lgNomLongs < min required size: " + 
              lgNomLongs + " < " + MIN_LG_NOM_LONGS);
    }
    
    int flagsMask = ORDERED_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | BIG_ENDIAN_FLAG_MASK;
    if ((flags & flagsMask) > 0) {
      throw new IllegalArgumentException(
          "Possible corruption: Input srcMem cannot be: big-endian, compact, ordered, or read-only");
    }
    
    Util.checkSeedHashes(seedHash, Util.computeSeedHash(seed));
    
    long curCapBytes = srcMem.getCapacity();
    int minReqBytes = getMemBytes(lgArrLongs, preambleLongs);
    if (curCapBytes < minReqBytes) {
      throw new IllegalArgumentException(
          "Possible corruption: Current Memory size < min required size: " + 
              curCapBytes + " < " + minReqBytes);
    }
    
    double theta = thetaLong/MAX_THETA_LONG_AS_DOUBLE;
    if ((lgArrLongs <= lgNomLongs) && (theta < p) ) {
      throw new IllegalArgumentException(
        "Possible corruption: Theta cannot be < p and lgArrLongs <= lgNomLongs. "+
            lgArrLongs + " <= " + lgNomLongs + ", Theta: "+theta + ", p: " + p);
    }
    
    HeapQuickSelectSketch hqss = new HeapQuickSelectSketch(lgNomLongs, seed, p, myRF, preambleLongs, 
        family);
    hqss.lgArrLongs_ = lgArrLongs;
    hqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    hqss.curCount_ = curCount;
    hqss.thetaLong_ = thetaLong;
    hqss.empty_ = (flags & EMPTY_FLAG_MASK) > 0;
    hqss.cache_ = new long[1 << lgArrLongs];
    srcMem.getLongArray(preambleLongs << 3, hqss.cache_, 0, 1 << lgArrLongs);  //read in as hash table
    return hqss;
  }
  
  //Sketch
  
  @Override
  public int getRetainedEntries(boolean valid) {
    return curCount_;
  }
  
  @Override
  public boolean isEmpty() {
    return empty_;
  }
  
  @Override
  public byte[] toByteArray() {
    return toByteArray(preambleLongs_, (byte) MY_FAMILY.getID());
  }
  
  @Override
  public Family getFamily() {
    return MY_FAMILY;
  }
  
  //UpdateSketch
  
  @Override
  public UpdateSketch rebuild() {
    if (getRetainedEntries(true) > (1 << getLgNomLongs())) {
      quickSelectAndRebuild();
    }
    return this;
  }
  
  @Override
  public final void reset() {
    ResizeFactor rf = getResizeFactor();
    int lgArrLongsSM = startingSubMultiple(lgNomLongs_+1, rf, MIN_LG_ARR_LONGS);
    if (lgArrLongsSM == lgArrLongs_) {
      int arrLongs = cache_.length;
      assert (1 << lgArrLongs_) == arrLongs; 
      java.util.Arrays.fill(cache_,  0L);
    } 
    else {
      cache_ = new long[1 << lgArrLongsSM];
      lgArrLongs_ = lgArrLongsSM;
    }
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
    empty_ = true;
    curCount_ = 0;
    thetaLong_ =  (long)(getP() * MAX_THETA_LONG_AS_DOUBLE);
  }
  
  //restricted methods
  
  @Override
  int getPreambleLongs() {
    return preambleLongs_;
  }
  
  @Override
  long[] getCache() {
    return cache_;
  }
  
  @Override
  long getThetaLong() {
    return thetaLong_;
  }
  
  @Override
  boolean isDirty() {
    return false;
  }
  
  @Override
  int getLgArrLongs() {
    return lgArrLongs_;
  }
  
  @Override
  UpdateReturnState hashUpdate(long hash) {
    HashOperations.checkHashCorruption(hash);
    empty_ = false;
    
    //The over-theta test
    if (HashOperations.continueCondition(thetaLong_, hash)) {
      return RejectedOverTheta; //signal that hash was rejected due to theta.
    }
    
    //The duplicate test
    if (HashOperations.hashSearchOrInsert(cache_, lgArrLongs_, hash) >= 0) {
      return RejectedDuplicate; //Duplicate, not inserted
    }
    //insertion occurred, must increment curCount
    curCount_++;
    
    if (curCount_ > hashTableThreshold_) { //we need to do something, we are out of space
      //must rebuild or resize
      if (lgArrLongs_ <= lgNomLongs_) { //resize
        resizeCache();
      } 
      else { //Already at tgt size, must rebuild
        assert (lgArrLongs_ == lgNomLongs_ + 1) : "lgArr: " + lgArrLongs_ + ", lgNom: " + lgNomLongs_;
        quickSelectAndRebuild(); //Changes thetaLong_, curCount_, reassigns cache
      }
    }
    return InsertedCountIncremented;

  }
  
  //Must resize. Changes lgArrLongs_ and cache_. theta and count don't change.
  // Used by hashUpdate()
  private final void resizeCache() {
    ResizeFactor rf = getResizeFactor();
    int lgTgtLongs = lgNomLongs_ + 1;
    int lgDeltaLongs = lgTgtLongs - lgArrLongs_;
    int lgResizeFactor = max(min(rf.lg(), lgDeltaLongs), 1); //rf_.lg() could be 0
    lgArrLongs_ += lgResizeFactor; // new tgt size
    
    long[] tgtArr = new long[1 << lgArrLongs_];
    int newCount = HashOperations.hashArrayInsert(cache_, tgtArr, lgArrLongs_, thetaLong_);
    
    assert newCount == curCount_;  //Assumes no dirty values.
    curCount_ = newCount;
    
    cache_ = tgtArr;
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
  }
  
  //array stays the same size. Changes theta and thus count
  private final void quickSelectAndRebuild() {
    int arrLongs = 1 << lgArrLongs_;
    
    int pivot = (1 << lgNomLongs_) + 1; // pivot for QS

    thetaLong_ = selectExcludingZeros(cache_, curCount_, pivot); //messes up the cache_ 
    
    // now we rebuild to clean up dirty data, update count, reconfigure as a hash table
    long[] tgtArr = new long[arrLongs];
    curCount_ = HashOperations.hashArrayInsert(cache_, tgtArr, lgArrLongs_, thetaLong_);
    cache_ = tgtArr;
    //hashTableThreshold stays the same
  }
  
  /**
   * Returns the cardinality limit given the current size of the hash table array.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  static final int setHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    double fraction = (lgArrLongs <= lgNomLongs) ? RESIZE_THRESHOLD : REBUILD_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }
  
}
