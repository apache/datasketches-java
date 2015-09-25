/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.QuickSelect.selectExcludingZeros;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_RESIZE_FACTOR_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.checkSeedHashes;
import static com.yahoo.sketches.theta.PreambleUtil.computeSeedHash;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedDuplicate;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;
import static java.lang.Math.max;
import static java.lang.Math.min;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class HeapQuickSelectSketch extends HeapUpdateSketch { //UpdateSketch implements UpdateInternal, SetArgument {

  static final int HQS_MIN_LG_ARR_LONGS = 5; //The smallest Log2 cache size allowed; => 32.
  static final int HQS_MIN_LG_NOM_LONGS = 4; //The smallest Log2 nom entries allowed; => 16.
  static final double HQS_REBUILD_THRESHOLD = 15.0 / 16.0;
  static final double HQS_RESIZE_THRESHOLD = .5; //tuned for speed
  
  private final Family MY_FAMILY;
  private final int preambleLongs_;
  
  private long[] cache_;
  
  private int lgArrLongs_;
  private int hashTableThreshold_;  //never serialized
  
  private int curCount_;
  private long thetaLong_;
  private boolean empty_;
  private boolean dirty_;
  
  /**
   * Construct a new sketch on the java heap. 
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param unionGadget true if this sketch is implementing the Union gadget function. 
   * Otherwise, it is behaving as a normal QuickSelectSketch.
   */
  HeapQuickSelectSketch(int lgNomLongs, long seed, float p, ResizeFactor rf, boolean unionGadget) {
    super(lgNomLongs, 
        seed, 
        p, 
        rf);
    if (lgNomLongs_ < HQS_MIN_LG_NOM_LONGS) throw new IllegalArgumentException(
        "This sketch requires a minimum nominal entries of "+(1 << HQS_MIN_LG_NOM_LONGS));
    
    if (unionGadget) {
      preambleLongs_ = Family.UNION.getMinPreLongs();
      MY_FAMILY = Family.UNION;
    } 
    else {
      preambleLongs_ = Family.QUICKSELECT.getMinPreLongs();
      MY_FAMILY = Family.QUICKSELECT;
    }
    
    lgArrLongs_ = startingSubMultiple(lgNomLongs_+1, rf, HQS_MIN_LG_ARR_LONGS);
    cache_ = new long[1 << lgArrLongs_];
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
    empty_ = true; //other flags: bigEndian = readOnly = compact = ordered = false; 
    curCount_ = 0;
    thetaLong_ = (long)(p * MAX_THETA_LONG_AS_DOUBLE);
    dirty_ = false;
  }
  
  /**
   * Heapify a sketch from a Memory UpdateSketch or Union object 
   * containing sketch data. 
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  HeapQuickSelectSketch(Memory srcMem, long seed) {
    super(
        srcMem.getByte(LG_NOM_LONGS_BYTE), 
        seed, 
        srcMem.getFloat(P_FLOAT),
        ResizeFactor.getRF(srcMem.getByte(LG_RESIZE_FACTOR_BYTE) >>> 6)
    );
    short seedHashMem = srcMem.getShort(SEED_HASH_SHORT); //check for seed conflict
    short seedHashArg = computeSeedHash(seed);
    checkSeedHashes(seedHashMem, seedHashArg);
    
    int familyID = srcMem.getByte(FAMILY_BYTE);
    if (familyID == Family.UNION.getID()) {
      preambleLongs_ = Family.UNION.getMinPreLongs() & 0X3F;
      MY_FAMILY = Family.UNION;
    } 
    else {
      preambleLongs_ = Family.QUICKSELECT.getMinPreLongs() & 0X3F;
      MY_FAMILY = Family.QUICKSELECT;
    }
    
    lgArrLongs_ = srcMem.getByte(LG_ARR_LONGS_BYTE);
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
    curCount_ = srcMem.getInt(RETAINED_ENTRIES_INT);
    thetaLong_ = srcMem.getLong(THETA_LONG);
    empty_ = srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    dirty_ = false;
    
    cache_ = new long[1 << lgArrLongs_];
    srcMem.getLongArray(preambleLongs_ << 3, cache_, 0, 1 << lgArrLongs_);  //read in as hash table
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
    int lgArrLongsSM = startingSubMultiple(lgNomLongs_+1, rf_, HQS_MIN_LG_ARR_LONGS);
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
    thetaLong_ =  (long)(p_ * MAX_THETA_LONG_AS_DOUBLE);
  }
  
  //restricted methods
  
  @Override
  int getPreambleLongs() {
    return preambleLongs_;
  }
  
  //Set Arguments 
  
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
    return dirty_;
  }
  
  //Update Internals
  
  @Override
  int getLgArrLongs() {
    return lgArrLongs_;
  }
  
  /**
   * All potential updates converge here.
   * <p>Don't ever call this unless you really know what you are doing!</p>
   * 
   * @param hash the given input hash value. It should never be zero.
   * @return <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  @Override
  UpdateReturnState hashUpdate(long hash) {
    assert (hash > 0L): "Corruption: negative hashes should not happen. ";
    empty_ = false;
    
    //The over-theta test
    if (hash >= thetaLong_) {
    // very very unlikely that hash == Long.MAX_VALUE. It is ignored just as zero is ignored.
      return RejectedOverTheta; //signal that hash was rejected due to theta.
    }
    
    //The duplicate/inserted tests
    boolean inserted = HashOperations.hashInsert(cache_, lgArrLongs_, hash);
    if (inserted) {
      curCount_++;
      if (curCount_ > hashTableThreshold_) {
        //must rebuild or resize
        if (lgArrLongs_ <= lgNomLongs_) { //resize
          resizeCache();
        } 
        else { //rebuild
          //Already at tgt size, must rebuild
          assert (lgArrLongs_ == lgNomLongs_ + 1) : "lgArr: " + lgArrLongs_ + ", lgNom: " + lgNomLongs_;
          quickSelectAndRebuild(); //Changes thetaLong_, curCount_
        }
      }
      return InsertedCountIncremented;
    } 
    return RejectedDuplicate;
  }
  
  //Private
  
  //Must resize. Changes lgArrLongs_ only. theta doesn't change, count doesn't change.
  // Used by hashUpdate()
  private final void resizeCache() {
    int lgTgtLongs = lgNomLongs_ + 1;
    int lgDeltaLongs = lgTgtLongs - lgArrLongs_;
    int lgResizeFactor = max(min(rf_.lg(), lgDeltaLongs), 1); //rf_.lg() could be 0
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

    thetaLong_ = selectExcludingZeros(cache_, curCount_, pivot); //changes cache_ 
    
    // now we rebuild to clean up dirty data, update count
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
    double fraction = (lgArrLongs <= lgNomLongs) ? HQS_RESIZE_THRESHOLD : HQS_REBUILD_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }
  
}