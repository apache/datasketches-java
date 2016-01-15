/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.MemoryImpl.*;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.checkSeedHashes;
import static com.yahoo.sketches.theta.PreambleUtil.computeSeedHash;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedDuplicate;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;
import static com.yahoo.sketches.Util.*;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRequest;
import com.yahoo.sketches.memory.MemoryUtil;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.HashOperations;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectQuickSelectSketch extends DirectUpdateSketch {
  private static final double DQS_RESIZE_THRESHOLD  = 15.0 / 16.0; //tuned for space
  
  //These values may be accessed on every update, thus are kept on-heap for speed.
  private final int preambleLongs_;
  private int lgArrLongs_;         //use setLgArrLongs()
  private int hashTableThreshold_; //only on heap, never serialized.
  private int curCount_;           //use setCurCount()
  private long thetaLong_;         //use setThetaLong()
  private boolean empty_;
  
  private Memory mem_;
  
  /**
   * Construct a new sketch using the given Memory as its backing store.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf Currently internallyfixed at 2.
   * <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param dstMem the given Memory object destination. Required. It will be cleared prior to use.
   * @param unionGadget true if this sketch is implementing the Union gadget function. 
   * Otherwise, it is behaving as a normal QuickSelectSketch.
   */
  DirectQuickSelectSketch(int lgNomLongs, long seed, float p, ResizeFactor rf, Memory dstMem, 
      boolean unionGadget) {
    super(lgNomLongs,
        seed, 
        p, 
        (rf = ResizeFactor.X2)
    );
    mem_ = dstMem; //cannot be null via builder
    
    if (lgNomLongs_ < MIN_LG_NOM_LONGS) {
      freeMem(mem_);
      throw new IllegalArgumentException(
        "This sketch requires a minimum nominal entries of "+(1 << MIN_LG_NOM_LONGS));
    }
    Family family;
    if (unionGadget) {
      preambleLongs_ = Family.UNION.getMinPreLongs();
      family = Family.UNION;
    } 
    else {
      preambleLongs_ = Family.QUICKSELECT.getMinPreLongs();
      family = Family.QUICKSELECT;
    }
    
    int minReqBytes;
    //If memReq is null require full memory, otherwise start small
    MemoryRequest memReq = mem_.getMemoryRequest();
    if (memReq == null) {
      lgArrLongs_ = setLgArrLongs(mem_, lgNomLongs_ +1);
      minReqBytes = getReqMemBytesFull(lgNomLongs_, preambleLongs_);
    }
    else {
      lgArrLongs_ = setLgArrLongs(mem_, MIN_LG_ARR_LONGS);
      minReqBytes = getMemBytes(lgArrLongs_, preambleLongs_);
    }
    //Make sure Memory is large enough
    long curMemCapBytes = mem_.getCapacity();
    if (curMemCapBytes < minReqBytes) {
      freeMem(mem_);
      throw new IllegalArgumentException(
        "Memory capacity is too small: "+curMemCapBytes+" < "+minReqBytes);
    }
    
    //build preamble and cache together in single Memory
    byte byte0 = (byte) (preambleLongs_ | (rf.lg() << 6));
    mem_.putByte(PREAMBLE_LONGS_BYTE, byte0);                 //byte 0 set local preambleLongs_ & RF
    mem_.putByte(SER_VER_BYTE, (byte) SER_VER);               //byte 1
    mem_.putByte(FAMILY_BYTE, (byte) family.getID());         //byte 2
    mem_.putByte(LG_NOM_LONGS_BYTE, (byte) lgNomLongs_);      //byte 3 local already set
                                                              //byte 4 local already set
    
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true.
    empty_ = true;
    mem_.putByte(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);         //byte 5
    
    mem_.putShort(SEED_HASH_SHORT, computeSeedHash(seed));    //bytes 6,7
    curCount_ = setCurCount(mem_, 0);                         //bytes 8-11
    
    mem_.putFloat(P_FLOAT, p);                                //byte 12-15
    thetaLong_ = setThetaLong(mem_, (long)(p * MAX_THETA_LONG_AS_DOUBLE));     //bytes 16-23
    
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
    mem_.clear(preambleLongs_ << 3, 8 << lgArrLongs_);      //clear hash table area only
  }
  
  /**
   * Wrap a sketch around the given source Memory containing sketch data. 
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * The given Memory object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a> 
   */
  DirectQuickSelectSketch(Memory srcMem, long seed) {
    super(
        srcMem.getByte(LG_NOM_LONGS_BYTE), 
        seed, 
        srcMem.getFloat(P_FLOAT), 
        ResizeFactor.getRF((srcMem.getByte(PREAMBLE_LONGS_BYTE) >>> 6) & 0X3)
    );
    short seedHashMem = srcMem.getShort(SEED_HASH_SHORT); //check for seed conflict
    short seedHashArg = computeSeedHash(seed);
    checkSeedHashes(seedHashMem, seedHashArg);
    
    int familyID = srcMem.getByte(FAMILY_BYTE);
    if (familyID == Family.UNION.getID()) {
      preambleLongs_ = Family.UNION.getMinPreLongs() & 0X3F;
      //MY_FAMILY = Family.UNION;
    } 
    else { //QS via Sketch.wrap(Memory, seed)
      preambleLongs_ = Family.QUICKSELECT.getMinPreLongs() & 0X3F;
      //MY_FAMILY = Family.QUICKSELECT;
    } //
    
    thetaLong_ = srcMem.getLong(THETA_LONG);
    lgArrLongs_ = srcMem.getByte(LG_ARR_LONGS_BYTE);
    
    long curCapBytes = srcMem.getCapacity();
    int minReqBytes = getMemBytes(lgArrLongs_, preambleLongs_);
    if (curCapBytes < minReqBytes) {
      freeMem(srcMem);
      throw new IllegalArgumentException(
          "Possible corruption: Current Memory size < min required size: " + 
              curCapBytes + " < " + minReqBytes);
    }
    
    if ((lgArrLongs_ <= lgNomLongs_) && (thetaLong_ < Long.MAX_VALUE) ) {
      freeMem(srcMem);
      throw new IllegalArgumentException(
        "Possible corruption: Theta cannot be < 1.0 and lgArrLongs <= lgNomLongs. "+
            lgArrLongs_ + " <= " + lgNomLongs_ + ", Theta: "+getTheta() );
    }
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
    curCount_ = srcMem.getInt(RETAINED_ENTRIES_INT);
    empty_ = srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    mem_ = srcMem;
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
    int lengthBytes = (preambleLongs_ + (1 << lgArrLongs_)) << 3;
    byte[] byteArray = new byte[lengthBytes];
    Memory mem = new NativeMemory(byteArray);
    MemoryUtil.copy(mem_, 0, mem, 0, lengthBytes);
    return byteArray;
  }
  
  //UpdateSketch
  
  @Override
  public UpdateSketch rebuild() {
    if (getRetainedEntries(true) > (1 << getLgNomLongs())) {
      quickSelectAndRebuild(mem_, preambleLongs_, lgNomLongs_, lgArrLongs_);
      //Reset local variables
      curCount_ = mem_.getInt(RETAINED_ENTRIES_INT);
      thetaLong_ = mem_.getLong(THETA_LONG);
    }
    return this;
  }
  
  @Override
  public final void reset() {
    //clear hash table
    //hash table size and threshold stays the same
    //lgArrLongs stays the same
    int arrLongs = 1 << getLgArrLongs();
    int preBytes = preambleLongs_ << 3;
    mem_.clear(preBytes, arrLongs*8); //clear data array
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true.
    mem_.putByte(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);       //byte 5
    empty_ = true; 
    curCount_ = setCurCount(mem_, 0);
    float p = mem_.getFloat(P_FLOAT);
    thetaLong_ = setThetaLong(mem_, (long)(p * MAX_THETA_LONG_AS_DOUBLE));
  }
  
  //restricted methods
  
  @Override
  int getPreambleLongs() {
    return preambleLongs_;
  }
  
  @Override
  long[] getCache() {
    long[] cacheArr = new long[1 << lgArrLongs_];
    Memory mem = new NativeMemory(cacheArr);
    MemoryUtil.copy(mem_, preambleLongs_ << 3, mem, 0, 8<< lgArrLongs_);
    return cacheArr;
  }
  
  @Override
  Memory getMemory() {
    return mem_;
  }
  
  @Override
  long getThetaLong() {
    return thetaLong_;
  }
  
  @Override
  boolean isDirty() {
    return false; //Always false for QuickSelectSketch
  }
  
  @Override
  int getLgArrLongs() {
    return lgArrLongs_;
  }
  
  /**
   * All potential updates converge here.
   * <p>Don't ever call this unless you really know what you are doing!</p>
   * 
   * @param hash the given input hash value.  It should never be zero.
   * @return <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  @Override
  UpdateReturnState hashUpdate(long hash) {
    HashOperations.checkHashCorruption(hash);
    
    if (empty_) {
      mem_.clearBits(FLAGS_BYTE, (byte)EMPTY_FLAG_MASK);
      empty_ = false;
    }
    
    //The over-theta test
    if (HashOperations.continueCondition(thetaLong_, hash)) {
      return RejectedOverTheta; //signal that hash was rejected due to theta.
    }
    
    //The duplicate test
    int preBytes = preambleLongs_ << 3;
    
    int index = HashOperations.hashSearchOrInsert(mem_, lgArrLongs_, hash, preBytes);
    if (index >= 0) return RejectedDuplicate; //not inserted
    
    //negative index means hash was inserted
    mem_.putInt(RETAINED_ENTRIES_INT, ++curCount_); //update curCount
    
    if (curCount_ > hashTableThreshold_) { //we need to do something, we are out of space
      mem_ = resizeMoveOrRebuild(mem_, preambleLongs_, lgNomLongs_, lgArrLongs_);
      curCount_ = mem_.getInt(RETAINED_ENTRIES_INT); 
      thetaLong_ = mem_.getLong(THETA_LONG);
      lgArrLongs_ = mem_.getByte(LG_ARR_LONGS_BYTE);
      hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
      
    } //else curCount <= hashTableThreshold
    return InsertedCountIncremented;
    
  }
  
  //special set methods
  
  /**
   * Returns the cardinality limit given the current size of the hash table array.
   * 
   * @param noRebuild if true the sketch cannot perform any rebuild or resizing operations. 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  private static final int setHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    double fraction = (lgArrLongs <= lgNomLongs) ? DQS_RESIZE_THRESHOLD : REBUILD_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }
  
  private static final int setLgArrLongs(Memory mem, int newLgArrLongs) {
    mem.putByte(LG_ARR_LONGS_BYTE, (byte) newLgArrLongs);
    return newLgArrLongs;
  }
  
  private static final long setThetaLong(Memory mem, long newThetaLong) {
    mem.putLong(THETA_LONG, newThetaLong);
    return newThetaLong;
  }
  
  private static final int setCurCount(Memory mem, int newCurCount) {
    mem.putInt(RETAINED_ENTRIES_INT, newCurCount);
    return newCurCount;
  }
  
}
