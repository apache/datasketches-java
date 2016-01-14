/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.QuickSelect.selectExcludingZeros;
import static com.yahoo.sketches.theta.HashOperations.hashInsert;
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

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectQuickSelectSketch extends DirectUpdateSketch {
  private static final double DQS_RESIZE_THRESHOLD  = 15.0 / 16.0; //tuned for space
  
  private final Family MY_FAMILY;
  private final int preambleLongs_;
  
  private Memory mem_;                  //only on heap, never serialized
  private final MemoryRequest memReq_;  //only on heap, never serialized
  
  private int lgArrLongs_;         //use setLgArrLongs()
  private int hashTableThreshold_; //only on heap, never serialized.
  
  private int curCount_;           //use setCurCount()
  private long thetaLong_;         //use setThetaLong()
  private boolean empty_;
  private final boolean dirty_ = false;   //always false with QS sketch
  
  /**
   * Construct a new sketch using the given Memory as its backing store.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param dstMem the given Memory object destination. Required. It will be cleared prior to use.
   * @param unionGadget true if this sketch is implementing the Union gadget function. 
   * Otherwise, it is behaving as a normal QuickSelectSketch.
   */
  DirectQuickSelectSketch(int lgNomLongs, long seed, float p, ResizeFactor rf, 
      Memory dstMem, boolean unionGadget) {
    super(lgNomLongs, 
        seed, 
        p, 
        rf 
    );
    if (lgNomLongs_ < MIN_LG_NOM_LONGS) {
      freeMem(dstMem);
      throw new IllegalArgumentException(
        "This sketch requires a minimum nominal entries of "+(1 << MIN_LG_NOM_LONGS));
    }
    
    mem_ = dstMem; //cannot be null via builder
    
    int myPreambleLongs;
    if (unionGadget) {
      myPreambleLongs = Family.UNION.getMinPreLongs();
      MY_FAMILY = Family.UNION;
    } 
    else {
      myPreambleLongs = Family.QUICKSELECT.getMinPreLongs();
      MY_FAMILY = Family.QUICKSELECT;
    }
    
    memReq_ = dstMem.getMemoryRequest();
    int myLgArrLongs = startingSubMultiple(lgNomLongs_ + 1, rf, MIN_LG_ARR_LONGS);
    long curCapBytes = dstMem.getCapacity();
    int minReqBytes = (memReq_ == null)? getFullCapBytes(lgNomLongs_, myPreambleLongs) 
        : getRequiredBytes(myLgArrLongs, myPreambleLongs);
    if (curCapBytes < minReqBytes) {
      freeMem(dstMem);
      throw new IllegalArgumentException(
        "Memory capacity is too small: "+curCapBytes+" < "+minReqBytes);
    }
    
    //build preamble and cache together in single Memory
    
    byte byte0 = (byte) (myPreambleLongs | (rf.lg() << 6));
    preambleLongs_ = myPreambleLongs;                       //set local preambleLongs_
    mem_.putByte(PREAMBLE_LONGS_BYTE, byte0);               //byte 0
    mem_.putByte(SER_VER_BYTE, (byte) SER_VER);             //byte 1
    mem_.putByte(FAMILY_BYTE, (byte) MY_FAMILY.getID());    //byte 2 local already set
    mem_.putByte(LG_NOM_LONGS_BYTE, (byte) lgNomLongs_);    //byte 3 local already set
    lgArrLongs_ = setLgArrLongs(mem_, myLgArrLongs);        //byte 4
    
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true.
    empty_ = true;
    mem_.putByte(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);       //byte 5
    
    mem_.putShort(SEED_HASH_SHORT, computeSeedHash(seed));  //bytes 6,7
    curCount_ = setCurCount(mem_, 0);                       //bytes 8-11
    
    mem_.putFloat(P_FLOAT, p);                              //byte 12-15
    thetaLong_ = setThetaLong(mem_, (long)(p * MAX_THETA_LONG_AS_DOUBLE));     //bytes 16-23
    
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
    mem_.clear(preambleLongs_ << 3, 8 << lgArrLongs_);      //clear data area only
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
      MY_FAMILY = Family.UNION;
    } 
    else { //QS via Sketch.wrap(Memory, seed)
      preambleLongs_ = Family.QUICKSELECT.getMinPreLongs() & 0X3F;
      MY_FAMILY = Family.QUICKSELECT;
    } //
    
    thetaLong_ = srcMem.getLong(THETA_LONG);
    lgArrLongs_ = srcMem.getByte(LG_ARR_LONGS_BYTE);
    
    long curCapBytes = srcMem.getCapacity();
    int minReqBytes = getRequiredBytes(lgArrLongs_, preambleLongs_);
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
    memReq_ = srcMem.getMemoryRequest();
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
      quickSelectAndRebuild();
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
  
  //Set Argument
  
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
   * @param hash the given input hash value.  It should never be zero
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
    
    //The duplicate test from hashInsert
    
    int lgArrLongs = getLgArrLongs();
    int preBytes = preambleLongs_ << 3;
    boolean inserted = hashInsert(mem_, lgArrLongs, hash, preBytes);
    if (inserted) {
      mem_.putInt(RETAINED_ENTRIES_INT, ++curCount_);
      
      if (curCount_ > hashTableThreshold_) {
        // curBytes < reqBytes <= fullBytes
        // curBytes <= curCapBytes
        int curBytes = getRequiredBytes(lgArrLongs_, preambleLongs_);
        int fullBytes = getFullCapBytes(lgNomLongs_, preambleLongs_);
        
        if (curBytes >= fullBytes) {
          //Already at tgt size, must rebuild 
          //Assumes no dirty values.
          //Changes thetaLong_, curCount_
          int lgNomLongs = getLgNomLongs();
          assert (lgArrLongs == lgNomLongs + 1) : "lgArr: " + lgArrLongs + ", lgNom: " + lgNomLongs;
          quickSelectAndRebuild();  //rebuild
          return InsertedCountIncremented;
        }
        else { //not at full size
          //Can we expand in current mem?
          int dstLgArrLongs = lgArrLongs_ + 1;
          int reqBytes = getRequiredBytes(dstLgArrLongs, preambleLongs_);
          long curCapBytes = mem_.getCapacity();
          
          if (reqBytes <= curCapBytes) { //yes
            resizeMe(dstLgArrLongs);
          }
          else { //no, request a bigger space
            Memory dstMem = memReq_.request(reqBytes);
            if (dstMem == null) {
              throw new IllegalArgumentException("Requested memory cannot be null.");
            }
            long newCap = dstMem.getCapacity();
            if (newCap < reqBytes) {
              freeMem(dstMem);
              throw new IllegalArgumentException("Requested memory not granted: "+newCap+" < "+reqBytes);
            }
            Memory srcMem = mem_; //keep link to me
            moveAndResizeMe(srcMem, dstMem, dstLgArrLongs);
            //Reset local variables
            mem_ = dstMem; //the new me
            lgArrLongs_ = setLgArrLongs(mem_, dstLgArrLongs);
            hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
            memReq_.free(srcMem, dstMem);
          } //end of expand in current mem or not
        } //end of curBytes vs fullBytes
      } //else curCount >= hashTableThreshold
    } //else not inserted 
    return RejectedDuplicate;
  }
  
  //private
  
  private static final int getRequiredBytes(int lgArrLongs, int preambleLongs) {
    return (8 << lgArrLongs) + (preambleLongs << 3);
  }
  
  private static final int getFullCapBytes(int lgNomLongs, int preambleLongs) {
    return (16 << lgNomLongs) + (preambleLongs << 3);
  }
  
  //array stays the same size. Changes theta and thus count
  private final void quickSelectAndRebuild() {
    //Pull data into tmp arr for QS algo
    int lgArrLongs = getLgArrLongs();
    int arrLongs = 1 << lgArrLongs;
    int pivot = (1 << getLgNomLongs()) + 1; // pivot for QS
    long[] tmpArr = new long[arrLongs];
    int preBytes = preambleLongs_ << 3;
    mem_.getLongArray(preBytes, tmpArr, 0, arrLongs); //copy mem data to tmpArr
    
    //do the QuickSelect on tmp arr
    long newThetaLong = selectExcludingZeros(tmpArr, getRetainedEntries(true), pivot);
    thetaLong_ = setThetaLong(mem_, newThetaLong); //changes tmpArr 
    
    // now we rebuild to clean up dirty data, update count
    long[] tgtArr = new long[arrLongs];
    int newCurCount = HashOperations.hashArrayInsert(tmpArr, tgtArr, lgArrLongs, getThetaLong());
    curCount_ = setCurCount(mem_, newCurCount);
    mem_.putLongArray(preBytes, tgtArr, 0, arrLongs); //put data back to mem
  }
  
  /**
   * Moves me (the entire sketch) to a new larger Memory location and rebuilds the hash table.
   * Caller must update necessary local class members afterwards.
   * @param srcMem the source Memory
   * @param dstMem the destination Memory
   * @param dstLgArrLongs the destination hash table target size
   */
  private static final void moveAndResizeMe(Memory srcMem, Memory dstMem, int dstLgArrLongs) {
    //Move Preamble to destination memory
    int preBytes = (srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0x3F) << 3;
    MemoryUtil.copy(srcMem, 0, dstMem, 0, preBytes);
    //Bulk copy source to on-heap buffer
    int srcHTLen = 1 << srcMem.getByte(LG_ARR_LONGS_BYTE);
    long[] srcHTArr = new long[srcHTLen];
    srcMem.getLongArray(preBytes, srcHTArr, 0, srcHTLen);
    //Create destination buffer
    int dstHTLen = 1 << dstLgArrLongs;
    long[] dstHTArr = new long[dstHTLen];
    //Rebuild hash table in destination buffer
    long thetaLong = srcMem.getLong(THETA_LONG);
    HashOperations.hashArrayInsert(srcHTArr, dstHTArr, dstLgArrLongs, thetaLong);
    //Bulk copy to destination memory
    dstMem.putLongArray(preBytes, dstHTArr, 0, dstHTLen);
  }
  
  /**
   * Resizes existing hash array into a larger one within a single Memory assuming enough space.
   * @param newLgArrLongs the LgArrLongs value for the new hash table
   */
  private final void resizeMe(int newLgArrLongs) {
    //Preamble stays in place
    int preBytes = preambleLongs_ << 3;
    //Bulk copy source to on-heap buffer
    int srcHTLen = 1 << lgArrLongs_; //current value
    long[] srcHTArr = new long[srcHTLen]; //on-heap src buffer
    mem_.getLongArray(preBytes, srcHTArr, 0, srcHTLen);
    //Create destination on-heap buffer
    int dstHTLen = 1 << newLgArrLongs;
    long[] dstHTArr = new long[dstHTLen]; //on-heap dst buffer
    //Rebuild hash table in destination buffer
    HashOperations.hashArrayInsert(srcHTArr, dstHTArr, newLgArrLongs, thetaLong_);
    //Bulk copy to destination memory
    mem_.putLongArray(preBytes, dstHTArr, 0, dstHTLen); //put it back, no need to clear
    //Reset local variables
    lgArrLongs_ = setLgArrLongs(mem_, newLgArrLongs);
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
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
  
  //Only used when MemoryRequest is not available
  private static final void freeMem(Memory mem) { 
    MemoryRequest memReq = mem.getMemoryRequest();
    if (memReq != null) {
      memReq.free(mem);
    }
    else if (mem instanceof NativeMemory) {
      ((NativeMemory)mem).freeMemory();
    }
  }
  
}
