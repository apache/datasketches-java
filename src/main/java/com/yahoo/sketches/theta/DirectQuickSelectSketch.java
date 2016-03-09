/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.Rebuilder.*;
import static com.yahoo.sketches.theta.PreambleUtil.BIG_ENDIAN_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.*;
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
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectQuickSelectSketch extends DirectUpdateSketch {
  private static final double DQS_RESIZE_THRESHOLD  = 15.0 / 16.0; //tuned for space
  
  //These values may be accessed on every update, thus are kept on-heap for speed.
  private final int preambleLongs_;
  private int lgArrLongs_;
  private int hashTableThreshold_; //only on heap, never serialized.
  private int curCount_;           //use setCurCount()
  private long thetaLong_;         //use setThetaLong()
  private boolean empty_;
  
  private Memory mem_;
  
  private DirectQuickSelectSketch(int lgNomLongs, long seed, float p, ResizeFactor rf, 
      int preambleLongs) {
    super(lgNomLongs,
        seed, 
        p, 
        rf
    );
    preambleLongs_ = preambleLongs;
  }
  
  /**
   * Get a new sketch instance using the given Memory as its backing store.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf Currently internally fixed at 2. Unless dstMem is not configured with a valid 
   * MemoryRequest, in which case the rf is effectively 1, which is no resizing at all and the 
   * dstMem must be large enough for a full sketch.
   * <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param dstMem the given Memory object destination. It cannot be null. It will be cleared prior to use.
   * @param unionGadget true if this sketch is implementing the Union gadget function. 
   * Otherwise, it is behaving as a normal QuickSelectSketch.
   * @return instance of this sketch
   */
  static DirectQuickSelectSketch getInstance(int lgNomLongs, long seed, float p, ResizeFactor rf, 
      Memory dstMem, boolean unionGadget) {
    
    //Check min k
    if (lgNomLongs < MIN_LG_NOM_LONGS) {
      throw new IllegalArgumentException(
        "This sketch requires a minimum nominal entries of "+(1 << MIN_LG_NOM_LONGS));
    }
    
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
    
    //Choose RF, minReqBytes, lgArrLongs. 
    ResizeFactor myRF;
    int minReqBytes;
    int lgArrLongs;
    MemoryRequest memReq = dstMem.getMemoryRequest();
    if (memReq == null) { //If memReq is null require full memory, RF = X1, no resizing; 
      lgArrLongs = lgNomLongs +1;
      myRF = ResizeFactor.X1;
      minReqBytes = PreambleUtil.getReqMemBytesFull(lgNomLongs, preambleLongs);
    } else { //otherwise start small with RF = X2.
      lgArrLongs = MIN_LG_ARR_LONGS;
      myRF = ResizeFactor.X2;
      minReqBytes = PreambleUtil.getMemBytes(lgArrLongs, preambleLongs);
    }
    
    //Make sure Memory is large enough
    long curMemCapBytes = dstMem.getCapacity();
    if (curMemCapBytes < minReqBytes) {
      throw new IllegalArgumentException(
        "Memory capacity is too small: "+curMemCapBytes+" < "+minReqBytes);
    }
    int curCount = 0;
    
    //Build preamble
    long pre0, pre1, thetaLong;
    pre0 = insertPreLongs(preambleLongs, 0L);                   //byte 0
    pre0 = insertResizeFactor(myRF.lg(), pre0);                 //byte 0
    pre0 = insertSerVer(SER_VER, pre0);                         //byte 1
    pre0 = insertFamilyID(family.getID(), pre0);                //byte 2
    pre0 = insertLgNomLongs(lgNomLongs, pre0);                  //byte 3
    pre0 = insertLgArrLongs(lgArrLongs, pre0);                  //byte 4
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true.
    pre0 = insertFlags(EMPTY_FLAG_MASK, pre0);                  //byte 5
    pre0 = insertSeedHash(Util.computeSeedHash(seed), pre0);         //bytes 6,7
    pre1 = curCount;                                            //bytes 8-11
    pre1 = insertP(p, pre1);                                    //bytes 12-15
    thetaLong = (long)(p * MAX_THETA_LONG_AS_DOUBLE);           //bytes 16-23
    
    //Insert preamble into Memory, only responsible for first 3 longs
    long[] preArr = {pre0, pre1, thetaLong};
    dstMem.putLongArray(0, preArr, 0, 3);

    //clear hash table area
    dstMem.clear(preambleLongs << 3, 8 << lgArrLongs); 
    
    DirectQuickSelectSketch dqss = new DirectQuickSelectSketch(lgNomLongs, seed, p, myRF, preambleLongs);
    dqss.lgArrLongs_ = lgArrLongs;
    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    dqss.curCount_ = curCount;
    dqss.thetaLong_ = thetaLong;
    dqss.empty_ = true;
    dqss.mem_ = dstMem;
    return dqss;
  }
  
  /**
   * Wrap a sketch around the given source Memory containing sketch data that originated from
   * this sketch.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * The given Memory object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketch getInstance(Memory srcMem, long seed) {
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
    } else {
      throw new IllegalArgumentException(
          "Possible corruption: Invalid Family: " + family.toString());
    }
    
    if (serVer != SER_VER) {
      throw new IllegalArgumentException(
          "Possible corruption: Invalid Serialization Version: "+serVer);
    }
    
    int flagsMask = ORDERED_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | BIG_ENDIAN_FLAG_MASK;
    if ((flags & flagsMask) > 0) {
      throw new IllegalArgumentException(
          "Possible corruption: Input srcMem cannot be: big-endian, compact, ordered, or read-only");
    }
    
    Util.checkSeedHashes(seedHash, Util.computeSeedHash(seed));
    
    long curCapBytes = srcMem.getCapacity();
    int minReqBytes = PreambleUtil.getMemBytes(lgArrLongs, preambleLongs);
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
    
    DirectQuickSelectSketch dqss = new DirectQuickSelectSketch(lgNomLongs, seed, p, myRF, preambleLongs);
    dqss.lgArrLongs_ = lgArrLongs;
    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    dqss.curCount_ = curCount;
    dqss.thetaLong_ = thetaLong;
    dqss.empty_ = (flags & EMPTY_FLAG_MASK) > 0;
    dqss.mem_ = srcMem;
    return dqss;
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
  public byte[] toByteArray() { //MY_FAMILY is stored in mem_
    int lengthBytes = (preambleLongs_ + (1 << lgArrLongs_)) << 3;
    byte[] byteArray = new byte[lengthBytes];
    Memory mem = new NativeMemory(byteArray);
    MemoryUtil.copy(mem_, 0, mem, 0, lengthBytes);
    return byteArray;
  }
  
  @Override
  public Family getFamily() {
    int familyID = mem_.getByte(FAMILY_BYTE);
    return Family.idToFamily(familyID);
  }
  
  @Override
  public ResizeFactor getResizeFactor() {
    return (mem_.getMemoryRequest() == null)? ResizeFactor.getRF(0) : ResizeFactor.getRF(1);
  }
  
  //UpdateSketch
  
  @Override
  public UpdateSketch rebuild() {
    if (getRetainedEntries(true) > (1 << getLgNomLongs())) {
      quickSelectAndRebuild(mem_, preambleLongs_, lgNomLongs_, lgArrLongs_, curCount_);
      //Reset local variables
      curCount_ = mem_.getInt(RETAINED_ENTRIES_INT);
      thetaLong_ = mem_.getLong(THETA_LONG);
    }
    return this;
  }
  
  @Override
  public final void reset() {
    //clear hash table
    //hash table size and hashTableThreshold stays the same
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
    if (HashOperations.hashSearchOrInsert(mem_, lgArrLongs_, hash, preambleLongs_ << 3) >= 0) {
      return RejectedDuplicate; //Duplicate, not inserted
    }
    //insertion occurred, must increment curCount
    mem_.putInt(RETAINED_ENTRIES_INT, ++curCount_); //update curCount
    
    if (curCount_ > hashTableThreshold_) { //we need to do something, we are out of space
      mem_ = resizeMoveOrRebuild(mem_, preambleLongs_, lgNomLongs_, lgArrLongs_, curCount_, thetaLong_);
      curCount_ = mem_.getInt(RETAINED_ENTRIES_INT); 
      thetaLong_ = mem_.getLong(THETA_LONG);
      lgArrLongs_ = mem_.getByte(LG_ARR_LONGS_BYTE);
      hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
    }
    return InsertedCountIncremented;
  }
  
  //special set methods
  
  /**
   * Returns the cardinality limit given the current size of the hash table array.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  private static final int setHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    double fraction = (lgArrLongs <= lgNomLongs) ? DQS_RESIZE_THRESHOLD : REBUILD_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }
  
  private static final long setThetaLong(Memory mem, long newThetaLong) {
    mem.putLong(THETA_LONG, newThetaLong);
    return newThetaLong;
  }
  
  private static final int setCurCount(Memory mem, int newCurCount) {
    mem.putInt(RETAINED_ENTRIES_INT, newCurCount);
    return newCurCount;
  }
  
  static void println(String s) {
    System.out.println(s); //disable here
  }
  
}
