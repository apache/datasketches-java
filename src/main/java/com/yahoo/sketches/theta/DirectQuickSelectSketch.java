/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.MIN_LG_ARR_LONGS;
import static com.yahoo.sketches.Util.MIN_LG_NOM_LONGS;
import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.theta.PreambleUtil.BIG_ENDIAN_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
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
import static com.yahoo.sketches.theta.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.insertFlags;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertP;
import static com.yahoo.sketches.theta.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.insertSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.theta.Rebuilder.actLgResizeFactor;
import static com.yahoo.sketches.theta.Rebuilder.moveAndResize;
import static com.yahoo.sketches.theta.Rebuilder.quickSelectAndRebuild;
import static com.yahoo.sketches.theta.Rebuilder.resize;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedDuplicate;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRequest;
import com.yahoo.sketches.memory.NativeMemory;

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
   * @param dstMem the given Memory object destination. It cannot be null. 
   * It will be cleared prior to use.
   * @param unionGadget true if this sketch is implementing the Union gadget function. 
   * Otherwise, it is behaving as a normal QuickSelectSketch.
   * @return instance of this sketch
   */
  static DirectQuickSelectSketch getInstance(int lgNomLongs, long seed, float p, ResizeFactor rf, 
      Memory dstMem, boolean unionGadget) {
    
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
    int lgRF = rf.lg();
    int lgArrLongs = (lgRF == 0)? lgNomLongs +1 : MIN_LG_ARR_LONGS;
    int minReqBytes = PreambleUtil.getMemBytes(lgArrLongs, preambleLongs);
    
    //Make sure Memory is large enough
    long curMemCapBytes = dstMem.getCapacity();
    if (curMemCapBytes < minReqBytes) {
      throw new IllegalArgumentException(
        "Memory capacity is too small: "+curMemCapBytes+" < "+minReqBytes);
    }
    int curCount = 0;
//@formatter:off
    //Build preamble
    long pre0, pre1, thetaLong;
    pre0 = insertPreLongs(preambleLongs, 0L);                   //byte 0
    pre0 = insertResizeFactor(lgRF, pre0);                      //byte 0
    pre0 = insertSerVer(SER_VER, pre0);                         //byte 1
    pre0 = insertFamilyID(family.getID(), pre0);                //byte 2
    pre0 = insertLgNomLongs(lgNomLongs, pre0);                  //byte 3
    pre0 = insertLgArrLongs(lgArrLongs, pre0);                  //byte 4
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true : 00100 = 4
    pre0 = insertFlags(EMPTY_FLAG_MASK, pre0);                  //byte 5
    pre0 = insertSeedHash(Util.computeSeedHash(seed), pre0);    //bytes 6,7
    pre1 = curCount;                                            //bytes 8-11
    pre1 = insertP(p, pre1);                                    //bytes 12-15
    thetaLong = (long)(p * MAX_THETA_LONG_AS_DOUBLE);           //bytes 16-23
//@formatter:on
    //Insert preamble into Memory, only responsible for first 3 longs
    long[] preArr = {pre0, pre1, thetaLong};
    dstMem.putLongArray(0, preArr, 0, 3);

    //clear hash table area
    dstMem.clear(preambleLongs << 3, 8 << lgArrLongs); 
    
    DirectQuickSelectSketch dqss = new DirectQuickSelectSketch(lgNomLongs, seed, p, rf, preambleLongs);
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
    } else {
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
    NativeMemory.copy(mem_, 0, mem, 0, lengthBytes);
    return byteArray;
  }
  
  @Override
  public Family getFamily() {
    int familyID = mem_.getByte(FAMILY_BYTE);
    return Family.idToFamily(familyID);
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
    NativeMemory.copy(mem_, preambleLongs_ << 3, mem, 0, 8<< lgArrLongs_);
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
    //insertion occurred, increment curCount
    mem_.putInt(RETAINED_ENTRIES_INT, ++curCount_); //update curCount
    
    if (curCount_ > hashTableThreshold_) { //we need to do something, we are out of space
      
      if (lgArrLongs_ > lgNomLongs_) { //at full size, rebuild
        //Assumes no dirty values, changes thetaLong_, curCount_
        assert (lgArrLongs_ == lgNomLongs_ + 1) : "lgArr: " + lgArrLongs_ + ", lgNom: " + lgNomLongs_;
        quickSelectAndRebuild(mem_, preambleLongs_, lgNomLongs_, lgArrLongs_, curCount_);  //rebuild
        curCount_ = mem_.getInt(RETAINED_ENTRIES_INT);
        thetaLong_ = mem_.getLong(THETA_LONG);
      } //end of rebuild
      
      else { //Not at full size, resize. Should not get here if lgRF = 0 and memCap is too small.
        int lgRF = getLgResizeFactor();
        int actLgRF = actLgResizeFactor(mem_.getCapacity(), lgArrLongs_, preambleLongs_, lgRF);
        int tgtLgArrLongs = Math.min(lgArrLongs_ + actLgRF, lgNomLongs_+1);
        if (actLgRF > 0) { //Expand in current Memory
          resize(mem_, preambleLongs_, lgArrLongs_, tgtLgArrLongs);
          //update locals
          lgArrLongs_ = mem_.getByte(LG_ARR_LONGS_BYTE);
          hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
        } //end of Expand in current memory
        
        else { //Request more memory, then resize
          int preBytes = preambleLongs_ << 3;
          tgtLgArrLongs = Math.min(lgArrLongs_ + lgRF, lgNomLongs_ + 1);
          int tgtArrBytes = 8 << tgtLgArrLongs;
          int reqBytes = tgtArrBytes + preBytes;
          //if (tgtArrBytes < 2*(curMemCap));
          
          MemoryRequest memReq = mem_.getMemoryRequest();
          Memory dstMem = memReq.request(reqBytes);
          if (dstMem == null) { //returned a null
            throw new IllegalArgumentException("MemoryRequest callback cannot be null.");
          }
          long newCap = dstMem.getCapacity();
          if (newCap < reqBytes) {
            memReq.free(dstMem);
            throw new IllegalArgumentException("Requested memory not granted: "+newCap+" < "+reqBytes);
          }
          moveAndResize(mem_, preambleLongs_, lgArrLongs_, dstMem, tgtLgArrLongs, thetaLong_);
          
          memReq.free(mem_, dstMem); //normal free mechanism via MemoryRequest
          mem_ = dstMem;
          lgArrLongs_ = mem_.getByte(LG_ARR_LONGS_BYTE);
          hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
        } //end of Request more memory to resize
      } //end of resize
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
  
}
