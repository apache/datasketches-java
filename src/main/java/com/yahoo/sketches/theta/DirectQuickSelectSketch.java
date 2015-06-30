/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.QuickSelect.selectExcludingZeros;
import static com.yahoo.sketches.Util.floorPowerOf2;
import static com.yahoo.sketches.theta.HashOperations.hashInsert;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.checkSeedHashes;
import static com.yahoo.sketches.theta.PreambleUtil.computeSeedHash;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedDuplicate;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedFull;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryUtil;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectQuickSelectSketch extends DirectUpdateSketch {
  static final int DQS_MIN_LG_ARR_LONGS = 5; //The smallest Log2 cache size allowed; => 32.
  static final int DQS_MIN_LG_NOM_LONGS = 4; //The smallest Log2 nom entries allowed; => 16.
  static final double DQS_REBUILD_THRESHOLD = 15.0 / 16.0;
  static final double DQS_RESIZE_THRESHOLD  = 15.0 / 16.0; //tuned for space
  
  private final Family MY_FAMILY;
  private final int preambleLongs_;
  
  private final Memory mem_;
  private final boolean noRebuild_;      //only on heap, never serialized
  
  private final int lgArrLongs_;
  private final int hashTableThreshold_; //only on heap, never serialized.
  
  private int curCount_;    //setCurCount()
  private long thetaLong_;  //setThetaLong()
  private boolean empty_;   //setEmpty()
  private boolean dirty_;
  
  /**
   * Construct a new sketch using the given Memory as its backing store.
   * If this backing store is big enough for the sketch at its maximum size, the sketch will
   * be initialized at the full size. Otherwise, the sketch will be initialized with the given
   * <i>lgNomLongs</i> but with the largest data cache that will fit in the given Memory. The
   * sketch will return an UpdateReturnState.RejectedFull state if a given input to one of the 
   * <i>update</i> methods cannot be accommodated in the provided Memory.  It is up to the caller
   * of the sketch to move the sketch into a larger Memory space, reinstantiate the sketch using
   * the UpdateSketch.wrap(Memory mem, long seed) method and retry the input update item.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param dstMem the given Memory object destination. It will be cleared prior to use.
   * @param unionGadget true if this sketch is implementing the Union gadget function. 
   * Otherwise, it is behaving as a normal QuickSelectSketch.
   */
  DirectQuickSelectSketch(int lgNomLongs, long seed, float p, Memory dstMem, boolean unionGadget) {
    super(lgNomLongs, 
        seed, 
        p, 
        ResizeFactor.X1 //override = 0.  Resize functionality not practical off-heap.
    );
    if (lgNomLongs_ < DQS_MIN_LG_NOM_LONGS) throw new IllegalArgumentException(
        "This sketch requires a minimum nominal entries of "+(1 << DQS_MIN_LG_NOM_LONGS));
    
    if (unionGadget) {
      preambleLongs_ = Family.UNION.getMinPreLongs();
      MY_FAMILY = Family.UNION;
    } 
    else {
      preambleLongs_ = Family.QUICKSELECT.getMinPreLongs();
      MY_FAMILY = Family.QUICKSELECT;
    }
    //build preamble and cache together in single Memory
    mem_ = dstMem;
    mem_.clear();
    
    int lgArrLongs;
    long memCapacityBytes = mem_.getCapacity();
    int reqCapacityBytes = (16 << lgNomLongs_) + (preambleLongs_ << 3);
    if (memCapacityBytes >= reqCapacityBytes) {
      //big enough for full size sketch
      lgArrLongs = lgNomLongs_ + 1; //Allocate cache at full size
      noRebuild_ = false;
    } 
    else { 
      //choose largest cache that will fit
      int floorPwrOf2Bytes = 
          floorPowerOf2((int)(memCapacityBytes - (preambleLongs_ << 3)));
      lgArrLongs = Integer.numberOfTrailingZeros(floorPwrOf2Bytes >>> 3);
      noRebuild_ = true;
      if (lgArrLongs < DQS_MIN_LG_ARR_LONGS) {
        throw new IllegalArgumentException("Not sufficent Memory capacity for minimal sketch.");
      }
    }
    
    //load preamble into mem
    mem_.putByte(PREAMBLE_LONGS_BYTE, (byte) preambleLongs_);  //RF not used = 0
    mem_.putByte(SER_VER_BYTE, (byte) SER_VER);
    mem_.putByte(FAMILY_BYTE, (byte) MY_FAMILY.getID());
    mem_.putByte(LG_NOM_LONGS_BYTE, (byte) lgNomLongs_);
    
    lgArrLongs_ = lgArrLongs;
    mem_.putByte(LG_ARR_LONGS_BYTE, (byte) lgArrLongs);
    
    hashTableThreshold_ = setHashTableThreshold(noRebuild_, lgNomLongs_, lgArrLongs_);
    
    //flags: bigEndian = readOnly = compact = ordered = false;
    mem_.setBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    empty_ = true;
    dirty_ = false;
    
    mem_.putShort(SEED_HASH_SHORT, computeSeedHash(seed));
    setCurCount(0);
    
    mem_.putFloat(P_FLOAT, p);
    setThetaLong((long)(p * MAX_THETA_LONG));
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
        ResizeFactor.X1 //Override
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
    
    int preBytes = preambleLongs_ << 3;
    thetaLong_ = srcMem.getLong(THETA_LONG);
    long memCapacityBytes = srcMem.getCapacity();
    int memLgArrLongs = srcMem.getByte(LG_ARR_LONGS_BYTE);
    int fullSizeBytes = (16 << lgNomLongs_) + (Family.QUICKSELECT.getMinPreLongs() << 3);
        //getMaxUpdateOrUnionSketchBytes(1 << lgNomLongs_);
    int newLgArrLongs;
    
    if (memCapacityBytes >= fullSizeBytes) {
      //big enough for full size sketch
      noRebuild_ = false;
      newLgArrLongs = lgNomLongs_ + 1; //Allocate cache at full size
      if (newLgArrLongs > memLgArrLongs) { //only resize if necessary
        resizeMemData(srcMem, memLgArrLongs, newLgArrLongs, thetaLong_, preambleLongs_);
      } //else done
    } 
    else {
      //choose largest cache that will fit
      noRebuild_ = true;
      int floorPwrOf2Bytes = floorPowerOf2((int)(memCapacityBytes - preBytes));
      newLgArrLongs = Integer.numberOfTrailingZeros(floorPwrOf2Bytes >>> 3);
      if (newLgArrLongs <= memLgArrLongs) {
        //If this memory is not at least 2X larger than it already is, this error is thrown.
        throw new IllegalArgumentException(
            "This Memory capacity must be at least 2X larger than what it already is: "
                +"New: "+newLgArrLongs+", Mem: "+memLgArrLongs);
      }
      
      resizeMemData(srcMem, memLgArrLongs, newLgArrLongs, thetaLong_, preambleLongs_);
    }
    
    lgArrLongs_ = newLgArrLongs;
    srcMem.putByte(LG_ARR_LONGS_BYTE, (byte) lgArrLongs_);
    
    hashTableThreshold_ = setHashTableThreshold(noRebuild_, lgNomLongs_, lgArrLongs_);
    curCount_ = srcMem.getInt(RETAINED_ENTRIES_INT);
    empty_ = srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    dirty_ = false;
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
  public final void reset() {
    //clear hash table
    //hash table size and threshold stays the same
    //noRebuild stays what it is
    //lgArrLongs stays the same
    int arrLongs = 1 << getLgArrLongs();
    Memory mem = getMemory();
    int preBytes = mem.getByte(PREAMBLE_LONGS_BYTE) << 3;
    mem.clear(preBytes, arrLongs*8);
    mem.clearBits(FLAGS_BYTE, (byte) READ_ONLY_FLAG_MASK); // may be removed later
    mem_.setBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    empty_ = true;
    setCurCount(0);
    float p = mem.getFloat(P_FLOAT);
    setThetaLong((long)(p * MAX_THETA_LONG));
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
  
  @Override
  void rebuild() {
    if (getRetainedEntries(true) > (1 << getLgNomLongs())) {
      quickSelectAndRebuild();
    }
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
    assert (hash > 0L): "Corruption: negative hashes should not happen. ";
    
    if (empty_) {
      mem_.clearBits(FLAGS_BYTE, (byte)EMPTY_FLAG_MASK);
      empty_ = false;
    }
    
    //The over-theta test
    if (hash >= thetaLong_) {
      return RejectedOverTheta; //signal that hash was rejected due to theta.
    }
    
    //The duplicate test from hashInsert
    Memory mem = getMemory();
    int lgArrLongs = getLgArrLongs();
    int preBytes = preambleLongs_ << 3;
    boolean inserted = hashInsert(mem, lgArrLongs, hash, preBytes);
    if (inserted) {
      mem.putInt(RETAINED_ENTRIES_INT, ++curCount_);
      
      if (curCount_ > hashTableThreshold_) {
        if (noRebuild_) {
          //user app must provide more space and retry the insert 
          //even before we know whether it is a duplicate or even < theta.
          return RejectedFull;
        
        } 
        else { 
          //Already at tgt size, must rebuild 
          //Assumes no dirty values.
          //Changes thetaLong_, curCount_
          int lgNomLongs = getLgNomLongs();
          assert (lgArrLongs == lgNomLongs + 1) : "lgArr: " + lgArrLongs + ", lgNom: " + lgNomLongs;
          quickSelectAndRebuild();
        }
      }
      return InsertedCountIncremented;
    } 
    return RejectedDuplicate;
  }
  
  //private
  
  //array stays the same size. Changes theta and thus count
  private final void quickSelectAndRebuild() {
    //Pull data into tmp arr for QS algo
    int lgArrLongs = getLgArrLongs();
    int arrLongs = 1 << lgArrLongs;
    int pivot = (1 << getLgNomLongs()) + 1; // pivot for QS
    long[] tmpArr = new long[arrLongs];
    int preBytes = preambleLongs_ << 3;
    Memory mem = getMemory();
    mem.getLongArray(preBytes, tmpArr, 0, arrLongs); //copy mem data to tmpArr
    
    //do the QuickSelect on tmp arr
    setThetaLong(selectExcludingZeros(tmpArr, getRetainedEntries(true), pivot)); //changes tmpArr 
    
    // now we rebuild to clean up dirty data, update count
    long[] tgtArr = new long[arrLongs];
    setCurCount(HashOperations.hashArrayInsert(tmpArr, tgtArr, lgArrLongs, getThetaLong()));
    mem.putLongArray(preBytes, tgtArr, 0, arrLongs); //put data back to mem
  }
  
  
  /**
   * Returns the cardinality limit given the current size of the hash table array.
   * 
   * @param noRebuild if true the sketch cannot perform any rebuild or resizing operations. 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  private static final int setHashTableThreshold(final boolean noRebuild, final int lgNomLongs, 
      final int lgArrLongs) {
    double fraction = (!noRebuild && (lgArrLongs <= lgNomLongs))
          ? DQS_RESIZE_THRESHOLD 
          : DQS_REBUILD_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }
  
  //Resize an existing hash array into a larger one.
  private static final void resizeMemData(
      Memory mem, int memLgArrLongs, int newLgArrLongs, long thetaLong, int preambleLongs) {
    int srcLen = 1 << memLgArrLongs;
    long[] srcArr = new long[srcLen];
    mem.getLongArray(preambleLongs << 3, srcArr, 0, srcLen);
    int dstLen = 1 << newLgArrLongs;
    long[] hashTable = new long[dstLen];
    HashOperations.hashArrayInsert(srcArr, hashTable, newLgArrLongs, thetaLong);
    mem.putLongArray(preambleLongs << 3, hashTable, 0, dstLen);
  }
  
  //special reset methods
  
  private final void setThetaLong(long thetaLong) {
    thetaLong_ = thetaLong;
    mem_.putLong(THETA_LONG, thetaLong);
  }
  
  private final void setCurCount(int curCount) {
    curCount_ = curCount;
    mem_.putInt(RETAINED_ENTRIES_INT, curCount);
  }
  
}