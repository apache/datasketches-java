/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the 
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import com.yahoo.sketches.*;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

import java.util.Arrays;

import static com.yahoo.sketches.Family.objectToFamily;
import static com.yahoo.sketches.Util.MIN_LG_ARR_LONGS;
import static com.yahoo.sketches.Util.floorPowerOf2;
import static com.yahoo.sketches.theta.CompactSketch.compactCachePart;
import static com.yahoo.sketches.theta.PreambleUtil.*;
import static java.lang.Math.min;

/**
 * Intersection operation for Theta Sketches.
 * 
 * <p>This implementation uses data either on-heap or off-heap in a given Memory 
 * that is owned and managed by the caller.
 * The off-heap Memory, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 * 
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class IntersectionImpl extends SetOperation implements Intersection {
  private final short seedHash_;
  //Note: Intersection does not use lgNomLongs or k, per se.
  private int lgArrLongs_; //current size of hash table
  private int curCount_; //curCount of HT, if < 0 means Universal Set (US) is true
  private long thetaLong_;
  private boolean empty_;
  
  private long[] hashTable_ = null;  //HT => Data.  Only used On Heap
  private int maxLgArrLongs_ = 0; //max size of hash table. Only used Off Heap
  private Memory mem_ = null; //must be set by one of the factory methods. Only used Off Heap.

  
  private IntersectionImpl(short seedHash) {
    seedHash_ = seedHash;
  }
  
  /**
   * Construct a new Intersection target on the java heap.
   * 
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   */
  static IntersectionImpl initNewHeapInstance(long seed) {
    IntersectionImpl impl = new IntersectionImpl(computeSeedHash(seed));
    impl.empty_ = false;  //A virgin intersection represents the Universal Set so empty is FALSE!
    impl.curCount_ = -1;  //Universal Set is true
    impl.thetaLong_ = Long.MAX_VALUE;
    impl.lgArrLongs_ = 0;
    impl.hashTable_ = null;
    impl.mem_ = null; //On the Heap
    return impl;
  }
  
  /**
   * Heapify an intersection target from a Memory image containing data. 
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  static IntersectionImpl heapifyInstance(Memory srcMem, long seed) {
    int preLongs = CONST_PREAMBLE_LONGS;
    long[] preArr = new long[preLongs];
    srcMem.getLongArray(0, preArr, 0, preLongs);
    
    long pre0 = preArr[0];
    int preambleLongs = extractPreLongs(pre0);
    if (preambleLongs != CONST_PREAMBLE_LONGS) {
      throw new SketchesArgumentException("PreambleLongs must equal " + CONST_PREAMBLE_LONGS);
    }
    int serVer = extractSerVer(pre0);
    if (serVer != SER_VER) {
      throw new SketchesArgumentException("Ser Version must equal " + SER_VER);
    }
    int famID = extractFamilyID(pre0);
    Family.INTERSECTION.checkFamilyID(famID);
    
    short seedHash = computeSeedHash(seed);
    short seedHashMem = (short) extractSeedHash(pre0);
    Util.checkSeedHashes(seedHashMem, seedHash); //check for seed hash conflict
    
    IntersectionImpl impl = new IntersectionImpl(seedHash);
    
    //Note: Intersection does not use lgNomLongs or k, per se.
    impl.lgArrLongs_ = extractLgArrLongs(pre0); //current hash table size
    
    int flags = extractFlags(pre0);
    impl.empty_ = (flags & EMPTY_FLAG_MASK) > 0;
    impl.curCount_ = srcMem.getInt(RETAINED_ENTRIES_INT);
    impl.thetaLong_ = srcMem.getLong(THETA_LONG);
    
    if (impl.empty_) {
      if (impl.curCount_ != 0) {
        throw new SketchesArgumentException(
            "srcMem empty state inconsistent with curCount: " + impl.empty_ + "," + impl.curCount_);
      }
      //empty = true AND curCount_ = 0: OK
    }
    else { //empty = false, curCount could be anything
      if (impl.curCount_ > 0) { //can't be virgin, empty, or curCount == 0
        impl.hashTable_ = new long[1 << impl.lgArrLongs_];
        srcMem.getLongArray(CONST_PREAMBLE_LONGS << 3, impl.hashTable_, 0, 1 << impl.lgArrLongs_);
      }
    }
    impl.mem_ = null; //On the Heap
    return impl;
  }
  
  /**
   * Construct a new Intersection target direct to the given destination Memory.
   * Called by SetOperation.Builder.
   * 
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @param dstMem destination Memory.  
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  static IntersectionImpl initNewDirectInstance(long seed, Memory dstMem) {
    short seedHash = computeSeedHash(seed);
    IntersectionImpl impl = new IntersectionImpl(seedHash);
    
    int preLongs = CONST_PREAMBLE_LONGS;
    impl.maxLgArrLongs_ = checkMaxLgArrLongs(dstMem); //Only Off Heap
    
    //build preamble and cache together in single Memory, insert fields into memory in one step
    long[] preArr = new long[preLongs]; //becomes the preamble
    
    long pre0 = 0;
    pre0 = insertPreLongs(preLongs, pre0); //RF not used = 0
    pre0 = insertSerVer(SER_VER, pre0);
    pre0 = insertFamilyID(Family.INTERSECTION.getID(), pre0);
    //Note: Intersection does not use lgNomLongs or k, per se.
    impl.lgArrLongs_ = MIN_LG_ARR_LONGS; //set initially to minimum, but don't clear cache in mem
    pre0 = insertLgArrLongs(MIN_LG_ARR_LONGS, pre0);
    //flags: bigEndian = readOnly = compact = ordered = false;
    impl.empty_ = false;
    
    pre0 = insertFlags(0, pre0);
    pre0 = insertSeedHash(seedHash, pre0);
    preArr[0] = pre0;
    
    long pre1 = 0;
    impl.curCount_ = -1; //set in mem below
    pre1 = insertCurCount(-1, pre1);
    pre1 = insertP((float) 1.0, pre1);
    preArr[1] = pre1;
    
    impl.thetaLong_ = Long.MAX_VALUE;
    preArr[2] = impl.thetaLong_;
    dstMem.putLongArray(0, preArr, 0, preLongs); //put into mem
    impl.mem_ = dstMem; //Off Heap
    return impl;
  }
  
  /**
   * Wrap an Intersection target around the given source Memory containing intersection data. 
   * @param srcMem The source Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  static IntersectionImpl wrapInstance(Memory srcMem, long seed) {
    int preLongs = CONST_PREAMBLE_LONGS;
    long[] preArr = new long[preLongs];
    srcMem.getLongArray(0, preArr, 0, preLongs);
    
    long pre0 = preArr[0];
    int preLongsMem = extractPreLongs(pre0);
    if (preLongsMem != CONST_PREAMBLE_LONGS) {
      throw new SketchesArgumentException("PreambleLongs must = 3.");
    }
    int serVer = extractSerVer(pre0);
    if (serVer != 3) {
      throw new SketchesArgumentException("Ser Version must = 3");
    }
    int famID = extractFamilyID(pre0);
    Family.INTERSECTION.checkFamilyID(famID);
    
    short seedHash = computeSeedHash(seed);
    short seedHashMem = (short) extractSeedHash(pre0);
    Util.checkSeedHashes(seedHashMem, seedHash); //check for seed hash conflict
    
    IntersectionImpl impl = new IntersectionImpl(seedHash);
    
    //Note: Intersection does not use lgNomLongs or k, per se.
    impl.lgArrLongs_ = extractLgArrLongs(pre0); //current hash table size
    impl.maxLgArrLongs_ = checkMaxLgArrLongs(srcMem); //Only Off Heap, check for min size
    int flags = extractFlags(pre0);
    impl.empty_ = (flags & EMPTY_FLAG_MASK) > 0;
    
    impl.curCount_ = extractCurCount(preArr[1]);
    impl.thetaLong_ = preArr[2];
    
    if (impl.empty_) {
      if (impl.curCount_ != 0) {
        throw new SketchesArgumentException(
            "srcMem empty state inconsistent with curCount: " + impl.empty_ + "," + impl.curCount_);
      }
      //empty = true AND curCount_ = 0: OK
    } //else empty = false, curCount could be anything
    impl.mem_ = srcMem; //Off Heap
    return impl;
  }
  
  @Override
  public void update(Sketch sketchIn) {
    if (sketchIn == null) { //null := Th = 1.0, count = 0, empty = true
      //Can't check the seedHash
      empty_ = setEmpty(true, mem_); //The Empty rule is OR
      if (curCount_ < 0) { //1st Call
        thetaLong_ = setThetaLong(Long.MAX_VALUE, mem_);
      } //else it is the Nth Call and theta stays the same
      curCount_ = setCurCount(0, mem_);
      return;
    }
    
    //The Intersection State Machine
    int sketchInEntries = sketchIn.getRetainedEntries(true);
    
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    thetaLong_ = setThetaLong(min(thetaLong_, sketchIn.getThetaLong()), mem_); //Theta rule
    empty_ = setEmpty(empty_ || sketchIn.isEmpty(), mem_);  //Empty rule
    
    if ((curCount_ == 0) || (sketchInEntries == 0)) {
      //The 1st Call (curCount  < 0) and sketchInEntries == 0.
      //The Nth Call (curCount == 0) and sketchInEntries == 0.
      //The Nth Call (curCount == 0) and sketchInEntries  > 0.
      //The Nth Call (curCount  > 0) and sketchInEntries == 0.
      //All future intersections result in zero data, but theta can still be reduced.
      curCount_ = setCurCount(0, mem_);
      hashTable_ = null; //No need for a HT.
    }
    else if (curCount_ < 0) { //virgin
      //The 1st Call (curCount  < 0) and sketchInEntries  > 0. Clone the incoming sketch
      curCount_ = setCurCount(sketchIn.getRetainedEntries(true), mem_);
      int requiredLgArrLongs = computeMinLgArrLongsFromCount(curCount_);
      int priorLgArrLongs = lgArrLongs_; //only used in error message
      lgArrLongs_ = setLgArrLongs(requiredLgArrLongs, mem_);
      
      if (mem_ != null) { //Off heap, check if current dstMem is large enough
        if (requiredLgArrLongs <= maxLgArrLongs_) { //OK
          mem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_);
        }
        else { //not enough space in dstMem //TODO move to request model?
          throw new SketchesArgumentException(
              "Insufficient dstMem hash table space: " 
                  + (1 << requiredLgArrLongs) + " > " + (1 << priorLgArrLongs));
        }
      } else { //On the heap, allocate a HT
        hashTable_ = new long[1 << lgArrLongs_];
      }
      moveDataToTgt(sketchIn.getCache(), curCount_);
    }
    else { //curCount > 0
      //The Nth Call (curCount  > 0) and sketchInEntries  > 0.
      //Must perform full intersect
      //Sets resulting hashTable, curCount and adjusts lgArrLongs
      performIntersect(sketchIn);
    }
  }

  @Override
  public CompactSketch getResult(boolean dstOrdered, Memory dstMem) {
    if (curCount_ < 0) {
      throw new SketchesStateException(
          "Calling getResult() with no intervening intersections is not a legal result.");
    }
    long[] compactCacheR;
    
    if (curCount_ == 0) {
      compactCacheR = new long[0];
      return CompactSketch.createCompactSketch(
          compactCacheR, empty_, seedHash_, curCount_, thetaLong_, dstOrdered, dstMem);
    } 
    //else curCount > 0
    long[] hashTable;
    if (mem_ != null) {
      int htLen = 1 << lgArrLongs_;
      hashTable = new long[htLen];
      mem_.getLongArray(CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen);
    } else {
      hashTable = hashTable_;
    }
    compactCacheR = compactCachePart(hashTable, lgArrLongs_, curCount_, thetaLong_, dstOrdered);
    
    //Create the CompactSketch
    return CompactSketch.createCompactSketch(
        compactCacheR, empty_, seedHash_, curCount_, thetaLong_, dstOrdered, dstMem);
  }

  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }

  @Override
  public boolean hasResult() {
    return (mem_ != null) ? mem_.getInt(RETAINED_ENTRIES_INT) >= 0 : curCount_ >= 0;
  }

  @Override
  public byte[] toByteArray() {
    int preBytes = CONST_PREAMBLE_LONGS << 3;
    int dataBytes = (curCount_ > 0) ? 8 << lgArrLongs_ : 0;
    byte[] byteArrOut = new byte[preBytes + dataBytes];
    if (mem_ != null) {
      mem_.getByteArray(0, byteArrOut, 0, preBytes + dataBytes);
    } 
    else {
      NativeMemory memOut = new NativeMemory(byteArrOut);
      
      //preamble
      memOut.putByte(PREAMBLE_LONGS_BYTE, (byte) CONST_PREAMBLE_LONGS); //RF not used = 0
      memOut.putByte(SER_VER_BYTE, (byte) SER_VER);
      memOut.putByte(FAMILY_BYTE, (byte) objectToFamily(this).getID());
      memOut.putByte(LG_NOM_LONGS_BYTE, (byte) 0); //not used
      memOut.putByte(LG_ARR_LONGS_BYTE, (byte) lgArrLongs_);
      if (empty_) {
        memOut.setBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
      } 
      else {
        memOut.clearBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
      }
      memOut.putShort(SEED_HASH_SHORT, seedHash_);
      memOut.putInt(RETAINED_ENTRIES_INT, curCount_);
      memOut.putFloat(P_FLOAT, (float) 1.0);
      memOut.putLong(THETA_LONG, thetaLong_);
      
      //data
      if (curCount_ > 0) {
        memOut.putLongArray(preBytes, hashTable_, 0, 1 << lgArrLongs_);
      }
    }
    return byteArrOut;
  }

  @Override
  public void reset() {
    lgArrLongs_ = setLgArrLongs(lgArrLongs_, mem_);
    curCount_ = setCurCount(-1, mem_); //Universal Set is true
    thetaLong_ = setThetaLong(Long.MAX_VALUE, mem_);
    empty_ = setEmpty(false, mem_);
    hashTable_ = null;
  }

  @Override
  public Family getFamily() {
    return Family.INTERSECTION;
  }

  //restricted
  
  private void performIntersect(Sketch sketchIn) {
    // curCount and input data are nonzero, match against HT
    assert ((curCount_ > 0) && (!empty_));
    long[] cacheIn = sketchIn.getCache();
    int arrLongsIn = cacheIn.length;
    long[] hashTable;
    if (mem_ != null) {
      int htLen = 1 << lgArrLongs_;
      hashTable = new long[htLen];
      mem_.getLongArray(CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen); 
    } else {
      hashTable = hashTable_;
    }
    //allocate space for matching
    long[] matchSet = new long[ min(curCount_, sketchIn.getRetainedEntries(true)) ];

    int matchSetCount = 0;
    if (sketchIn.isOrdered()) {
      //ordered compact, which enables early stop
      for (int i = 0; i < arrLongsIn; i++ ) {
        long hashIn = cacheIn[i];
        if (hashIn <= 0L) continue;
        if (hashIn >= thetaLong_) {
          break; //early stop assumes that hashes in input sketch are ordered!
        }
        int foundIdx = HashOperations.hashSearch(hashTable, lgArrLongs_, hashIn);
        if (foundIdx == -1) continue;
        matchSet[matchSetCount++] = hashIn;
      }
    } 
    else {
      //either unordered compact or hash table
      for (int i = 0; i < arrLongsIn; i++ ) {
        long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= thetaLong_)) continue;
        int foundIdx = HashOperations.hashSearch(hashTable, lgArrLongs_, hashIn);
        if (foundIdx == -1) continue;
        matchSet[matchSetCount++] = hashIn;
      }
    }
    //reduce effective array size to minimum
    lgArrLongs_ = setLgArrLongs(computeMinLgArrLongsFromCount(curCount_), mem_);
    curCount_ = setCurCount(matchSetCount, mem_);
    if (mem_ != null) {
      mem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_); //clear for rebuild
    } else {
      Arrays.fill(hashTable_, 0, 1 << lgArrLongs_, 0L); //clear for rebuild
    }
    //move matchSet to target
    moveDataToTgt(matchSet, matchSetCount);
  }
  
  private void moveDataToTgt(long[] arr, int count) {
    int arrLongsIn = arr.length;
    int tmpCnt = 0;
    if (mem_ != null) { //Off Heap puts directly into mem
      int preBytes = CONST_PREAMBLE_LONGS << 3;
      for (int i = 0; i < arrLongsIn; i++ ) {
        long hashIn = arr[i];
        if (HashOperations.continueCondition(thetaLong_, hashIn)) continue;
        HashOperations.hashInsertOnly(mem_, lgArrLongs_, hashIn, preBytes);
        tmpCnt++;
      }
    } else { //On Heap. Assumes HT exists and is large enough
      for (int i = 0; i < arrLongsIn; i++ ) {
        long hashIn = arr[i];
        if (HashOperations.continueCondition(thetaLong_, hashIn)) continue;
        HashOperations.hashInsertOnly(hashTable_, lgArrLongs_, hashIn);
        tmpCnt++;
      }
    }
    assert (tmpCnt == count) : "Intersection Count Check: got: " + tmpCnt + ", expected: " + count;
  }

  //special handlers for Off Heap
  /**
   * Returns the correct maximum lgArrLongs given the capacity of the Memory. Checks that the 
   * capacity is large enough for the minimum sized hash table.
   * @param dstMem the given Memory
   * @return the correct maximum lgArrLongs given the capacity of the Memory
   */
  private static final int checkMaxLgArrLongs(Memory dstMem) {
    int preBytes = CONST_PREAMBLE_LONGS << 3;
    long cap = dstMem.getCapacity();
    int maxLgArrLongs = Integer.numberOfTrailingZeros(floorPowerOf2((int)(cap - preBytes)) >>> 3);
    if (maxLgArrLongs < MIN_LG_ARR_LONGS) {
      throw new SketchesArgumentException(
        "dstMem not large enough for minimum sized hash table: " + cap);
    }
    return maxLgArrLongs;
  }
  
  private static final boolean setEmpty(boolean empty, Memory mem) {
    if (mem != null) {
      if (empty) {
        mem.setBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
      } 
      else {
        mem.clearBits(FLAGS_BYTE, (byte)EMPTY_FLAG_MASK);
      }
    }
    return empty;
  }
  
  private static final int setLgArrLongs(int lgArrLongs, Memory mem) {
    if (mem != null) {
      mem.putByte(LG_ARR_LONGS_BYTE, (byte) lgArrLongs);
    }
    return lgArrLongs;
  }
  
  private static final long setThetaLong(long thetaLong, Memory mem) {
    if (mem != null) {
      mem.putLong(THETA_LONG, thetaLong);
    }
    return thetaLong;
  }
  
  private static final int setCurCount(int curCount, Memory mem) {
    if (mem != null) {
      mem.putInt(RETAINED_ENTRIES_INT, curCount);
    }
    return curCount;
  }

}
