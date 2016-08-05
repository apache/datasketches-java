/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the 
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.objectToFamily;
import static com.yahoo.sketches.Util.MIN_LG_ARR_LONGS;
import static com.yahoo.sketches.Util.floorPowerOf2;
import static com.yahoo.sketches.theta.CompactSketch.compactCachePart;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.insertCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.insertFlags;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertP;
import static com.yahoo.sketches.theta.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.insertSerVer;
import static java.lang.Math.min;

import java.util.Arrays;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

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
    impl.mem_ = null; //On the heap
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
    impl.maxLgArrLongs_ = checkMaxLgArrLongs(srcMem); //Only Off Heap
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
      if (mem_ != null) {
        empty_ = setEmpty(true); //The Empty rule is OR
        if (curCount_ < 0) { //1st Call
          thetaLong_ = setThetaLong(Long.MAX_VALUE);
        } //else it is the Nth Call and theta stays the same
        curCount_ = setCurCount(0);
      } else {
        empty_ |= true; //empty rule
        if (curCount_ < 0) { //1st Call
          thetaLong_ = Long.MAX_VALUE;
        } //else it is the Nth Call and theta stays the same
        curCount_ = 0;
      }
      return;
    }
    
    //The Intersection State Machine
    int sketchInEntries = sketchIn.getRetainedEntries(true);
    
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    if (mem_ != null) {
      thetaLong_ = setThetaLong(minThetaLong(sketchIn.getThetaLong())); //Theta rule
      empty_ = setEmpty(empty_ || sketchIn.isEmpty());  //Empty rule
    } else {
      thetaLong_ = min(thetaLong_, sketchIn.getThetaLong()); //Theta rule
      empty_ |= sketchIn.isEmpty();  //Empty rule
    }
    
    if ((curCount_ == 0) || (sketchInEntries == 0)) {
      //The 1st Call (curCount  < 0) and sketchInEntries == 0.
      //The Nth Call (curCount == 0) and sketchInEntries == 0.
      //The Nth Call (curCount == 0) and sketchInEntries  > 0.
      //The Nth Call (curCount  > 0) and sketchInEntries == 0.
      //All future intersections result in zero data, but theta can still be reduced.
      curCount_ = (mem_ != null) ? setCurCount(0) : 0;
      hashTable_ = null; //No need for a HT.
    }
    else if (curCount_ < 0) { //virgin
      //The 1st Call (curCount  < 0) and sketchInEntries  > 0. Clone the incoming sketch
      int curCnt = sketchIn.getRetainedEntries(true);
      
      if (mem_ != null) {
        curCount_ = setCurCount(curCnt);
        //checks lgArrLongs, then moves data to Target
        int requiredLgArrLongs = computeMinLgArrLongsFromCount(curCount_);
        if (requiredLgArrLongs <= maxLgArrLongs_) { //OK
          lgArrLongs_ = setLgArrLongs(requiredLgArrLongs);
          mem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_);
        }
        else { //not enough space in dstMem //TODO move to request model?
          throw new SketchesArgumentException(
              "Insufficient dstMem hash table space: " 
                  + (1 << requiredLgArrLongs) + " > " + (1 << lgArrLongs_));
        }
      } else {
        curCount_ = sketchIn.getRetainedEntries(true);
        //Allocate a HT, checks lgArrLongs, then moves data to Target
        lgArrLongs_ = computeMinLgArrLongsFromCount(curCount_);
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
    byte[] byteArrOut;
    if (mem_ != null) {
      int preBytes = CONST_PREAMBLE_LONGS << 3;
      int dataBytes = (curCount_ > 0) ? 8 << lgArrLongs_ : 0;
      int totalBytes = preBytes + dataBytes;
      byteArrOut = new byte[totalBytes];
      mem_.getByteArray(0, byteArrOut, 0, totalBytes);
    } 
    else {
      int preBytes = CONST_PREAMBLE_LONGS << 3;
      int dataBytes = (curCount_ > 0) ? 8 << lgArrLongs_ : 0;
      byteArrOut = new byte[preBytes + dataBytes];
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
    if (mem_ != null) {
      lgArrLongs_ = setLgArrLongs(lgArrLongs_);
      curCount_ = setCurCount(-1); //Universal Set is true
      thetaLong_ = setThetaLong(Long.MAX_VALUE);
      empty_ = setEmpty(false);
    } else {
      lgArrLongs_ = 0;
      hashTable_ = null;
      curCount_ = -1; //Universal Set is true
      thetaLong_ = Long.MAX_VALUE;
      empty_ = false;
    }
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
    if (mem_ != null) {
      lgArrLongs_ = setLgArrLongs(computeMinLgArrLongsFromCount(curCount_));
      curCount_ = setCurCount(matchSetCount);
      mem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_); //clear for rebuild
    } else {
      lgArrLongs_ = computeMinLgArrLongsFromCount(matchSetCount);
      curCount_ = matchSetCount;
      Arrays.fill(hashTable_, 0, 1 << lgArrLongs_, 0L); //clear for rebuild
    }
    //move matchSet to hash table
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
    if (tmpCnt != count) {
      throw new SketchesArgumentException("Count Check Exception: got: " + tmpCnt 
          + ", expected: " + count);
    }
  }

  //special handlers for Off Heap

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
  
  private final boolean setEmpty(boolean empty) {
    if (empty) {
      mem_.setBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    } 
    else {
      mem_.clearBits(FLAGS_BYTE, (byte)EMPTY_FLAG_MASK);
    }
    return empty;
  }
  
  private final int setLgArrLongs(int lgArrLongs) {
    mem_.putByte(LG_ARR_LONGS_BYTE, (byte) lgArrLongs);
    return lgArrLongs;
  }
  
  private final long setThetaLong(long thetaLong) {
    mem_.putLong(THETA_LONG, thetaLong);
    return thetaLong;
  }
  
  private final long minThetaLong(long skThetaLong) {
    if (skThetaLong < thetaLong_) {
      mem_.putLong(THETA_LONG, skThetaLong);
      return skThetaLong;
    }
    return thetaLong_;
  }
  
  private final int setCurCount(int curCount) {
    mem_.putInt(RETAINED_ENTRIES_INT, curCount);
    return curCount;
  }

}
