/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.MIN_LG_ARR_LONGS;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.clearEmpty;
import static com.yahoo.sketches.theta.PreambleUtil.insertCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.insertFlags;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertP;
import static com.yahoo.sketches.theta.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.insertThetaLong;
import static java.lang.Math.min;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

final class IntersectionImpl extends IntersectionImplR {

  private IntersectionImpl(final WritableMemory wmem, final long seed, final boolean newMem) {
    super(wmem, seed, newMem);
  }

  IntersectionImpl(final short seedHash) {
    super(seedHash);
  }

  /**
   * Construct a new Intersection target on the java heap.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return a new IntersectionImpl on the Java heap
   */
  static IntersectionImpl initNewHeapInstance(final long seed) {
    final IntersectionImpl impl = new IntersectionImpl(null, seed, false);
    impl.lgArrLongs_ = 0;
    impl.curCount_ = -1;  //Universal Set is true
    impl.thetaLong_ = Long.MAX_VALUE;
    impl.empty_ = false;  //A virgin intersection represents the Universal Set so empty is FALSE!
    impl.hashTable_ = null;
    return impl;
  }


  /**
   * Construct a new Intersection target direct to the given destination Memory.
   * Called by SetOperation.Builder.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @param dstMem destination Memory.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a new IntersectionImpl that may be off-heap
   */
  static IntersectionImpl initNewDirectInstance(final long seed, final WritableMemory dstMem) {
    final IntersectionImpl impl = new IntersectionImpl(dstMem, seed, true);

    //Load Preamble
    insertPreLongs(dstMem, CONST_PREAMBLE_LONGS); //RF not used = 0
    insertSerVer(dstMem, SER_VER);
    insertFamilyID(dstMem, Family.INTERSECTION.getID());
    //Note: Intersection does not use lgNomLongs or k, per se.
    //set lgArrLongs initially to minimum.  Don't clear cache in mem
    insertLgArrLongs(dstMem, MIN_LG_ARR_LONGS);
    insertFlags(dstMem, 0); //bigEndian = readOnly = compact = ordered = empty = false;
    //seedHash loaded and checked in private constructor
    insertCurCount(dstMem, -1);
    insertP(dstMem, (float) 1.0);
    insertThetaLong(dstMem, Long.MAX_VALUE);

    //Initialize
    impl.lgArrLongs_ = MIN_LG_ARR_LONGS;
    impl.curCount_ = -1; //set in mem below
    impl.thetaLong_ = Long.MAX_VALUE;
    impl.empty_ = false;
    impl.maxLgArrLongs_ = checkMaxLgArrLongs(dstMem); //Only Off Heap

    return impl;
  }

  /**
   * Heapify an intersection target from a Memory image containing data.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return a IntersectionImplR instance on the Java heap
   */
  static IntersectionImplR heapifyInstance(final Memory srcMem, final long seed) {
    final IntersectionImpl impl = new IntersectionImpl(null, seed, false);

    //Get Preamble
    //Note: Intersection does not use lgNomLongs (or k), per se.
    //seedHash loaded and checked in private constructor
    final int preLongsMem = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int serVer = srcMem.getByte(SER_VER_BYTE) & 0XFF;
    final int famID = srcMem.getByte(FAMILY_BYTE) & 0XFF;
    final int lgArrLongs = srcMem.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
    final int flags = srcMem.getByte(FLAGS_BYTE) & 0XFF;
    final int curCount = srcMem.getInt(RETAINED_ENTRIES_INT);
    final long thetaLong = srcMem.getLong(THETA_LONG);
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;

    //Checks
    if (preLongsMem != CONST_PREAMBLE_LONGS) {
      throw new SketchesArgumentException(
          "Memory PreambleLongs must equal " + CONST_PREAMBLE_LONGS + ": " + preLongsMem);
    }

    if (serVer != SER_VER) {
      throw new SketchesArgumentException("Serialization Version must equal " + SER_VER);
    }

    Family.INTERSECTION.checkFamilyID(famID);

    if (empty) {
      if (curCount != 0) {
        throw new SketchesArgumentException(
            "srcMem empty state inconsistent with curCount: " + empty + "," + curCount);
      }
      //empty = true AND curCount_ = 0: OK
    }

    //Initialize
    impl.lgArrLongs_ = lgArrLongs;
    impl.curCount_ = curCount;
    impl.thetaLong_ = thetaLong;
    impl.empty_ = empty;

    if (!empty) {
      if (curCount > 0) { //can't be virgin, empty, or curCount == 0
        impl.hashTable_ = new long[1 << lgArrLongs];
        srcMem.getLongArray(CONST_PREAMBLE_LONGS << 3, impl.hashTable_, 0, 1 << lgArrLongs);
      }
    }
    return impl;
  }

  /**
   * Wrap an Intersection target around the given source Memory containing intersection data.
   * @param srcMem The source Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return a IntersectionImpl that wraps a source Memory that contains an Intersection image
   */
  static IntersectionImpl wrapInstance(final WritableMemory srcMem, final long seed) {
    final IntersectionImpl impl = new IntersectionImpl(srcMem, seed, false);
    return (IntersectionImpl) internalWrapInstance(srcMem, impl);
  }

  @Override
  public void update(final Sketch sketchIn) {
    if (sketchIn != null) {
      Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    }
    //Null / Empty cases.
    //Note: null == empty := Th = 1.0, count = 0, empty = true
    if ((sketchIn == null) || sketchIn.isEmpty() || empty_) { //empty rule
      //Because of the def of null above and the Empty Rule (which is OR), empty_ must be true.
      //Whatever the current internal state, we make it empty.
      empty_ = true;
      thetaLong_ = Long.MAX_VALUE;
      curCount_ = 0;
      lgArrLongs_ = 0;
      maxLgArrLongs_ = 0;
      hashTable_ = null;
      if (mem_ != null) {
        PreambleUtil.setEmpty(mem_); //true
        insertThetaLong(mem_, thetaLong_);
        insertCurCount(mem_, 0);
        insertLgArrLongs(mem_, lgArrLongs_);
      }
      return;
    }

    thetaLong_ = min(thetaLong_, sketchIn.getThetaLong()); //Theta rule
    empty_ = false;
    if (mem_ != null) {
      insertThetaLong(mem_, thetaLong_);
      PreambleUtil.clearEmpty(mem_); //false
    }

    final int sketchInEntries = sketchIn.getRetainedEntries(true);

    // The truth table for the following state machine
    //   Case  curCount  sketchInEntries | Actions
    //     1      <0            0        | First update, curCount = 0; HT = null; exit
    //     2       0            0        | CurCount = 0; HT = null; exit
    //     3      >0            0        | CurCount = 0; HT = null; exit
    //     5      <0           >0        | First update, clone SketchIn; exit
    //     6       0           >0        | CurCount = 0; HT = null; exit
    //     7      >0           >0        | Perform full intersect
    final int sw = ((curCount_ < 0) ? 1 : (curCount_ == 0) ? 2 : 3)
        | (((sketchInEntries > 0) ? 1 : 0) << 2) ;
    switch (sw) {
      case 1:
      case 2:
      case 3:
      case 6: { //(curCount_ == 0) || (sketchInEntries == 0)
        //All future intersections result in zero data, but theta can still be reduced.
        curCount_ = 0;
        if (mem_ != null) { insertCurCount(mem_, 0); }
        hashTable_ = null; //No need for a HT. Don't bother clearing mem if valid
        break;
      }
      case 5: { // curCount_ < 0; This is the 1st update, clone the incoming sketch
        curCount_ = sketchIn.getRetainedEntries(true);
        final int requiredLgArrLongs = computeMinLgArrLongsFromCount(curCount_);
        final int priorLgArrLongs = lgArrLongs_; //prior only used in error message
        lgArrLongs_ = requiredLgArrLongs;

        if (mem_ != null) { //Off heap, check if current dstMem is large enough
          insertCurCount(mem_, curCount_);
          insertLgArrLongs(mem_, lgArrLongs_);
          if (requiredLgArrLongs <= maxLgArrLongs_) { //OK
            mem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_); //clear only what required
          }
          else { //not enough space in dstMem
            throw new SketchesArgumentException(
                "Insufficient dstMem hash table space: "
                    + (1 << requiredLgArrLongs) + " > " + (1 << priorLgArrLongs));
          }
        }
        else { //On the heap, allocate a HT
          hashTable_ = new long[1 << lgArrLongs_];
        }
        moveDataToTgt(sketchIn.getCache(), curCount_);
        break;
      }
      case 7: { // (curCount > 0) && (sketchInEntries > 0); Perform full intersect
        //Sets resulting hashTable, curCount and adjusts lgArrLongs
        performIntersect(sketchIn);
        break;
      }
      //default: not possible
    }
  }

  @Override
  public CompactSketch intersect(final Sketch a, final Sketch b, final boolean dstOrdered,
     final WritableMemory dstMem) {
    reset();
    update(a);
    update(b);
    return getResult(dstOrdered, dstMem);
  }

  @Override
  public void reset() {
    curCount_ = -1;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = false;
    hashTable_ = null;
    if (mem_ != null) {
      insertLgArrLongs(mem_, lgArrLongs_); //make sure
      insertCurCount(mem_, -1);
      insertThetaLong(mem_, Long.MAX_VALUE);
      clearEmpty(mem_);
    }
  }

  //restricted

  private void performIntersect(final Sketch sketchIn) {
    // curCount and input data are nonzero, match against HT
    assert ((curCount_ > 0) && (!empty_));
    final long[] cacheIn = sketchIn.getCache();
    final int arrLongsIn = cacheIn.length;
    final long[] hashTable;
    if (mem_ != null) {
      final int htLen = 1 << lgArrLongs_;
      hashTable = new long[htLen];
      mem_.getLongArray(CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen);
    } else {
      hashTable = hashTable_;
    }
    //allocate space for matching
    final long[] matchSet = new long[ min(curCount_, sketchIn.getRetainedEntries(true)) ];

    int matchSetCount = 0;
    if (sketchIn.isOrdered()) {
      //ordered compact, which enables early stop
      for (int i = 0; i < arrLongsIn; i++ ) {
        final long hashIn = cacheIn[i];
        //if (hashIn <= 0L) continue;  //<= 0 should not happen
        if (hashIn >= thetaLong_) {
          break; //early stop assumes that hashes in input sketch are ordered!
        }
        final int foundIdx = HashOperations.hashSearch(hashTable, lgArrLongs_, hashIn);
        if (foundIdx == -1) { continue; }
        matchSet[matchSetCount++] = hashIn;
      }
    }
    else {
      //either unordered compact or hash table
      for (int i = 0; i < arrLongsIn; i++ ) {
        final long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= thetaLong_)) { continue; }
        final int foundIdx = HashOperations.hashSearch(hashTable, lgArrLongs_, hashIn);
        if (foundIdx == -1) { continue; }
        matchSet[matchSetCount++] = hashIn;
      }
    }
    //reduce effective array size to minimum
    curCount_ = matchSetCount;
    lgArrLongs_ = computeMinLgArrLongsFromCount(matchSetCount);
    if (mem_ != null) {
      insertCurCount(mem_, matchSetCount);
      insertLgArrLongs(mem_, lgArrLongs_);
      mem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_); //clear for rebuild
    } else {
      Arrays.fill(hashTable_, 0, 1 << lgArrLongs_, 0L); //clear for rebuild
    }

    if (curCount_ > 0) {
      moveDataToTgt(matchSet, matchSetCount); //move matchSet to target
    } else {
      if (thetaLong_ == Long.MAX_VALUE) {
        empty_ = true;
      }
    }
  }

  private void moveDataToTgt(final long[] arr, final int count) {
    final int arrLongsIn = arr.length;
    int tmpCnt = 0;
    if (mem_ != null) { //Off Heap puts directly into mem
      final int preBytes = CONST_PREAMBLE_LONGS << 3;
      final int lgArrLongs = lgArrLongs_;
      final long thetaLong = thetaLong_;
      for (int i = 0; i < arrLongsIn; i++ ) {
        final long hashIn = arr[i];
        if (HashOperations.continueCondition(thetaLong, hashIn)) { continue; }
        HashOperations.fastHashInsertOnly(mem_, lgArrLongs, hashIn, preBytes);
        tmpCnt++;
      }
    } else { //On Heap. Assumes HT exists and is large enough
      for (int i = 0; i < arrLongsIn; i++ ) {
        final long hashIn = arr[i];
        if (HashOperations.continueCondition(thetaLong_, hashIn)) { continue; }
        HashOperations.hashInsertOnly(hashTable_, lgArrLongs_, hashIn);
        tmpCnt++;
      }
    }
    assert (tmpCnt == count) : "Intersection Count Check: got: " + tmpCnt + ", expected: " + count;
  }

}
