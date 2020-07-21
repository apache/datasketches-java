/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.theta;

import static java.lang.Math.min;
import static org.apache.datasketches.HashOperations.continueCondition;
import static org.apache.datasketches.HashOperations.hashInsertOnly;
import static org.apache.datasketches.HashOperations.hashInsertOnlyMemory;
import static org.apache.datasketches.HashOperations.hashSearch;
import static org.apache.datasketches.HashOperations.minLgHashTableSize;
import static org.apache.datasketches.Util.MIN_LG_ARR_LONGS;
import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.THETA_LONG;
import static org.apache.datasketches.theta.PreambleUtil.clearEmpty;
import static org.apache.datasketches.theta.PreambleUtil.insertCurCount;
import static org.apache.datasketches.theta.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.insertFlags;
import static org.apache.datasketches.theta.PreambleUtil.insertLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertLgNomLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertP;
import static org.apache.datasketches.theta.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertSerVer;
import static org.apache.datasketches.theta.PreambleUtil.insertThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.setEmpty;

import java.util.Arrays;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

final class IntersectionImpl extends IntersectionImplR {

  private IntersectionImpl(final WritableMemory wmem, final long seed, final boolean newMem) {
    super(wmem, seed, newMem);
  }

  /**
   * Construct a new Intersection target on the java heap.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return a new IntersectionImpl on the Java heap
   */
  static IntersectionImpl initNewHeapInstance(final long seed) {
    //compute & store seedHash,
    final IntersectionImpl impl = new IntersectionImpl(null, seed, false);
    impl.lgArrLongs_ = 0;
    impl.curCount_ = -1;  //Universal Set is true
    impl.thetaLong_ = Long.MAX_VALUE;
    impl.empty_ = false;  //A virgin intersection represents the Universal Set so empty is FALSE!
    impl.hashTable_ = null; //retained entries of the intersection as a hash table, on-heap only.
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
    //DstMem: compute & store seedHash, true means no seedhash checking
    final IntersectionImpl impl = new IntersectionImpl(dstMem, seed, true); //compute & store seedHash

    //Load Preamble

    insertPreLongs(dstMem, CONST_PREAMBLE_LONGS); //RF not used = 0
    insertSerVer(dstMem, SER_VER);
    insertFamilyID(dstMem, Family.INTERSECTION.getID());
    insertLgNomLongs(dstMem, 0); //Note: Intersection does not use lgNomLongs or k, per se.
    //set lgArrLongs initially to minimum.  Don't clear cache in mem
    insertLgArrLongs(dstMem, MIN_LG_ARR_LONGS);
    insertFlags(dstMem, 0); //bigEndian = readOnly = compact = ordered = empty = false;
    //seedHash loaded and checked in private constructor
    insertCurCount(dstMem, -1);
    insertP(dstMem, (float) 1.0);
    //pre2
    insertThetaLong(dstMem, Long.MAX_VALUE);

    //internal initialize
    impl.lgArrLongs_ = MIN_LG_ARR_LONGS;
    impl.curCount_ = -1; //set in mem below
    impl.thetaLong_ = Long.MAX_VALUE;
    impl.empty_ = false;
    impl.maxLgArrLongs_ = getMaxLgArrLongs(dstMem); //Only Off Heap

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
    //compute & store seedHash,
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
   * Wrap an Intersection target around the given source WritableMemory containing intersection data.
   * @param srcMem The source WritableMemory image.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return a IntersectionImpl that wraps a source WritableMemory that contains an Intersection image
   */
  static IntersectionImpl wrapInstance(final WritableMemory srcMem, final long seed) {
    //SrcMem:gets and stores the seedHash, checks mem_seedHash against the seed
    final IntersectionImpl impl = new IntersectionImpl(srcMem, seed, false);
    return (IntersectionImpl) internalWrapInstance(srcMem, impl);
  }

  @Deprecated
  @Override
  public void update(final Sketch sketchIn) {
    intersect(sketchIn);
  }

  @Override
  public void intersect(final Sketch sketchIn) {
    if (sketchIn == null) {
      throw new SketchesArgumentException("Intersection argument must not be null.");
    }
    if (empty_ || sketchIn.isEmpty()) { //empty rule
      //Because of the def of null above and the Empty Rule (which is OR), empty_ must be true.
      //Whatever the current internal state, we make it empty.
      lgArrLongs_ = 0;
      curCount_ = 0;
      thetaLong_ = Long.MAX_VALUE;
      empty_ = true;
      hashTable_ = null;
      if (wmem_ != null) {
        setEmpty(wmem_); //true
        insertThetaLong(wmem_, thetaLong_);
        insertCurCount(wmem_, 0);
        insertLgArrLongs(wmem_, lgArrLongs_);
      }
      return;
    }
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    thetaLong_ = min(thetaLong_, sketchIn.getThetaLong()); //Theta rule
    empty_ = false;
    if (wmem_ != null) {
      insertThetaLong(wmem_, thetaLong_);
      clearEmpty(wmem_); //false
    }

    final int sketchInEntries = sketchIn.getRetainedEntries(true);

    // The truth table for the following state machine
    //   Case  curCount  sketchInEntries | Actions
    //     1      <0            0        | First intersect, curCount < 0; HT = null; exit
    //     2       0            0        | CurCount = 0; HT = null; exit
    //     3      >0            0        | CurCount = 0; HT = null; exit
    //     4                             | Not used
    //     5      <0           >0        | First intersect, clone SketchIn; exit
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
        if (wmem_ != null) { insertCurCount(wmem_, 0); }
        hashTable_ = null; //No need for a HT. Don't bother clearing mem if valid
        break;
      }
      case 5: { // curCount_ < 0; This is the 1st intersect, clone the incoming sketch
        curCount_ = sketchIn.getRetainedEntries(true);
        final int requiredLgArrLongs = minLgHashTableSize(curCount_, REBUILD_THRESHOLD);
        final int priorLgArrLongs = lgArrLongs_; //prior only used in error message
        lgArrLongs_ = requiredLgArrLongs;

        if (wmem_ != null) { //Off heap, check if current dstMem is large enough
          insertCurCount(wmem_, curCount_);
          insertLgArrLongs(wmem_, lgArrLongs_);
          if (requiredLgArrLongs <= maxLgArrLongs_) { //OK
            wmem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_); //clear only what required
          }
          else { //not enough space in dstMem
            final int requiredBytes = (8 << requiredLgArrLongs) + 24;
            final int givenBytes = (8 << priorLgArrLongs) + 24;
            throw new SketchesArgumentException(
                "Insufficient internal Memory space: " + requiredBytes + " > " + givenBytes);
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
      //default: assert false : "Should not happen";
    }
  }

  @Override
  public CompactSketch intersect(final Sketch a, final Sketch b, final boolean dstOrdered,
     final WritableMemory dstMem) {
    reset();
    intersect(a);
    intersect(b);
    return getResult(dstOrdered, dstMem);
  }

  @Override
  public void reset() {
    lgArrLongs_ = 0;
    curCount_ = -1;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = false;
    hashTable_ = null;
    if (wmem_ != null) {
      insertLgArrLongs(wmem_, 0);
      insertCurCount(wmem_, -1);
      insertThetaLong(wmem_, Long.MAX_VALUE);
      clearEmpty(wmem_);
    }
  }

  private void hardReset() { //Universal Set
    resetToEmpty();
    curCount_ = -1;
    empty_ = false;
    if (wmem_ != null) {
      clearEmpty(wmem_);
      insertCurCount(wmem_, -1);
    }
  }

  private void resetToEmpty() { //empty state
    lgArrLongs_ = 0;
    curCount_ = 0;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = true;
    hashTable_ = null;
    if (wmem_ != null) {
      insertLgArrLongs(wmem_, 0);
      insertCurCount(wmem_, 0);
      insertThetaLong(wmem_, Long.MAX_VALUE);
      setEmpty(wmem_); //true
    }
  }

  //restricted

  private void performIntersect(final Sketch sketchIn) {
    // curCount and input data are nonzero, match against HT
    assert ((curCount_ > 0) && (!empty_));
    final long[] cacheIn = sketchIn.getCache();
    final int arrLongsIn = cacheIn.length;
    final long[] hashTable;
    if (wmem_ != null) {
      final int htLen = 1 << lgArrLongs_;
      hashTable = new long[htLen];
      wmem_.getLongArray(CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen);
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
        final int foundIdx = hashSearch(hashTable, lgArrLongs_, hashIn);
        if (foundIdx == -1) { continue; }
        matchSet[matchSetCount++] = hashIn;
      }
    }
    else {
      //either unordered compact or hash table
      for (int i = 0; i < arrLongsIn; i++ ) {
        final long hashIn = cacheIn[i];
        if ((hashIn <= 0L) || (hashIn >= thetaLong_)) { continue; }
        final int foundIdx = hashSearch(hashTable, lgArrLongs_, hashIn);
        if (foundIdx == -1) { continue; }
        matchSet[matchSetCount++] = hashIn;
      }
    }
    //reduce effective array size to minimum
    curCount_ = matchSetCount;
    lgArrLongs_ = minLgHashTableSize(matchSetCount, REBUILD_THRESHOLD);
    if (wmem_ != null) {
      insertCurCount(wmem_, matchSetCount);
      insertLgArrLongs(wmem_, lgArrLongs_);
      wmem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_); //clear for rebuild
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
    if (wmem_ != null) { //Off Heap puts directly into mem
      final int preBytes = CONST_PREAMBLE_LONGS << 3;
      final int lgArrLongs = lgArrLongs_;
      final long thetaLong = thetaLong_;
      for (int i = 0; i < arrLongsIn; i++ ) {
        final long hashIn = arr[i];
        if (continueCondition(thetaLong, hashIn)) { continue; }
        hashInsertOnlyMemory(wmem_, lgArrLongs, hashIn, preBytes);
        tmpCnt++;
      }
    } else { //On Heap. Assumes HT exists and is large enough
      for (int i = 0; i < arrLongsIn; i++ ) {
        final long hashIn = arr[i];
        if (continueCondition(thetaLong_, hashIn)) { continue; }
        hashInsertOnly(hashTable_, lgArrLongs_, hashIn);
        tmpCnt++;
      }
    }
    assert (tmpCnt == count) : "Intersection Count Check: got: " + tmpCnt + ", expected: " + count;
  }

}
