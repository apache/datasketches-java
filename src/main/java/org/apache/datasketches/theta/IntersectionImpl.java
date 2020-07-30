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
import static org.apache.datasketches.Util.computeSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.P_FLOAT;
import static org.apache.datasketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.THETA_LONG;
import static org.apache.datasketches.theta.PreambleUtil.clearEmpty;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.insertCurCount;
import static org.apache.datasketches.theta.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.insertLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertP;
import static org.apache.datasketches.theta.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertSerVer;
import static org.apache.datasketches.theta.PreambleUtil.insertThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.setEmpty;

import java.util.Arrays;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesReadOnlyException;
import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Intersection operation for Theta Sketches.
 *
 * <p>This implementation uses data either on-heap or off-heap in a given Memory
 * that is owned and managed by the caller.
 * The off-heap Memory, which if managed properly, will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class IntersectionImpl extends Intersection {
  protected final short seedHash_;
  protected final boolean readOnly_; //True if this sketch is to be treated as read only
  protected final WritableMemory wmem_;
  protected final int maxLgArrLongs_; //only used with WritableMemory, not serialized

  //Note: Intersection does not use lgNomLongs or k, per se.
  protected int lgArrLongs_; //current size of hash table
  protected int curCount_; //curCount of HT, if < 0 means Universal Set (US) is true
  protected long thetaLong_;
  protected boolean empty_; //A virgin intersection represents the Universal Set, so empty is FALSE!
  protected long[] hashTable_; //retained entries of the intersection, on-heap only.

  /**
   * Constructor: Sets the class finals and computes, sets and checks the seedHash.
   * @param wmem Can be either a Source(e.g. wrap) or Destination (new Direct) WritableMemory.
   * @param seed Used to validate incoming sketch arguments.
   * @param dstMemFlag The given memory is a Destination (new Direct) WritableMemory.
   * @param readOnly True if memory is to be treated as read only.
   */
  protected IntersectionImpl(final WritableMemory wmem, final long seed, final boolean dstMemFlag,
      final boolean readOnly) {
    readOnly_ = readOnly;
    if (wmem != null) {
      wmem_ = wmem;
      if (dstMemFlag) { //DstMem: compute & store seedHash, no seedhash checking
        checkMinSizeMemory(wmem);
        maxLgArrLongs_ = !readOnly ? getMaxLgArrLongs(wmem) : 0; //Only Off Heap
        seedHash_ = computeSeedHash(seed);
        wmem_.putShort(SEED_HASH_SHORT, seedHash_);
      } else { //SrcMem:gets and stores the seedHash, checks mem_seedHash against the seed
        seedHash_ = wmem_.getShort(SEED_HASH_SHORT);
        Util.checkSeedHashes(seedHash_, computeSeedHash(seed)); //check for seed hash conflict
        maxLgArrLongs_ = 0;
      }
    } else { //compute & store seedHash
      wmem_ = null;
      maxLgArrLongs_ = 0;
      seedHash_ = computeSeedHash(seed);
    }
  }

  /**
   * Factory: Construct a new Intersection target on the java heap.
   * Called by SetOperationBuilder, test.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return a new IntersectionImpl on the Java heap
   */
  static IntersectionImpl initNewHeapInstance(final long seed) {
    final boolean dstMemFlag = false;
    final boolean readOnly = false;
    final IntersectionImpl impl = new IntersectionImpl(null, seed, dstMemFlag, readOnly);
    impl.hardReset();
    return impl;
  }

  /**
   * Factory: Construct a new Intersection target direct to the given destination Memory.
   * Called by SetOperationBuilder, test.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @param dstMem destination Memory
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a new IntersectionImpl that may be off-heap
   */
  static IntersectionImpl initNewDirectInstance(final long seed, final WritableMemory dstMem) {
    //Load Preamble
    //Pre0
    dstMem.clear(0, CONST_PREAMBLE_LONGS << 3);
    insertPreLongs(dstMem, CONST_PREAMBLE_LONGS); //RF not used = 0
    insertSerVer(dstMem, SER_VER);
    insertFamilyID(dstMem, Family.INTERSECTION.getID());
    //lgNomLongs not used by Intersection
    //lgArrLongs set by hardReset
    //flags are already 0: bigEndian = readOnly = compact = ordered = empty = false;
    //seedHash loaded and checked in IntersectionImpl constructor
    //Pre1
    //CurCount set by hardReset
    insertP(dstMem, (float) 1.0); //not used by intersection
    //Pre2
    //thetaLong set by hardReset

    //Initialize
    final boolean dstMemFlag = true;
    final boolean readOnly = false;
    final IntersectionImpl impl = new IntersectionImpl(dstMem, seed, dstMemFlag, readOnly);
    impl.hardReset();
    return impl;
  }

  /**
   * Factory: Heapify an intersection target from a Memory image containing data.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return a IntersectionImpl instance on the Java heap
   */
  static IntersectionImpl heapifyInstance(final Memory srcMem, final long seed) {
    final boolean dstMemFlag = false;
    final boolean readOnly = false;
    final IntersectionImpl impl = new IntersectionImpl(null, seed, dstMemFlag, readOnly);
    memChecks(srcMem);

    //Initialize
    impl.lgArrLongs_ = extractLgArrLongs(srcMem);
    impl.curCount_ = extractCurCount(srcMem);
    impl.thetaLong_ = extractThetaLong(srcMem);
    impl.empty_ = (extractFlags(srcMem) & EMPTY_FLAG_MASK) > 0;
    if (!impl.empty_) {
      if (impl.curCount_ > 0) {
        impl.hashTable_ = new long[1 << impl.lgArrLongs_];
        srcMem.getLongArray(CONST_PREAMBLE_LONGS << 3, impl.hashTable_, 0, 1 << impl.lgArrLongs_);
      }
    }
    return impl;
  }

  /**
   * Factory: Wrap an Intersection target around the given source WritableMemory containing
   * intersection data.
   * @param srcMem The source WritableMemory image.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param readOnly True if memory is to be treated as read only
   * @return a IntersectionImpl that wraps a source WritableMemory that contains an Intersection image
   */
  static IntersectionImpl wrapInstance(
      final WritableMemory srcMem,
      final long seed,
      final boolean readOnly) {
    final boolean dstMemFlag = false;
    final IntersectionImpl impl = new IntersectionImpl(srcMem, seed, dstMemFlag, readOnly);
    memChecks(srcMem);
    impl.lgArrLongs_ = extractLgArrLongs(srcMem);
    impl.curCount_ = extractCurCount(srcMem);
    impl.thetaLong_ = extractThetaLong(srcMem);
    impl.empty_ = (extractFlags(srcMem) & EMPTY_FLAG_MASK) > 0;
    return impl;
  }

  @Override
  public CompactSketch intersect(final Sketch a, final Sketch b, final boolean dstOrdered,
     final WritableMemory dstMem) {
    if ((wmem_ != null) && readOnly_) { throw new SketchesReadOnlyException(); }
    hardReset();
    intersect(a);
    intersect(b);
    final CompactSketch csk = getResult(dstOrdered, dstMem);
    hardReset();
    return csk;
  }

  @Override
  public void intersect(final Sketch sketchIn) {
    if (sketchIn == null) {
      throw new SketchesArgumentException("Intersection argument must not be null.");
    }
    if ((wmem_ != null) && readOnly_) { throw new SketchesReadOnlyException(); }
    if (empty_ || sketchIn.isEmpty()) { //empty rule
      //Because of the def of null above and the Empty Rule (which is OR), empty_ must be true.
      //Whatever the current internal state, we make our local empty.
      resetToEmpty();
      return;
    }
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    //Set minTheta
    thetaLong_ = min(thetaLong_, sketchIn.getThetaLong()); //Theta rule
    empty_ = false;
    if (wmem_ != null) {
      insertThetaLong(wmem_, thetaLong_);
      clearEmpty(wmem_); //false
    }

    // The truth table for the following state machine. MinTheta is set above.
    // Incoming sketch is not null and not empty, but could have 0 count and Theta < 1.0
    //   Case  curCount  sketchInEntries | Actions
    //     1      <0            0        | First intersect, set curCount = 0; HT = null; minTh; exit
    //     2       0            0        | set curCount = 0; HT = null; minTh; exit
    //     3      >0            0        | set curCount = 0; HT = null; minTh; exit
    //     4                             | Not used
    //     5      <0           >0        | First intersect, clone SketchIn; exit
    //     6       0           >0        | set curCount = 0; HT = null; minTh; exit
    //     7      >0           >0        | Perform full intersect
    final int sketchInEntries = sketchIn.getRetainedEntries(true);

    //states 1,2,3,6
    if ((curCount_ == 0) || (sketchInEntries == 0)) {
      curCount_ = 0;
      if (wmem_ != null) { insertCurCount(wmem_, 0); }
      hashTable_ = null; //No need for a HT. Don't bother clearing mem if valid
    } //end of states 1,2,3,6

    // state 5
    else if ((curCount_ < 0) && (sketchInEntries > 0)) {
      curCount_ = sketchIn.getRetainedEntries(true);
      final int requiredLgArrLongs = minLgHashTableSize(curCount_, REBUILD_THRESHOLD);
      final int priorLgArrLongs = lgArrLongs_; //prior only used in error message
      lgArrLongs_ = requiredLgArrLongs;

      if (wmem_ != null) { //Off heap, check if current dstMem is large enough
        insertCurCount(wmem_, curCount_);
        insertLgArrLongs(wmem_, lgArrLongs_);
        if (requiredLgArrLongs <= maxLgArrLongs_) {
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
    } //end of state 5

    //state 7
    else if ((curCount_ > 0) && (sketchInEntries > 0)) {
      //Sets resulting hashTable, curCount and adjusts lgArrLongs
      performIntersect(sketchIn);
    } //end of state 7

    else {
      assert false : "Should not happen";
    }
  }

  @Override
  public CompactSketch getResult(final boolean dstOrdered, final WritableMemory dstMem) {
    if (curCount_ < 0) {
      throw new SketchesStateException(
          "Calling getResult() with no intervening intersections would represent the infinite set, "
          + "which is not a legal result.");
    }
    long[] compactCache;
    final boolean srcOrdered, srcCompact;
    if (curCount_ == 0) {
      compactCache = new long[0];
      srcCompact = true;
      srcOrdered = false; //hashTable, even tho empty
      return CompactOperations.componentsToCompact(
          thetaLong_, curCount_, seedHash_, empty_, srcCompact, srcOrdered, dstOrdered,
          dstMem, compactCache);
    }
    //else curCount > 0
    final long[] hashTable;
    if (wmem_ != null) {
      final int htLen = 1 << lgArrLongs_;
      hashTable = new long[htLen];
      wmem_.getLongArray(CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen);
    } else {
      hashTable = hashTable_;
    }
    compactCache = compactCachePart(hashTable, lgArrLongs_, curCount_, thetaLong_, dstOrdered);
    srcCompact = true;
    srcOrdered = dstOrdered;
    return CompactOperations.componentsToCompact(
        thetaLong_, curCount_, seedHash_, empty_, srcCompact, srcOrdered, dstOrdered,
        dstMem, compactCache);
  }

  @Override
  public void reset() {
    hardReset();
  }

  @Override
  public byte[] toByteArray() {
    final int preBytes = CONST_PREAMBLE_LONGS << 3;
    final int dataBytes = (curCount_ > 0) ? 8 << lgArrLongs_ : 0;
    final byte[] byteArrOut = new byte[preBytes + dataBytes];
    if (wmem_ != null) {
      wmem_.getByteArray(0, byteArrOut, 0, preBytes + dataBytes);
    }
    else {
      final WritableMemory memOut = WritableMemory.wrap(byteArrOut);

      //preamble
      memOut.putByte(PREAMBLE_LONGS_BYTE, (byte) CONST_PREAMBLE_LONGS); //RF not used = 0
      memOut.putByte(SER_VER_BYTE, (byte) SER_VER);
      memOut.putByte(FAMILY_BYTE, (byte) Family.INTERSECTION.getID());
      memOut.putByte(LG_NOM_LONGS_BYTE, (byte) 0); //not used
      memOut.putByte(LG_ARR_LONGS_BYTE, (byte) lgArrLongs_);
      if (empty_) { memOut.setBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); }
      else { memOut.clearBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); }
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
  public boolean hasResult() {
    return (wmem_ != null) ? wmem_.getInt(RETAINED_ENTRIES_INT) >= 0 : curCount_ >= 0;
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return (wmem_ != null) ? wmem_.isSameResource(that) : false;
  }

  //restricted

  /**
   * Gets the number of retained entries from this operation. If negative, it is interpreted
   * as the infinite <i>Universal Set</i>.
   */
  @Override
  int getRetainedEntries() {
    return curCount_;
  }

  @Override
  boolean isEmpty() {
    return empty_;
  }

  @Override
  long[] getCache() {
    if (wmem_ == null) {
      return (hashTable_ != null) ? hashTable_ : new long[0];
    }
    //Direct
    final int arrLongs = 1 << lgArrLongs_;
    final long[] outArr = new long[arrLongs];
    wmem_.getLongArray(CONST_PREAMBLE_LONGS << 3, outArr, 0, arrLongs);
    return outArr;
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

  @Override
  long getThetaLong() {
    return thetaLong_;
  }

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

  private void hardReset() {
    resetCommon();
    if (wmem_ != null) {
      insertCurCount(wmem_, -1); //Universal Set
      clearEmpty(wmem_); //false
    }
    curCount_ = -1; //Universal Set
    empty_ = false;
  }

  private void resetToEmpty() {
    resetCommon();
    if (wmem_ != null) {
      insertCurCount(wmem_, 0);
      setEmpty(wmem_); //true
    }
    curCount_ = 0;
    empty_ = true;
  }

  private void resetCommon() {
    if (wmem_ != null) {
      if (readOnly_) { throw new SketchesReadOnlyException(); }
      wmem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << MIN_LG_ARR_LONGS);
      insertLgArrLongs(wmem_, MIN_LG_ARR_LONGS);
      insertThetaLong(wmem_, Long.MAX_VALUE);
    }
    lgArrLongs_ = MIN_LG_ARR_LONGS;
    thetaLong_ = Long.MAX_VALUE;
    hashTable_ = null;
  }
}
