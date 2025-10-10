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
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.common.Util.clearBits;
import static org.apache.datasketches.common.Util.floorPowerOf2;
import static org.apache.datasketches.common.Util.setBits;
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
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractSerVer;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.insertCurCount;
import static org.apache.datasketches.theta.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.insertLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertP;
import static org.apache.datasketches.theta.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertSerVer;
import static org.apache.datasketches.theta.PreambleUtil.insertThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.setEmpty;
import static org.apache.datasketches.thetacommon.HashOperations.continueCondition;
import static org.apache.datasketches.thetacommon.HashOperations.hashInsertOnly;
import static org.apache.datasketches.thetacommon.HashOperations.hashInsertOnlyMemorySegment;
import static org.apache.datasketches.thetacommon.HashOperations.hashSearch;
import static org.apache.datasketches.thetacommon.HashOperations.minLgHashTableSize;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * The intersection operation for ThetaSketches.
 *
 * <p>This implementation uses data either on-heap or off-heap in a given MemorySegment
 * that is owned and managed by the caller.
 * The off-heap MemorySegment, which if managed properly, will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class ThetaIntersectionImpl extends ThetaIntersection {
  private final short seedHash_;
  private final boolean readOnly_; //True if this sketch is to be treated as read only
  private final MemorySegment wseg_;
  private final int maxLgArrLongs_; //only used with MemorySegment, not serialized

  //Note: intersection operation does not use lgNomLongs or k, per se.
  private int lgArrLongs_; //current size of hash table
  private int curCount_; //curCount of HT, if < 0 means Universal Set (US) is true
  private long thetaLong_;
  private boolean empty_; //A virgin intersection represents the Universal Set, so empty is FALSE!
  private long[] hashTable_; //retained entries of the intersection, on-heap only.

  /**
   * Constructor: Sets the class finals and computes, sets and checks the seedHash.
   * @param wseg Can be either a Source(e.g. wrap) or Destination (new offHeap) MemorySegment.
   * @param seed Used to validate incoming sketch arguments.
   * @param dstSegFlag The given MemorySegment is a Destination (new offHeap) MemorySegment.
   * @param readOnly True if MemorySegment is to be treated as read only.
   */
  private ThetaIntersectionImpl(final MemorySegment wseg, final long seed, final boolean dstSegFlag,
      final boolean readOnly) {
    readOnly_ = readOnly;
    if (wseg != null) {
      wseg_ = wseg;
      if (dstSegFlag) { //DstSeg: compute & store seedHash, no seedHash checking
        ThetaIntersectionImpl.checkMinSizeMemorySegment(wseg);
        maxLgArrLongs_ = !readOnly ? ThetaIntersectionImpl.getMaxLgArrLongs(wseg) : 0; //Only Off Heap
        seedHash_ = Util.computeSeedHash(seed);
        wseg_.set(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT, seedHash_);
      } else { //SrcSeg:gets and stores the seedHash, checks seg_seedHash against the seed
        seedHash_ = wseg_.get(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT);
        Util.checkSeedHashes(seedHash_, Util.computeSeedHash(seed)); //check for seed hash conflict
        maxLgArrLongs_ = 0;
      }
    } else { //compute & store seedHash
      wseg_ = null;
      maxLgArrLongs_ = 0;
      seedHash_ = Util.computeSeedHash(seed);
    }
  }

  /**
   * Factory: Construct a new ThetaIntersection target on the java heap.
   * Called by ThetaSetOperationBuilder, test.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return a new IntersectionImpl on the Java heap
   */
  static ThetaIntersectionImpl initNewHeapInstance(final long seed) {
    final boolean dstSegFlag = false;
    final boolean readOnly = false;
    final ThetaIntersectionImpl impl = new ThetaIntersectionImpl(null, seed, dstSegFlag, readOnly);
    impl.hardReset();
    return impl;
  }

  /**
   * Factory: Construct a new ThetaIntersection target direct to the given destination MemorySegment.
   * Called by ThetaSetOperationBuilder, test.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @param dstSeg destination MemorySegment
   * @return a new IntersectionImpl that may be off-heap
   */
  static ThetaIntersectionImpl initNewDirectInstance(final long seed, final MemorySegment dstSeg) {
    //Load Preamble
    //Pre0
    dstSeg.asSlice(0, CONST_PREAMBLE_LONGS << 3).fill((byte)0);
    insertPreLongs(dstSeg, CONST_PREAMBLE_LONGS); //RF not used = 0
    insertSerVer(dstSeg, SER_VER);
    insertFamilyID(dstSeg, Family.INTERSECTION.getID());
    //lgNomLongs not used by this intersection operation
    //lgArrLongs set by hardReset
    //flags are already 0: reserved = readOnly = compact = ordered = empty = false;
    //seedHash loaded and checked in IntersectionImpl constructor
    //Pre1
    //CurCount set by hardReset
    insertP(dstSeg, (float) 1.0); //not used by intersection
    //Pre2
    //thetaLong set by hardReset

    //Initialize
    final boolean dstSegFlag = true;
    final boolean readOnly = false;
    final ThetaIntersectionImpl impl = new ThetaIntersectionImpl(dstSeg, seed, dstSegFlag, readOnly);
    impl.hardReset();
    return impl;
  }

  /**
   * Factory: Heapify an intersection target from a MemorySegment image containing data.
   * @param srcSeg The source MemorySegment object.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return a IntersectionImpl instance on the Java heap
   */
  static ThetaIntersectionImpl heapifyInstance(final MemorySegment srcSeg, final long seed) {
    final boolean dstSegFlag = false;
    final boolean readOnly = false;
    ThetaIntersectionImpl.segChecks(srcSeg);
    final ThetaIntersectionImpl impl = new ThetaIntersectionImpl(null, seed, dstSegFlag, readOnly);

    //Initialize
    impl.lgArrLongs_ = extractLgArrLongs(srcSeg);
    impl.curCount_ = extractCurCount(srcSeg);
    impl.thetaLong_ = extractThetaLong(srcSeg);
    impl.empty_ = (extractFlags(srcSeg) & EMPTY_FLAG_MASK) > 0;
    if (!impl.empty_ && (impl.curCount_ > 0)) {
        impl.hashTable_ = new long[1 << impl.lgArrLongs_];
        MemorySegment.copy(srcSeg, JAVA_LONG_UNALIGNED, CONST_PREAMBLE_LONGS << 3, impl.hashTable_, 0, 1 << impl.lgArrLongs_);
      }
    return impl;
  }

  /**
   * Factory: Wrap a ThetaIntersection target around the given source MemorySegment containing intersection data.
   * If the given source MemorySegment is read-only, the returned object will also be read-only.
   * @param srcSeg The source MemorySegment image.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param readOnly True if MemorySegment is to be treated as read only
   * @return a IntersectionImpl that wraps a source MemorySegment that contains a ThetaIntersection image
   */
  static ThetaIntersectionImpl wrapInstance(
      final MemorySegment srcSeg,
      final long seed,
      final boolean readOnly) {
    final boolean dstSegFlag = false;
    ThetaIntersectionImpl.segChecks(srcSeg);
    final ThetaIntersectionImpl impl = new ThetaIntersectionImpl(srcSeg, seed, dstSegFlag, readOnly);
    impl.lgArrLongs_ = extractLgArrLongs(srcSeg);
    impl.curCount_ = extractCurCount(srcSeg);
    impl.thetaLong_ = extractThetaLong(srcSeg);
    impl.empty_ = (extractFlags(srcSeg) & EMPTY_FLAG_MASK) > 0;
    return impl;
  }

  @Override
  public CompactThetaSketch intersect(final ThetaSketch a, final ThetaSketch b, final boolean dstOrdered, final MemorySegment dstSeg) {
    if ((wseg_ != null) && readOnly_) { throw new SketchesReadOnlyException(); }
    hardReset();
    intersect(a);
    intersect(b);
    final CompactThetaSketch csk = getResult(dstOrdered, dstSeg);
    hardReset();
    return csk;
  }

  @Override
  public void intersect(final ThetaSketch sketchIn) {
    if (sketchIn == null) {
      throw new SketchesArgumentException("The input argument must not be null.");
    }
    if ((wseg_ != null) && readOnly_) { throw new SketchesReadOnlyException(); }
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
    if (wseg_ != null) {
      insertThetaLong(wseg_, thetaLong_);
      clearEmpty(wseg_); //false
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
      if (wseg_ != null) { insertCurCount(wseg_, 0); }
      hashTable_ = null; //No need for a HT. Don't bother clearing seg if valid
    } //end of states 1,2,3,6

    // state 5
    else if ((curCount_ < 0) && (sketchInEntries > 0)) {
      curCount_ = sketchIn.getRetainedEntries(true);
      final int requiredLgArrLongs = minLgHashTableSize(curCount_, ThetaUtil.REBUILD_THRESHOLD);
      final int priorLgArrLongs = lgArrLongs_; //prior only used in error message
      lgArrLongs_ = requiredLgArrLongs;

      if (wseg_ != null) { //Off heap, check if current dstSeg is large enough
        insertCurCount(wseg_, curCount_);
        insertLgArrLongs(wseg_, lgArrLongs_);
        if (requiredLgArrLongs <= maxLgArrLongs_) {
          wseg_.asSlice(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_).fill((byte)0);
        }
        else { //not enough space in dstSeg
          final int requiredBytes = (8 << requiredLgArrLongs) + 24;
          final int givenBytes = (8 << priorLgArrLongs) + 24;
          throw new SketchesArgumentException(
              "Insufficient internal MemorySegment space: " + requiredBytes + " > " + givenBytes);
        }
      }
      else { //On the heap, allocate a HT
        hashTable_ = new long[1 << lgArrLongs_];
      }
      moveDataToTgt(sketchIn);
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
  MemorySegment getMemorySegment() { return wseg_; }

  @Override
  public CompactThetaSketch getResult(final boolean dstOrdered, final MemorySegment dstSeg) {
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
      srcOrdered = false; //hashTable, even though empty
      return CompactOperations.componentsToCompact(
          thetaLong_, curCount_, seedHash_, empty_, srcCompact, srcOrdered, dstOrdered,
          dstSeg, compactCache);
    }
    //else curCount > 0
    final long[] hashTable;
    if (wseg_ != null) {
      final int htLen = 1 << lgArrLongs_;
      hashTable = new long[htLen];
      MemorySegment.copy(wseg_, JAVA_LONG_UNALIGNED, CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen);
    } else {
      hashTable = hashTable_;
    }
    compactCache = ThetaIntersectionImpl.compactCachePart(hashTable, lgArrLongs_, curCount_, thetaLong_, dstOrdered);
    srcCompact = true;
    srcOrdered = dstOrdered;
    return CompactOperations.componentsToCompact(
        thetaLong_, curCount_, seedHash_, empty_, srcCompact, srcOrdered, dstOrdered,
        dstSeg, compactCache);
  }

  @Override
  public boolean hasMemorySegment() {
    return (wseg_ != null) && wseg_.scope().isAlive();
  }

  @Override
  public boolean hasResult() {
    return hasMemorySegment() ? wseg_.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT) >= 0 : curCount_ >= 0;
  }

  @Override
  public boolean isOffHeap() {
    return hasMemorySegment() && wseg_.isNative();
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return hasMemorySegment() && MemorySegmentStatus.isSameResource(wseg_, that);
  }

  @Override
  public void reset() {
    hardReset();
  }

  @Override
  public byte[] toByteArray() {
    final int preBytes = CONST_PREAMBLE_LONGS << 3;
    final int dataBytes = curCount_ > 0 ? 8 << lgArrLongs_ : 0;
    final byte[] byteArrOut = new byte[preBytes + dataBytes];
    if (wseg_ != null) {
      MemorySegment.copy(wseg_, JAVA_BYTE, 0, byteArrOut, 0, preBytes + dataBytes);
    }
    else {
      final MemorySegment segOut = MemorySegment.ofArray(byteArrOut);

      //preamble
      segOut.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) CONST_PREAMBLE_LONGS); //RF not used = 0
      segOut.set(JAVA_BYTE, SER_VER_BYTE, (byte) SER_VER);
      segOut.set(JAVA_BYTE, FAMILY_BYTE, (byte) Family.INTERSECTION.getID());
      segOut.set(JAVA_BYTE, LG_NOM_LONGS_BYTE, (byte) 0); //not used
      segOut.set(JAVA_BYTE, LG_ARR_LONGS_BYTE, (byte) lgArrLongs_);
      if (empty_) { setBits(segOut, FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); }
      else { clearBits(segOut, FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); }
      segOut.set(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT, seedHash_);
      segOut.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, curCount_);
      segOut.set(JAVA_FLOAT_UNALIGNED, P_FLOAT, (float) 1.0);
      segOut.set(JAVA_LONG_UNALIGNED, THETA_LONG, thetaLong_);

      //data
      if (curCount_ > 0) {
        MemorySegment.copy(hashTable_, 0, segOut, JAVA_LONG_UNALIGNED, preBytes, 1 << lgArrLongs_);
      }
    }
    return byteArrOut;
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
    if (wseg_ == null) {
      return hashTable_ != null ? hashTable_ : new long[0];
    }
    //offHeap
    final int arrLongs = 1 << lgArrLongs_;
    final long[] outArr = new long[arrLongs];
    MemorySegment.copy(wseg_, JAVA_LONG_UNALIGNED, CONST_PREAMBLE_LONGS << 3, outArr, 0, arrLongs);
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

  private void performIntersect(final ThetaSketch sketchIn) {
    // curCount and input data are nonzero, match against HT
    assert (curCount_ > 0) && !empty_;
    final long[] hashTable;
    if (wseg_ != null) {
      final int htLen = 1 << lgArrLongs_;
      hashTable = new long[htLen];
      MemorySegment.copy(wseg_, JAVA_LONG_UNALIGNED, CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen);
    } else {
      hashTable = hashTable_;
    }
    //allocate space for matching
    final long[] matchSet = new long[ min(curCount_, sketchIn.getRetainedEntries(true)) ];

    int matchSetCount = 0;
    final boolean isOrdered = sketchIn.isOrdered();
    final HashIterator it = sketchIn.iterator();
    while (it.next()) {
      final long hashIn = it.get();
      if (hashIn < thetaLong_) {
        final int foundIdx = hashSearch(hashTable, lgArrLongs_, hashIn);
        if (foundIdx != -1) {
          matchSet[matchSetCount++] = hashIn;
        }
      } else if (isOrdered) { break; } // early stop
    }
    //reduce effective array size to minimum
    curCount_ = matchSetCount;
    lgArrLongs_ = minLgHashTableSize(matchSetCount, ThetaUtil.REBUILD_THRESHOLD);
    if (wseg_ != null) {
      insertCurCount(wseg_, matchSetCount);
      insertLgArrLongs(wseg_, lgArrLongs_);
      wseg_.asSlice(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_).fill((byte)0); //clear for rebuild
    } else {
      Arrays.fill(hashTable_, 0, 1 << lgArrLongs_, 0L); //clear for rebuild
    }

    if (curCount_ > 0) {
      moveDataToTgt(matchSet, matchSetCount); //move matchSet to target
    } else if (thetaLong_ == Long.MAX_VALUE) {
        empty_ = true;
      }
  }

  private void moveDataToTgt(final long[] arr, final int count) {
    final int arrLongsIn = arr.length;
    int tmpCnt = 0;
    if (wseg_ != null) { //Off Heap puts directly into mem
      final int preBytes = CONST_PREAMBLE_LONGS << 3;
      final int lgArrLongs = lgArrLongs_;
      final long thetaLong = thetaLong_;
      for (int i = 0; i < arrLongsIn; i++ ) {
        final long hashIn = arr[i];
        if (continueCondition(thetaLong, hashIn)) { continue; }
        hashInsertOnlyMemorySegment(wseg_, lgArrLongs, hashIn, preBytes);
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
    assert tmpCnt == count : "ThetaIntersection Count Check: got: " + tmpCnt + ", expected: " + count;
  }

  private void moveDataToTgt(final ThetaSketch sketch) {
    final int count = sketch.getRetainedEntries();
    int tmpCnt = 0;
    if (wseg_ != null) { //Off Heap puts directly into mem
      final int preBytes = CONST_PREAMBLE_LONGS << 3;
      final int lgArrLongs = lgArrLongs_;
      final long thetaLong = thetaLong_;
      final HashIterator it = sketch.iterator();
      while (it.next()) {
        final long hash = it.get();
        if (continueCondition(thetaLong, hash)) { continue; }
        hashInsertOnlyMemorySegment(wseg_, lgArrLongs, hash, preBytes);
        tmpCnt++;
      }
    } else { //On Heap. Assumes HT exists and is large enough
      final HashIterator it = sketch.iterator();
      while (it.next()) {
        final long hash = it.get();
        if (continueCondition(thetaLong_, hash)) { continue; }
        hashInsertOnly(hashTable_, lgArrLongs_, hash);
        tmpCnt++;
      }
    }
    assert tmpCnt == count : "ThetaIntersection Count Check: got: " + tmpCnt + ", expected: " + count;
  }

  private void hardReset() {
    resetCommon();
    if (wseg_ != null) {
      insertCurCount(wseg_, -1); //Universal Set
      clearEmpty(wseg_); //false
    }
    curCount_ = -1; //Universal Set
    empty_ = false;
  }

  private void resetToEmpty() {
    resetCommon();
    if (wseg_ != null) {
      insertCurCount(wseg_, 0);
      setEmpty(wseg_); //true
    }
    curCount_ = 0;
    empty_ = true;
  }

  private void resetCommon() {
    if (wseg_ != null) {
      if (readOnly_) { throw new SketchesReadOnlyException(); }
      wseg_.asSlice(CONST_PREAMBLE_LONGS << 3, 8 << ThetaUtil.MIN_LG_ARR_LONGS).fill((byte)0);
      insertLgArrLongs(wseg_, ThetaUtil.MIN_LG_ARR_LONGS);
      insertThetaLong(wseg_, Long.MAX_VALUE);
    }
    lgArrLongs_ = ThetaUtil.MIN_LG_ARR_LONGS;
    thetaLong_ = Long.MAX_VALUE;
    hashTable_ = null;
  }

  /**
   * Compact first 2^lgArrLongs of given array
   * @param srcCache anything
   * @param lgArrLongs The correct
   * <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">lgArrLongs</a>.
   * @param curCount must be correct
   * @param thetaLong The correct
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @param dstOrdered true if output array must be sorted
   * @return the compacted array
   */ //used in Test
  static final long[] compactCachePart(final long[] srcCache, final int lgArrLongs,
      final int curCount, final long thetaLong, final boolean dstOrdered) {
    if (curCount == 0) {
      return new long[0];
    }
    final long[] cacheOut = new long[curCount];
    final int len = 1 << lgArrLongs;
    int j = 0;
    for (int i = 0; i < len; i++) {
      final long v = srcCache[i];
      if (v <= 0L || v >= thetaLong ) { continue; }
      cacheOut[j++] = v;
    }
    assert curCount == j;
    if (dstOrdered) {
      Arrays.sort(cacheOut);
    }
    return cacheOut;
  }

  private static void checkMinSizeMemorySegment(final MemorySegment seg) {
    final int minBytes = (CONST_PREAMBLE_LONGS << 3) + (8 << ThetaUtil.MIN_LG_ARR_LONGS);//280
    final long cap = seg.byteSize();
    if (cap < minBytes) {
      throw new SketchesArgumentException(
          "MemorySegment must be at least " + minBytes + " bytes. Actual capacity: " + cap);
    }
  }

  /**
   * Returns the maximum lgArrLongs given the capacity of the MemorySegment.
   * @param dstSeg the given MemorySegment
   * @return the maximum lgArrLongs given the capacity of the MemorySegment
   */
  private static int getMaxLgArrLongs(final MemorySegment dstSeg) {
    final int preBytes = CONST_PREAMBLE_LONGS << 3;
    final long cap = dstSeg.byteSize();
    return Integer.numberOfTrailingZeros(floorPowerOf2((int)(cap - preBytes)) >>> 3);
  }

  private static void segChecks(final MemorySegment srcSeg) {
    //Get Preamble
    //Note: This intersection operation does not use lgNomLongs (or k), per se.
    //seedHash loaded and checked in private constructor
    final int preLongs = ThetaSketch.getPreambleLongs(srcSeg);
    final int serVer = extractSerVer(srcSeg);
    final int famID = extractFamilyID(srcSeg);
    final boolean empty = (extractFlags(srcSeg) & EMPTY_FLAG_MASK) > 0;
    final int curCount = extractCurCount(srcSeg);
    //Checks
    if (preLongs != CONST_PREAMBLE_LONGS) {
      throw new SketchesArgumentException(
          "MemorySegment PreambleLongs must equal " + CONST_PREAMBLE_LONGS + ": " + preLongs);
    }
    if (serVer != SER_VER) {
      throw new SketchesArgumentException("Serialization Version must equal " + SER_VER);
    }
    Family.INTERSECTION.checkFamilyID(famID);
    if (empty) {
      if (curCount != 0) {
        throw new SketchesArgumentException(
            "srcSeg empty state inconsistent with curCount: " + empty + "," + curCount);
      }
      //empty = true AND curCount_ = 0: OK
    } //else empty = false, curCount could be anything
  }

}
