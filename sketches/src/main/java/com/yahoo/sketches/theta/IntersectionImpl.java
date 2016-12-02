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
import static com.yahoo.sketches.theta.PreambleUtil.clearEmpty;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;
import static com.yahoo.sketches.theta.PreambleUtil.insertCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.insertFlags;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertP;
import static com.yahoo.sketches.theta.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.insertThetaLong;
import static java.lang.Math.min;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.Util;

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
  private final Memory mem_;
  private final Object memObj_;
  private final long memAdd_;
  private final boolean memValid_;

  //Note: Intersection does not use lgNomLongs or k, per se.
  private int lgArrLongs_; //current size of hash table
  private int curCount_; //curCount of HT, if < 0 means Universal Set (US) is true
  private long thetaLong_;
  private boolean empty_;

  private long[] hashTable_ = null;  //HT => Data.  Only used On Heap
  private int maxLgArrLongs_ = 0; //max size of hash table. Only used Off Heap

  private IntersectionImpl(final Memory mem, final long seed, final boolean newMem) {
    mem_ = mem;
    if (mem != null) {
      memObj_ = mem.array();
      memAdd_ = mem.getCumulativeOffset(0L);
      memValid_ = true;
      if (newMem) {
        seedHash_ = computeSeedHash(seed);
        insertSeedHash(memObj_, memAdd_, seedHash_);
      } else {
        seedHash_ = (short) extractSeedHash(memObj_, memAdd_);
        Util.checkSeedHashes(seedHash_, computeSeedHash(seed)); //check for seed hash conflict
      }
    } else {
      memObj_ = null;
      memAdd_ = -1L;
      memValid_ = false;
      seedHash_ = computeSeedHash(seed);
    }
  }

  /**
   * Construct a new Intersection target on the java heap.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
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
   * Heapify an intersection target from a Memory image containing data.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  static IntersectionImpl heapifyInstance(final Memory srcMem, final long seed) {
    final IntersectionImpl impl = new IntersectionImpl(null, seed, false);
    final Object memObj = srcMem.array(); //may be null
    final long memAdd = srcMem.getCumulativeOffset(0L);

    //Get Preamble
    final int preLongsMem = extractPreLongs(memObj, memAdd);
    final int serVer = extractSerVer(memObj, memAdd);
    final int famID = extractFamilyID(memObj, memAdd);
    //Note: Intersection does not use lgNomLongs (or k), per se.
    final int lgArrLongs = extractLgArrLongs(memObj, memAdd); //current hash table size
    final int flags = extractFlags(memObj, memAdd);
    //seedHash loaded and checked in private constructor
    final int curCount = extractCurCount(memObj, memAdd);
    final long thetaLong = extractThetaLong(memObj, memAdd);
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
   * Construct a new Intersection target direct to the given destination Memory.
   * Called by SetOperation.Builder.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @param dstMem destination Memory.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  static IntersectionImpl initNewDirectInstance(final long seed, final Memory dstMem) {
    final IntersectionImpl impl = new IntersectionImpl(dstMem, seed, true);
    final Object memObj = impl.memObj_;
    final long memAdd = impl.memAdd_;

    //Load Preamble
    insertPreLongs(memObj, memAdd, CONST_PREAMBLE_LONGS); //RF not used = 0
    insertSerVer(memObj, memAdd, SER_VER);
    insertFamilyID(memObj, memAdd, Family.INTERSECTION.getID());
    //Note: Intersection does not use lgNomLongs or k, per se.
    //set lgArrLongs initially to minimum.  Don't clear cache in mem
    insertLgArrLongs(memObj, memAdd, MIN_LG_ARR_LONGS);
    insertFlags(memObj, memAdd, 0); //bigEndian = readOnly = compact = ordered = empty = false;
    //seedHash loaded and checked in private constructor
    insertCurCount(memObj, memAdd, -1);
    insertP(memObj, memAdd, (float) 1.0);
    insertThetaLong(memObj, memAdd, Long.MAX_VALUE);

    //Initialize
    impl.lgArrLongs_ = MIN_LG_ARR_LONGS;
    impl.curCount_ = -1; //set in mem below
    impl.thetaLong_ = Long.MAX_VALUE;
    impl.empty_ = false;
    impl.maxLgArrLongs_ = checkMaxLgArrLongs(dstMem); //Only Off Heap

    return impl;
  }

  /**
   * Wrap an Intersection target around the given source Memory containing intersection data.
   * @param srcMem The source Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  static IntersectionImpl wrapInstance(final Memory srcMem, final long seed) {
    final IntersectionImpl impl = new IntersectionImpl(srcMem, seed, false);
    final Object memObj = impl.memObj_;
    final long memAdd = impl.memAdd_;

    //Get Preamble
    final int preLongsMem = extractPreLongs(memObj, memAdd);
    final int serVer = extractSerVer(memObj, memAdd);
    final int famID = extractFamilyID(memObj, memAdd);
    //Note: Intersection does not use lgNomLongs (or k), per se.
    final int lgArrLongs = extractLgArrLongs(memObj, memAdd); //current hash table size
    final int flags = extractFlags(memObj, memAdd);
    //seedHash loaded and checked in private constructor
    final int curCount = extractCurCount(memObj, memAdd);
    final long thetaLong = extractThetaLong(memObj, memAdd);
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
    } //else empty = false, curCount could be anything

    //Initialize
    impl.lgArrLongs_ = lgArrLongs;
    impl.curCount_ = curCount;
    impl.thetaLong_ = thetaLong;
    impl.empty_ = empty;
    impl.maxLgArrLongs_ = checkMaxLgArrLongs(srcMem); //Only Off Heap, check for min size
    return impl;
  }

  @Override
  public void update(final Sketch sketchIn) {
    final boolean firstCall = curCount_ < 0;

    //Corner cases
    if (sketchIn == null) { //null -> Th = 1.0, count = 0, empty = true
      //No seedHash to check
      empty_ = true;
      thetaLong_ = firstCall ? Long.MAX_VALUE : thetaLong_; //if Nth call, stays the same
      curCount_ = 0;
      if (memValid_) {
        PreambleUtil.setEmpty(memObj_, memAdd_);
        insertThetaLong(memObj_, memAdd_, thetaLong_);
        insertCurCount(memObj_, memAdd_, 0);
      }
      return;
    }

    //Checks
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());

    thetaLong_ = min(thetaLong_, sketchIn.getThetaLong()); //Theta rule
    empty_ = empty_ || sketchIn.isEmpty();  //Empty rule

    if (memValid_) {
      insertThetaLong(memObj_, memAdd_, thetaLong_);
      if (empty_) { PreambleUtil.setEmpty(memObj_, memAdd_); }
      else { clearEmpty(memObj_, memAdd_); }
    }

    final int sketchInEntries = sketchIn.getRetainedEntries(true);

    // The truth table for the following state machine for corner cases:
    //   Case  CurCount  SketchInEntries | Actions
    //     1      <0            0        | CurCount = 0; HT = null; exit
    //     2       0            0        | CurCount = 0; HT = null; exit
    //     3      >0            0        | CurCount = 0; HT = null; exit
    //     4      <0           >0        | Clone SketchIn; exit
    //     5       0           >0        | CurCount = 0; HT = null; exit
    //     6      >0           >0        | Perform full intersect

    if ((curCount_ == 0) || (sketchInEntries == 0)) { //Cases 1,2,3,5
      //All future intersections result in zero data, but theta can still be reduced.
      curCount_ = 0;
      if (memValid_) { insertCurCount(memObj_, memAdd_, 0); }
      hashTable_ = null; //No need for a HT. Don't bother clearing mem if valid
    }
    else if (firstCall) { //Case 4: Clone the incoming sketch
      curCount_ = sketchIn.getRetainedEntries(true);
      final int requiredLgArrLongs = computeMinLgArrLongsFromCount(curCount_);
      final int priorLgArrLongs = lgArrLongs_; //prior only used in error message
      lgArrLongs_ = requiredLgArrLongs;

      if (memValid_) { //Off heap, check if current dstMem is large enough
        insertCurCount(memObj_, memAdd_, curCount_);
        insertLgArrLongs(memObj_, memAdd_, lgArrLongs_);
        if (requiredLgArrLongs <= maxLgArrLongs_) { //OK
          mem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_); //clear only what required
        }
        else { //not enough space in dstMem //TODO move to request model?
          throw new SketchesArgumentException(
              "Insufficient dstMem hash table space: "
                  + (1 << requiredLgArrLongs) + " > " + (1 << priorLgArrLongs));
        }
      }
      else { //On the heap, allocate a HT
        hashTable_ = new long[1 << lgArrLongs_];
      }

      moveDataToTgt(sketchIn.getCache(), curCount_);
    }
    else { //Case 6: Perform full intersect
      //Sets resulting hashTable, curCount and adjusts lgArrLongs
      performIntersect(sketchIn);
    }

  }

  @Override
  public CompactSketch getResult(final boolean dstOrdered, final Memory dstMem) {
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
    final long[] hashTable;
    if (mem_ != null) {
      final int htLen = 1 << lgArrLongs_;
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
    final int preBytes = CONST_PREAMBLE_LONGS << 3;
    final int dataBytes = (curCount_ > 0) ? 8 << lgArrLongs_ : 0;
    final byte[] byteArrOut = new byte[preBytes + dataBytes];
    if (memValid_) {
      mem_.getByteArray(0, byteArrOut, 0, preBytes + dataBytes);
    }
    else {
      final NativeMemory memOut = new NativeMemory(byteArrOut);

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
    curCount_ = -1;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = false;
    hashTable_ = null;
    if (mem_ != null) {
      insertLgArrLongs(memObj_, memAdd_, lgArrLongs_); //make sure
      insertCurCount(memObj_, memAdd_, -1);
      insertThetaLong(memObj_, memAdd_, Long.MAX_VALUE);
      clearEmpty(memObj_, memAdd_);
    }
  }

  @Override
  public Family getFamily() {
    return Family.INTERSECTION;
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
    if (memValid_) {
      insertCurCount(memObj_, memAdd_, matchSetCount);
      insertLgArrLongs(memObj_, memAdd_, lgArrLongs_);
      mem_.clear(CONST_PREAMBLE_LONGS << 3, 8 << lgArrLongs_); //clear for rebuild
    } else {
      Arrays.fill(hashTable_, 0, 1 << lgArrLongs_, 0L); //clear for rebuild
    }
    //move matchSet to target
    moveDataToTgt(matchSet, matchSetCount);
  }

  private void moveDataToTgt(final long[] arr, final int count) {
    final int arrLongsIn = arr.length;
    int tmpCnt = 0;
    if (memValid_) { //Off Heap puts directly into mem
      final int preBytes = CONST_PREAMBLE_LONGS << 3;
      final Object memObj = memObj_;
      final long memAdd = memAdd_;
      final int lgArrLongs = lgArrLongs_;
      final long thetaLong = thetaLong_;
      for (int i = 0; i < arrLongsIn; i++ ) {
        final long hashIn = arr[i];
        if (HashOperations.continueCondition(thetaLong, hashIn)) { continue; }
        HashOperations.fastHashInsertOnly(memObj, memAdd, lgArrLongs, hashIn, preBytes);
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

  //special handlers for Off Heap
  /**
   * Returns the correct maximum lgArrLongs given the capacity of the Memory. Checks that the
   * capacity is large enough for the minimum sized hash table.
   * @param dstMem the given Memory
   * @return the correct maximum lgArrLongs given the capacity of the Memory
   */
  private static final int checkMaxLgArrLongs(final Memory dstMem) {
    final int preBytes = CONST_PREAMBLE_LONGS << 3;
    final long cap = dstMem.getCapacity();
    final int maxLgArrLongs =
        Integer.numberOfTrailingZeros(floorPowerOf2((int)(cap - preBytes)) >>> 3);
    if (maxLgArrLongs < MIN_LG_ARR_LONGS) {
      throw new SketchesArgumentException(
        "dstMem not large enough for minimum sized hash table: " + cap);
    }
    return maxLgArrLongs;
  }

}
