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
import static org.apache.datasketches.HashOperations.convertToHashTable;
import static org.apache.datasketches.HashOperations.hashArrayInsert;
import static org.apache.datasketches.HashOperations.hashSearch;
import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.Util.checkSeedHashes;
import static org.apache.datasketches.Util.simpleIntLog2;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;

import java.util.Arrays;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Implements the A-and-not-B operation.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class AnotBimpl extends AnotB {
  private final short seedHash_;
  private boolean empty_;
  private long thetaLong_;
  private long[] hashArr_; //compact array w curCount_ entries
  private int curCount_;

  //Remove all 4 of these with deprecated
  private Sketch skA_;
  private Sketch skB_;
  private int lgArrLongsHT_; //for Hash Table only. may not need to be member after refactoring
  private long[] bHashTable_; //may not need to be member after refactoring.

  /**
   * Construct a new AnotB SetOperation on the java heap.  Called by SetOperation.Builder.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  AnotBimpl(final long seed) {
    this(computeSeedHash(seed));
  }

  /**
   * Construct a new AnotB SetOperation on the java heap.  Called by PairwiseSetOperation.
   *
   * @param seedHash 16 bit hash of the chosen update seed.
   */
  AnotBimpl(final short seedHash) {
    seedHash_ = seedHash;
    reset();
  }

  @Override
  public void setA(final Sketch skA) {
    if (skA == null) {
      reset();
      return;
      //throw new SketchesArgumentException("The input argument may not be null");
    }
    if (skA.isEmpty()) {
      reset();
      return;
    }
    //skA is not empty
    checkSeedHashes(seedHash_, skA.getSeedHash());
    empty_ = false;
    thetaLong_ = skA.getThetaLong();
    final CompactSketch cskA = (skA instanceof CompactSketch)
        ? (CompactSketch) skA
        : ((UpdateSketch) skA).compact();
    hashArr_ = skA.isDirect() ? cskA.getCache() : cskA.getCache().clone();
    curCount_ = cskA.getRetainedEntries(true);
  }

  @Override
  public void notB(final Sketch skB) {
    if (empty_ || (skB == null) || skB.isEmpty()) { return; }
    //skB is not empty
    checkSeedHashes(seedHash_, skB.getSeedHash());
    final long thetaLongB = skB.getThetaLong();
    thetaLong_ = Math.min(thetaLong_, thetaLongB);

    //Build hashtable and removes hashes of skB >= theta
    final int countB = skB.getRetainedEntries();
    CompactSketch cskB = null;
    UpdateSketch uskB = null;
    final long[] hashTableB;
    if (skB instanceof CompactSketch) {
      cskB = (CompactSketch) skB;
      hashTableB = convertToHashTable(cskB.getCache(), countB, thetaLong_, REBUILD_THRESHOLD);
    } else {
      uskB = (UpdateSketch) skB;
      hashTableB = (thetaLong_ < thetaLongB)
          ? convertToHashTable(uskB.getCache(), countB, thetaLong_, REBUILD_THRESHOLD)
          : uskB.getCache();
      cskB = uskB.compact();
    }

    //build temporary arrays of skA
    final long[] tmpHashArrA = new long[curCount_];

    //search for non matches and build temp arrays
    final int lgHTBLen = simpleIntLog2(hashTableB.length);
    int nonMatches = 0;
    for (int i = 0; i < curCount_; i++) {
      final long hash = hashArr_[i];
      if ((hash != 0) && (hash < thetaLong_)) { //skips hashes of A >= theta
        final int index = hashSearch(hashTableB, lgHTBLen, hash);
        if (index == -1) {
          tmpHashArrA[nonMatches] = hash;
          nonMatches++;
        }
      }
    }
    hashArr_ = Arrays.copyOfRange(tmpHashArrA, 0, nonMatches);
    curCount_ = nonMatches;
    empty_ = (nonMatches == 0) && (thetaLong_ == Long.MAX_VALUE);
  }

  @Override
  public CompactSketch getResult(final boolean reset) {
    return getResult(true, null, reset);
  }

  @Override
  public CompactSketch getResult(final boolean dstOrdered, final WritableMemory dstMem,
      final boolean reset) {
    final CompactSketch result =
        getResult(hashArr_, empty_, seedHash_, curCount_, thetaLong_, dstOrdered, dstMem);
    if (reset) { reset(); }
    return result;
  }

  private static CompactSketch getResult(
      final long[] hashArr,
      final boolean empty,
      final short seedHash,
      final int curCount,
      final long thetaLong,
      final boolean dstOrdered,
      final WritableMemory dstMem) {
    final CompactSketch result;
    if (dstMem == null) { //Heap
      if (empty) { return EmptyCompactSketch.getInstance(); }
      if (curCount == 1) { return new SingleItemSketch(hashArr[0], seedHash); }
      //curCount > 1
      if (dstOrdered) {
        Arrays.sort(hashArr);
        result = new HeapCompactOrderedSketch(hashArr, false, seedHash, curCount, thetaLong);
      } else {
        result = new HeapCompactUnorderedSketch(hashArr, false, seedHash, curCount, thetaLong);
      }
    }
    else { //Direct
      if (empty) {
        dstMem.putByteArray(0, EmptyCompactSketch.EMPTY_COMPACT_SKETCH_ARR, 0, 8);
        return EmptyCompactSketch.getInstance();
      }
      if (curCount == 1) {
        final SingleItemSketch sis = new SingleItemSketch(hashArr[0], seedHash);
        dstMem.putByteArray(0, sis.toByteArray(), 0, 16);
      }
      final int preLongs = CompactOperations.computeCompactPreLongs(thetaLong, false, curCount);
      if (dstOrdered) {
        final byte flags = (byte)(READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK | ORDERED_FLAG_MASK);
        Arrays.sort(hashArr);
        CompactOperations.loadCompactMemory(hashArr, seedHash, curCount, thetaLong, dstMem, flags, preLongs);
        result = new DirectCompactOrderedSketch(dstMem);
      } else {
        final byte flags = (byte)(READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK);
        CompactOperations.loadCompactMemory(hashArr, seedHash, curCount, thetaLong, dstMem, flags, preLongs);
        result = new DirectCompactUnorderedSketch(dstMem);
      }
    }
    return result;
  }

  @Override
  public CompactSketch aNotB(final Sketch skA, final Sketch skB, final boolean dstOrdered,
      final WritableMemory dstMem) {
    if ((skA == null) || skA.isEmpty()) { return EmptyCompactSketch.getInstance(); }
    if ((skB == null) || skB.isEmpty()) { return skA.compact(dstOrdered, dstMem); }
    final short seedHashA = skA.getSeedHash();
    final short seedHashB = skB.getSeedHash();
    checkSeedHashes(seedHashA, seedHash_);
    checkSeedHashes(seedHashB, seedHash_);

    //Both skA & skB are not empty
    //Load skA into local tmp registers
    boolean empty = false;
    final long thetaLongA = skA.getThetaLong();
    final CompactSketch cskA = (skA instanceof CompactSketch)
        ? (CompactSketch)skA
        : ((UpdateSketch)skA).compact();
    final long[] hashArrA = cskA.getCache().clone();
    final int countA = cskA.getRetainedEntries();

    //Compare with skB
    final long thetaLongB = skB.getThetaLong();
    final long thetaLong = Math.min(thetaLongA, thetaLongB);
    final int countB = skB.getRetainedEntries();

    //Rebuild hashtable and removes hashes of skB >= thetaLong
    final long[] hashTableB = convertToHashTable(skB.getCache(), countB, thetaLong, REBUILD_THRESHOLD);

    //build temporary hash array for values from skA
    final long[] tmpHashArrA = new long[countA];

    //search for non matches and build temp hash array
    int nonMatches = 0;
    for (int i = 0; i < countA; i++) {
      final long hash = hashArrA[i];
      if ((hash != 0) && (hash < thetaLong)) { //skips hashes of A >= theta
        final int index =
            hashSearch(hashTableB, simpleIntLog2(hashTableB.length), hash);
        if (index == -1) {
          tmpHashArrA[nonMatches] = hash;
          nonMatches++;
        }
      }
    }
    //final compaction
    empty = ((nonMatches == 0) && (thetaLong == Long.MAX_VALUE));
    final long[] hashArrOut = Arrays.copyOfRange(tmpHashArrA, 0, nonMatches);
    final CompactSketch result =
        AnotBimpl.getResult(hashArrOut, empty, seedHash_, nonMatches, thetaLong, dstOrdered, dstMem);
    return result;
  }

  @Override
  int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  boolean isEmpty() {
    return empty_;
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return false;
  }

  //Deprecated methods

  @Deprecated
  @Override
  public void update(final Sketch a, final Sketch b) {
    skA_ = a;
    skB_ = b;
    thetaLong_ = Long.MAX_VALUE; //*
    empty_ = true; //*
    hashArr_ = null; //*
    curCount_ = 0; //*
    lgArrLongsHT_ = 5;
    bHashTable_ = null;
    compute();
  }

  @Deprecated
  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }

  @Deprecated
  @Override
  public CompactSketch getResult(final boolean dstOrdered, final WritableMemory dstMem) {
    final long[] compactCache = (curCount_ <= 0)
        ? new long[0]
        : Arrays.copyOfRange(hashArr_, 0, curCount_);
    if (dstOrdered && (curCount_ > 1)) {
      Arrays.sort(compactCache);
    }
    //Create the CompactSketch
    final CompactSketch csk = CompactOperations.componentsToCompact(
        thetaLong_, curCount_, seedHash_, empty_, true, dstOrdered, dstOrdered, dstMem, compactCache);
    reset();
    return csk;
  }

  //restricted

  void compute() {
    final int swA = ((skA_ == null) || (skA_ instanceof EmptyCompactSketch))
        ? 0
        : (skA_.isEmpty())
          ? 1
          : (skA_ instanceof UpdateSketch)
            ? 4
            : (skA_.isOrdered())
              ? 3
              : 2;
    final int swB = ((skB_ == null) || (skB_ instanceof EmptyCompactSketch))
        ? 0
        : (skB_.isEmpty())
          ? 1
          : (skB_ instanceof UpdateSketch)
            ? 4
            : (skB_.isOrdered())
              ? 3
              : 2;
    final int sw = (swA * 8) | swB;

    //  NOTES:
    //    In the table below, A and B refer to the two input sketches in the order A-not-B.
    //    The Theta rule: min(ThetaA, ThetaB)
    //    The Empty rule: Whatever the empty state of A is: E(A)
    //    The Return triple is defined as: (Theta, Count, EmptyFlag).
    //    bHashTable temporarily stores the values of B.
    //    A sketch in stored form can be in one of 5 states.
    //    Null is not actually a state, but is included for completeness.
    //    Null is interpreted as {Theta = 1.0, count = 0, empty = true}.
    //    In some cases the empty state may have Theta < 1.0 but it is ignored; count must be zero.
    //    State:
    //      0 N Null or instance of EmptyCompactSketch
    //      1 E Empty bit set
    //      2 C Compact, not ordered
    //      3 O Compact Ordered
    //      4 H Hash-Table
    //
    //A    B    swA  swB  Case  Actions
    //N    N    0    0    0     Return (1.0, 0, T)
    //N    E    0    1    1     CheckB,  Return (1.0, 0, T)
    //N    C    0    2    2     CheckB,  Return (1.0, 0, T)
    //N    O    0    3    3     CheckB,  Return (1.0, 0, T)
    //N    H    0    4    4     CheckB,  Return (1.0, 0, T)
    //E    N    1    0    8     CheckA,  Return (1.0, 0, T)
    //E    E    1    1    9     CheckAB, Return (1.0, 0, T)
    //E    C    1    2    10    CheckAB, Return (1.0, 0, T)
    //E    O    1    3    11    CheckAB, Return (1.0, 0, T)
    //E    H    1    4    12    CheckAB, Return (1.0, 0, T)
    //C    N    2    0    16    CheckA,  Return (ThA, |A|, F), copyA
    //C    E    2    1    17    CheckAB, Return (ThA, |A|, F)), copyA
    //C    C    2    2    18    CheckAB, B -> H; => C,H; scanAllAsearchB()
    //C    O    2    3    19    CheckAB, B -> H; => C,H; scanAllAsearchB()
    //C    H    2    4    20    CheckAB, scanAllAsearchB()
    //O    N    3    0    24    CheckA,  Return (ThA, |A|, F), copyA
    //O    E    3    1    25    CheckAB, Return (ThA, |A|, F), copyA
    //O    C    3    2    26    CheckAB, B -> H; => O,H; scanEarlyStopAsearchB()
    //O    O    3    3    27    CheckAB, B -> H; => O,H; scanEarlyStopAsearchB()
    //O    H    3    4    28    CheckAB, scanEarlyStopAsearchB()
    //H    N    4    0    32    CheckA,  Return (ThA, |A|, F), copyA
    //H    E    4    1    33    CheckAB, Return (ThA, |A|, F), copyA
    //H    C    4    2    34    CheckAB, B -> H; => H,H; scanAllAsearchB()
    //H    O    4    3    35    CheckAB, B -> H; => H,H; scanAllAsearchB()
    //H    H    4    4    36    CheckAB, scanAllAsearchB()

    switch (sw) {
      case 0 :  //A Null, B Null;    Return (1.0, 0, T)
        thetaLong_ = Long.MAX_VALUE;
        empty_ = true;
        break;

      case 10:   //A Empty, B Compact; CheckAB, Return (1.0, 0, T)
      case 11:   //A Empty, B Ordered; CheckAB, Return (1.0, 0, T)
      case 12:   //A Empty, B HashTbl; CheckAB, Return (1.0, 0, T)
        checkSeedHashes(seedHash_, skA_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        //$FALL-THROUGH$
      case 1:    //A Null, B Empty;   CheckB,  Return (1.0, 0, T)
      case 2:    //A Null, B Compact; CheckB,  Return (1.0, 0, T)
      case 3:    //A Null, B Ordered; CheckB,  Return (1.0, 0, T)
      case 4:    //A Null, B HashTbl; CheckB,  Return (1.0, 0, T)
        checkSeedHashes(seedHash_, skB_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = Long.MAX_VALUE;
        empty_ = true;
        break;

      case 9:   //A Empty, B Empty;   CheckAB, Return (1.0, 0, T)
        checkSeedHashes(seedHash_, skB_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        //$FALL-THROUGH$
      case 8:   //A Empty, B Null;    CheckA,  Return (1.0, 0, T)
        checkSeedHashes(seedHash_, skA_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = Long.MAX_VALUE;
        empty_ = true;
        break;

      case 17:   //A Compact, B Empty; CheckAB, Return (ThA, |A|, F), copyA
      case 25:   //A Ordered, B Empty; CheckAB, Return (ThA, |A|, F), copyA
      case 33:  //A HashTbl, B Empty; CheckAB, Return (ThA, |A|, F), copyA
        checkSeedHashes(seedHash_, skB_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        //$FALL-THROUGH$
      case 16:   //A Compact, B Null;  CheckA,  Return (ThA, |A|, F), copyA
      case 24:   //A Ordered, B Null;  CheckA,  Return (ThA, |A|, F), copyA
      case 32:  //A HashTbl, B Null;  CheckA,  Return (ThA, |A|, F), copyA
        checkSeedHashes(seedHash_, skA_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = skA_.getThetaLong();
        empty_ = false;
        curCount_ = skA_.getRetainedEntries(true);
        hashArr_ = CompactOperations.compactCache(skA_.getCache(), curCount_, thetaLong_, false);
        break;

      case 18:   //A Compact, B Compact; CheckAB, B -> H; => C,H; scanAllAsearchB()
      case 19:   //A Compact, B Ordered; CheckAB, B -> H; => C,H; scanAllAsearchB()
      case 34:   //A HashTbl, B Compact; CheckAB, B -> H; => H,H; scanAllAsearchB()
      case 35:  //A HashTbl, B Ordered; CheckAB, B -> H; => H,H; scanAllAsearchB()
        checkSeedHashes(seedHash_, skA_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        checkSeedHashes(seedHash_, skB_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = min(skA_.getThetaLong(), skB_.getThetaLong());
        empty_ = false;
        convertBtoHT();
        scanAllAsearchB();
        break;

      case 26:   //A Ordered, B Compact; CheckAB, B -> H; => O,H; scanEarlyStopAsearchB()
      case 27:  //A Ordered, B Ordered; CheckAB, B -> H; => O,H; scanEarlyStopAsearchB()
        checkSeedHashes(seedHash_, skA_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        checkSeedHashes(seedHash_, skB_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = min(skA_.getThetaLong(), skB_.getThetaLong());
        empty_ = false;
        convertBtoHT();
        scanEarlyStopAsearchB();
        break;

      case 20:   //A Compact, B HashTbl; CheckAB, scanAllAsearchB()
      case 36:  //A HashTbl, B HashTbl; CheckAB, scanAllAsearchB()
        checkSeedHashes(seedHash_, skA_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        checkSeedHashes(seedHash_, skB_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = min(skA_.getThetaLong(), skB_.getThetaLong());
        empty_ = false;
        lgArrLongsHT_ = ((UpdateSketch)skB_).getLgArrLongs();
        bHashTable_ = skB_.getCache();
        scanAllAsearchB();
        break;

      case 28:  //A Ordered, B HashTbl; CheckAB, scanEarlyStopAsearchB()
        checkSeedHashes(seedHash_, skA_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        checkSeedHashes(seedHash_, skB_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = min(skA_.getThetaLong(), skB_.getThetaLong());
        empty_ = false;
        lgArrLongsHT_ = ((UpdateSketch)skB_).getLgArrLongs();
        bHashTable_ = skB_.getCache();
        scanEarlyStopAsearchB();
        break;

      //default: //This cannot happen and cannot be tested
    }
  }

  private void convertBtoHT() {
    final int curCountB = skB_.getRetainedEntries(true);
    lgArrLongsHT_ = computeMinLgArrLongsFromCount(curCountB);
    bHashTable_ = new long[1 << lgArrLongsHT_];
    hashArrayInsert(skB_.getCache(), bHashTable_, lgArrLongsHT_, thetaLong_);
  }

  //Sketch A is either unordered compact or hash table
  private void scanAllAsearchB() {
    final long[] scanAArr = skA_.getCache();
    final int arrLongsIn = scanAArr.length;
    hashArr_ = new long[arrLongsIn];
    for (int i = 0; i < arrLongsIn; i++ ) {
      final long hashIn = scanAArr[i];
      if ((hashIn <= 0L) || (hashIn >= thetaLong_)) { continue; }
      final int foundIdx = hashSearch(bHashTable_, lgArrLongsHT_, hashIn);
      if (foundIdx > -1) { continue; }
      hashArr_[curCount_++] = hashIn;
    }
  }

  //Sketch A is ordered compact, which enables early stop
  private void scanEarlyStopAsearchB() {
    final long[] scanAArr = skA_.getCache();
    final int arrLongsIn = scanAArr.length;
    hashArr_ = new long[arrLongsIn]; //maybe 2x what is needed, but getRetainedEntries can be slow.
    for (int i = 0; i < arrLongsIn; i++ ) {
      final long hashIn = scanAArr[i];
      if (hashIn <= 0L) { continue; }
      if (hashIn >= thetaLong_) {
        break; //early stop assumes that hashes in input sketch are ordered!
      }
      final int foundIdx = hashSearch(bHashTable_, lgArrLongsHT_, hashIn);
      if (foundIdx > -1) { continue; }
      hashArr_[curCount_++] = hashIn;
    }
  }

  private void reset() {
    skA_ = null;
    skB_ = null;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = true;
    hashArr_ = null;
    curCount_ = 0;
    lgArrLongsHT_ = 5;
    bHashTable_ = null;
  }

  @Override
  long[] getCache() {
    return null;
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

  @Override
  long getThetaLong() {
    return thetaLong_;
  }

}
