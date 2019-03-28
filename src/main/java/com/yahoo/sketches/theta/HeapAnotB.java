/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.HashOperations.hashArrayInsert;
import static com.yahoo.sketches.HashOperations.hashSearch;
import static com.yahoo.sketches.theta.CompactSketch.compactCache;
import static java.lang.Math.min;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class HeapAnotB extends AnotB {
  private final short seedHash_;
  private Sketch a_;
  private Sketch b_;
  private long thetaLong_;
  private boolean empty_;
  private long[] cache_; // no match set
  private int curCount_;

  private int lgArrLongsHT_; //for Hash Table only. may not need to be member after refactoring
  private long[] bHashTable_; //may not need to be member after refactoring.

  /**
   * Construct a new AnotB SetOperation on the java heap.  Called by SetOperation.Builder.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  HeapAnotB(final long seed) {
    this(Util.computeSeedHash(seed));
  }

  /**
   * Construct a new AnotB SetOperation on the java heap.  Called by PairwiseSetOperation.
   *
   * @param seedHash 16 bit hash of the chosen update seed.
   */
  HeapAnotB(final short seedHash) {
    seedHash_ = seedHash;
    reset();
  }

  @Override
  public void update(final Sketch a, final Sketch b) {
    a_ = a;
    b_ = b;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = true;
    cache_ = null;
    curCount_ = 0;
    lgArrLongsHT_ = 5;
    bHashTable_ = null;
    compute();
  }

  @Override
  public CompactSketch aNotB(final Sketch a, final Sketch b, final boolean dstOrdered,
      final WritableMemory dstMem) {
    update(a, b);
    return getResult(dstOrdered, dstMem);
  }

  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }

  @Override
  public CompactSketch getResult(final boolean dstOrdered, final WritableMemory dstMem) {
    final long[] compactCache = (curCount_ <= 0)
        ? new long[0]
        : Arrays.copyOfRange(cache_, 0, curCount_);
    if (dstOrdered && (curCount_ > 1)) {
      Arrays.sort(compactCache);
    }
    //Create the CompactSketch
    final boolean empty = (curCount_ == 0) && (thetaLong_ == Long.MAX_VALUE);
    final CompactSketch comp = createCompactSketch(
        compactCache, empty, seedHash_, curCount_, thetaLong_, dstOrdered, dstMem);
    reset();
    return comp;
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

  //restricted

  void compute() {
    final int swA = (a_ == null) ? 0 : (a_.isEmpty())
        ? 1 : (a_ instanceof UpdateSketch) ? 4 : (a_.isOrdered()) ? 3 : 2;
    final int swB = (b_ == null) ? 0 : (b_.isEmpty()) ? 1 : (b_ instanceof UpdateSketch)
        ? 4 : (b_.isOrdered()) ? 3 : 2;
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
    //    The empty state may have Theta < 1.0 but it is ignored; count must be zero.
    //    State:
    //      0 N Null
    //      1 E Empty
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
        Util.checkSeedHashes(seedHash_, a_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        //$FALL-THROUGH$
      case 1:    //A Null, B Empty;   CheckB,  Return (1.0, 0, T)
      case 2:    //A Null, B Compact; CheckB,  Return (1.0, 0, T)
      case 3:    //A Null, B Ordered; CheckB,  Return (1.0, 0, T)
      case 4:    //A Null, B HashTbl; CheckB,  Return (1.0, 0, T)
        Util.checkSeedHashes(seedHash_, b_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = Long.MAX_VALUE;
        empty_ = true;
        break;

      case 9:   //A Empty, B Empty;   CheckAB, Return (1.0, 0, T)
        Util.checkSeedHashes(seedHash_, b_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        //$FALL-THROUGH$
      case 8:   //A Empty, B Null;    CheckA,  Return (1.0, 0, T)
        Util.checkSeedHashes(seedHash_, a_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = Long.MAX_VALUE;
        empty_ = true;
        break;

      case 17:   //A Compact, B Empty; CheckAB, Return (ThA, |A|, F), copyA
      case 25:   //A Ordered, B Empty; CheckAB, Return (ThA, |A|, F), copyA
      case 33:  //A HashTbl, B Empty; CheckAB, Return (ThA, |A|, F), copyA
        Util.checkSeedHashes(seedHash_, b_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        //$FALL-THROUGH$
      case 16:   //A Compact, B Null;  CheckA,  Return (ThA, |A|, F), copyA
      case 24:   //A Ordered, B Null;  CheckA,  Return (ThA, |A|, F), copyA
      case 32:  //A HashTbl, B Null;  CheckA,  Return (ThA, |A|, F), copyA
        Util.checkSeedHashes(seedHash_, a_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = a_.getThetaLong();
        empty_ = false;
        curCount_ = a_.getRetainedEntries(true);
        cache_ = compactCache(a_.getCache(), curCount_, thetaLong_, false);
        break;

      case 18:   //A Compact, B Compact; CheckAB, B -> H; => C,H; scanAllAsearchB()
      case 19:   //A Compact, B Ordered; CheckAB, B -> H; => C,H; scanAllAsearchB()
      case 34:   //A HashTbl, B Compact; CheckAB, B -> H; => H,H; scanAllAsearchB()
      case 35:  //A HashTbl, B Ordered; CheckAB, B -> H; => H,H; scanAllAsearchB()
        Util.checkSeedHashes(seedHash_, a_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        Util.checkSeedHashes(seedHash_, b_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = min(a_.getThetaLong(), b_.getThetaLong());
        empty_ = false;
        convertBtoHT();
        scanAllAsearchB();
        break;

      case 26:   //A Ordered, B Compact; CheckAB, B -> H; => O,H; scanEarlyStopAsearchB()
      case 27:  //A Ordered, B Ordered; CheckAB, B -> H; => O,H; scanEarlyStopAsearchB()
        Util.checkSeedHashes(seedHash_, a_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        Util.checkSeedHashes(seedHash_, b_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = min(a_.getThetaLong(), b_.getThetaLong());
        empty_ = false;
        convertBtoHT();
        scanEarlyStopAsearchB();
        break;

      case 20:   //A Compact, B HashTbl; CheckAB, scanAllAsearchB()
      case 36:  //A HashTbl, B HashTbl; CheckAB, scanAllAsearchB()
        Util.checkSeedHashes(seedHash_, a_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        Util.checkSeedHashes(seedHash_, b_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = min(a_.getThetaLong(), b_.getThetaLong());
        empty_ = false;
        lgArrLongsHT_ = ((UpdateSketch)b_).getLgArrLongs();
        bHashTable_ = b_.getCache();
        scanAllAsearchB();
        break;

      case 28:  //A Ordered, B HashTbl; CheckAB, scanEarlyStopAsearchB()
        Util.checkSeedHashes(seedHash_, a_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        Util.checkSeedHashes(seedHash_, b_.getSeedHash());//lgtm [java/dereferenced-value-may-be-null]
        thetaLong_ = min(a_.getThetaLong(), b_.getThetaLong());
        empty_ = false;
        lgArrLongsHT_ = ((UpdateSketch)b_).getLgArrLongs();
        bHashTable_ = b_.getCache();
        scanEarlyStopAsearchB();
        break;

      //default: //This cannot happen and cannot be tested
    }
  }

  private void convertBtoHT() {
    final int curCountB = b_.getRetainedEntries(true);
    lgArrLongsHT_ = computeMinLgArrLongsFromCount(curCountB);
    bHashTable_ = new long[1 << lgArrLongsHT_];
    hashArrayInsert(b_.getCache(), bHashTable_, lgArrLongsHT_, thetaLong_);
  }

  //Sketch A is either unordered compact or hash table
  private void scanAllAsearchB() {
    final long[] scanAArr = a_.getCache();
    final int arrLongsIn = scanAArr.length;
    cache_ = new long[arrLongsIn];
    for (int i = 0; i < arrLongsIn; i++ ) {
      final long hashIn = scanAArr[i];
      if ((hashIn <= 0L) || (hashIn >= thetaLong_)) { continue; }
      final int foundIdx = hashSearch(bHashTable_, lgArrLongsHT_, hashIn);
      if (foundIdx > -1) { continue; }
      cache_[curCount_++] = hashIn;
    }
  }

  //Sketch A is ordered compact, which enables early stop
  private void scanEarlyStopAsearchB() {
    final long[] scanAArr = a_.getCache();
    final int arrLongsIn = scanAArr.length;
    cache_ = new long[arrLongsIn]; //maybe 2x what is needed, but getRetainedEntries can be slow.
    for (int i = 0; i < arrLongsIn; i++ ) {
      final long hashIn = scanAArr[i];
      if (hashIn <= 0L) { continue; }
      if (hashIn >= thetaLong_) {
        break; //early stop assumes that hashes in input sketch are ordered!
      }
      final int foundIdx = hashSearch(bHashTable_, lgArrLongsHT_, hashIn);
      if (foundIdx > -1) { continue; }
      cache_[curCount_++] = hashIn;
    }
  }

  private void reset() {
    a_ = null;
    b_ = null;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = true;
    cache_ = null;
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
