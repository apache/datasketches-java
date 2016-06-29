/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import java.util.Arrays;

import com.yahoo.sketches.memory.Memory;

import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.Util.ceilingPowerOf2;

/**
 * This is an on-heap implementation
 */
class HeapArrayOfDoublesAnotB extends ArrayOfDoublesAnotB {

  private boolean isEmpty_ = true;
  private long theta_ = Long.MAX_VALUE;
  private long[] keys_;
  private double[] values_;
  private int count_;
  private final short seedHash_;
  private final int numValues_;
  /**
   * Creates an instance of HeapArrayOfDoublesAnotB given a custom seed
   * @param numValues Number of double values to keep for each key.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  HeapArrayOfDoublesAnotB(final int numValues, final long seed) {
    numValues_ = numValues;
    seedHash_ = Util.computeSeedHash(seed);
  }

  @Override
  public void update(final ArrayOfDoublesSketch a, final ArrayOfDoublesSketch b) {
    if (a != null) Util.checkSeedHashes(seedHash_, a.getSeedHash());
    if (b != null) Util.checkSeedHashes(seedHash_, b.getSeedHash());
    if (a != null) isEmpty_ = a.isEmpty(); // stays this way even if we end up with no entries in the result
    final long thetaA = a == null ? Long.MAX_VALUE : a.getThetaLong();
    final long thetaB = b == null ? Long.MAX_VALUE : b.getThetaLong();
    theta_ = Math.min(thetaA, thetaB);
    if (a == null || a.getRetainedEntries() == 0) return;
    if (b == null || b.getRetainedEntries() == 0) {
      getNoMatchSetFromSketch(a);
    } else {
      final long[] hashTable;
      hashTable = convertToHashTable(b);
      final int lgHashTableSize = Integer.numberOfTrailingZeros(hashTable.length);
      final int noMatchSize = a.getRetainedEntries();
      keys_ = new long[noMatchSize];
      values_ = new double[noMatchSize * numValues_];
      ArrayOfDoublesSketchIterator it = a.iterator();
      while (it.next()) {
        final int index = HashOperations.hashSearch(hashTable, lgHashTableSize, it.getKey());
        if (index == -1) {
          keys_[count_] = it.getKey();
          System.arraycopy(it.getValues(), 0, values_, count_ * numValues_, numValues_);
          count_++;
        }
      }
    }
  }

  @Override
  public ArrayOfDoublesCompactSketch getResult() {
    if (count_ == 0) return new HeapArrayOfDoublesCompactSketch(null, null, Long.MAX_VALUE, true, numValues_, seedHash_);
    ArrayOfDoublesCompactSketch result = new HeapArrayOfDoublesCompactSketch(
      Arrays.copyOfRange(keys_, 0, count_),
      Arrays.copyOfRange(values_, 0, count_ * numValues_),
      theta_,
      isEmpty_,
      numValues_,
      seedHash_
    );
    reset();
    return result;
  }

  @Override
  public ArrayOfDoublesCompactSketch getResult(final Memory mem) {
    if (mem == null || count_ == 0) return getResult();
    ArrayOfDoublesCompactSketch result = new DirectArrayOfDoublesCompactSketch(
      Arrays.copyOfRange(keys_, 0, count_),
      Arrays.copyOfRange(values_, 0, count_ * numValues_),
      theta_,
      isEmpty_,
      numValues_,
      seedHash_,
      mem
    );
    reset();
    return result;
  }

  private static long[] convertToHashTable(final ArrayOfDoublesSketch sketch) {
    final int size = Math.max(
      ceilingPowerOf2((int) Math.ceil(sketch.getRetainedEntries() / REBUILD_THRESHOLD)),
      ArrayOfDoublesQuickSelectSketch.MIN_NOM_ENTRIES
    );
    final long[] hashTable = new long[size];
    ArrayOfDoublesSketchIterator it = sketch.iterator();
    final int lgSize = Integer.numberOfTrailingZeros(size);
    while (it.next()) {
      HashOperations.hashInsertOnly(hashTable, lgSize, it.getKey());
    }
    return hashTable;
  }

  private void reset() {
    isEmpty_ = true;
    theta_ = Long.MAX_VALUE;
    keys_ = null;
    values_ = null;
    count_ = 0;
  }

  private void getNoMatchSetFromSketch(final ArrayOfDoublesSketch sketch) {
    count_ = sketch.getRetainedEntries();
    keys_ = new long[count_];
    values_ = new double[count_ * numValues_];
    ArrayOfDoublesSketchIterator it = sketch.iterator();
    int i = 0;
    while (it.next()) {
      keys_[i] = it.getKey();
      System.arraycopy(it.getValues(), 0, values_, i * numValues_, numValues_);
      i++;
    }
  }

}
