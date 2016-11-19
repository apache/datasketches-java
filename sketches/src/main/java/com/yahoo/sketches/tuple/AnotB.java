/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.MIN_LG_NOM_LONGS;
import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.Util.ceilingPowerOf2;

import java.lang.reflect.Array;
import java.util.Arrays;

import com.yahoo.sketches.HashOperations;

/**
 * Computes a set difference of two generic tuple sketches
 * @param <S> Type of Summary
 */
public final class AnotB<S extends Summary> {
  private boolean isEmpty_ = true;
  private long theta_ = Long.MAX_VALUE;
  private long[] keys_;
  private S[] summaries_;
  private int count_;

  /**
   * Perform A-and-not-B set operation on the two given sketches.
   * A null sketch is interpreted as an empty sketch.
   * This is not an accumulating update. Calling update() more than once
   * without calling getResult() will discard the result of previous update()
   *
   * @param a The incoming sketch for the first argument
   * @param b The incoming sketch for the second argument
   */
  @SuppressWarnings("unchecked")
  public void update(final Sketch<S> a, final Sketch<S> b) {
    if (a != null) { isEmpty_ = a.isEmpty(); } //stays this way even if we end up with no result entries
    final long thetaA = a == null ? Long.MAX_VALUE : a.getThetaLong();
    final long thetaB = b == null ? Long.MAX_VALUE : b.getThetaLong();
    theta_ = Math.min(thetaA, thetaB);
    if (a == null || a.getRetainedEntries() == 0) { return; }
    if (b == null || b.getRetainedEntries() == 0) {
      getNoMatchSetFromSketch(a);
    } else {
      final long[] hashTable;
      if (b instanceof CompactSketch) {
        hashTable = convertToHashTable(b);
      } else {
        hashTable = b.keys_;
      }
      final int lgHashTableSize = Integer.numberOfTrailingZeros(hashTable.length);
      final int noMatchSize = a.getRetainedEntries();
      keys_ = new long[noMatchSize];
      summaries_ = (S[]) Array.newInstance(a.summaries_.getClass().getComponentType(), noMatchSize);
      for (int i = 0; i < a.keys_.length; i++) {
        if (a.keys_[i] != 0) {
          final int index = HashOperations.hashSearch(hashTable, lgHashTableSize, a.keys_[i]);
          if (index == -1) {
            keys_[count_] = a.keys_[i];
            summaries_[count_] = a.summaries_[i];
            count_++;
          }
        }
      }
    }
  }

  /**
   * Gets the result of this operation
   * @return the result of this operation as a CompactSketch
   */
  public CompactSketch<S> getResult() {
    if (count_ == 0) {
      return new CompactSketch<S>(null, null, theta_, isEmpty_);
    }
    final CompactSketch<S> result =
        new CompactSketch<S>(Arrays.copyOfRange(keys_, 0, count_),
            Arrays.copyOfRange(summaries_, 0, count_), theta_, isEmpty_);
    reset();
    return result;
  }

  private long[] convertToHashTable(final Sketch<S> sketch) {
    final int size = Math.max(
      ceilingPowerOf2((int) Math.ceil(sketch.getRetainedEntries() / REBUILD_THRESHOLD)),
      1 << MIN_LG_NOM_LONGS
    );
    final long[] hashTable = new long[size];
    HashOperations.hashArrayInsert(
        sketch.keys_, hashTable, Integer.numberOfTrailingZeros(size), theta_);
    return hashTable;
  }

  private void reset() {
    isEmpty_ = true;
    theta_ = Long.MAX_VALUE;
    keys_ = null;
    summaries_ = null;
    count_ = 0;
  }

  private void getNoMatchSetFromSketch(final Sketch<S> sketch) {
    if (sketch instanceof CompactSketch) {
      keys_ = sketch.keys_.clone();
      summaries_ = sketch.summaries_.clone();
    } else { // assuming only two types: CompactSketch and QuickSelectSketch
      final CompactSketch<S> compact = ((QuickSelectSketch<S>)sketch).compact();
      keys_ = compact.keys_;
      summaries_ = compact.summaries_;
    }
    count_ = sketch.getRetainedEntries();
  }
}
