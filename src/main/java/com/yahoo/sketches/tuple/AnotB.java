/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.ceilingPowerOf2;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * This is to compute a set difference of two tuple sketches
 */
public class AnotB<S extends Summary> {
  private boolean isEmpty_ = true;
  private long theta_ = Long.MAX_VALUE;
  private long[] keys_;
  private S[] summaries_;
  private int count;

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
  public void update(Sketch<S> a, Sketch<S> b) {
    if (a != null) isEmpty_ = a.isEmpty(); // stays this way even if we end up with no entries in the result
    long thetaA = a == null ? Long.MAX_VALUE : a.getThetaLong();
    long thetaB = b == null ? Long.MAX_VALUE : b.getThetaLong();
    theta_ = Math.min(thetaA, thetaB);
    if (a == null || a.getRetainedEntries() == 0) return;
    if (b == null || b.getRetainedEntries() == 0) {
      getNoMatchSetFromSketch(a);
    } else {
      long[] hashTable;
      if (b instanceof CompactSketch) {
        hashTable = convertToHashTable(b);
      } else {
        hashTable = b.keys_;
      }
      int lgHashTableSize = Integer.numberOfTrailingZeros(hashTable.length);
      int noMatchSize = a.getRetainedEntries();
      keys_ = new long[noMatchSize];
      summaries_ = (S[]) Array.newInstance(a.summaries_.getClass().getComponentType(), noMatchSize);
      for (int i = 0; i < a.keys_.length; i++) {
        if (a.keys_[i] != 0) {
          int index = HashOperations.hashSearch(hashTable, lgHashTableSize, a.keys_[i]);
          if (index == -1) {
            keys_[count] = a.keys_[i];
            summaries_[count] = a.summaries_[i];
            count++;
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
    if (count == 0) return new CompactSketch<S>(null, null, theta_, isEmpty_);
    CompactSketch<S> result = new CompactSketch<S>(Arrays.copyOfRange(keys_, 0, count), Arrays.copyOfRange(summaries_, 0, count), theta_, isEmpty_);
    reset();
    return result;
  }

  private long[] convertToHashTable(Sketch<S> sketch) {
    int size = Math.max(
      ceilingPowerOf2((int) Math.ceil(sketch.getRetainedEntries() / QuickSelectSketch.REBUILD_RATIO_AT_TARGET_SIZE)),
      QuickSelectSketch.MIN_NOM_ENTRIES
    );
    long[] hashTable = new long[size];
    HashOperations.hashArrayInsert(sketch.keys_, hashTable, Integer.numberOfTrailingZeros(size), theta_);
    return hashTable;
  }

  private void reset() {
    isEmpty_ = true;
    theta_ = Long.MAX_VALUE;
    keys_ = null;
    summaries_ = null;
    count = 0;
  }

  private void getNoMatchSetFromSketch(Sketch<S> sketch) {
    if (sketch instanceof CompactSketch) {
      keys_ = sketch.keys_.clone();
      summaries_ = sketch.summaries_.clone();
    } else { // assuming only two types: CompactSketch and QuickSelectSketch
      CompactSketch<S> compact = ((QuickSelectSketch<S>)sketch).compact();
      keys_ = compact.keys_;
      summaries_ = compact.summaries_;
    }
    count = sketch.getRetainedEntries();
  }
}
