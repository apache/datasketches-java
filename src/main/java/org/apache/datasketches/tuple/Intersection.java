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

package org.apache.datasketches.tuple;

import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.HashOperations.hashInsertOnly;
import static org.apache.datasketches.HashOperations.hashSearch;
import static org.apache.datasketches.Util.MIN_LG_NOM_LONGS;
import static org.apache.datasketches.Util.ceilingPowerOf2;

import java.lang.reflect.Array;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesStateException;


/**
 * Computes an intersection of two or more generic tuple sketches.
 * A new instance represents the Universal Set. Because the Universal Set
 * cannot be realized a <i>getResult()</i> on a new instance will produce an error.
 * Every update() computes an intersection with the internal state, which will never
 * grow larger and may be reduced to zero.
 *
 * @param <S> Type of Summary
 */
@SuppressWarnings("unchecked")
public class Intersection<S extends Summary> {
  private final SummarySetOperations<S> summarySetOps_;
  private boolean empty_;
  private long thetaLong_;
  private HashTables hashTables_;
  private boolean firstCall_;

  /**
   * Creates new instance
   * @param summarySetOps instance of SummarySetOperations
   */
  public Intersection(final SummarySetOperations<S> summarySetOps) {
    summarySetOps_ = summarySetOps;
    empty_ = false; // universal set at the start
    thetaLong_ = Long.MAX_VALUE;
    hashTables_ = new HashTables();
    firstCall_ = true;
  }

  /**
   * Updates the internal state by intersecting it with the given sketch.
   * @param sketchIn input sketch to intersect with the internal state. It may not be null.
   */
  public void update(final Sketch<S> sketchIn) {
    if (sketchIn == null) { throw new SketchesArgumentException("Sketch may not be null"); }
    final boolean firstCall = firstCall_;
    firstCall_ = false;

    // input sketch could be first or next call
    final long thetaLongIn = sketchIn.getThetaLong();
    final int countIn = sketchIn.getRetainedEntries();
    thetaLong_ = min(thetaLong_, thetaLongIn); //Theta rule
    // Empty rule extended in case incoming sketch does not have empty bit properly set
    empty_ |= (countIn == 0) && (thetaLongIn == Long.MAX_VALUE);
    if (countIn == 0) {
      hashTables_.clear();
      return;
    }
    // input sketch will have valid entries > 0

    if (firstCall) {
      final Sketch<S> firstSketch = sketchIn;
      //Copy firstSketch data into local instance hashTables_
      hashTables_.fromSketch(firstSketch);
    }

    //Next Call
    else {
      if (hashTables_.count_ == 0) {
        return;
      }
      final Sketch<S> nextSketch = sketchIn;
      //Match nextSketch data with local instance data, filtering by theta
      final int maxMatchSize = min(hashTables_.count_, nextSketch.getRetainedEntries());

      final long[] matchHashArr = new long[maxMatchSize];
      S[] matchSummaries = null;
      int matchCount = 0;

      final SketchIterator<S> it = nextSketch.iterator();
      final Class<S> summaryType = (Class<S>) hashTables_.summaryTable_.getClass().getComponentType();
      while (it.next()) {
        final long hash = it.getHash();
        if (hash >= thetaLong_) { continue; }
        final int index = hashSearch(hashTables_.hashTable_, hashTables_.lgTableSize_, hash);
        if (index < 0) { continue; }
        //Copy the intersecting items from local hashTables_
        // sequentially into local matchHashArr_ and matchSummaries_
        final S mySummary = hashTables_.summaryTable_[index];

        if (matchSummaries == null) {
          matchSummaries = (S[]) Array.newInstance(summaryType, maxMatchSize);
        }
        matchHashArr[matchCount] = hash;
        matchSummaries[matchCount] = summarySetOps_.intersection(mySummary, it.getSummary());
        matchCount++;
      }
      hashTables_.fromArrays(matchHashArr, matchSummaries, matchCount);
    }
  }

  /**
   * Updates the internal set by intersecting it with the given Theta sketch.
   * @param sketchIn input Theta Sketch to intersect with the internal state. It may not be null.
   * @param summary the given proxy summary for the Theta Sketch, which doesn't have one.
   * It may not be null.
   */
  public void update(final org.apache.datasketches.theta.Sketch sketchIn, final S summary) {
    if (sketchIn == null) { throw new SketchesArgumentException("Sketch may not be null"); }
    if (summary == null) { throw new SketchesArgumentException("Summary cannot be null."); }
    final boolean firstCall = firstCall_;
    firstCall_ = false;
    // input sketch is not null, could be first or next call
    final long thetaLongIn = sketchIn.getThetaLong();
    final int countIn = sketchIn.getRetainedEntries(true);
    thetaLong_ = min(thetaLong_, thetaLongIn); //Theta rule
    // Empty rule extended in case incoming sketch does not have empty bit properly set
    empty_ |= (countIn == 0) && (thetaLongIn == Long.MAX_VALUE);
    if (countIn == 0) {
      hashTables_.clear();
      return;
    }
    // input sketch will have valid entries > 0
    if (firstCall) {
      final org.apache.datasketches.theta.Sketch firstSketch = sketchIn;
      //Copy firstSketch data into local instance hashTables_
      hashTables_.fromSketch(firstSketch, summary);
    }

    //Next Call
    else {
      if (hashTables_.count_ == 0) {
        return;
      }
      final org.apache.datasketches.theta.Sketch nextSketch = sketchIn;
      //Match nextSketch data with local instance data, filtering by theta
      final int maxMatchSize = min(hashTables_.count_, nextSketch.getRetainedEntries(true));

      final long[] matchHashArr = new long[maxMatchSize];
      S[] matchSummaries = null;
      int matchCount = 0;

      final org.apache.datasketches.theta.HashIterator it = sketchIn.iterator();
      final Class<S> summaryType = (Class<S>) hashTables_.summaryTable_.getClass().getComponentType();
      while (it.next()) {
        final long hash = it.get();
        if (hash >= thetaLong_) { continue; }
        final int index = hashSearch(hashTables_.hashTable_, hashTables_.lgTableSize_, hash);
        if (index < 0) { continue; }
        //Copy the intersecting items from local hashTables_
        // sequentially into local matchHashArr and matchSummaries
        final S mySummary = hashTables_.summaryTable_[index];

        if (matchSummaries == null) {
          matchSummaries = (S[]) Array.newInstance(summaryType, maxMatchSize);
        }
        matchHashArr[matchCount] = hash;
        matchSummaries[matchCount] = summarySetOps_.intersection(mySummary, (S)mySummary.copy());
        matchCount++;
      }
      hashTables_.fromArrays(matchHashArr, matchSummaries, matchCount);
    }
  }

  /**
   * Gets the internal set as a CompactSketch
   * @return result of the intersections so far
   */
  public CompactSketch<S> getResult() {
    if (firstCall_) {
      throw new SketchesStateException(
        "getResult() with no intervening intersections is not a legal result.");
    }
    if (hashTables_.count_ == 0) {
      return new CompactSketch<>(null, null, thetaLong_, empty_);
    }
    //Compact the hash tables
    final int tableSize = hashTables_.hashTable_.length;
    final long[] hashArr = new long[hashTables_.count_];
    S[] summaries = null;
    final Class<S> summaryType = (Class<S>) hashTables_.summaryTable_.getClass().getComponentType();
    int cnt = 0;
    for (int i = 0; i < tableSize; i++) {
      final long hash = hashTables_.hashTable_[i];
      if ((hash == 0) || (hash > thetaLong_)) { continue; }
      final S summary = hashTables_.summaryTable_[i];
      if (summaries == null) {
        summaries = (S[]) Array.newInstance(summaryType, hashTables_.count_);
      }
      hashArr[cnt] = hash;
      summaries[cnt] = summary;
      cnt++;
    }
    assert cnt == hashTables_.count_;
    return new CompactSketch<>(hashArr, summaries, thetaLong_, empty_);
  }

  /**
   * Resets the internal set to the initial state, which represents the Universal Set
   */
  public void reset() {
    empty_ = false;
    thetaLong_ = Long.MAX_VALUE;
    hashTables_.clear();
    firstCall_ = true;
  }

  private static int getLgTableSize(final int count) {
    final int tableSize = max(ceilingPowerOf2((int) ceil(count / 0.75)), 1 << MIN_LG_NOM_LONGS);
    return Integer.numberOfTrailingZeros(tableSize);
  }

  private class HashTables {
    long[] hashTable_ = null;
    S[] summaryTable_ = null;
    int lgTableSize_ = 0;
    int count_ = 0;

    HashTables() { }

    void fromSketch(final Sketch<S> sketch) {
      count_ = sketch.getRetainedEntries();
      lgTableSize_ = getLgTableSize(count_);
      S mySummary = null;

      hashTable_ = new long[1 << lgTableSize_];
      final SketchIterator<S> it = sketch.iterator();
      while (it.next()) {
        final long hash = it.getHash();
        final int index = hashInsertOnly(hashTable_, lgTableSize_, hash);
        mySummary = (S)it.getSummary().copy();
        if (summaryTable_ == null) {
          summaryTable_ = (S[]) Array.newInstance(mySummary.getClass(), 1 << lgTableSize_);
        }
        summaryTable_[index] = mySummary;
      }
    }

    void fromSketch(final org.apache.datasketches.theta.Sketch sketch, final S summary) {
      count_ = sketch.getRetainedEntries(true);
      lgTableSize_ = getLgTableSize(count_);
      S mySummary = null;

      hashTable_ = new long[1 << lgTableSize_];
      final org.apache.datasketches.theta.HashIterator it = sketch.iterator();
      while (it.next()) {
        final long hash = it.get();
        final int index = hashInsertOnly(hashTable_, lgTableSize_, hash);
        mySummary = summary;
        if (summaryTable_ == null) {
          summaryTable_ = (S[]) Array.newInstance(mySummary.getClass(), 1 << lgTableSize_);
        }
        summaryTable_[index] = mySummary;
      }
    }


    void fromArrays(final long[] hashArr, final S[] summaryArr, final int count) {
      count_ = count;
      lgTableSize_ = getLgTableSize(count);

      S mySummary = null;
      summaryTable_ = null;
      hashTable_ = new long[1 << lgTableSize_];
      for (int i = 0; i < count; i++) {
        final long hash = hashArr[i];
        final int index = hashInsertOnly(hashTable_, lgTableSize_, hash);
        mySummary = summaryArr[i];
        if (summaryTable_ == null) {
          summaryTable_ = (S[]) Array.newInstance(mySummary.getClass(), 1 << lgTableSize_);
        }
        summaryTable_[index] = summaryArr[i];
      }
    }

    void clear() {
      hashTable_ = null;
      summaryTable_ = null;
      lgTableSize_ = 0;
      count_ = 0;
    }
  }

}
