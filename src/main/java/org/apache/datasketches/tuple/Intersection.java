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
import static org.apache.datasketches.Util.MIN_LG_NOM_LONGS;
import static org.apache.datasketches.Util.ceilingPowerOf2;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesStateException;


/**
 * Computes an intersection of two or more generic tuple sketches or generic tuple sketches
 * combined with theta sketches.
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
  private HashTables<S> hashTables_;
  private boolean firstCall_;

  /**
   * Creates new Intersection instance with instructions on how to process two summaries that
   * intersect.
   * @param summarySetOps instance of SummarySetOperations
   */
  public Intersection(final SummarySetOperations<S> summarySetOps) {
    summarySetOps_ = summarySetOps;
    empty_ = false; // universal set at the start
    thetaLong_ = Long.MAX_VALUE;
    hashTables_ = new HashTables<>();
    firstCall_ = true;
  }

  /**
   * Perform a stateless intersect set operation on the two given tuple sketches and returns the
   * result as an unordered CompactSketch on the heap.
   * @param tupleSketchA The first sketch argument.  It must not be null.
   * @param tupleSketchB The second sketch argument.  It must not be null.
   * @return an unordered CompactSketch on the heap
   */
  public CompactSketch<S> intersect(
      final Sketch<S> tupleSketchA,
      final Sketch<S> tupleSketchB) {
    reset();
    intersect(tupleSketchA);
    intersect(tupleSketchB);
    final CompactSketch<S> csk = getResult();
    reset();
    return csk;
  }

  /**
   * Perform a stateless intersect set operation on a tuple sketch and a theta sketch and returns the
   * result as an unordered CompactSketch on the heap.
   * @param tupleSketch The first sketch argument. It must not be null.
   * @param thetaSketch The second sketch argument. It must not be null.
   * @param summary the given proxy summary for the theta sketch, which doesn't have one.
   * This must not be null.
   * @return an unordered CompactSketch on the heap
   */
  public CompactSketch<S> intersect(
      final Sketch<S> tupleSketch,
      final org.apache.datasketches.theta.Sketch
      thetaSketch, final S summary) {
    reset();
    intersect(tupleSketch);
    intersect(thetaSketch, summary);
    final CompactSketch<S> csk = getResult();
    reset();
    return csk;
  }

  /**
   * Performs a stateful intersection of the internal set with the given tupleSketch.
   * @param tupleSketch input sketch to intersect with the internal state. It must not be null.
   */
  public void intersect(final Sketch<S> tupleSketch) {
    if (tupleSketch == null) { throw new SketchesArgumentException("Sketch must not be null"); }

    final boolean firstCall = firstCall_;
    firstCall_ = false;

    // input sketch could be first or next call

    final boolean emptyIn = tupleSketch.isEmpty();
    if (empty_ || emptyIn) { //empty rule
      //Whatever the current internal state, we make our local empty.
      resetToEmpty();
      return;
    }

    final long thetaLongIn = tupleSketch.getThetaLong();
    thetaLong_ = min(thetaLong_, thetaLongIn); //Theta rule

    if (tupleSketch.getRetainedEntries() == 0) {
      hashTables_.clear();
      return;
    }
    // input sketch will have valid entries > 0

    if (firstCall) {
      //Copy firstSketch data into local instance hashTables_
      hashTables_.fromSketch(tupleSketch);
    }

    //Next Call
    else {
      if (hashTables_.numKeys == 0) { return; }
      //process intersect with current hashTables
      hashTables_ = hashTables_.getIntersectHashTables(tupleSketch, thetaLong_, summarySetOps_);
    }
  }

  /**
   * Performs a stateful intersection of the internal set with the given thetaSketch by combining entries
   * using the hashes from the theta sketch and summary values from the given summary and rules
   * from the summarySetOps defined by the Intersection constructor.
   * @param thetaSketch input theta sketch to intersect with the internal state. It must not be null.
   * @param summary the given proxy summary for the theta sketch, which doesn't have one.
   * It will be copied for each matching index. It must not be null.
   */
  public void intersect(final org.apache.datasketches.theta.Sketch thetaSketch, final S summary) {
    if (thetaSketch == null) { throw new SketchesArgumentException("Sketch must not be null"); }
    if (summary == null) { throw new SketchesArgumentException("Summary cannot be null."); }
    final boolean firstCall = firstCall_;
    firstCall_ = false;
    // input sketch is not null, could be first or next call

    final boolean emptyIn = thetaSketch.isEmpty();
    if (empty_ || emptyIn) { //empty rule
      //Whatever the current internal state, we make our local empty.
      resetToEmpty();
      return;
    }

    final long thetaLongIn = thetaSketch.getThetaLong();
    thetaLong_ = min(thetaLong_, thetaLongIn); //Theta rule

    final int countIn = thetaSketch.getRetainedEntries();
    if (countIn == 0) {
      hashTables_.clear();
      return;
    }
    // input sketch will have valid entries > 0

    if (firstCall) {
      final org.apache.datasketches.theta.Sketch firstSketch = thetaSketch;
      //Copy firstSketch data into local instance hashTables_
      hashTables_.fromSketch(firstSketch, summary);
    }

    //Next Call
    else {
      if (hashTables_.numKeys == 0) { return; }
      hashTables_ = hashTables_.getIntersectHashTables(thetaSketch, thetaLongIn, summarySetOps_, summary);
    }
  }

  /**
   * Gets the internal set as an unordered CompactSketch
   * @return result of the intersections so far
   */
  public CompactSketch<S> getResult() {
    if (firstCall_) {
      throw new SketchesStateException(
        "getResult() with no intervening intersections is not a legal result.");
    }
    final int countIn = hashTables_.numKeys;
    if (countIn == 0) {
      return new CompactSketch<>(null, null, thetaLong_, empty_);
    }

    final int tableSize = hashTables_.hashTable.length;

    final long[] hashArr = new long[countIn];
    final S[] summaryArr = Util.newSummaryArray(hashTables_.summaryTable, countIn);

    //compact the arrays
    int cnt = 0;
    for (int i = 0; i < tableSize; i++) {
      final long hash = hashTables_.hashTable[i];
      if (hash == 0 || hash > thetaLong_) { continue; }
      hashArr[cnt] = hash;
      summaryArr[cnt] = (S) hashTables_.summaryTable[i].copy();
      cnt++;
    }
    assert cnt == countIn;
    return new CompactSketch<>(hashArr, summaryArr, thetaLong_, empty_);
  }

  /**
   * Returns true if there is a valid intersection result available
   * @return true if there is a valid intersection result available
   */
  public boolean hasResult() {
    return !firstCall_;
  }

  /**
   * Resets the internal set to the initial state, which represents the Universal Set
   */
  public void reset() {
    hardReset();
  }

  private void hardReset() {
    empty_ = false;
    thetaLong_ = Long.MAX_VALUE;
    hashTables_.clear();
    firstCall_ = true;
  }

  private void resetToEmpty() {
    empty_ = true;
    thetaLong_ = Long.MAX_VALUE;
    hashTables_.clear();
    firstCall_ = false;
  }

  static int getLgTableSize(final int count) {
    final int tableSize = max(ceilingPowerOf2((int) ceil(count / 0.75)), 1 << MIN_LG_NOM_LONGS);
    return Integer.numberOfTrailingZeros(tableSize);
  }

}
