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

import static java.lang.Math.min;
import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;

import org.apache.datasketches.QuickSelect;
import org.apache.datasketches.SketchesArgumentException;

/**
 * Compute the union of two or more generic tuple sketches or generic tuple sketches combined with
 * theta sketches. A new instance represents an empty set.
 * @param <S> Type of Summary
 */
public class Union<S extends Summary> {
  private final SummarySetOperations<S> summarySetOps_;
  private QuickSelectSketch<S> qsk_;
  private long unionThetaLong_; // need to maintain outside of the sketch
  private boolean empty_;

  /**
   * Creates new Union instance with instructions on how to process two summaries that
   * overlap. This will have the default nominal entries (K).
   * @param summarySetOps instance of SummarySetOperations
   */
  public Union(final SummarySetOperations<S> summarySetOps) {
    this(DEFAULT_NOMINAL_ENTRIES, summarySetOps);
  }

  /**
   * Creates new Union instance.
   * @param nomEntries nominal entries (K). Forced to the nearest power of 2 greater than
   * given value.
   * @param summarySetOps instance of SummarySetOperations
   */
  public Union(final int nomEntries, final SummarySetOperations<S> summarySetOps) {
    summarySetOps_ = summarySetOps;
    qsk_ = new QuickSelectSketch<>(nomEntries, null);
    unionThetaLong_ = qsk_.getThetaLong();
    empty_ = true;
  }

  /**
   * Perform a stateless, pair-wise union operation between two tuple sketches.
   * The returned sketch will be cut back to the smaller of the two k values if required.
   *
   * <p>Nulls and empty sketches are ignored.</p>
   *
   * @param tupleSketchA The first argument
   * @param tupleSketchB The second argument
   * @return the result ordered CompactSketch on the heap.
   */
  public CompactSketch<S> union(final Sketch<S> tupleSketchA, final Sketch<S> tupleSketchB) {
    reset();
    union(tupleSketchA);
    union(tupleSketchB);
    final CompactSketch<S> csk = getResult(true);
    return csk;
  }

  /**
   * Perform a stateless, pair-wise union operation between a tupleSketch and a thetaSketch.
   * The returned sketch will be cut back to the smaller of the two k values if required.
   *
   * <p>Nulls and empty sketches are ignored.</p>
   *
   * @param tupleSketch The first argument
   * @param thetaSketch The second argument
   * @param summary the given proxy summary for the theta sketch, which doesn't have one.
   * This may not be null.
   * @return the result ordered CompactSketch on the heap.
   */
  public CompactSketch<S> union(final Sketch<S> tupleSketch,
      final org.apache.datasketches.theta.Sketch thetaSketch, final S summary) {
    reset();
    union(tupleSketch);
    union(thetaSketch, summary);
    final CompactSketch<S> csk = getResult(true);
    return csk;
  }

  /**
   * Performs a stateful union of the internal set with the given tupleSketch.
   * @param tupleSketch input tuple sketch to merge with the internal set.
   *
   * <p>Nulls and empty sketches are ignored.</p>
   */
  public void union(final Sketch<S> tupleSketch) {
    if (tupleSketch == null || tupleSketch.isEmpty()) { return; }
    empty_ = false;
    unionThetaLong_ = min(tupleSketch.thetaLong_, unionThetaLong_);
    final SketchIterator<S> it = tupleSketch.iterator();
    while (it.next()) {
      qsk_.merge(it.getHash(), it.getSummary(), summarySetOps_);
    }
    unionThetaLong_ = min(unionThetaLong_, qsk_.thetaLong_);
  }

  /**
   * Performs a stateful union of the internal set with the given thetaSketch by combining entries
   * using the hashes from the theta sketch and summary values from the given summary.
   * @param thetaSketch the given theta sketch input. If null or empty, it is ignored.
   * @param summary the given proxy summary for the theta sketch, which doesn't have one. This may
   * not be null.
   */
  public void union(final org.apache.datasketches.theta.Sketch thetaSketch, final S summary) {
    if (summary == null) {
      throw new SketchesArgumentException("Summary cannot be null."); }
    if (thetaSketch == null || thetaSketch.isEmpty()) { return; }
    empty_ = false;
    final long thetaIn = thetaSketch.getThetaLong();
    unionThetaLong_ = min(thetaIn, unionThetaLong_);
    final org.apache.datasketches.theta.HashIterator it = thetaSketch.iterator();
    while (it.next()) {
      qsk_.merge(it.get(), summary, summarySetOps_); //copies summary
    }
    unionThetaLong_ = min(unionThetaLong_, qsk_.thetaLong_);
  }

  /**
   * Gets the result of a sequence of stateful <i>union</i> operations as an unordered CompactSketch
   * @return result of the stateful unions so far. The state of this operation is not reset after the
   * result is returned.
   */
  public CompactSketch<S> getResult() {
    return getResult(false);
  }

  /**
   * Gets the result of a sequence of stateful <i>union</i> operations as an unordered CompactSketch.
   * @param reset If <i>true</i>, clears this operator to the empty state after this result is
   * returned. Set this to <i>false</i> if you wish to obtain an intermediate result.
   * @return result of the stateful union
   */
  @SuppressWarnings("unchecked")
  public CompactSketch<S> getResult(final boolean reset) {
    final CompactSketch<S> result;
    if (empty_) {
      result = qsk_.compact();
    } else if (unionThetaLong_ >= qsk_.thetaLong_ && qsk_.getRetainedEntries() <= qsk_.getNominalEntries()) {
      //unionThetaLong_ >= qsk_.thetaLong_ means we can ignore unionThetaLong_. We don't need to rebuild.
      //qsk_.getRetainedEntries() <= qsk_.getNominalEntries() means we don't need to pull back to k.
      result = qsk_.compact();
    } else {
      final long tmpThetaLong = min(unionThetaLong_, qsk_.thetaLong_);

      //count the number of valid hashes in because Alpha can have dirty values
      int numHashesIn = 0;
      SketchIterator<S> it = qsk_.iterator();
      while (it.next()) { //counts valid hashes
        if (it.getHash() < tmpThetaLong) { numHashesIn++; }
      }

      if (numHashesIn == 0) {
        //numHashes == 0 && empty == false means Theta < 1.0
        //Therefore, this is a degenerate sketch: theta < 1.0, count = 0, empty = false
        result = new CompactSketch<>(null, null, tmpThetaLong, empty_);
      }

      else {
        //we know: empty == false, count > 0
        final int numHashesOut;
        final long thetaLongOut;
        if (numHashesIn > qsk_.getNominalEntries()) {
          //we need to trim hashes and need a new thetaLong
          final long[] tmpHashArr = new long[numHashesIn]; // temporary, order will be destroyed by quick select
          it = qsk_.iterator();
          int i = 0;
          while (it.next()) {
            final long hash = it.getHash();
            if (hash < tmpThetaLong) { tmpHashArr[i++] = hash; }
          }
          numHashesOut = qsk_.getNominalEntries();
          thetaLongOut = QuickSelect.select(tmpHashArr, 0, numHashesIn - 1, numHashesOut);
        } else {
          numHashesOut = numHashesIn;
          thetaLongOut = tmpThetaLong;
        }
        //now prepare the output arrays
        final long[] hashArr = new long[numHashesOut];
        final S[] summaries = Util.newSummaryArray(qsk_.getSummaryTable(), numHashesOut);
        it = qsk_.iterator();
        int i = 0;
        while (it.next()) { //select the qualifying hashes from the gadget synchronized with the summaries
          final long hash = it.getHash();
          if (hash < thetaLongOut) {
            hashArr[i] = hash;
            summaries[i] = (S) it.getSummary().copy();
            i++;
          }
        }
        result = new CompactSketch<>(hashArr, summaries, thetaLongOut, empty_);
      }
    }
    if (reset) { reset(); }
    return result;
  }

  /**
   * Resets the internal set to the initial state, which represents an empty set. This is only useful
   * after sequences of stateful union operations.
   */
  public void reset() {
    qsk_.reset();
    unionThetaLong_ = qsk_.getThetaLong();
    empty_ = true;
  }
}
