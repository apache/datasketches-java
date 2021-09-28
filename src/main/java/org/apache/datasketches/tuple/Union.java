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

import java.lang.reflect.Array;

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
  private long thetaLong_; // need to maintain outside of the sketch
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
   * Creates new Union instance with instructions on how to process two summaries that
   * overlap.
   * @param nomEntries nominal entries (K). Forced to the nearest power of 2 greater than
   * given value.
   * @param summarySetOps instance of SummarySetOperations
   */
  public Union(final int nomEntries, final SummarySetOperations<S> summarySetOps) {
    summarySetOps_ = summarySetOps;
    qsk_ = new QuickSelectSketch<>(nomEntries, null);
    thetaLong_ = qsk_.getThetaLong();
    empty_ = true;
  }

  /**
   * Perform a stateless, pair-wise union operation between two tuple sketches.
   * The returned sketch will be cutback to the smaller of the two k values if required.
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
    final CompactSketch<S> csk = getResult();
    reset();
    return csk;
  }

  /**
   * Perform a stateless, pair-wise union operation between a tupleSketch and a thetaSketch.
   * The returned sketch will be cutback to the smaller of the two k values if required.
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
    final CompactSketch<S> csk = getResult();
    reset();
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
    if (tupleSketch.thetaLong_ < thetaLong_) { thetaLong_ = tupleSketch.thetaLong_; }
    final SketchIterator<S> it = tupleSketch.iterator();
    while (it.next()) {
      qsk_.merge(it.getHash(), it.getSummary(), summarySetOps_);
    }
    if (qsk_.thetaLong_ < thetaLong_) {
      thetaLong_ = qsk_.thetaLong_;
    }
  }

  /**
   * Performs a stateful union of the internal set with the given thetaSketch by combining entries
   * using the hashes from the theta sketch and summary values from the given summary and rules
   * from the summarySetOps defined by the Union constructor.
   * @param thetaSketch the given theta sketch input. If null or empty, it is ignored.
   * @param summary the given proxy summary for the theta sketch, which doesn't have one. This may
   * not be null.
   * @deprecated 2.0.0. Please use union(org.apache.datasketches.theta.Sketch, S).
   */
  @Deprecated //note the {at_link} does not work in the above
  public void update(final org.apache.datasketches.theta.Sketch thetaSketch, final S summary) {
    union(thetaSketch, summary);
  }

  /**
   * Performs a stateful union of the internal set with the given thetaSketch by combining entries
   * using the hashes from the theta sketch and summary values from the given summary and rules
   * from the summarySetOps defined by the Union constructor.
   * @param thetaSketch the given theta sketch input. If null or empty, it is ignored.
   * @param summary the given proxy summary for the theta sketch, which doesn't have one. This may
   * not be null.
   */
  @SuppressWarnings("unchecked")
  public void union(final org.apache.datasketches.theta.Sketch thetaSketch, final S summary) {
    if (summary == null) {
      throw new SketchesArgumentException("Summary cannot be null."); }
    if (thetaSketch == null || thetaSketch.isEmpty()) { return; }
    empty_ = false;
    final long thetaIn = thetaSketch.getThetaLong();
    if (thetaIn < thetaLong_) { thetaLong_ = thetaIn; }
    final org.apache.datasketches.theta.HashIterator it = thetaSketch.iterator();
    while (it.next()) {
      qsk_.merge(it.get(), (S)summary.copy(), summarySetOps_);
    }
    if (qsk_.thetaLong_ < thetaLong_) {
      thetaLong_ = qsk_.thetaLong_;
    }
  }

  /**
   * Gets the result of a sequence of stateful <i>union</i> operations as an unordered CompactSketch
   * @return result of the stateful unions so far
   */
  @SuppressWarnings("unchecked")
  public CompactSketch<S> getResult() {
    if (empty_) {
      return qsk_.compact();
    }
    if (thetaLong_ >= qsk_.thetaLong_ && qsk_.getRetainedEntries() <= qsk_.getNominalEntries()) {
      return qsk_.compact();
    }
    long theta = min(thetaLong_, qsk_.thetaLong_);

    int numHashes = 0;
    {
      final SketchIterator<S> it = qsk_.iterator();
      while (it.next()) {
        if (it.getHash() < theta) { numHashes++; }
      }
    }
    if (numHashes == 0) {
      return new CompactSketch<>(null, null, theta, empty_);
    }
    if (numHashes > qsk_.getNominalEntries()) {
      final long[] hashArr = new long[numHashes]; // temporary, order will be destroyed by quick select
      final SketchIterator<S> it = qsk_.iterator();
      int i = 0;
      while (it.next()) {
        final long hash = it.getHash();
        if (hash < theta) { hashArr[i++] = hash; }
      }
      theta = QuickSelect.select(hashArr, 0, numHashes - 1, qsk_.getNominalEntries());
      numHashes = qsk_.getNominalEntries();
    }
    final Class<S> summaryType = (Class<S>) qsk_.getSummaryTable().getClass().getComponentType();
    final long[] hashArr = new long[numHashes];
    final S[] summaries = (S[]) Array.newInstance(summaryType, numHashes);
    final SketchIterator<S> it = qsk_.iterator();
    int i = 0;
    while (it.next()) {
      final long hash = it.getHash();
      if (hash < theta) {
        hashArr[i] = hash;
        summaries[i] = (S) it.getSummary().copy();
        i++;
      }
    }
    return new CompactSketch<>(hashArr, summaries, theta, empty_);
  }

  /**
   * Resets the internal set to the initial state, which represents an empty set. This is only useful
   * after sequences of stateful union operations.
   */
  public void reset() {
    qsk_.reset();
    thetaLong_ = qsk_.getThetaLong();
    empty_ = true;
  }
}
