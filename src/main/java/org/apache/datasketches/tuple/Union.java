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
 * Compute a union of two or more tuple sketches.
 * A new instance represents an empty set.
 * Every update() computes a union with the internal set
 * and can only grow the internal set.
 * @param <S> Type of Summary
 */
public class Union<S extends Summary> {
  private final SummarySetOperations<S> summarySetOps_;
  private QuickSelectSketch<S> qsk_;
  private long thetaLong_; // need to maintain outside of the sketch
  private boolean empty_;

  /**
   * Creates new instance with default nominal entries
   * @param summarySetOps instance of SummarySetOperations
   */
  public Union(final SummarySetOperations<S> summarySetOps) {
    this(DEFAULT_NOMINAL_ENTRIES, summarySetOps);
  }

  /**
   * Creates new instance
   * @param nomEntries nominal number of entries. Forced to the nearest power of 2 greater than
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
   * Updates the internal set by adding entries from the given sketch
   * @param sketchIn input sketch to add to the internal set.
   * If null or empty, it is ignored.
   */
  public void update(final Sketch<S> sketchIn) {
    if ((sketchIn == null) || sketchIn.isEmpty()) { return; }
    empty_ = false;
    if (sketchIn.thetaLong_ < thetaLong_) { thetaLong_ = sketchIn.thetaLong_; }
    final SketchIterator<S> it = sketchIn.iterator();
    while (it.next()) {
      qsk_.merge(it.getHash(), it.getSummary(), summarySetOps_);
    }
    if (qsk_.thetaLong_ < thetaLong_) {
      thetaLong_ = qsk_.thetaLong_;
    }
  }

  /**
   * Updates the internal set by combining entries using the hashes from the Theta Sketch and
   * summary values from the given summary and rules from the summarySetOps defined by the
   * Union constructor.
   * @param sketchIn the given Theta Sketch input. If null or empty, it is ignored.
   * @param summary the given proxy summary for the Theta Sketch, which doesn't have one. This may
   * not be null.
   */
  @SuppressWarnings("unchecked")
  public void update(final org.apache.datasketches.theta.Sketch sketchIn, final S summary) {
    if (summary == null) {
      throw new SketchesArgumentException("Summary cannot be null."); }
    if ((sketchIn == null) || sketchIn.isEmpty()) { return; }
    empty_ = false;
    final long thetaIn = sketchIn.getThetaLong();
    if (thetaIn < thetaLong_) { thetaLong_ = thetaIn; }
    final org.apache.datasketches.theta.HashIterator it = sketchIn.iterator();
    while (it.next()) {
      qsk_.merge(it.get(), (S)summary.copy(), summarySetOps_);
    }
    if (qsk_.thetaLong_ < thetaLong_) {
      thetaLong_ = qsk_.thetaLong_;
    }
  }

  /**
   * Gets the internal set as a CompactSketch
   * @return result of the unions so far
   */
  @SuppressWarnings("unchecked")
  public CompactSketch<S> getResult() {
    if (empty_) {
      return qsk_.compact();
    }
    if ((thetaLong_ >= qsk_.thetaLong_) && (qsk_.getRetainedEntries() <= qsk_.getNominalEntries())) {
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
   * Resets the internal set to the initial state, which represents an empty set
   */
  public void reset() {
    qsk_.reset();
    thetaLong_ = qsk_.getThetaLong();
    empty_ = true;
  }
}
