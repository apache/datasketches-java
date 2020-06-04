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
  private QuickSelectSketch<S> sketch_;
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
    sketch_ = new QuickSelectSketch<>(nomEntries, null);
    thetaLong_ = sketch_.getThetaLong();
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
    if (sketchIn.theta_ < thetaLong_) { thetaLong_ = sketchIn.theta_; }
    final SketchIterator<S> it = sketchIn.iterator();
    while (it.next()) {
      sketch_.merge(it.getKey(), it.getSummary(), summarySetOps_);
    }
    if (sketch_.theta_ < thetaLong_) {
      thetaLong_ = sketch_.theta_;
    }
  }

  /**
   * Updates the internal set by combining entries using the hash keys from the Theta Sketch and
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
      sketch_.merge(it.get(), (S)summary.copy(), summarySetOps_);
    }
    if (sketch_.theta_ < thetaLong_) {
      thetaLong_ = sketch_.theta_;
    }
  }

  /**
   * Gets the internal set as a CompactSketch
   * @return result of the unions so far
   */
  @SuppressWarnings("unchecked")
  public CompactSketch<S> getResult() {
    if (empty_) {
      return sketch_.compact();
    }
    if ((thetaLong_ >= sketch_.theta_) && (sketch_.getRetainedEntries() <= sketch_.getNominalEntries())) {
      return sketch_.compact();
    }
    long theta = min(thetaLong_, sketch_.theta_);

    int num = 0;
    {
      final SketchIterator<S> it = sketch_.iterator();
      while (it.next()) {
        if (it.getKey() < theta) { num++; }
      }
    }
    if (num == 0) {
      return new CompactSketch<>(null, null, theta, empty_);
    }
    if (num > sketch_.getNominalEntries()) {
      final long[] keys = new long[num]; // temporary since the order will be destroyed by quick select
      final SketchIterator<S> it = sketch_.iterator();
      int i = 0;
      while (it.next()) {
        if (it.getKey() < theta) { keys[i++] = it.getKey(); }
      }
      theta = QuickSelect.select(keys, 0, num - 1, sketch_.getNominalEntries());
      num = sketch_.getNominalEntries();
    }
    final long[] keys = new long[num];
    final S[] summaries = (S[]) Array.newInstance(sketch_.summaries_.getClass().getComponentType(), num);
    final SketchIterator<S> it = sketch_.iterator();
    int i = 0;
    while (it.next()) {
      if (it.getKey() < theta) {
        keys[i] = it.getKey();
        summaries[i] = (S) it.getSummary().copy();
        i++;
      }
    }
    return new CompactSketch<>(keys, summaries, theta, empty_);
  }

  /**
   * Resets the internal set to the initial state, which represents an empty set
   */
  public void reset() {
    sketch_.reset();
    thetaLong_ = sketch_.getThetaLong();
    empty_ = true;
  }
}
