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

/**
 * Compute a union of two or more tuple sketches.
 * A new instance represents an empty set.
 * Every update() computes a union with the internal set
 * and can only grow the internal set.
 * @param <S> Type of Summary
 */
public class Union<S extends Summary> {
  private final int nomEntries_;
  private final SummarySetOperations<S> summarySetOps_;
  private QuickSelectSketch<S> sketch_;
  private long theta_; // need to maintain outside of the sketch
  private boolean isEmpty_;

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
    nomEntries_ = nomEntries;
    summarySetOps_ = summarySetOps;
    sketch_ = new QuickSelectSketch<S>(nomEntries, null);
    theta_ = sketch_.getThetaLong();
    isEmpty_ = true;
  }

  /**
   * Updates the internal set by adding entries from the given sketch
   * @param sketchIn input sketch to add to the internal set
   */
  public void update(final Sketch<S> sketchIn) {
    if (sketchIn == null || sketchIn.isEmpty()) { return; }
    isEmpty_ = false;
    if (sketchIn.theta_ < theta_) { theta_ = sketchIn.theta_; }
    final SketchIterator<S> it = sketchIn.iterator();
    while (it.next()) {
      sketch_.merge(it.getKey(), it.getSummary(), summarySetOps_);
    }
    if (sketch_.theta_ < theta_) theta_ = sketch_.theta_;
  }

  /**
   * Gets the internal set as a CompactSketch
   * @return result of the unions so far
   */
  @SuppressWarnings("unchecked")
  public CompactSketch<S> getResult() {
    if (isEmpty_) return sketch_.compact();
    if (theta_ >= sketch_.theta_ && sketch_.getRetainedEntries() <= sketch_.getNominalEntries()) {
      return sketch_.compact();
    }
    long theta = min(theta_, sketch_.theta_);

    int num = 0;
    {
      final SketchIterator<S> it = sketch_.iterator();
      while (it.next()) {
        if (it.getKey() < theta) { num++; }
      }
    }
    if (num == 0) return new CompactSketch<>(null, null, theta, isEmpty_);
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
    return new CompactSketch<>(keys, summaries, theta, isEmpty_);
  }

  /**
   * Resets the internal set to the initial state, which represents an empty set
   */
  public void reset() {
    sketch_.reset();
    theta_ = sketch_.getThetaLong();
    isEmpty_ = true;
  }
}
