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

package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;

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
  }

  /**
   * Updates the internal set by adding entries from the given sketch
   * @param sketchIn input sketch to add to the internal set
   */
  public void update(final Sketch<S> sketchIn) {
    if (sketchIn == null || sketchIn.isEmpty()) { return; }
    if (sketchIn.theta_ < theta_) { theta_ = sketchIn.theta_; }
    final SketchIterator<S> it = sketchIn.iterator();
    while (it.next()) {
      sketch_.merge(it.getKey(), it.getSummary(), summarySetOps_);
    }
  }

  /**
   * Gets the internal set as a CompactSketch
   * @return result of the unions so far
   */
  public CompactSketch<S> getResult() {
    sketch_.trim();
    if (theta_ < sketch_.theta_) {
      sketch_.setThetaLong(theta_);
      sketch_.rebuild();
    }
    return sketch_.compact();
  }

  /**
   * Resets the internal set to the initial state, which represents an empty set
   */
  public void reset() {
    sketch_ = new QuickSelectSketch<S>(nomEntries_, null);
  }
}
