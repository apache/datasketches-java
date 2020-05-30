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

import java.lang.reflect.Array;

import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.SketchesStateException;

/**
 * Computes an intersection of two or more generic tuple sketches.
 * A new instance represents the Universal Set. Because the Universal Set
 * cannot be realized a <i>getResult()</i> on a new instance will produce an error.
 * Every update() computes an intersection with the internal state, which will never
 * grow larger and may be reduced to zero.
 * @param <S> Type of Summary
 */
public class Intersection<S extends Summary> {
  private final SummarySetOperations<S> summarySetOps_;
  private QuickSelectSketch<S> sketch_;
  private boolean empty_;
  private long thetaLong_;
  private boolean firstCall_;

  /**
   * Creates new instance
   * @param summarySetOps instance of SummarySetOperations
   */
  public Intersection(final SummarySetOperations<S> summarySetOps) {
    summarySetOps_ = summarySetOps;
    sketch_ = null;
    empty_ = false; // universal set at the start
    thetaLong_ = Long.MAX_VALUE;
    firstCall_ = true;
  }

  /**
   * Updates the internal state by intersecting it with the given sketch
   * @param sketchIn input sketch to intersect with the internal state
   */
  @SuppressWarnings({ "unchecked", "null" })
  public void update(final Sketch<S> sketchIn) {
    final boolean firstCall = firstCall_;
    firstCall_ = false;
    if (sketchIn == null) {
      empty_ = (thetaLong_ == Long.MAX_VALUE);
      sketch_ = null;
      return;
    }
    thetaLong_ = min(thetaLong_, sketchIn.getThetaLong()); //Theta rule
    empty_ |= sketchIn.isEmpty();                          //Empty rule
    if (empty_ || (sketchIn.getRetainedEntries() == 0)) {
      empty_ = (thetaLong_ == Long.MAX_VALUE);
      sketch_ = null;
      return;
    }
    if (firstCall) {
      sketch_ = new QuickSelectSketch<>(sketchIn.getRetainedEntries(), ResizeFactor.X1.lg(), null);
      final SketchIterator<S> it = sketchIn.iterator();
      while (it.next()) {
        sketch_.insert(it.getKey(), (S)it.getSummary().copy());
      }
    } else {
      if (sketch_ == null) {
        empty_ = (thetaLong_ == Long.MAX_VALUE);
        return;
      }
      final int maxMatchSize = min(sketch_.getRetainedEntries(), sketchIn.getRetainedEntries());
      final long[] matchKeys = new long[maxMatchSize];
      S[] matchSummaries = null;
      int matchCount = 0;
      final SketchIterator<S> it = sketchIn.iterator();
      while (it.next()) {
        final long key = it.getKey();
        if (key >= thetaLong_) {
          continue;
        }
        final S mySummary = sketch_.find(key);
        if (mySummary != null) { //key found
          matchKeys[matchCount] = key;
          if (matchSummaries == null) {
            matchSummaries = (S[]) Array.newInstance(mySummary.getClass(), maxMatchSize);
          }
          matchSummaries[matchCount] = summarySetOps_.intersection(mySummary, it.getSummary());
          matchCount++;
        }
      }
      if (matchCount > 0) { //therefore matchSummaries != null.
        // assumes that constructor of QuickSelectSketch bumps the requested size
        // up to the nearest power of 2
        sketch_ = new QuickSelectSketch<>(matchCount, ResizeFactor.X1.lg(), null);
        for (int i = 0; i < matchCount; i++) {
          sketch_.insert(matchKeys[i], matchSummaries[i]);
        }
        sketch_.setThetaLong(thetaLong_);
        empty_ = (thetaLong_ == Long.MAX_VALUE) && (sketch_.getRetainedEntries() == 0);
        sketch_.setEmpty(empty_);
      } else {
        sketch_ = null;
        empty_ = (thetaLong_ == Long.MAX_VALUE);
      }
    }
  }

  /**
   * Updates the internal set by intersecting it with the given Theta sketch
   * @param sketchIn input Theta Sketch to intersect with the internal state.
   * @param summary the given proxy summary for the Theta Sketch, which doesn't have one.
   */
  @SuppressWarnings({ "unchecked", "null" })
  public void update(final org.apache.datasketches.theta.Sketch sketchIn, final S summary) {
    final boolean firstCall = firstCall_;
    firstCall_ = false;
    if (sketchIn == null) {
      empty_ = (thetaLong_ == Long.MAX_VALUE);
      sketch_ = null;
      return;
    }
    thetaLong_ = min(thetaLong_, sketchIn.getThetaLong()); //Theta rule
    empty_ |= sketchIn.isEmpty();                          //Empty rule
    if (empty_ || (sketchIn.getRetainedEntries() == 0)) {
      empty_ = (thetaLong_ == Long.MAX_VALUE);
      sketch_ = null;
      return;
    }
    if (firstCall) {
      sketch_ = new QuickSelectSketch<>(sketchIn.getRetainedEntries(), ResizeFactor.X1.lg(), null);
      final org.apache.datasketches.theta.HashIterator it = sketchIn.iterator();
      while (it.next()) {
        sketch_.insert(it.get(), (S)summary.copy());
      }
    } else {
      if (sketch_ == null) {
        empty_ = (thetaLong_ == Long.MAX_VALUE);
        return;
      }
      final int maxMatchSize = min(sketch_.getRetainedEntries(), sketchIn.getRetainedEntries());
      final long[] matchKeys = new long[maxMatchSize];
      S[] matchSummaries = null;
      int matchCount = 0;
      final org.apache.datasketches.theta.HashIterator it = sketchIn.iterator();
      while (it.next()) {
        final long key = it.get();
        if (key >= thetaLong_) {
          continue;
        }
        final S mySummary = sketch_.find(key);
        if (mySummary != null) { //key found
          matchKeys[matchCount] = key;
          if (matchSummaries == null) {
            matchSummaries = (S[]) Array.newInstance(mySummary.getClass(), maxMatchSize);
          }
          matchSummaries[matchCount] = summarySetOps_.intersection(mySummary, (S)summary.copy());
          matchCount++;
        }
      }
      if (matchCount > 0) { //therefore matchSummaries != null.
        // assumes that constructor of QuickSelectSketch bumps the requested size
        // up to the nearest power of 2
        sketch_ = new QuickSelectSketch<>(matchCount, ResizeFactor.X1.lg(), null);
        for (int i = 0; i < matchCount; i++) {
          sketch_.insert(matchKeys[i], matchSummaries[i]);
        }
        sketch_.setThetaLong(thetaLong_);
        empty_ = (thetaLong_ == Long.MAX_VALUE) && (sketch_.getRetainedEntries() == 0);
        sketch_.setEmpty(empty_);
      } else {
        sketch_ = null;
        empty_ = (thetaLong_ == Long.MAX_VALUE);
      }
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
    if (sketch_ == null) {
      return new CompactSketch<>(null, null, thetaLong_, empty_);
    }
    return sketch_.compact();
  }

  /**
   * Resets the internal set to the initial state, which represents the Universal Set
   */
  public void reset() {
    empty_ = false;
    thetaLong_ = Long.MAX_VALUE;
    sketch_ = null;
    firstCall_ = true;
  }
}
