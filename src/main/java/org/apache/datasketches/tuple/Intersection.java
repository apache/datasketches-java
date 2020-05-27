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
 * A new instance represents the Universal Set.
 * Every update() computes an intersection with the internal set
 * and can only reduce the internal set.
 * @param <S> Type of Summary
 */
public class Intersection<S extends Summary> {

  private final SummarySetOperations<S> summarySetOps_;
  private QuickSelectSketch<S> sketch_;
  private boolean isEmpty_;
  private long theta_;
  private boolean isFirstCall_;

  /**
   * Creates new instance
   * @param summarySetOps instance of SummarySetOperations
   */
  public Intersection(final SummarySetOperations<S> summarySetOps) {
    summarySetOps_ = summarySetOps;
    isEmpty_ = false; // universal set at the start
    theta_ = Long.MAX_VALUE;
    isFirstCall_ = true;
  }

  /**
   * Updates the internal set by intersecting it with the given sketch
   * @param sketchIn input sketch to intersect with the internal set
   */
  @SuppressWarnings({ "unchecked", "null" })
  public void update(final Sketch<S> sketchIn) {
    final boolean isFirstCall = isFirstCall_;
    isFirstCall_ = false;
    if (sketchIn == null) {
      isEmpty_ = true;
      sketch_ = null;
      return;
    }
    theta_ = min(theta_, sketchIn.getThetaLong());
    isEmpty_ |= sketchIn.isEmpty();
    if (isEmpty_ || (sketchIn.getRetainedEntries() == 0)) {
      sketch_ = null;
      return;
    }
    if (isFirstCall) {
      sketch_ = new QuickSelectSketch<>(sketchIn.getRetainedEntries(), ResizeFactor.X1.lg(), null);
      final SketchIterator<S> it = sketchIn.iterator();
      while (it.next()) {
        final S summary = (S)it.getSummary().copy();
        sketch_.insert(it.getKey(), summary);
      }
    } else {
      if (sketch_ == null) {
        return;
      }
      final int matchSize = min(sketch_.getRetainedEntries(), sketchIn.getRetainedEntries());
      final long[] matchKeys = new long[matchSize];
      S[] matchSummaries = null;
      int matchCount = 0;
      final SketchIterator<S> it = sketchIn.iterator();
      while (it.next()) {
        final long key = it.getKey();
        final S summary = sketch_.find(key);
        if (summary != null) { //key found
          matchKeys[matchCount] = key;
          if (matchSummaries == null) {
            matchSummaries = (S[]) Array.newInstance(summary.getClass(), matchSize);
          }
          matchSummaries[matchCount] = summarySetOps_.intersection(summary, it.getSummary());
          matchCount++;
        }
      }
      sketch_ = null;
      if (matchCount > 0) { //therefore matchSummaries != null.
        // assumes that constructor of QuickSelectSketch bumps the requested size
        // up to the nearest power of 2
        sketch_ = new QuickSelectSketch<>(matchCount, ResizeFactor.X1.lg(), null);
        for (int i = 0; i < matchCount; i++) {
          sketch_.insert(matchKeys[i], matchSummaries[i]);
        }
      }
    }
    if (sketch_ != null) {
      sketch_.setThetaLong(theta_);
      sketch_.setNotEmpty();
    }
  }

  /**
   * Updates the internal set by intersecting it with the given Theta sketch
   * @param sketchIn input Theta Sketch to intersect with the internal set
   * @param summary the given proxy summary for the Theta Sketch, which doesn't have one.
   */
  @SuppressWarnings({ "unchecked", "null" })
  public void update(final org.apache.datasketches.theta.Sketch sketchIn, final S summary) {
    final boolean isFirstCall = isFirstCall_;
    isFirstCall_ = false;
    if (sketchIn == null) {
      isEmpty_ = true;
      sketch_ = null;
      return;
    }
    theta_ = min(theta_, sketchIn.getThetaLong());
    isEmpty_ |= sketchIn.isEmpty();
    if (isEmpty_ || (sketchIn.getRetainedEntries() == 0)) {
      sketch_ = null;
      return;
    }
    if (isFirstCall) {
      sketch_ = new QuickSelectSketch<>(sketchIn.getRetainedEntries(), ResizeFactor.X1.lg(), null);
      final org.apache.datasketches.theta.HashIterator it = sketchIn.iterator();
      while (it.next()) {
        sketch_.insert(it.get(), (S)summary.copy());
      }
    } else {
      if (sketch_ == null) {
        return;
      }
      final int matchSize = min(sketch_.getRetainedEntries(), sketchIn.getRetainedEntries());
      final long[] matchKeys = new long[matchSize];
      S[] matchSummaries = null;
      int matchCount = 0;
      final org.apache.datasketches.theta.HashIterator it = sketchIn.iterator();
      while (it.next()) {
        final long key = it.get();
        final S mySummary = sketch_.find(key);
        if (mySummary != null) { //key found
          matchKeys[matchCount] = key;
          if (matchSummaries == null) {
            matchSummaries = (S[]) Array.newInstance(mySummary.getClass(), matchSize);
          }
          matchSummaries[matchCount] = summarySetOps_.intersection(mySummary, (S)summary.copy());
          matchCount++;
        }
      }
      sketch_ = null;
      if (matchCount > 0) { //therefore matchSummaries != null.
        // assumes that constructor of QuickSelectSketch bumps the requested size
        // up to the nearest power of 2
        sketch_ = new QuickSelectSketch<>(matchCount, ResizeFactor.X1.lg(), null);
        for (int i = 0; i < matchCount; i++) {
          sketch_.insert(matchKeys[i], matchSummaries[i]);
        }
      }
    }
    if (sketch_ != null) {
      sketch_.setThetaLong(theta_);
      sketch_.setNotEmpty();
    }
  }


  /**
   * Gets the internal set as a CompactSketch
   * @return result of the intersections so far
   */
  public CompactSketch<S> getResult() {
    if (isFirstCall_) {
      throw new SketchesStateException(
        "getResult() with no intervening intersections is not a legal result.");
    }
    if (sketch_ == null) {
      return new CompactSketch<>(null, null, theta_, isEmpty_);
    }
    return sketch_.compact();
  }

  /**
   * Resets the internal set to the initial state, which represents the Universal Set
   */
  public void reset() {
    isEmpty_ = false;
    theta_ = Long.MAX_VALUE;
    sketch_ = null;
    isFirstCall_ = true;
  }
}
