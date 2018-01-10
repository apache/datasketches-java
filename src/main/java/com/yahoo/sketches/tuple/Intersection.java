/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import static java.lang.Math.min;

import java.lang.reflect.Array;

import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesStateException;

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
    // assumes that constructor of QuickSelectSketch bumps the requested size up to the nearest power of 2
    if (isFirstCall) {
      sketch_ = new QuickSelectSketch<>(sketchIn.getRetainedEntries(), ResizeFactor.X1.lg(), null);
      final SketchIterator<S> it = sketchIn.iterator();
      while (it.next()) {
        final S summary = it.getSummary().copy();
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
        final S summary = sketch_.find(it.getKey());
        if (summary != null) {
          matchKeys[matchCount] = it.getKey();
          if (matchSummaries == null) {
            matchSummaries = (S[]) Array.newInstance(summary.getClass(), matchSize);
          }
          matchSummaries[matchCount] =
              summarySetOps_.intersection(summary, it.getSummary());
          matchCount++;
        }
      }
      sketch_ = null;
      if (matchCount > 0) {
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
