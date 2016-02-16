/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import static java.lang.Math.min;

import java.lang.reflect.Array;

/**
 * This is to compute an intersection of two or more tuple sketches.
 * A new instance represents the Universal Set.
 * Every update() computes an intersection with the internal set
 * and can only reduce the internal set.
 * @param <S> Type of Summary
 */
public class Intersection<S extends Summary> {

  private SummaryFactory<S> summaryFactory_;
  private QuickSelectSketch<S> sketch_;
  private boolean isEmpty_;
  private long theta_;
  private boolean isFirstCall_;

  /**
   * Creates new instance
   * @param summaryFactory
   */
  public Intersection(SummaryFactory<S> summaryFactory) {
    summaryFactory_ = summaryFactory;
    isEmpty_ = false; // universal set at the start
    theta_ = Long.MAX_VALUE;
    isFirstCall_ = true;
  }

  /**
   * Updates the internal set by intersecting it with the given sketch
   * @param sketchIn input sketch to intersect with the internal set
   */
  public void update(Sketch<S> sketchIn) {
    boolean isFirstCall = isFirstCall_;
    isFirstCall_ = false;
    if (sketchIn == null) {
      isEmpty_ = true;
      sketch_ = null;
      return;
    }
    theta_ = min(theta_, sketchIn.getThetaLong());
    isEmpty_ |= sketchIn.isEmpty();
    if (sketchIn.getRetainedEntries() == 0) {
      sketch_ = null;
      return;
    }
    // assumes that constructor of QuickSelectSketch bumps the requested size up to the nearest power of 2
    if (isFirstCall) {
      sketch_ = new QuickSelectSketch<S>(sketchIn.getRetainedEntries(), 0, summaryFactory_);
      SketchIterator<S> it = sketchIn.iterator();
      while (it.next()) {
        S summary = it.getSummary().copy();
        sketch_.insert(it.getKey(), summary);
      }
    } else {
      int matchSize = min(sketch_.getRetainedEntries(), sketchIn.getRetainedEntries());
      long[] matchKeys = new long[matchSize];
      @SuppressWarnings("unchecked")
      S[] matchSummaries = (S[]) Array.newInstance(summaryFactory_.newSummary().getClass(), matchSize);
      int matchCount = 0;
      SketchIterator<S> it = sketchIn.iterator();
      while (it.next()) {
        S summary = sketch_.find(it.getKey());
        if (summary != null) {
          matchKeys[matchCount] = it.getKey();
          matchSummaries[matchCount] = summaryFactory_.getSummarySetOperations().intersection(summary, it.getSummary());
          matchCount++;
        }
      }
      sketch_ = null;
      if (matchCount > 0) {
        sketch_ = new QuickSelectSketch<S>(matchCount, 0, summaryFactory_);
        for (int i = 0; i < matchCount; i++) sketch_.insert(matchKeys[i], matchSummaries[i]);
      }
    }
    if (sketch_ != null) {
      sketch_.setThetaLong(theta_);
      sketch_.setIsEmpty(isEmpty_);
    }
  }

  /**
   * Gets the internal set as a CompactSketch
   * @return result of the intersections so far
   */
  public CompactSketch<S> getResult() {
    if (isFirstCall_) throw new IllegalStateException("getResult() with no intervening intersections is not a legal result.");
    if (sketch_ == null) return new CompactSketch<S>(null, null, theta_, isEmpty_);
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
