/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

/**
 * This is to compute a union of two or more tuple sketches.
 * A new instance represents an empty set.
 * Every update() computes a union with the internal set
 * and can only grow the internal set.
 * @param <S> Type of Summary
 */
public class Union<S extends Summary> {
  private int nomEntries_;
  private SummaryFactory<S> summaryFactory_;
  private QuickSelectSketch<S> sketch_;
  private long theta_; // need to maintain outside of the sketch

  /**
   * Creates new instance
   * @param nomEntries nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param summaryFactory
   */
  public Union(int nomEntries, SummaryFactory<S> summaryFactory) {
    nomEntries_ = nomEntries;
    summaryFactory_ = summaryFactory;
    sketch_ = new QuickSelectSketch<S>(nomEntries, summaryFactory);
    theta_ = sketch_.getThetaLong();
  }

  /**
   * Updates the internal set by adding entries from the given sketch
   * @param sketchIn input sketch to add to the internal set
   */
  public void update(Sketch<S> sketchIn) {
    if (sketchIn == null || sketchIn.isEmpty()) return;
    if (sketchIn.theta_ < theta_) theta_ = sketchIn.theta_;
    SketchIterator<S> it = sketchIn.iterator();
    while (it.next()) sketch_.merge(it.getKey(), it.getSummary());
  }

  /**
   * Gets the internal set as a CompactSketch
   * @return result of the unions so far
   */
  public CompactSketch<S> getResult() {
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
    sketch_ = new QuickSelectSketch<S>(nomEntries_, summaryFactory_);
  }
}
