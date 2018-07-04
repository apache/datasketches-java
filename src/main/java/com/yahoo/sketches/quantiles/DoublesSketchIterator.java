/*
 * Copyright 2018, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

/**
 * Iterator over DoublesSketch. The order is not defined.
 */
public class DoublesSketchIterator {

  private final DoublesSketch sketch_;
  private DoublesSketchAccessor sketchAccessor_;
  private long bits_;
  private int level_;
  private long weight_;
  private int i_;

  DoublesSketchIterator(final DoublesSketch sketch, final long bitPattern) {
    sketch_ = sketch;
    bits_ = bitPattern;
    level_ = -1;
    weight_ = 1;
    i_ = 0;
  }

  /**
   * Advancing the iterator and checking existence of the next entry
   * is combined here for efficiency. This results in an undefined
   * state of the iterator before the first call of this method.
   * @return true if the next element exists
   */
  public boolean next() {
    if (sketchAccessor_ == null) { // initial setup
      sketchAccessor_ = DoublesSketchAccessor.wrap(sketch_);
    } else { // advance index within the current level
      i_++;
    }
    if (i_ < sketchAccessor_.numItems()) {
      return true;
    }
    // go to the next non-empty level
    do {
      level_++;
      if (level_ > 0) {
        bits_ >>>= 1;
      }
      if (bits_ == 0L) {
        return false; // run out of levels
      }
      weight_ *= 2;
    } while ((bits_ & 1L) == 0L);
    i_ = 0;
    sketchAccessor_.setLevel(level_);
    return true;
  }

  /**
   * Gets a value from the current entry in the sketch.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return value from the current entry
   */
  public double getValue() {
    return sketchAccessor_.get(i_);
  }

  /**
   * Gets a weight for the value from the current entry in the sketch.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return weight for the value from the current entry
   */
  public long getWeight() {
    return weight_;
  }

}
