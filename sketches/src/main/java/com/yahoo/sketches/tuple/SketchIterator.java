/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

/**
 * Iterator over a generic tuple sketch
 * @param <S> Type of Summary
 */
public class SketchIterator<S extends Summary> {

  private long[] keys_;
  private S[] summaries_;
  private int i_;

  SketchIterator(final long[] keys, final S[] summaries) {
    keys_ = keys;
    summaries_ =  summaries;
    i_ = -1;
  }

  /**
   * Advancing the iterator and checking existence of the next entry
   * is combined here for efficiency. This results in an undefined
   * state of the iterator before the first call of this method.
   * @return true if the next element exists
   */
  public boolean next() {
    if (keys_ == null) { return false; }
    i_++;
    while (i_ < keys_.length) {
      if (keys_[i_] != 0) { return true; }
      i_++;
    }
    return false;
  }

  /**
   * Gets a key from the current entry in the sketch, which is a hash
   * of the original key passed to update(). The original keys are not
   * retained. Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return hash key from the current entry
   */
  public long getKey() {
    return keys_[i_];
  }

  /**
   * Gets a Summary object from the current entry in the sketch.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return Summary object for the current entry (this is not a copy!)
   */
  public S getSummary() {
    return summaries_[i_];
  }

}
