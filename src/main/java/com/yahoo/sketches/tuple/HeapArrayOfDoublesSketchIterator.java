/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import java.util.Arrays;

/**
 * Iterator over the on-heap ArrayOfDoublesSketch (compact or hash table)
 */
final class HeapArrayOfDoublesSketchIterator implements ArrayOfDoublesSketchIterator {

  private long[] keys_;
  private double[] values_;
  private int numValues_;
  private int i_;

  HeapArrayOfDoublesSketchIterator(final long[] keys, final double[] values, final int numValues) {
    keys_ = keys;
    values_ = values;
    numValues_ = numValues;
    i_ = -1;
  }

  @Override
  public boolean next() {
    if (keys_ == null) { return false; }
    i_++;
    while (i_ < keys_.length) {
      if (keys_[i_] != 0) { return true; }
      i_++;
    }
    return false;
  }

  @Override
  public long getKey() {
    return keys_[i_];
  }

  @Override
  public double[] getValues() {
    if (numValues_ == 1) {
      return new double[] { values_[i_] };
    }
    return Arrays.copyOfRange(values_, i_ * numValues_, (i_ + 1) *  numValues_);
  }

}
