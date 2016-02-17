/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import com.yahoo.sketches.memory.Memory;

/**
 * Iterator over the off-heap ArrayOfDoublesSketch (compact or hash table)
 */
public class DirectArrayOfDoublesSketchIterator implements ArrayOfDoublesSketchIterator {

  private Memory mem_;
  private int offset_;
  private int numEntries_;
  private int numValues_;
  private int i_;
  private static final int SIZE_OF_KEY_BYTES = 8;
  private static final int SIZE_OF_VALUE_BYTES = 8;

  DirectArrayOfDoublesSketchIterator(final Memory mem, int offset, int numEntries, int numValues) {
    mem_ = mem;
    offset_ = offset;
    numEntries_ = numEntries;
    numValues_ = numValues;
    i_ = -1;
  }

  @Override
  public boolean next() {
    i_++;
    while (i_ < numEntries_) {
      if (mem_.getLong(offset_ + SIZE_OF_KEY_BYTES * i_) != 0) return true;
      i_++;
    }
    return false;
  }

  @Override
  public long getKey() {
    return mem_.getLong(offset_ + SIZE_OF_KEY_BYTES * i_);
  }

  @Override
  public double[] getValues() {
    double[] array = new double[numValues_];
    mem_.getDoubleArray(offset_ + SIZE_OF_KEY_BYTES * numEntries_ + SIZE_OF_VALUE_BYTES * i_ * numValues_, array, 0, numValues_);
    return array;
  }

}
