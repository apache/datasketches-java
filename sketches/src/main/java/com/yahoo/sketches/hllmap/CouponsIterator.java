/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

public class CouponsIterator {

  private final int offset_;
  private final int maxEntries_;
  private final short[] couponsArr_;
  private int index_;

  CouponsIterator(final short[] couponsArr, final int offset, final int maxEntries) {
    offset_ = offset;
    maxEntries_ = maxEntries;
    couponsArr_ = couponsArr;
    index_ = -1;
  }

  //This skips over zero values
  boolean next() {
    index_++;
    while (index_ < maxEntries_) {
      if (couponsArr_[offset_ + index_] != 0) return true;
      index_++;
    }
    return false;
  }

  short getValue() {
    return couponsArr_[offset_ + index_];
  }

}
