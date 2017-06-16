/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

/**
 * Common iterator class for maps that need one.
 *
 * @author Alex Saydakov
 */
class CouponsIterator {

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

  /**
   * next() must be called before the first getValue(). This skips over zero values.
   * @return the next coupon in the array.
   */
  boolean next() {
    index_++;
    while (index_ < maxEntries_) {
      if (couponsArr_[offset_ + index_] != 0) { return true; }
      index_++;
    }
    return false;
  }

  /**
   * Returns the value at the current index.
   * @return the value at the current index.
   */
  short getValue() {
    return couponsArr_[offset_ + index_];
  }

}
