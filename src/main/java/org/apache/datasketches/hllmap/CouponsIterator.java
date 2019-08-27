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

package org.apache.datasketches.hllmap;

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
