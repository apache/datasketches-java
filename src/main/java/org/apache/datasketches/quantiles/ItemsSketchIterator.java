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

package org.apache.datasketches.quantiles;

/**
 * Iterator over ItemsSketch. The order is not defined.
 * @param <T> type of item
 */
public class ItemsSketchIterator<T> {

  private final ItemsSketch<T> sketch_;
  private Object[] combinedBuffer_;
  private long bits_;
  private int level_;
  private long weight_;
  private int i_;
  private int offset_;
  private int num_;

  ItemsSketchIterator(final ItemsSketch<T> sketch, final long bitPattern) {
    sketch_ = sketch;
    bits_ = bitPattern;
    level_ = -1;
    weight_ = 1;
    i_ = 0;
    offset_ = 0;
  }

  /**
   * Advancing the iterator and checking existence of the next entry
   * is combined here for efficiency. This results in an undefined
   * state of the iterator before the first call of this method.
   * @return true if the next element exists
   */
  public boolean next() {
    if (combinedBuffer_ == null) { // initial setup
      combinedBuffer_ = sketch_.combinedBuffer_;
      num_ = sketch_.getBaseBufferCount();
    } else { // advance index within the current level
      i_++;
    }
    if (i_ < num_) {
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
    offset_ = (2 + level_) * sketch_.getK();
    num_ = sketch_.getK();
    return true;
  }

  /**
   * Gets a value from the current entry in the sketch.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return value from the current entry
   */
  @SuppressWarnings("unchecked")
  public T getValue() {
    return (T) combinedBuffer_[offset_ + i_];
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
