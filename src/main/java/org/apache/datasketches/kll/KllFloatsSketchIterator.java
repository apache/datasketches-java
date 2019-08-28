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

package org.apache.datasketches.kll;

/**
 * Iterator over KllFloatsSketch. The order is not defined.
 */
public class KllFloatsSketchIterator {

  private final float[] items_;
  private final int[] levels_;
  private final int numLevels_;
  private int level_;
  private int i_;
  private long weight_;
  private boolean isInitialized_;

  KllFloatsSketchIterator(final float[] items, final int[] levels, final int numLevels) {
    items_ = items;
    levels_ = levels;
    numLevels_ = numLevels;
    isInitialized_ = false;
  }

  /**
   * Advancing the iterator and checking existence of the next entry
   * is combined here for efficiency. This results in an undefined
   * state of the iterator before the first call of this method.
   * @return true if the next element exists
   */
  public boolean next() {
    if (!isInitialized_) {
      level_ = 0;
      i_ = levels_[level_];
      weight_ = 1;
      isInitialized_ = true;
    } else {
      i_++;
    }
    if (i_ < levels_[level_ + 1]) {
      return true;
    }
    // go to the next non-empty level
    do {
      level_++;
      if (level_ == numLevels_) {
        return false; // run out of levels
      }
      weight_ *= 2;
    } while (levels_[level_] == levels_[level_ + 1]);
    i_ = levels_[level_];
    return true;
  }

  /**
   * Gets a value from the current entry in the sketch.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return value from the current entry
   */
  public float getValue() {
    return items_[i_];
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
