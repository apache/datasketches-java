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
