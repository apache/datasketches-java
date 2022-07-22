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
 * Iterator over KllDoublesSketchSortedView
 */
public class KllDoublesSketchSortedViewIterator {

  private final double[] items_;
  private final long[] weights_;
  private int i_;

  KllDoublesSketchSortedViewIterator(final double[] items, final long[] weights) {
    items_ = items;
    weights_ = weights;
    i_ = -1;
  }

  /**
   * Gets a value from the current entry.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return value from the current entry
   */
  public double getValue() {
    return items_[i_];
  }

  /**
   * Gets a weight for the value from the current entry.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return weight for the value from the current entry
   */
  public long getWeight() {
    return weights_[i_];
  }

  /**
   * Advancing the iterator and checking existence of the next entry
   * is combined here for efficiency. This results in an undefined
   * state of the iterator before the first call of this method.
   * @return true if the next element exists
   */
  public boolean next() {
    i_++;
    return i_ < items_.length;
  }

}