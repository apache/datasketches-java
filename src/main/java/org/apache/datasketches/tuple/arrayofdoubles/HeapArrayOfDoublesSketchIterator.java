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

package org.apache.datasketches.tuple.arrayofdoubles;

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
