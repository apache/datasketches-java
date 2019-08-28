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

import java.util.Arrays;

/**
 * @author Jon Malkin
 */
class HeapDoublesSketchAccessor extends DoublesSketchAccessor {
  HeapDoublesSketchAccessor(final DoublesSketch ds,
                            final boolean forceSize,
                            final int level) {
    super(ds, forceSize, level);
    assert !ds.isDirect();
  }

  @Override
  DoublesSketchAccessor copyAndSetLevel(final int level) {
    return new HeapDoublesSketchAccessor(ds_, forceSize_, level);
  }

  @Override
  double get(final int index) {
    assert index >= 0 && index < numItems_;
    assert n_ == ds_.getN();

    return ds_.getCombinedBuffer()[offset_ + index];
  }

  @Override
  double set(final int index, final double value) {
    assert index >= 0 && index < numItems_;
    assert n_ == ds_.getN();

    final int idxOffset = offset_ + index;
    final double oldVal = ds_.getCombinedBuffer()[idxOffset];
    ds_.getCombinedBuffer()[idxOffset] = value;

    return oldVal;
  }

  @Override
  double[] getArray(final int fromIdx, final int numItems) {
    final int stIdx = offset_ + fromIdx;
    return Arrays.copyOfRange(ds_.getCombinedBuffer(), stIdx, stIdx + numItems);
  }

  @Override
  void putArray(final double[] srcArray, final int srcIndex,
                final int dstIndex, final int numItems) {
    final int tgtIdx = offset_ + dstIndex;
    System.arraycopy(srcArray, srcIndex, ds_.getCombinedBuffer(), tgtIdx, numItems);
  }

  @Override
  void sort() {
    assert currLvl_ == BB_LVL_IDX;

    if (!ds_.isCompact()) { // compact sketch is already sorted; not an error but a no-op
      Arrays.sort(ds_.getCombinedBuffer(), offset_, offset_ + numItems_);
    }
  }
}
