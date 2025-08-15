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

import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

/**
 * @author Jon Malkin
 */
final class DirectDoublesSketchAccessor extends DoublesSketchAccessor {

  DirectDoublesSketchAccessor(final DoublesSketch ds,
                              final boolean forceSize,
                              final int level) {
    super(ds, forceSize, level);
    assert ds.hasMemorySegment();
  }

  @Override
  DoublesSketchAccessor copyAndSetLevel(final int level) {
    return new DirectDoublesSketchAccessor(ds_, forceSize_, level);
  }

  @Override
  double get(final int index) {
    assert (index >= 0) && (index < numItems_);
    assert n_ == ds_.getN();

    final int byteOffset = offset_ + (index << 3);
    return ds_.getMemorySegment().get(JAVA_DOUBLE_UNALIGNED, byteOffset);
  }

  @Override
  double set(final int index, final double quantile) {
    assert (index >= 0) && (index < numItems_);
    assert n_ == ds_.getN();
    assert !ds_.isCompact(); // can't write to a compact sketch

    final int byteOffset = offset_ + (index << 3);
    final MemorySegment seg = ds_.getMemorySegment();
    final double oldVal = seg.get(JAVA_DOUBLE_UNALIGNED, byteOffset);
    seg.set(JAVA_DOUBLE_UNALIGNED, byteOffset, quantile);
    return oldVal;
  }

  @Override
  double[] getArray(final int fromIdx, final int numItems) {
    final int byteOffset = offset_ + (fromIdx << 3);
    final MemorySegment seg = ds_.getMemorySegment();
    return seg.asSlice(byteOffset, numItems << 3).toArray(JAVA_DOUBLE_UNALIGNED);
  }

  @Override
  void putArray(final double[] srcArray, final int srcIndex, final int dstIndex, final int numItems) {
    assert !ds_.isCompact(); // can't write to compact sketch

    final int byteOffset = offset_ + (dstIndex << 3);
    MemorySegment.copy(srcArray, srcIndex, ds_.getMemorySegment(), JAVA_DOUBLE_UNALIGNED, byteOffset, numItems);
  }

  @Override
  void sort() {
    assert currLvl_ == BB_LVL_IDX;

    final double[] tmpBuffer = new double[numItems_];
    final MemorySegment seg = ds_.getMemorySegment();
    MemorySegment.copy(seg, JAVA_DOUBLE_UNALIGNED, offset_, tmpBuffer, 0, numItems_);
    Arrays.sort(tmpBuffer, 0, numItems_);
    MemorySegment.copy(tmpBuffer, 0, seg, JAVA_DOUBLE_UNALIGNED, offset_, numItems_);
  }

}
