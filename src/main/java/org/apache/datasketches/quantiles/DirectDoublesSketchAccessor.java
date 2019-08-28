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

import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Jon Malkin
 */
class DirectDoublesSketchAccessor extends DoublesSketchAccessor {
  DirectDoublesSketchAccessor(final DoublesSketch ds,
                              final boolean forceSize,
                              final int level) {
    super(ds, forceSize, level);
    assert ds.isDirect();
  }

  @Override
  DoublesSketchAccessor copyAndSetLevel(final int level) {
    return new DirectDoublesSketchAccessor(ds_, forceSize_, level);
  }

  @Override
  double get(final int index) {
    assert index >= 0 && index < numItems_;
    assert n_ == ds_.getN();

    final int idxOffset = offset_ + (index << 3);
    return ds_.getMemory().getDouble(idxOffset);
  }

  @Override
  double set(final int index, final double value) {
    assert index >= 0 && index < numItems_;
    assert n_ == ds_.getN();
    assert !ds_.isCompact(); // can't write to a compact sketch

    final int idxOffset = offset_ + (index << 3);
    final WritableMemory mem = ds_.getMemory();
    final double oldVal = mem.getDouble(idxOffset);
    mem.putDouble(idxOffset, value);
    return oldVal;
  }

  @Override
  double[] getArray(final int fromIdx, final int numItems) {
    final double[] dstArray = new double[numItems];
    final int offsetBytes = offset_ + (fromIdx << 3);
    ds_.getMemory().getDoubleArray(offsetBytes, dstArray, 0, numItems);
    return dstArray;
  }

  @Override
  void putArray(final double[] srcArray, final int srcIndex,
                final int dstIndex, final int numItems) {
    assert !ds_.isCompact(); // can't write to compact sketch
    final int offsetBytes = offset_ + (dstIndex << 3);
    ds_.getMemory().putDoubleArray(offsetBytes, srcArray, srcIndex, numItems);
  }

  @Override
  void sort() {
    assert currLvl_ == BB_LVL_IDX;

    final double[] tmpBuffer = new double[numItems_];
    final WritableMemory mem = ds_.getMemory();
    mem.getDoubleArray(offset_, tmpBuffer, 0, numItems_);
    Arrays.sort(tmpBuffer, 0, numItems_);
    mem.putDoubleArray(offset_, tmpBuffer, 0, numItems_);
  }

}
