/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

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
