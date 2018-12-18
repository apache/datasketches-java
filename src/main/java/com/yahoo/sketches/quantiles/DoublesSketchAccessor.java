/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMBINED_BUFFER;

import com.yahoo.sketches.Family;

/**
 * This allows access to package-private levels and data in whatever quantiles sketch you give
 * it: on-heap, off-heap; compact and non-compact
 * @author Jon Malkin
 */
abstract class DoublesSketchAccessor extends DoublesBufferAccessor {
  static final int BB_LVL_IDX = -1;

  final DoublesSketch ds_;
  final boolean forceSize_;

  long n_;
  int currLvl_;
  int numItems_;
  int offset_;

  DoublesSketchAccessor(final DoublesSketch ds,
                        final boolean forceSize,
                        final int level) {
    ds_ = ds;
    forceSize_ = forceSize;

    setLevel(level);
  }

  static DoublesSketchAccessor wrap(final DoublesSketch ds) {
    return wrap(ds, false);
  }

  static DoublesSketchAccessor wrap(final DoublesSketch ds,
                                    final boolean forceSize) {

    if (ds.isDirect()) {
      return new DirectDoublesSketchAccessor(ds, forceSize, BB_LVL_IDX);
    }
    return new HeapDoublesSketchAccessor(ds, forceSize, BB_LVL_IDX);
  }

  abstract DoublesSketchAccessor copyAndSetLevel(final int level);

  DoublesSketchAccessor setLevel(final int lvl) {
    currLvl_ = lvl;
    if (lvl == BB_LVL_IDX) {
      numItems_ = (forceSize_ ? ds_.getK() * 2 : ds_.getBaseBufferCount());
      offset_ = (ds_.isDirect() ? COMBINED_BUFFER : 0);
    } else {
      assert lvl >= 0;
      if (((ds_.getBitPattern() & (1L << lvl)) > 0) || forceSize_) {
        numItems_ = ds_.getK();
      } else {
        numItems_ = 0;
      }

      // determine offset in two parts
      // 1. index into combined buffer (compact vs update)
      // 2. adjust if byte offset (direct) instead of array index (heap)
      final int levelStart;
      if (ds_.isCompact()) {
        levelStart = ds_.getBaseBufferCount() + (countValidLevelsBelow(lvl) * ds_.getK());
      } else {
        levelStart = (2 + currLvl_) * ds_.getK();
      }

      if (ds_.isDirect()) {
        final int preLongsAndExtra = Family.QUANTILES.getMaxPreLongs() + 2; // +2 for min, max vals
        offset_ = (preLongsAndExtra + levelStart) << 3;
      } else {
        offset_ = levelStart;
      }
    }

    n_ = ds_.getN();

    return this;
  }

  // getters/queries

  @Override
  int numItems() {
    return numItems_;
  }

  @Override
  abstract double get(final int index);

  @Override
  abstract double[] getArray(final int fromIdx, final int numItems);

  // setters/modifying methods

  @Override
  abstract double set(final int index, final double value);

  @Override
  abstract void putArray(final double[] srcArray, final int srcIndex,
                         final int dstIndex, final int numItems);

  abstract void sort();

  /**
   * Counts number of full levels in the sketch below tgtLvl. Useful for computing the level
   * offset in a compact sketch.
   * @param tgtLvl Target level in the sketch
   * @return Number of full levels in the sketch below tgtLvl
   */
  private int countValidLevelsBelow(final int tgtLvl) {
    int count = 0;
    long bitPattern = ds_.getBitPattern();
    for (int i = 0; (i < tgtLvl) && (bitPattern > 0); ++i, bitPattern >>>= 1) {
      if ((bitPattern & 1L) > 0L) {
        ++count;
      }
    }
    return count;

    // shorter implementation, testing suggests a tiny bit slower
    //final long mask = (1 << tgtLvl) - 1;
    //return Long.bitCount(ds_.getBitPattern() & mask);
  }
}
