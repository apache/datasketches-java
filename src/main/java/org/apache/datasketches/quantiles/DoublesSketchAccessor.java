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

import static org.apache.datasketches.quantiles.PreambleUtil.COMBINED_BUFFER;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;

/**
 * This allows access to package-private levels and data in whatever doubles quantiles sketch you give
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
  int offset_; //bytes for Direct, doubles for heap

  DoublesSketchAccessor(
      final DoublesSketch ds,
      final boolean forceSize,
      final int level) {
    checkLvl(level);
    ds_ = ds;
    forceSize_ = forceSize;
    setLevel(level);
  }

  private static final void checkLvl(final int level) {
    if ((level != BB_LVL_IDX) && (level < 0)) {
      throw new SketchesArgumentException("Parameter level is < 0.");
    }
  }

  /**
   * Wrap the given DoublesSketch
   * @param ds the given DoublesSketch
   * @param forceSize Generally, this must be true if modeling an updatable sketch, i.e., not compact.
   * See {@link #setLevel(int) setLevel(int)} below.
   *
   * @return this
   */
  static DoublesSketchAccessor wrap(final DoublesSketch ds, final boolean forceSize) {
    if (ds.hasMemorySegment()) {
      return new DirectDoublesSketchAccessor(ds, forceSize, BB_LVL_IDX);
    }
    return new HeapDoublesSketchAccessor(ds, forceSize, BB_LVL_IDX);
  }

  abstract DoublesSketchAccessor copyAndSetLevel(final int level);

  /*
   * This initializes the following internal member variables:
   * <ul>
   * <li><i>n_</i> The local copy of DoublesSketch.getN().</li>
   * <li><i>currLvl_</i> The current level being processed.</li>
   * <li><i>numItems_</i> The number of items in this level.</li>
   * <li><i>offset_</i> If accessing a MemorySegment sketch, this is bytes offset from the start of the segment.
   * If the sketch is on-heap, this is the item offset within the given level in item units.</li>
   * </ul>
   *
   * <p>If the constructor parameter <i>forceSize</i> is true and accessing the BaseBuffer,
   * it forces the <i>numItems_</i> to be 2K independent of the actual getBaseBufferCount().</p>
   *
   * <p>If <i>forceSize</i> is false and accessing the BaseBuffer, <i>numItems_</i> is set to <i>getBaseBufferCount()</i>.
   *
   * <p>If <i>forceSize</i> is true and accessing a higher level, the <i>numItems_</i> is always <i>K</i>.</p>
   *
   * <p>If <i>forceSize</i> is false and accessing a higher level, <i>numItems_</i> is <i>K</i>
   * only if the level is valid (i.e., a corresponding bit in <i>bitPattern</i> is one), otherwise it is zero.</p>
   *
   * @param lvl the given level to process.
   */
  DoublesSketchAccessor setLevel(final int lvl) {
    currLvl_ = lvl;
    if (lvl == BB_LVL_IDX) {
      numItems_ = (forceSize_ ? ds_.getK() * 2 : ds_.getBaseBufferCount());
      offset_ = (ds_.hasMemorySegment() ? COMBINED_BUFFER : 0);
    } else {
      //compact only keeps valid levels, updatable retains all levels even if not valid.
      if (((ds_.getBitPattern() & (1L << lvl)) > 0) || forceSize_) {
        numItems_ = ds_.getK();
      } else {
        numItems_ = 0;
      }

      // determine offset in two parts
      // 1. index into combined buffer (compact vs update)
      // 2. adjust if byte offset (direct) instead of doubles array index (heap)
      final int levelStart;
      if (ds_.isCompact()) {
        levelStart = ds_.getBaseBufferCount() + (countValidLevelsBelow(lvl) * ds_.getK());
      } else {
        levelStart = (2 + currLvl_) * ds_.getK();
      }

      if (ds_.hasMemorySegment()) {
        final int preLongsAndExtra = Family.QUANTILES.getMaxPreLongs() + 2; // +2 for min, max
        offset_ = (preLongsAndExtra + levelStart) << 3; //bytes relative to MemorySegment start
      } else {
        offset_ = levelStart; //double array index
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
  abstract double set(final int index, final double quantile);

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
