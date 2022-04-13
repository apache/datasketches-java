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

import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;

import org.apache.datasketches.memory.Memory;

/**
 * This class implements an on-heap doubles KllSketch.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
final class KllHeapDoublesSketch extends KllDoublesSketch {
  private final int k_;    // configured value of K.
  private final int m_;    // configured value of M.
  private long n_;        // number of items input into this sketch.
  private int minK_;    // dynamic minK for error estimation after merging with different k.
  private int numLevels_; // one-based number of current levels.
  private boolean isLevelZeroSorted_;
  private double minDoubleValue_;
  private double maxDoubleValue_;
  private double[] doubleItems_;

  /**
   * New instance heap constructor with a given parameters <em>k</em> and <em>m</em>.
   *
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * <em>k</em> can be any value between <em>m</em> and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about 1.65%.
   * Higher values of <em>k</em> will have smaller error but the sketch will be larger (and slower).
   * @param m parameter controls the minimum level width in items. It can be 2, 4, 6 or 8.
   * The DEFAULT_M, which is 8 is recommended. Other values of <em>m</em> should be considered
   * experimental as they have not been as well characterized.
   */
  KllHeapDoublesSketch(final int k, final int m) {
    super(null, null);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    this.k_ = k;
    this.m_ = m;
    n_ = 0;
    minK_ = k;
    numLevels_ = 1;
    isLevelZeroSorted_ = false;
    levelsArr = new int[] {k, k};
    minDoubleValue_ = Double.NaN;
    maxDoubleValue_ = Double.NaN;
    doubleItems_ = new double[k];
  }

  /**
   * Heapify constructor.
   * @param mem Memory object that contains data serialized by this sketch.
   * @param memVal the MemoryCheck object
   */
  KllHeapDoublesSketch(final Memory srcMem, final KllMemoryValidate memVal) {
    super(null, null );
    k_ = memVal.k;
    m_ = memVal.m;
    n_ = memVal.n;
    minK_ = memVal.minK;
    numLevels_ = memVal.numLevels;
    isLevelZeroSorted_ = memVal.level0Sorted;
    final boolean updatableMemFormat = memVal.updatableMemFormat;

    if (memVal.empty && !updatableMemFormat) {
      levelsArr = memVal.levelsArr;
      minDoubleValue_ = Double.NaN;
      maxDoubleValue_ = Double.NaN;
      doubleItems_ = new double[k_];
    }
    else if (memVal.singleItem && !updatableMemFormat) {
      levelsArr = memVal.levelsArr;
      final double value = srcMem.getDouble(DATA_START_ADR_SINGLE_ITEM);
      minDoubleValue_ = maxDoubleValue_ = value;
      doubleItems_ = new double[k_];
      doubleItems_[k_ - 1] = value;
    }
    else { //Full or updatableMemFormat
      int offsetBytes = DATA_START_ADR;
      levelsArr = memVal.levelsArr;
      offsetBytes += (updatableMemFormat ? levelsArr.length * Integer.BYTES : (levelsArr.length - 1) * Integer.BYTES);
      minDoubleValue_ = srcMem.getDouble(offsetBytes);
      offsetBytes += Double.BYTES;
      maxDoubleValue_ = srcMem.getDouble(offsetBytes);
      offsetBytes += Double.BYTES;
      final int capacityItems = levelsArr[numLevels_];
      final int retainedItems = capacityItems - levelsArr[0];
      doubleItems_ = new double[capacityItems];
      final int shift = levelsArr[0];
      if (updatableMemFormat) {
        offsetBytes += shift * Double.BYTES;
        srcMem.getDoubleArray(offsetBytes, doubleItems_, shift, retainedItems);
      } else {
        srcMem.getDoubleArray(offsetBytes, doubleItems_, shift, retainedItems);
      }
    }
  }

  @Override
  public int getK() {
    return k_;
  }

  @Override
  public long getN() {
    return n_;
  }

  @Override
  double[] getDoubleItemsArray() { return doubleItems_; }

  @Override
  double getDoubleItemsArrayAt(final int index) { return doubleItems_[index]; }

  @Override
  int getM() {
    return m_;
  }

  @Override
  double getMaxDoubleValue() { return maxDoubleValue_; }

  @Override
  double getMinDoubleValue() { return minDoubleValue_; }

  @Override
  int getMinK() {
    return minK_;
  }

  @Override
  int getNumLevels() {
    return numLevels_;
  }

  @Override
  void incN() {
    n_++;
  }

  @Override
  void incNumLevels() {
    numLevels_++;
  }

  @Override
  boolean isLevelZeroSorted() {
    return isLevelZeroSorted_;
  }

  @Override
  void setDoubleItemsArray(final double[] doubleItems) { doubleItems_ = doubleItems; }

  @Override
  void setDoubleItemsArrayAt(final int index, final double value) { doubleItems_[index] = value; }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    this.isLevelZeroSorted_ = sorted;
  }

  @Override
  void setMaxDoubleValue(final double value) { maxDoubleValue_ = value; }

  @Override
  void setMinDoubleValue(final double value) { minDoubleValue_ = value; }

  @Override
  void setMinK(final int minK) {
    minK_ = minK;
  }

  @Override
  void setN(final long n) {
    n_ = n;
  }

  @Override
  void setNumLevels(final int numLevels) {
    numLevels_ = numLevels;
  }

}
