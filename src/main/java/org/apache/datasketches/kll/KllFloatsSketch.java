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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.Util.isOdd;
import static org.apache.datasketches.kll.KllHelper.getAllLevelStatsGivenN;
import static org.apache.datasketches.kll.PreambleUtil.DATA_START_ADR_FLOAT;
import static org.apache.datasketches.kll.PreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.PreambleUtil.DEFAULT_K;
import static org.apache.datasketches.kll.PreambleUtil.DEFAULT_M;
import static org.apache.datasketches.kll.PreambleUtil.EMPTY_BIT_MASK;
import static org.apache.datasketches.kll.PreambleUtil.FAMILY_BYTE_ADR;
import static org.apache.datasketches.kll.PreambleUtil.FLAGS_BYTE_ADR;
import static org.apache.datasketches.kll.PreambleUtil.K_SHORT_ADR;
import static org.apache.datasketches.kll.PreambleUtil.LEVEL_ZERO_SORTED_BIT_MASK;
import static org.apache.datasketches.kll.PreambleUtil.MIN_K_SHORT_ADR;
import static org.apache.datasketches.kll.PreambleUtil.M_BYTE_ADR;
import static org.apache.datasketches.kll.PreambleUtil.NUM_LEVELS_BYTE_ADR;
import static org.apache.datasketches.kll.PreambleUtil.N_LONG_ADR;
import static org.apache.datasketches.kll.PreambleUtil.PREAMBLE_INTS_BYTE_ADR;
import static org.apache.datasketches.kll.PreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.PreambleUtil.PREAMBLE_INTS_FLOAT;
import static org.apache.datasketches.kll.PreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.PreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.PreambleUtil.SER_VER_BYTE_ADR;
import static org.apache.datasketches.kll.PreambleUtil.SINGLE_ITEM_BIT_MASK;
import static org.apache.datasketches.kll.PreambleUtil.UPDATABLE_BIT_MASK;

import java.util.Arrays;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.kll.KllHelper.LevelStats;
import org.apache.datasketches.kll.PreambleUtil.MemoryCheck;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

// (leave blank)
/**
 * Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}
 */
public class KllFloatsSketch extends BaseKllSketch {

  // Specific to the floats sketch
  private float[] items_; // the continuous array of float items
  private float minValue_;
  private float maxValue_;
  private static final boolean IS_DOUBLE = false;

  /**
   * Heap constructor with the default <em>k = 200</em>, which has a rank error of about 1.65%.
   */
  public KllFloatsSketch() {
    this(DEFAULT_K);
  }

  /**
   * Heap constructor with a given parameter <em>k</em>. <em>k</em> can be any value between 8 and
   * 65535, inclusive. The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Higher values of K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates
   */
  public KllFloatsSketch(final int k) {
    this(k, DEFAULT_M, true);
  }

  /**
   * Used for testing only.
   * @param k configured size of sketch. Range [m, 2^16]
   * @param compatible if true, compatible with quantiles sketch treatment of rank 0.0 and 1.0.
   */
  KllFloatsSketch(final int k, final boolean compatible) {
    this(k, DEFAULT_M, compatible);
  }

  /**
   * Heap constructor.
   * @param k configured size of sketch. Range [m, 2^16]
   * @param m minimum level size. Default is 8.
   * @param compatible if true, compatible with quantiles sketch treatment of rank 0.0 and 1.0.
   */
  private KllFloatsSketch(final int k, final int m, final boolean compatible) {
    super(k, m, compatible);
    items_ = new float[k];
    minValue_ = Float.NaN;
    maxValue_ = Float.NaN;
  }

  /**
   * Private heapify constructor.
   * @param mem Memory object that contains data serialized by this sketch.
   * @param memChk the MemoryCheck object
   */
  private KllFloatsSketch(final Memory mem, final MemoryCheck memChk) {
    super(memChk.k, memChk.m, true);
    isLevelZeroSorted_ = memChk.level0Sorted;
    if (memChk.empty) {
      numLevels_ = 1;
      levels_ = new int[] {k_, k_};
      isLevelZeroSorted_ = false;
      minK_ = k_;
      items_ = new float[k_];
      minValue_ = Float.NaN;
      maxValue_ = Float.NaN;
    } else if (memChk.singleItem) {
      n_ = 1;
      minK_ = k_;
      numLevels_ = 1;
      levels_ = new int[numLevels_ + 1];
      final int itemCapacity = KllHelper.computeTotalItemCapacity(k_, m_, numLevels_);
      levels_[0] = itemCapacity - 1;
      levels_[numLevels_] = itemCapacity; //load the last integer in levels_
      items_ = new float[itemCapacity];
      items_[levels_[0]] = mem.getFloat(DATA_START_ADR_SINGLE_ITEM);
      minValue_ = items_[levels_[0]];
      maxValue_ = items_[levels_[0]];
    } else {
      n_ = memChk.n;
      minK_ = memChk.minK;
      numLevels_ = memChk.numLevels;
      levels_ = new int[numLevels_ + 1];
      int offset = DATA_START_ADR_FLOAT;
      final int itemCapacity = KllHelper.computeTotalItemCapacity(k_, m_, numLevels_);
      if (memChk.updatable) {
        // If updatable the last integer in levels_ IS serialized.
        mem.getIntArray(offset, levels_, 0, numLevels_ + 1); //load levels_
        offset += (numLevels_ + 1) * Integer.BYTES;
      } else {
        // If compact the last integer in levels_ is not serialized.
        mem.getIntArray(offset, levels_, 0, numLevels_); //load levels_
        offset += numLevels_ * Integer.BYTES;
        levels_[numLevels_] = itemCapacity; //load the last integer in levels_
      }
      minValue_ = mem.getFloat(offset);
      offset += Float.BYTES;
      maxValue_ = mem.getFloat(offset);
      offset += Float.BYTES;
      items_ = new float[itemCapacity];
      mem.getFloatArray(offset, items_, levels_[0], getNumRetained());
    }
  }

  /**
   * Factory heapify takes the sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param mem a Memory image of a sketch serialized by this sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based sketch based on the given Memory.
   */
  //To simplify the code, the PreambleUtil.MemoryCheck does nearly all the validity checking.
  //The verified Memory is then passed to the actual private heapify constructor.
  public static KllFloatsSketch heapify(final Memory mem) {
    final MemoryCheck memChk = new MemoryCheck(mem);
    if (memChk.doublesSketch) {
      throw new SketchesArgumentException("Memory object is not a KllFloatsSketch.");
    }
    return new KllFloatsSketch(mem, memChk);
  }

  // public functions

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of splitPoint (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(false) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing float values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @return an array of m+1 double values, which are a consecutive approximation to the CDF
   * of the input stream given the splitPoints. The value at array position j of the returned
   * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
   * array.
   */
  public double[] getCDF(final float[] splitPoints) {
    return getPmfOrCdf(splitPoints, true);
  }

  /**
   * Returns the max value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the max value of the stream
   */
  public float getMaxValue() {
    return maxValue_;
  }

  /**
   * Returns the min value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the min value of the stream
   */
  public float getMinValue() {
    return minValue_;
  }

  //Size related

  /**
   * Returns upper bound on the compact serialized size of a sketch given a parameter <em>k</em> and stream
   * length. This method can be used if allocation of storage is necessary beforehand.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @return upper bound on the compact serialized size
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n) {
    final LevelStats lvlStats = getAllLevelStatsGivenN(k, DEFAULT_M, n, false, false, IS_DOUBLE);
    return lvlStats.getCompactBytes();
  }

  /**
   * Returns the current compact number of bytes this sketch would require to store.
   * @return the current compact number of bytes this sketch would require to store.
   */
  public int getCurrentCompactSerializedSizeBytes() {
    return KllHelper.getSerializedSizeBytes(numLevels_, getNumRetained(), IS_DOUBLE, false);
  }

  /**
   * Returns the current updatable number of bytes this sketch would require to store.
   * @return the current updatable number of bytes this sketch would require to store.
   */
  public int getCurrentUpdatableSerializedSizeBytes() {
    return KllHelper.getSerializedSizeBytes(numLevels_, getNumRetained(), IS_DOUBLE, true);
  }

  /**
   * Returns the number of bytes this sketch would require to store.
   * @return the number of bytes this sketch would require to store.
   * @deprecated use {@link #getCurrentCompactSerializedSizeBytes() }
   */
  @Deprecated
  public int getSerializedSizeBytes() {
    return getCurrentCompactSerializedSizeBytes();
  }

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of splitPoints (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(true) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing float values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream values (the mass) that fall into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint, with the exception that the last interval will include maximum value.
   */
  public double[] getPMF(final float[] splitPoints) {
    return getPmfOrCdf(splitPoints, false);
  }

  /**
   * Returns an approximation to the value of the data item
   * that would be preceded by the given fraction of a hypothetical sorted
   * version of the input stream so far.
   *
   * <p>We note that this method has a fairly large overhead (microseconds instead of nanoseconds)
   * so it should not be called multiple times to get different quantiles from the same
   * sketch. Instead use getQuantiles(), which pays the overhead only once.
   *
   * <p>If the sketch is empty this returns NaN.
   *
   * @param fraction the specified fractional position in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * If fraction = 0.0, the true minimum value of the stream is returned.
   * If fraction = 1.0, the true maximum value of the stream is returned.
   *
   * @return the approximation to the value at the given fraction
   */
  public float getQuantile(final double fraction) {
    if (isEmpty()) { return Float.NaN; }
    if (fraction < 0.0 || fraction > 1.0) {
      throw new SketchesArgumentException("Fraction cannot be less than zero nor greater than 1.0");
    }
    if (compatible) {
      if (fraction == 0.0) { return minValue_; }
      if (fraction == 1.0) { return maxValue_; }
    }
    final KllFloatsQuantileCalculator quant = getQuantileCalculator();
    return quant.getQuantile(fraction);
  }

  /**
   * Gets the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public float getQuantileUpperBound(final double fraction) {
    return getQuantile(min(1.0, fraction + KllHelper.getNormalizedRankError(minK_, false)));
  }

  /**
   * Gets the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public float getQuantileLowerBound(final double fraction) {
    return getQuantile(max(0, fraction - KllHelper.getNormalizedRankError(minK_, false)));
  }

  /**
   * This is a more efficient multiple-query version of getQuantile().
   *
   * <p>This returns an array that could have been generated by using getQuantile() with many
   * different fractional ranks, but would be very inefficient.
   * This method incurs the internal set-up overhead once and obtains multiple quantile values in
   * a single query. It is strongly recommend that this method be used instead of multiple calls
   * to getQuantile().
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param fractions given array of fractional positions in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * These fractions must be in the interval [0.0, 1.0], inclusive.
   *
   * @return array of approximations to the given fractions in the same order as given fractions
   * array.
   */
  public float[] getQuantiles(final double[] fractions) {
    if (isEmpty()) { return null; }
    KllFloatsQuantileCalculator quant = null;
    final float[] quantiles = new float[fractions.length];
    for (int i = 0; i < fractions.length; i++) {
      final double fraction = fractions[i];
      if (fraction < 0.0 || fraction > 1.0) {
        throw new SketchesArgumentException("Fraction cannot be less than zero nor greater than 1.0");
      }
      if      (fraction == 0.0 && compatible) { quantiles[i] = minValue_; }
      else if (fraction == 1.0 && compatible) { quantiles[i] = maxValue_; }
      else {
        if (quant == null) {
          quant = getQuantileCalculator();
        }
        quantiles[i] = quant.getQuantile(fraction);
      }
    }
    return quantiles;
  }

  /**
   * This is also a more efficient multiple-query version of getQuantile() and allows the caller to
   * specify the number of evenly spaced fractional ranks.
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param numEvenlySpaced an integer that specifies the number of evenly spaced fractional ranks.
   * This must be a positive integer greater than 0. A value of 1 will return the min value.
   * A value of 2 will return the min and the max value. A value of 3 will return the min,
   * the median and the max value, etc.
   *
   * @return array of approximations to the given fractions in the same order as given fractions
   * array.
   */
  public float[] getQuantiles(final int numEvenlySpaced) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.Util.evenlySpaced(0.0, 1.0, numEvenlySpaced));
  }

  /**
   * Returns an approximation to the normalized (fractional) rank of the given value from 0 to 1,
   * inclusive.
   *
   * <p>The resulting approximation has a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(false) function.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @param value to be ranked
   * @return an approximate rank of the given value
   */
  public double getRank(final float value) {
    if (isEmpty()) { return Double.NaN; }
    int level = 0;
    int weight = 1;
    long total = 0;
    while (level < numLevels_) {
      final int fromIndex = levels_[level];
      final int toIndex = levels_[level + 1]; // exclusive
      for (int i = fromIndex; i < toIndex; i++) {
        if (items_[i] < value) {
          total += weight;
        } else if (level > 0 || isLevelZeroSorted_) {
          break; // levels above 0 are sorted, no point comparing further
        }
      }
      level++;
      weight *= 2;
    }
    return (double) total / n_;
  }

  /**
   * @return the iterator for this class
   */
  public KllFloatsSketchIterator iterator() {
    return new KllFloatsSketchIterator(items_, levels_, numLevels_);
  }

  /**
   * Merges another sketch into this one.
   * @param other sketch to merge into this one
   */
  public void merge(final KllFloatsSketch other) {
    if (other == null || other.isEmpty()) { return; }
    if (m_ != other.m_) {
      throw new SketchesArgumentException("incompatible M: " + m_ + " and " + other.m_);
    }
    final long finalN = n_ + other.n_;
    //update this sketch with level0 items from the other sketch
    for (int i = other.levels_[0]; i < other.levels_[1]; i++) {
      update(other.items_[i]);
    }
    if (other.numLevels_ >= 2) { //now merge other levels if they exist
      mergeHigherLevels(other, finalN);
    }
    //update min, max values, n
    if (Float.isNaN(minValue_) || other.minValue_ < minValue_) { minValue_ = other.minValue_; }
    if (Float.isNaN(maxValue_) || other.maxValue_ > maxValue_) { maxValue_ = other.maxValue_; }
    n_ = finalN;

    assert KllHelper.sumTheSampleWeights(numLevels_, levels_) == n_;
    if (other.isEstimationMode()) {
      minK_ = min(minK_, other.minK_);
    }
  }

  @Override
  public byte[] toByteArray() {
    final byte[] bytes = new byte[getCurrentCompactSerializedSizeBytes()];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);
    final boolean singleItem = n_ == 1;
    final boolean empty = isEmpty();
    //load the preamble
    wmem.putByte(PREAMBLE_INTS_BYTE_ADR, (byte)
        (empty || singleItem ? PREAMBLE_INTS_EMPTY_SINGLE : PREAMBLE_INTS_FLOAT));
    wmem.putByte(SER_VER_BYTE_ADR, singleItem ? SERIAL_VERSION_SINGLE : SERIAL_VERSION_EMPTY_FULL);
    wmem.putByte(FAMILY_BYTE_ADR, (byte) Family.KLL.getID());
    final byte flags = (byte) (
        (empty ? EMPTY_BIT_MASK : 0)
      | (isLevelZeroSorted_ ? LEVEL_ZERO_SORTED_BIT_MASK : 0)
      | (singleItem ? SINGLE_ITEM_BIT_MASK : 0));
    // (leave blank)
    wmem.putByte(FLAGS_BYTE_ADR, flags);
    wmem.putShort(K_SHORT_ADR, (short) k_);
    wmem.putByte(M_BYTE_ADR, (byte) m_);
    if (empty) { return bytes; }
    //load data
    int offset = DATA_START_ADR_SINGLE_ITEM;
    if (!singleItem) {
      wmem.putLong(N_LONG_ADR, n_);
      wmem.putShort(MIN_K_SHORT_ADR, (short) minK_);
      wmem.putByte(NUM_LEVELS_BYTE_ADR, (byte) numLevels_);
      offset = DATA_START_ADR_FLOAT;
      // the last integer in levels_ is not serialized because it can be derived
      final int len = levels_.length - 1;
      wmem.putIntArray(offset, levels_, 0, len);
      offset += len * Integer.BYTES;
      wmem.putFloat(offset, minValue_);
      offset += Float.BYTES;
      wmem.putFloat(offset, maxValue_);
      offset += Float.BYTES;
    }
    wmem.putFloatArray(offset, items_, levels_[0], getNumRetained());
    return bytes;
  }

  @Override
  public byte[] toUpdatableByteArray() {
    final int itemCap = KllHelper.computeTotalItemCapacity(k_, m_, numLevels_);
    final int numBytes = KllHelper.getSerializedSizeBytes(numLevels_, itemCap, IS_DOUBLE, true);
    final byte[] bytes = new byte[numBytes];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);
    //load the preamble
    wmem.putByte(PREAMBLE_INTS_BYTE_ADR, (byte) PREAMBLE_INTS_FLOAT);
    wmem.putByte(SER_VER_BYTE_ADR, SERIAL_VERSION_EMPTY_FULL);
    wmem.putByte(FAMILY_BYTE_ADR, (byte) Family.KLL.getID());
    final byte flags = (byte)
        ((isLevelZeroSorted_ ? LEVEL_ZERO_SORTED_BIT_MASK : 0)
        | UPDATABLE_BIT_MASK);
    // (leave blank)
    wmem.putByte(FLAGS_BYTE_ADR, flags);
    wmem.putShort(K_SHORT_ADR, (short) k_);
    wmem.putByte(M_BYTE_ADR, (byte) m_);
    //load data
    wmem.putLong(N_LONG_ADR, n_);
    wmem.putShort(MIN_K_SHORT_ADR, (short) minK_);
    wmem.putByte(NUM_LEVELS_BYTE_ADR, (byte) numLevels_);
    int offset = DATA_START_ADR_FLOAT;
    // the last integer in levels_ IS serialized
    final int len = levels_.length;
    wmem.putIntArray(offset, levels_, 0, len);
    offset += len * Integer.BYTES;
    wmem.putDouble(offset, minValue_);
    offset += Float.BYTES;
    wmem.putDouble(offset, maxValue_);
    offset += Float.BYTES;
    wmem.putFloatArray(offset, items_, levels_[0], getNumRetained());
    return bytes;
  }

  @Override
  public String toString(final boolean withLevels, final boolean withData) {
    final String epsPct = String.format("%.3f%%", getNormalizedRankError(false) * 100);
    final String epsPMFPct = String.format("%.3f%%", getNormalizedRankError(true) * 100);
    final StringBuilder sb = new StringBuilder();
    sb.append(Util.LS).append("### KLL Floats Sketch summary:").append(Util.LS);
    sb.append("   K                    : ").append(k_).append(Util.LS);
    sb.append("   min K                : ").append(minK_).append(Util.LS);
    sb.append("   M                    : ").append(m_).append(Util.LS);
    sb.append("   N                    : ").append(n_).append(Util.LS);
    sb.append("   Epsilon              : ").append(epsPct).append(Util.LS);
    sb.append("   Epsison PMF          : ").append(epsPMFPct).append(Util.LS);
    sb.append("   Empty                : ").append(isEmpty()).append(Util.LS);
    sb.append("   Estimation Mode      : ").append(isEstimationMode()).append(Util.LS);
    sb.append("   Levels               : ").append(numLevels_).append(Util.LS);
    sb.append("   Level 0 Sorted       : ").append(isLevelZeroSorted_).append(Util.LS);
    sb.append("   Capacity Items       : ").append(items_.length).append(Util.LS);
    sb.append("   Retained Items       : ").append(getNumRetained()).append(Util.LS);
    sb.append("   Storage Bytes        : ").append(getSerializedSizeBytes()).append(Util.LS);
    sb.append("   Min Value            : ").append(minValue_).append(Util.LS);
    sb.append("   Max Value            : ").append(maxValue_).append(Util.LS);
    sb.append("### End sketch summary").append(Util.LS);

    if (withLevels) {
      sb.append("### KLL sketch levels:").append(Util.LS)
      .append(" level, offset: nominal capacity, actual size").append(Util.LS);
      for (int i = 0; i < numLevels_; i++) {
        sb.append("   ").append(i).append(", ").append(levels_[i]).append(": ")
        .append(KllHelper.levelCapacity(k_, numLevels_, i, m_))
        .append(", ").append(KllHelper.currentLevelSize(i, numLevels_, levels_)).append(Util.LS);
      }
      sb.append("### End sketch levels").append(Util.LS);
    }

    if (withData) {
      sb.append("### KLL sketch data {index, item}:").append(Util.LS);
      if (levels_[0] > 0) {
        sb.append(" Garbage:" + Util.LS);
        for (int i = 0; i < levels_[0]; i++) {
          if (items_[i] == 0.0f) { continue; }
          sb.append("   ").append(i + ", ").append(items_[i]).append(Util.LS);
        }
      }
      int level = 0;
      while (level < numLevels_) {
        final int fromIndex = levels_[level];
        final int toIndex = levels_[level + 1]; // exclusive
        if (fromIndex < toIndex) {
          sb.append(" level[").append(level).append("]: offset: " + levels_[level] + " wt: " + (1 << level));
          sb.append(Util.LS);
        }
        for (int i = fromIndex; i < toIndex; i++) {
          sb.append("   ").append(i + ", ").append(items_[i]).append(Util.LS);
        }
        level++;
      }
      sb.append(" level[" + level + "]: offset: " + levels_[level] + " (Exclusive)");
      sb.append(Util.LS);
      sb.append("### End sketch data").append(Util.LS);
    }

    return sb.toString();
  }

  /**
   * Updates this sketch with the given data item.
   *
   * @param value an item from a stream of items. NaNs are ignored.
   */
  public void update(final float value) {
    if (Float.isNaN(value)) { return; }
    if (isEmpty()) {
      minValue_ = value;
      maxValue_ = value;
    } else {
      if (value < minValue_) { minValue_ = value; }
      if (value > maxValue_) { maxValue_ = value; }
    }
    if (levels_[0] == 0) {
      compressWhileUpdating();
    }
    n_++;
    isLevelZeroSorted_ = false;
    final int nextPos = levels_[0] - 1;
    assert levels_[0] >= 0;
    levels_[0] = nextPos;
    items_[nextPos] = value;
  }

  // Restricted Methods

  private KllFloatsQuantileCalculator getQuantileCalculator() {
    sortLevelZero(); // sort in the sketch to reuse if possible
    return new KllFloatsQuantileCalculator(items_, levels_, numLevels_, n_);
  }

  private double[] getPmfOrCdf(final float[] splitPoints, final boolean isCdf) {
    if (isEmpty()) { return null; }
    KllFloatsHelper.validateFloatValues(splitPoints);
    final double[] buckets = new double[splitPoints.length + 1];
    int level = 0;
    int weight = 1;
    while (level < numLevels_) {
      final int fromIndex = levels_[level];
      final int toIndex = levels_[level + 1]; // exclusive
      if (level == 0 && !isLevelZeroSorted_) {
        incrementBucketsUnsortedLevel(fromIndex, toIndex, weight, splitPoints, buckets);
      } else {
        incrementBucketsSortedLevel(fromIndex, toIndex, weight, splitPoints, buckets);
      }
      level++;
      weight *= 2;
    }
    // normalize and, if CDF, convert to cumulative
    if (isCdf) {
      double subtotal = 0;
      for (int i = 0; i < buckets.length; i++) {
        subtotal += buckets[i];
        buckets[i] = subtotal / n_;
      }
    } else {
      for (int i = 0; i < buckets.length; i++) {
        buckets[i] /= n_;
      }
    }
    return buckets;
  }

  private void incrementBucketsUnsortedLevel(final int fromIndex, final int toIndex,
      final int weight, final float[] splitPoints, final double[] buckets) {
    for (int i = fromIndex; i < toIndex; i++) {
      int j;
      for (j = 0; j < splitPoints.length; j++) {
        if (items_[i] < splitPoints[j]) {
          break;
        }
      }
      buckets[j] += weight;
    }
  }

  private void incrementBucketsSortedLevel(final int fromIndex, final int toIndex,
      final int weight, final float[] splitPoints, final double[] buckets) {
    int i = fromIndex;
    int j = 0;
    while (i <  toIndex && j < splitPoints.length) {
      if (items_[i] < splitPoints[j]) {
        buckets[j] += weight; // this sample goes into this bucket
        i++; // move on to next sample and see whether it also goes into this bucket
      } else {
        j++; // no more samples for this bucket
      }
    }
    // now either i == toIndex (we are out of samples), or
    // j == numSplitPoints (we are out of buckets, but there are more samples remaining)
    // we only need to do something in the latter case
    if (j == splitPoints.length) {
      buckets[j] += weight * (toIndex - i);
    }
  }

  // The following code is only valid in the special case of exactly reaching capacity while updating.
  // It cannot be used while merging, while reducing k, or anything else.
  private void compressWhileUpdating() {
    final int level = KllHelper.findLevelToCompact(k_, m_, numLevels_, levels_);

    // It is important to do add the new top level right here. Be aware that this operation
    // grows the buffer and shifts the data and also the boundaries of the data and grows the
    // levels array and increments numLevels_
    if (level == numLevels_ - 1) {
      addEmptyTopLevelToCompletelyFullSketch();
    }

    final int rawBeg = levels_[level];
    final int rawLim = levels_[level + 1];
    // +2 is OK because we already added a new top level if necessary
    final int popAbove = levels_[level + 2] - rawLim;
    final int rawPop = rawLim - rawBeg;
    final boolean oddPop = isOdd(rawPop);
    final int adjBeg = oddPop ? rawBeg + 1 : rawBeg;
    final int adjPop = oddPop ? rawPop - 1 : rawPop;
    final int halfAdjPop = adjPop / 2;

    // level zero might not be sorted, so we must sort it if we wish to compact it
    if (level == 0) {
      Arrays.sort(items_, adjBeg, adjBeg + adjPop);
    }
    if (popAbove == 0) {
      KllFloatsHelper.randomlyHalveUpFloats(items_, adjBeg, adjPop, random);
    } else {
      KllFloatsHelper.randomlyHalveDownFloats(items_, adjBeg, adjPop, random);
      KllFloatsHelper.mergeSortedFloatArrays(
          items_, adjBeg, halfAdjPop,
          items_, rawLim, popAbove,
          items_, adjBeg + halfAdjPop);
    }
    levels_[level + 1] -= halfAdjPop;          // adjust boundaries of the level above
    if (oddPop) {
      levels_[level] = levels_[level + 1] - 1; // the current level now contains one item
      items_[levels_[level]] = items_[rawBeg]; // namely this leftover guy
    } else {
      levels_[level] = levels_[level + 1];     // the current level is now empty
    }

    // verify that we freed up halfAdjPop array slots just below the current level
    assert levels_[level] == rawBeg + halfAdjPop;

    // finally, we need to shift up the data in the levels below
    // so that the freed-up space can be used by level zero
    if (level > 0) {
      final int amount = rawBeg - levels_[0];
      System.arraycopy(items_, levels_[0], items_, levels_[0] + halfAdjPop, amount);
      for (int lvl = 0; lvl < level; lvl++) {
        levels_[lvl] += halfAdjPop;
      }
    }
  }

  private void addEmptyTopLevelToCompletelyFullSketch() {
    final int curTotalCap = levels_[numLevels_];

    // make sure that we are following a certain growth scheme
    assert levels_[0] == 0; //definition of full
    assert items_.length == curTotalCap;

    // note that merging MIGHT over-grow levels_, in which case we might not have to grow it here
    if (levels_.length < numLevels_ + 2) {
      levels_ = KllHelper.growIntArray(levels_, numLevels_ + 2);
    }

    final int deltaCap = KllHelper.levelCapacity(k_, numLevels_ + 1, 0, m_);
    final int newTotalCap = curTotalCap + deltaCap;

    final float[] newBuf = new float[newTotalCap];

    // copy (and shift) the current data into the new buffer
    System.arraycopy(items_, levels_[0], newBuf, levels_[0] + deltaCap, curTotalCap);
    items_ = newBuf;

    // this loop includes the old "extra" index at the top
    for (int i = 0; i <= numLevels_; i++) {
      levels_[i] += deltaCap;
    }

    assert levels_[numLevels_] == newTotalCap;

    numLevels_++;
    levels_[numLevels_] = newTotalCap; // initialize the new "extra" index at the top
  }

  private void sortLevelZero() {
    if (!isLevelZeroSorted_) {
      Arrays.sort(items_, levels_[0], levels_[1]);
      isLevelZeroSorted_ = true;
    }
  }

  private void mergeHigherLevels(final KllFloatsSketch other, final long finalN) {
    final int tmpSpaceNeeded = getNumRetained()
        + KllHelper.getNumRetainedAboveLevelZero(other.numLevels_, other.levels_);
    final float[] workbuf = new float[tmpSpaceNeeded];
    final int ub = KllHelper.ubOnNumLevels(finalN);
    final int[] worklevels = new int[ub + 2]; // ub+1 does not work
    final int[] outlevels  = new int[ub + 2];

    final int provisionalNumLevels = max(numLevels_, other.numLevels_);

    populateWorkArrays(other, workbuf, worklevels, provisionalNumLevels);

    // notice that workbuf is being used as both the input and output here
    final int[] result = KllFloatsHelper.generalFloatsCompress(k_, m_, provisionalNumLevels, workbuf,
        worklevels, workbuf, outlevels, isLevelZeroSorted_, random);
    final int finalNumLevels = result[0];
    final int finalCapacity = result[1];
    final int finalPop = result[2];

    assert finalNumLevels <= ub; // ub may be much bigger

    // now we need to transfer the results back into the "self" sketch
    final float[] newbuf = finalCapacity == items_.length ? items_ : new float[finalCapacity];
    final int freeSpaceAtBottom = finalCapacity - finalPop;
    System.arraycopy(workbuf, outlevels[0], newbuf, freeSpaceAtBottom, finalPop);
    final int theShift = freeSpaceAtBottom - outlevels[0];

    if (levels_.length < finalNumLevels + 1) {
      levels_ = new int[finalNumLevels + 1];
    }

    for (int lvl = 0; lvl < finalNumLevels + 1; lvl++) { // includes the "extra" index
      levels_[lvl] = outlevels[lvl] + theShift;
    }

    items_ = newbuf;
    numLevels_ = finalNumLevels;
  }

  private void populateWorkArrays(final KllFloatsSketch other, final float[] workbuf,
      final int[] worklevels, final int provisionalNumLevels) {
    worklevels[0] = 0;

    // Note: the level zero data from "other" was already inserted into "self"
    final int selfPopZero = KllHelper.currentLevelSize(0, numLevels_, levels_);
    System.arraycopy(items_, levels_[0], workbuf, worklevels[0], selfPopZero);
    worklevels[1] = worklevels[0] + selfPopZero;

    for (int lvl = 1; lvl < provisionalNumLevels; lvl++) {
      final int selfPop = KllHelper.currentLevelSize(lvl, numLevels_, levels_);
      final int otherPop = KllHelper.currentLevelSize(lvl, other.numLevels_, other.levels_);
      worklevels[lvl + 1] = worklevels[lvl] + selfPop + otherPop;

      if (selfPop > 0 && otherPop == 0) {
        System.arraycopy(items_, levels_[lvl], workbuf, worklevels[lvl], selfPop);
      } else if (selfPop == 0 && otherPop > 0) {
        System.arraycopy(other.items_, other.levels_[lvl], workbuf, worklevels[lvl], otherPop);
      } else if (selfPop > 0 && otherPop > 0) {
        KllFloatsHelper.mergeSortedFloatArrays(items_, levels_[lvl], selfPop, other.items_,
            other.levels_[lvl], otherPop, workbuf, worklevels[lvl]);
      }
    }
  }

  // for testing

  float[] getItems() {
    return items_;
  }

}
