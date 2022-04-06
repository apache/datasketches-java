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
import static org.apache.datasketches.kll.KllSketch.Error.SRC_MUST_BE_FLOAT;
import static org.apache.datasketches.kll.KllSketch.Error.MUST_NOT_CALL;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class implements an on-heap floats KllSketch.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
public final class KllHeapFloatsSketch extends KllSketch {
  private float[] floatItems_;
  private float minFloatValue_;
  private float maxFloatValue_;

  /**
   * Private heapify constructor.
   * @param mem Memory object that contains data serialized by this sketch.
   * @param memVal the MemoryCheck object
   */
  private KllHeapFloatsSketch(final Memory mem, final KllMemoryValidate memVal) {
    super(SketchType.FLOATS_SKETCH, null, null);
    k = memVal.k;
    m = memVal.m;
    KllHelper.buildHeapKllSketchFromMemory(this, memVal);
  }

  /**
   * Heap constructor with the default <em>k = 200</em>.
   * This will have a rank error of about 1.65%.
   */
  public KllHeapFloatsSketch() {
    this(KllSketch.DEFAULT_K, KllSketch.DEFAULT_M);
  }

  /**
   * Heap constructor with a given parameter <em>k</em>. <em>k</em> can be any value between 8 and
   * 65535, inclusive. The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Higher values of K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates
   */
  public KllHeapFloatsSketch(final int k) {
    this(k, KllSketch.DEFAULT_M);
  }

  /**
   * Heap constructor with a given parameters <em>k</em> and <em>m</em>.
   *
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * <em>k</em> can be any value between <em>m</em> and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about 1.65%.
   * Higher values of <em>k</em> will have smaller error but the sketch will be larger (and slower).
   * @param m parameter that controls the minimum level width in items. It can be 2, 4, 6 or 8.
   * The DEFAULT_M, which is 8 is recommended. Other values of <em>m</em> should be considered
   * experimental as they have not been as well characterized.
   */
  KllHeapFloatsSketch(final int k, final int m) {
    super(SketchType.FLOATS_SKETCH, null, null);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    this.k = k;
    this.m = m;
    n_ = 0;
    minK_ = k;
    numLevels_ = 1;
    levels_ = new int[] {k, k};
    isLevelZeroSorted_ = false;
    floatItems_ = new float[k];
    minFloatValue_ = Float.NaN;
    maxFloatValue_ = Float.NaN;
  }

  /**
   * Factory heapify takes the sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param mem a Memory image of a sketch serialized by this sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based sketch based on the given Memory.
   */
  public static KllHeapFloatsSketch heapify(final Memory mem) {
    final KllMemoryValidate memVal = new KllMemoryValidate(mem);
    if (memVal.doublesSketch) { Error.kllSketchThrow(SRC_MUST_BE_FLOAT); }
    return new KllHeapFloatsSketch(mem, memVal);
  }

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
   * @return an array of m+1 double values on the interval [0.0, 1.0),
   * which are a consecutive approximation to the CDF of the input stream given the splitPoints.
   * The value at array position j of the returned CDF array is the sum of the returned values
   * in positions 0 through j of the returned PMF array.
   */
  public double[] getCDF(final float[] splitPoints) {
    return KllFloatsHelper.getFloatsPmfOrCdf(this, splitPoints, true);
  }

  /**
   * Returns the max value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the max value of the stream
   */
  public float getMaxValue() { return getMaxFloatValue(); }

  /**
   * Returns the min value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the min value of the stream
   */
  public float getMinValue() { return getMinFloatValue(); }

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
   * @return an array of m+1 doubles on the interval [0.0, 1.0),
   * each of which is an approximation to the fraction of the total input stream values
   * (the mass) that fall into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint, with the exception that the last interval will include maximum value.
   */
  public double[] getPMF(final float[] splitPoints) {
    return KllFloatsHelper.getFloatsPmfOrCdf(this, splitPoints, false);
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
    return KllFloatsHelper.getFloatsQuantile(this, fraction);
  }

  /**
   * Gets the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public float getQuantileLowerBound(final double fraction) {
    return getQuantile(max(0, fraction - KllHelper.getNormalizedRankError(getMinK(), false)));
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
    return KllFloatsHelper.getFloatsQuantiles(this, fractions);
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
   * Gets the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public float getQuantileUpperBound(final double fraction) {
    return getQuantile(min(1.0, fraction + KllHelper.getNormalizedRankError(getMinK(), false)));
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
    return KllFloatsHelper.getFloatRank(this, value);
  }

  /**
   * @return the iterator for this class
   */
  public KllFloatsSketchIterator iterator() {
    return new KllFloatsSketchIterator(getFloatItemsArray(), getLevelsArray(), getNumLevels());
  }

  /**
   * Merges another sketch into this one.
   * @param other sketch to merge into this one
   */
  public void merge(final KllHeapFloatsSketch other) {
    if (!other.isFloatsSketch()) { kllSketchThrow(SRC_MUST_BE_FLOAT); }
    KllFloatsHelper.mergeFloatImpl(this, other);
  }

  @Override
  public void reset() {
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelsArray(new int[] {k, k});
    setLevelZeroSorted(false);
    floatItems_ = new float[k];
    minFloatValue_ = Float.NaN;
    maxFloatValue_ = Float.NaN;
  }

  /**
   * Updates this sketch with the given data item.
   *
   * @param value an item from a stream of items. NaNs are ignored.
   */
  public void update(final float value) {
    KllFloatsHelper.updateFloat(this, value);
  }

  @Override //Dummy
  double[] getDoubleItemsArray() { kllSketchThrow(MUST_NOT_CALL); return null; }

  @Override //Dummy
  double getDoubleItemsArrayAt(final int index) { kllSketchThrow(MUST_NOT_CALL); return Double.NaN; }

  @Override //Used internally
  float[] getFloatItemsArray() { return floatItems_; }

  @Override //Used internally
  float getFloatItemsArrayAt(final int index) { return floatItems_[index]; }

  @Override //Dummy
  double getMaxDoubleValue() { kllSketchThrow(MUST_NOT_CALL); return maxFloatValue_; }

  @Override //Used internally
  float getMaxFloatValue() { return maxFloatValue_; }

  @Override //Dummy
  double getMinDoubleValue() { kllSketchThrow(MUST_NOT_CALL); return minFloatValue_; }

  @Override //Used internally
  float getMinFloatValue() { return minFloatValue_; }

  @Override //Dummy
  void setDoubleItemsArray(final double[] doubleItems) { kllSketchThrow(MUST_NOT_CALL); }

  @Override //Dummy
  void setDoubleItemsArrayAt(final int index, final double value) { kllSketchThrow(MUST_NOT_CALL); }

  @Override //Used internally
  void setFloatItemsArray(final float[] floatItems) { floatItems_ = floatItems; }

  @Override //Used internally
  void setFloatItemsArrayAt(final int index, final float value) { floatItems_[index] = value; }

  @Override //Dummy
  void setMaxDoubleValue(final double value) { kllSketchThrow(MUST_NOT_CALL); }

  @Override //Used internally
  void setMaxFloatValue(final float value) { maxFloatValue_ = value; }

  @Override //Dummy
  void setMinDoubleValue(final double value) { kllSketchThrow(MUST_NOT_CALL); }

  @Override //Used internally
  void setMinFloatValue(final float value) { minFloatValue_ = value; }

  //************************************************************************************************
  private final int k;    // configured value of K.
  private final int m;    // configured value of M.
  private long n_;        // number of items input into this sketch.
  private int minK_;    // dynamic minK for error estimation after merging with different k.
  private int numLevels_; // one-based number of current levels.
  private int[] levels_;  // array of index offsets into the items[]. Size = numLevels + 1.
  private boolean isLevelZeroSorted_;

  @Override
  public int getK() {
    return k;
  }

  @Override
  public long getN() {
    return n_;
  }

  @Override
  int[] getLevelsArray() {
    return levels_;
  }

  @Override
  int getLevelsArrayAt(final int index) { return levels_[index]; }

  @Override
  int getM() {
    return m;
  }

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
  void setItemsArrayUpdatable(final WritableMemory itemsMem) { } //dummy

  @Override
  void setLevelsArray(final int[] levelsArr) {
    levels_ = levelsArr;
  }

  @Override
  void setLevelsArrayAt(final int index, final int value) { levels_[index] = value; }

  @Override
  void setLevelsArrayAtMinusEq(final int index, final int minusEq) {
    levels_[index] -= minusEq;
  }

  @Override
  void setLevelsArrayAtPlusEq(final int index, final int plusEq) {
    levels_[index] += plusEq;
  }

  @Override
  void setLevelsArrayUpdatable(final WritableMemory levelsMem) { } //dummy

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    this.isLevelZeroSorted_ = sorted;
  }

  @Override
  void setMinK(final int minK) {
    minK_ = minK;
  }

  @Override
  void setMinMaxArrayUpdatable(final WritableMemory minMaxMem) { } //dummy

  @Override
  void setN(final long n) {
    n_ = n;
  }

  @Override
  void setNumLevels(final int numLevels) {
    numLevels_ = numLevels;
  }

}
