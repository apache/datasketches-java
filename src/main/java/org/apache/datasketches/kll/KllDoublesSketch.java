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
import static org.apache.datasketches.kll.KllPreambleUtil.DEFAULT_K;
import static org.apache.datasketches.kll.KllPreambleUtil.DEFAULT_M;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;

/**
 * This class implements an on-heap doubles KllSketch.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
public final class KllDoublesSketch extends KllHeapSketch {

  // Specific to the doubles sketch
  private double[] doubleItems_; // the continuous array of double items
  private double minDoubleValue_;
  private double maxDoubleValue_;

  /**
   * Heap constructor with the default <em>k = 200</em>, and DEFAULT_M of 8.
   * This will have a rank error of about 1.65%.
   */
  public KllDoublesSketch() {
    this(DEFAULT_K);
  }

  /**
   * Heap constructor with a given parameter <em>k</em>. <em>k</em> can be any value between DEFAULT_M and
   * 65535, inclusive. The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Higher values of K will have smaller error but the sketch will be larger (and slower).
   * This constructor assumes the DEFAULT_M, which is 8.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   */
  public KllDoublesSketch(final int k) {
    this(k, DEFAULT_M);
  }

  /**
   * Heap constructor with a given parameter <em>k</em> and <em>m</em>.
   * <em>k</em> can be any value between DEFAULT_M and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about 1.65%.
   * Higher values of K will have smaller error but the sketch will be larger (and slower).
   * The DEFAULT_M, which is 8 is recommended for the given parameter <em>m</em>.
   * Other values of <em>m</em> should be considered experimental as they have not been
   * as well characterized.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param m parameter that controls the minimum level width.
   */
  public KllDoublesSketch(final int k, final int m) {
    super(k, m, SketchType.DOUBLES_SKETCH);
    doubleItems_ = new double[k];
    minDoubleValue_ = Double.NaN;
    maxDoubleValue_ = Double.NaN;
  }

  /**
   * Private heapify constructor.
   * @param mem Memory object that contains data serialized by this sketch.
   * @param memVal the MemoryCheck object
   */
  private KllDoublesSketch(final Memory mem, final MemoryValidate memVal) {
    super(memVal.k, memVal.m, SketchType.DOUBLES_SKETCH);
    buildHeapKllSketchFromMemory(memVal);
  }

  /**
   * Factory heapify takes the sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param mem a Memory image of a sketch serialized by this sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based sketch based on the given Memory.
   */
  //To simplify the code, the MemoryValidate class does nearly all the validity checking.
  //The validated Memory is then passed to the actual private heapify constructor.
  public static KllDoublesSketch heapify(final Memory mem) {
    final MemoryValidate memChk = new MemoryValidate(mem);
    if (!memChk.doublesSketch) {
      throw new SketchesArgumentException("Memory object is not a KllDoublesSketch.");
    }
    return new KllDoublesSketch(mem, memChk);
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
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing double values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @return an array of m+1 double values on the interval [0.0, 1.0) exclusive,
   * which are a consecutive approximation to the CDF of the input stream given the splitPoints.
   * The value at array position j of the returned CDF array is the sum of the returned values
   * in positions 0 through j of the returned PMF array.
   */
  public double[] getCDF(final double[] splitPoints) {
    return getDoublesPmfOrCdf(splitPoints, true);
  }

  /**
   * Returns the max value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the max value of the stream
   */
  public double getMaxValue() { return getMaxDoubleValue(); }

  /**
   * Returns the min value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the min value of the stream
   */
  public double getMinValue() { return getMinDoubleValue(); }

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of splitPoints (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(true) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing double values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @return an array of m+1 doubles on the interval [0.0, 1.0) exclusive,
   * each of which is an approximation to the fraction of the total input stream values
   * (the mass) that fall into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint, with the exception that the last interval will include maximum value.
   */
  public double[] getPMF(final double[] splitPoints) {
    return getDoublesPmfOrCdf(splitPoints, false);
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
  public double getQuantile(final double fraction) {
    return getDoublesQuantile(fraction);
  }

  /**
   * Gets the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public double getQuantileLowerBound(final double fraction) {
    return getQuantile(max(0, fraction - KllHelper.getNormalizedRankError(getDyMinK(), false)));
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
  public double[] getQuantiles(final double[] fractions) {
    return getDoublesQuantiles(fractions);
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
  public double[] getQuantiles(final int numEvenlySpaced) {
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
  public double getQuantileUpperBound(final double fraction) {
    return getQuantile(min(1.0, fraction + KllHelper.getNormalizedRankError(getDyMinK(), false)));
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
  public double getRank(final double value) {
    return getDoubleRank(value);
  }

  /**
   * @return the iterator for this class
   */
  public KllDoublesSketchIterator iterator() {
    return new KllDoublesSketchIterator(getDoubleItemsArray(), getLevelsArray(), getNumLevels());
  }

  /**
   * Merges another sketch into this one.
   * @param other sketch to merge into this one
   */
  public void merge(final KllSketch other) {
    if (other.isDirect()) { kllSketchThrow(35); }
    if (!other.isDoublesSketch()) { kllSketchThrow(33); }
    mergeDoubleImpl(other);
  }

  /**
   * Updates this sketch with the given data item.
   *
   * @param value an item from a stream of items. NaNs are ignored.
   */
  public void update(final double value) {  //possibly move proxy
    updateDouble(value);
  }

  @Override //Used internally
  double[] getDoubleItemsArray() { return doubleItems_; }

  @Override
  double getDoubleItemsArrayAt(final int index) { return doubleItems_[index]; }

  @Override //Dummy
  float[] getFloatItemsArray() { return null; }

  @Override //Dummy
  float getFloatItemsArrayAt(final int index) { return Float.NaN; }

  @Override //Used internally
  double getMaxDoubleValue() { return maxDoubleValue_; }

  @Override //Dummy
  float getMaxFloatValue() { return (float) maxDoubleValue_; }

  @Override //Used internally
  double getMinDoubleValue() { return minDoubleValue_; }

  @Override //Dummy
  float getMinFloatValue() { return (float) minDoubleValue_; }

  @Override //Used internally
  void setDoubleItemsArray(final double[] doubleItems) { doubleItems_ = doubleItems; }

  @Override //Used internally
  void setDoubleItemsArrayAt(final int index, final double value) { doubleItems_[index] = value; }

  @Override //Dummy
  void setFloatItemsArray(final float[] floatItems) { }

  @Override //Dummy
  void setFloatItemsArrayAt(final int index, final float value) { }

  @Override //Used internally
  void setMaxDoubleValue(final double value) { maxDoubleValue_ = value; }

  @Override //Dummy
  void setMaxFloatValue(final float value) { }

  @Override //Used internally
  void setMinDoubleValue(final double value) { minDoubleValue_ = value; }

  @Override //Dummy
  void setMinFloatValue(final float value) { }

}
