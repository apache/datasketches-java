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
import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryUpdatableFormatFlag;
import static org.apache.datasketches.kll.KllSketch.Error.MUST_NOT_BE_UPDATABLE_FORMAT;
import static org.apache.datasketches.kll.KllSketch.Error.MUST_NOT_CALL;
import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_READ_ONLY;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;

import java.util.Objects;

import org.apache.datasketches.QuantileSearchCriteria;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This variation of the KllSketch implements primitive doubles for the quantile values.
 *
 * @see <a href="https://datasketches.apache.org/docs/KLL/KLLSketch.html">KLL Sketch</a>
 * @see org.apache.datasketches.kll.KllSketch
 * @see <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
 * KLL package summary</a>
 * @see <a href="https://datasketches.apache.org/docs/Quantiles/SketchingQuantilesAndRanksTutorial.html">
 * Sketching Quantiles and Ranks, Tutorial</a>
 * @see org.apache.datasketches.QuantileSearchCriteria
 * @author Lee Rhodes
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
public abstract class KllDoublesSketch extends KllSketch {
  KllDoublesSketchSortedView kllDoublesSV = null;

  KllDoublesSketch(final WritableMemory wmem, final MemoryRequestServer memReqSvr) {
    super(SketchType.DOUBLES_SKETCH, wmem, memReqSvr);
  }

  /**
   * Returns upper bound on the serialized size of a KllDoublesSketch given the following parameters.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @param updatableMemoryFormat true if updatable Memory format, otherwise the standard compact format.
   * @return upper bound on the serialized size of a KllSketch.
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n, final boolean updatableMemoryFormat) {
    return getMaxSerializedSizeBytes(k, n, SketchType.DOUBLES_SKETCH, updatableMemoryFormat);
  }

  /**
   * Factory heapify takes the sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem a Memory image of a sketch serialized by this sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based sketch based on the given Memory.
   */
  public static KllDoublesSketch heapify(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    if (getMemoryUpdatableFormatFlag(srcMem)) { Error.kllSketchThrow(MUST_NOT_BE_UPDATABLE_FORMAT); }
    return KllHeapDoublesSketch.heapifyImpl(srcMem);
  }

  /**
   * Create a new direct instance of this sketch with a given <em>k</em>.
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new direct instance of this sketch
   */
  public static KllDoublesSketch newDirectInstance(
      final int k,
      final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(dstMem, "Parameter 'dstMem' must not be null");
    Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
    return KllDirectDoublesSketch.newDirectInstance(k, DEFAULT_M, dstMem, memReqSvr);
  }

  /**
   * Create a new direct instance of this sketch with the default <em>k</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Higher values of <em>k</em> will have smaller error but the sketch will be larger (and slower).
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new direct instance of this sketch
   */
  public static KllDoublesSketch newDirectInstance(
      final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(dstMem, "Parameter 'dstMem' must not be null");
    Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
    return KllDirectDoublesSketch.newDirectInstance(DEFAULT_K, DEFAULT_M, dstMem, memReqSvr);
  }

  /**
   * Create a new heap instance of this sketch with the default <em>k = 200</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Higher values of K will have smaller error but the sketch will be larger (and slower).
   * This will have a rank error of about 1.65%.
   * @return new KllDoublesSketch on the heap.
   */
  public static KllDoublesSketch  newHeapInstance() {
    return new KllHeapDoublesSketch(DEFAULT_K, DEFAULT_M);
  }

  /**
   * Create a new heap instance of this sketch with a given parameter <em>k</em>.
   * <em>k</em> can be any value between DEFAULT_M and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Higher values of K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @return new KllDoublesSketch on the heap.
   */
  public static KllDoublesSketch newHeapInstance(final int k) {
    return new KllHeapDoublesSketch(k, DEFAULT_M);
  }

  /**
   * Wrap a sketch around the given read only source Memory containing sketch data
   * that originated from this sketch.
   * @param srcMem the read only source Memory
   * @return instance of this sketch
   */
  public static KllDoublesSketch wrap(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem);
    if (memVal.updatableMemFormat) {
      return new KllDirectDoublesSketch((WritableMemory) srcMem, null, memVal);
    } else {
      return new KllDirectCompactDoublesSketch(srcMem, memVal);
    }
  }

  /**
   * Wrap a sketch around the given source Memory containing sketch data that originated from
   * this sketch.
   * @param srcMem a WritableMemory that contains data.
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return instance of this sketch
   */
  public static KllDoublesSketch writableWrap(
      final WritableMemory srcMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem);
    if (memVal.updatableMemFormat) {
      if (!memVal.readOnly) {
        Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
      }
      return new KllDirectDoublesSketch(srcMem, memReqSvr, memVal);
    } else {
      return new KllDirectCompactDoublesSketch(srcMem, memVal);
    }
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
   * Same as {@link #getCDF(double[], QuantileSearchCriteria) getCDF(double[] splitPoints, INCLUSIVE)}
   * @param splitPoints splitPoints
   * @return CDF
   */
  public double[] getCDF(final double[] splitPoints) {
    return getCDF(splitPoints, INCLUSIVE);
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
   * The definition of an "interval" is inclusive of the left splitPoint (or smallest value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the largest value.
   * It is not necessary to include either the minimum or maximum values in these split points.
   *
   * @param searchCrit if INCLUSIVE the weight of the given value is included into the rank.
   * Otherwise the rank equals the sum of the weights of all values that are less than the given value
   *
   * @return an array of m+1 double values on the interval [0.0, 1.0),
   * which are a consecutive approximation to the CDF of the input stream given the splitPoints.
   * The value at array position j of the returned CDF array is the sum of the returned values
   * in positions 0 through j of the returned PMF array.
   */
  public double[] getCDF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    return kllDoublesSV.getCDF(splitPoints, searchCrit);
  }

  /**
   * Same as {@link #getPMF(double[], QuantileSearchCriteria) getPMF(double[] splitPoints, INCLUSIVE)}
   * @param splitPoints splitPoints
   * @return PMF
   */
  public double[] getPMF(final double[] splitPoints) {
    return getPMF(splitPoints, INCLUSIVE);
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
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing double values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or smallest value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the largest value.
   * It is not necessary to include either the minimum or maximum values in these split points.
   *
   * @param searchCrit if INCLUSIVE, each interval within the distribution will include its highest value and exclude its
   * lowest value. Otherwise, it will be the reverse.  The only exception is that the highest interval will always include
   * the highest value retained by the sketch.
   *
   * @return an array of m+1 doubles on the interval [0.0, 1.0),
   * each of which is an approximation to the fraction, or mass, of the total input stream values
   * that fall into that interval.
   */
  public double[] getPMF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    return kllDoublesSV.getPMF(splitPoints, searchCrit);
  }

  /**
   * Same as {@link #getQuantile(double, QuantileSearchCriteria) getQuantile(rank, INCLUSIVE)}
   * @param rank the given normalized rank, a value in the interval [0.0,1.0].
   * @return quantile
   * @see org.apache.datasketches.QuantileSearchCriteria QuantileSearchCriteria
   */
  public double getQuantile(final double rank) {
    return getQuantile(rank, INCLUSIVE);
  }

  /**
   * Returns the quantile associated with the given rank.
   *
   * <p>If the sketch is empty this returns NaN.
   *
   * @param rank the given normalized rank, a value in the interval [0.0,1.0].
   * @param searchCrit if INCLUSIVE, the given rank includes all values &le; the value directly
   * corresponding to the given rank.
   * @return the quantile associated with the given rank.
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return Float.NaN; }
    refreshSortedView();
    return kllDoublesSV.getQuantile(rank, searchCrit);
  }

  /**
   * Same as {@link #getQuantiles(double[], QuantileSearchCriteria) getQuantiles(ranks, INCLUSIVE)}
   * @param ranks normalied ranks on the interval [0.0, 1.0].
   * @return quantiles
   * @see org.apache.datasketches.QuantileSearchCriteria QuantileSearchCriteria
   */
  public double[] getQuantiles(final double[] ranks) {
    return getQuantiles(ranks, INCLUSIVE);
  }

  /**
   * Returns an array of quantiles from the given array of normalized ranks.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param ranks the given array of normalized ranks, each of which must be in the interval [0.0,1.0].
   * @param searchCrit if INCLUSIVE, the given ranks include all values &le; the value directly corresponding to each
   * rank.
   * @return array of quantiles
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    final int len = ranks.length;
    final double[] quantiles = new double[len];
    for (int i = 0; i < len; i++) {
      quantiles[i] = kllDoublesSV.getQuantile(ranks[i], searchCrit);
    }
    return quantiles;
  }

  /**
   * Same as {@link #getQuantiles(int, QuantileSearchCriteria) getQuantiles(numEvenlySpaced, INCLUSIVE)}
   * @param numEvenlySpaced number of evenly spaced normalied ranks
   * @return array of quantiles.
   * @see org.apache.datasketches.QuantileSearchCriteria QuantileSearchCriteria
   */
  public double[] getQuantiles(final int numEvenlySpaced) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.Util.evenlySpaced(0.0, 1.0, numEvenlySpaced), INCLUSIVE);
  }

  /**
   * This is a version of getQuantiles() and allows the caller to
   * specify the number of evenly spaced normalized ranks.
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param numEvenlySpaced an integer that specifies the number of evenly spaced normalized ranks.
   * This must be a positive integer greater than 0. Based on the specified searchCrit:
   * a value of 1 will return the lowest value;
   * a value of 2 will return the lowest and the highest values;
   * a value of 3 will return the lowest, the median and the highest values; etc.
   *
   * @param searchCrit if INCLUSIVE, the given ranks include all values &le; the value directly corresponding to each rank.
   * @return array of quantiles.
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double[] getQuantiles(final int numEvenlySpaced, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.Util.evenlySpaced(0.0, 1.0, numEvenlySpaced), searchCrit);
  }

  /**
   * Gets the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param rank the given normalized rank
   * @return the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public double getQuantileLowerBound(final double rank) {
    return getQuantile(max(0, rank - KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  /**
   * Gets the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param rank the given normalized rank
   * @return the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public double getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  /**
   * Same as {@link #getRank(double, QuantileSearchCriteria) getRank(value, INCLUSIVE)}
   * @param value value to be ranked
   * @return normalized rank
   */
  public double getRank(final double value) {
    return getRank(value, INCLUSIVE);
  }

  /**
   * Returns a normalized rank given a quantile value.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @param value to be ranked
   * @param searchCrit if INCLUSIVE the given quantile value is included into the rank.
   * @return an approximate rank of the given value
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double getRank(final double value, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return Double.NaN; }
    refreshSortedView();
    return kllDoublesSV.getRank(value, searchCrit);
  }

  /**
   * Same as {@link #getRanks(double[], QuantileSearchCriteria) getRanks(values, INCLUSIVE)}
   * @param values array of values to be ranked.
   * @return the array of normalized ranks.
   */
  public double[] getRanks(final double[] values) {
    return getRanks(values, INCLUSIVE);
  }

  /**
   * Returns an array of normalized ranks corresponding to the given array of quantile values and the given
   * search criterion.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param values the given quantile values from which to obtain their corresponding ranks.
   * @param searchCrit if INCLUSIVE, the given values include the rank directly corresponding to each value.
   * @return an array of normalized ranks corresponding to the given array of quantile values.
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double[] getRanks(final double[] values, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    final int len = values.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = kllDoublesSV.getRank(values[i], searchCrit);
    }
    return ranks;
  }

  /**
   * @return the iterator for this class
   */
  public KllDoublesSketchIterator iterator() {
    return new KllDoublesSketchIterator(getDoubleValuesArray(), getLevelsArray(), getNumLevels());
  }

  /**
   * Updates this sketch with the given data value.
   *
   * @param value a value from a stream of values. NaNs are ignored.
   */
  public void update(final double value) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    KllDoublesHelper.updateDouble(this, value);
    kllDoublesSV = null;
  }

  /**
   * Sorted view of the sketch.
   * Complexity: linear merge of sorted levels plus sorting of the level 0.
   * @return sorted view object
   */
  public KllDoublesSketchSortedView getSortedView() {
    refreshSortedView();
    return kllDoublesSV;
  }

  @Override //Artifact of inheritance
  float[] getFloatValuesArray() { kllSketchThrow(MUST_NOT_CALL); return null; }

  @Override //Artifact of inheritance
  float getMaxFloatValue() { kllSketchThrow(MUST_NOT_CALL); return Float.NaN; }

  @Override //Artifact of inheritance
  float getMinFloatValue() { kllSketchThrow(MUST_NOT_CALL); return Float.NaN; }

  @Override //Artifact of inheritance
  void setFloatValuesArray(final float[] floatValues) { kllSketchThrow(MUST_NOT_CALL); }

  @Override //Artifact of inheritance
  void setFloatValuesArrayAt(final int index, final float value) { kllSketchThrow(MUST_NOT_CALL); }

  @Override //Artifact of inheritance
  void setMaxFloatValue(final float value) { kllSketchThrow(MUST_NOT_CALL); }

  @Override //Artifact of inheritance
  void setMinFloatValue(final float value) { kllSketchThrow(MUST_NOT_CALL); }

  private final void refreshSortedView() {
    kllDoublesSV = (kllDoublesSV == null) ? new KllDoublesSketchSortedView(this) : kllDoublesSV;
  }

}
