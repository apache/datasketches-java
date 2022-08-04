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
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryUpdatableFormatFlag;
import static org.apache.datasketches.kll.KllSketch.Error.MUST_NOT_BE_UPDATABLE_FORMAT;
import static org.apache.datasketches.kll.KllSketch.Error.MUST_NOT_CALL;
import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_READ_ONLY;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;
import static org.apache.datasketches.QuantileSearchCriteria.*;

import java.util.Objects;

import org.apache.datasketches.QuantileSearchCriteria;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This variation of the KllSketch implements primitive floats for the quantile values.
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
public abstract class KllFloatsSketch extends KllSketch {
  KllFloatsSketchSortedView kllFloatsSV = null;

  KllFloatsSketch(final WritableMemory wmem, final MemoryRequestServer memReqSvr) {
    super(SketchType.FLOATS_SKETCH, wmem, memReqSvr);
  }

  /**
   * Returns upper bound on the serialized size of a KllFloatsSketch given the following parameters.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @param updatableMemoryFormat true if updatable Memory format, otherwise the standard compact format.
   * @return upper bound on the serialized size of a KllSketch.
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n, final boolean updatableMemoryFormat) {
    return getMaxSerializedSizeBytes(k, n, SketchType.FLOATS_SKETCH, updatableMemoryFormat);
  }

  /**
   * Factory heapify takes the sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem a Memory image of a sketch serialized by this sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based sketch based on the given Memory.
   */
  public static KllFloatsSketch heapify(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    if (getMemoryUpdatableFormatFlag(srcMem)) { Error.kllSketchThrow(MUST_NOT_BE_UPDATABLE_FORMAT); }
    return KllHeapFloatsSketch.heapifyImpl(srcMem);
  }

  /**
   * Create a new direct instance of this sketch with a given <em>k</em>.
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new direct instance of this sketch
   */
  public static KllFloatsSketch newDirectInstance(
      final int k,
      final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(dstMem, "Parameter 'dstMem' must not be null");
    Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
    return KllDirectFloatsSketch.newDirectInstance(k, DEFAULT_M, dstMem, memReqSvr);
  }

  /**
   * Create a new direct instance of this sketch with the default <em>k</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Higher values of <em>k</em> will have smaller error but the sketch will be larger (and slower).
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new direct instance of this sketch
   */
  public static KllFloatsSketch newDirectInstance(
      final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(dstMem, "Parameter 'dstMem' must not be null");
    Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
    return KllDirectFloatsSketch.newDirectInstance(DEFAULT_K, DEFAULT_M, dstMem, memReqSvr);
  }

  /**
   * Create a new heap instance of this sketch with the default <em>k = 200</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Higher values of K will have smaller error but the sketch will be larger (and slower).
   * This will have a rank error of about 1.65%.
   * @return new KllFloatsSketch on the heap.
   */
  public static KllFloatsSketch newHeapInstance() {
    return new KllHeapFloatsSketch(DEFAULT_K, DEFAULT_M);
  }

  /**
   * Create a new heap instance of this sketch with a given parameter <em>k</em>.
   * <em>k</em> can be any value between DEFAULT_M and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Higher values of K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @return new KllFloatsSketch on the heap.
   */
  public static KllFloatsSketch newHeapInstance(final int k) {
    return new KllHeapFloatsSketch(k, DEFAULT_M);
  }

  /**
   * Wrap a sketch around the given read only source Memory containing sketch data
   * that originated from this sketch.
   * @param srcMem the read only source Memory
   * @return instance of this sketch
   */
  public static KllFloatsSketch wrap(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem);
    if (memVal.updatableMemFormat) {
      return new KllDirectFloatsSketch((WritableMemory) srcMem, null, memVal);
    } else {
      return new KllDirectCompactFloatsSketch(srcMem, memVal);
    }
  }

  /**
   * Wrap a sketch around the given source Writable Memory containing sketch data
   * that originated from this sketch.
   * @param srcMem a WritableMemory that contains data.
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return instance of this sketch
   */
  public static KllFloatsSketch writableWrap(
      final WritableMemory srcMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem);
    if (memVal.updatableMemFormat) {
      if (!memVal.readOnly) {
        Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
      }
      return new KllDirectFloatsSketch(srcMem, memReqSvr, memVal);
    } else {
      return new KllDirectCompactFloatsSketch(srcMem, memVal);
    }
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
   * Same as {@link #getCDF(float[], QuantileSearchCriteria) getCDF(splitPoints, NON_INCLUSIVE)}
   * @param splitPoints splitPoints
   * @return CDF
   */
  public double[] getCDF(final float[] splitPoints) {
    return getCDF(splitPoints, NON_INCLUSIVE);
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
   * @param inclusive if true the weight of the given value is included into the rank.
   * Otherwise, the rank equals the sum of the weights of all values that are less than the given value
   *
   * @return an array of m+1 double values on the interval [0.0, 1.0),
   * which are a consecutive approximation to the CDF of the input stream given the splitPoints.
   * The value at array position j of the returned CDF array is the sum of the returned values
   * in positions 0 through j of the returned PMF array.
   */
  public double[] getCDF(final float[] splitPoints, final QuantileSearchCriteria inclusive) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    return kllFloatsSV.getPmfOrCdf(splitPoints, true, inclusive);
  }

  /**
   * Same as {@link #getPMF(float[], QuantileSearchCriteria) getPMF(splitPoints, NON_INCLUSIVE)}
   * @param splitPoints splitPoints
   * @return PMF
   */
  public double[] getPMF(final float[] splitPoints) {
    return getPMF(splitPoints, NON_INCLUSIVE);
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
   * that divide the real number line into <i>m+1</i> consecutive non overlapping intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @param inclusive  if INCLUSIVE, each interval within the distribution will include its top value and exclude its
   * bottom value. Otherwise, it will be the reverse.  The only exception is that the top portion will always include
   * the top value retained by the sketch.
   *
   * @return an array of m+1 doubles on the interval [0.0, 1.0),
   * each of which is an approximation to the fraction, or mass, of the total input stream values
   * that fall into that interval.
   */
  public double[] getPMF(final float[] splitPoints, final QuantileSearchCriteria inclusive) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    return kllFloatsSV.getPmfOrCdf(splitPoints, false, inclusive);
  }

  /**
   * Same as {@link #getQuantile(double, QuantileSearchCriteria) getQuantile(rank, NON_INCLUSIVE)}
   * @param rank  the given normalized rank, a value in the interval [0.0,1.0].
   * @return quantile
   * @see org.apache.datasketches.QuantileSearchCriteria QuantileSearchCriteria
   */
  public float getQuantile(final double rank) {
    return getQuantile(rank, NON_INCLUSIVE);
  }

  /**
   * Returns the quantile associated with the given rank.
   *
   * <p>We note that this method has an overhead (microseconds instead of nanoseconds) when called for the first time
   * after an update or sketch merge.  Use getQuantiles() if there is a requirement to obtain multiple quantiles.
   *
   * <p>If the sketch is empty this returns NaN.
   *
   * @param rank the given normalized rank, a value in the interval [0.0,1.0].
   * @param inclusive is INCLUSIVE, the given rank includes all values &le; the value directly
   * corresponding to the given rank.
   * @return the quantile associated with the given rank.
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public float getQuantile(final double rank, final QuantileSearchCriteria inclusive) {
    if (this.isEmpty()) { return Float.NaN; }
    refreshSortedView();
    return kllFloatsSV.getQuantile(rank, inclusive);
  }

  /**
   * Same as {@link #getQuantiles(double[], QuantileSearchCriteria) getQuantiles(ranks, NON_INCLUSIVE)}
   * @param ranks normalied ranks on the interval [0.0, 1.0].
   * @return quantiles
   * @see org.apache.datasketches.QuantileSearchCriteria QuantileSearchCriteria
   */
  public float[] getQuantiles(final double[] ranks) {
    return getQuantiles(ranks, NON_INCLUSIVE);
  }

  /**
   * This is a more efficient multiple-query version of getQuantile().
   *
   * <p>Returns an array of quantiles from the given array of normalized ranks.
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param ranks the given array of normalized ranks, each of which must be in the interval [0.0,1.0].
   * @param inclusive if INCLUSIVE, the given ranks include all values &le; the value directly corresponding to each rank.
   * @return array of quantiles
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public float[] getQuantiles(final double[] ranks, final QuantileSearchCriteria inclusive) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    final int len = ranks.length;
    final float[] quantiles = new float[len];
    for (int i = 0; i < len; i++) {
      quantiles[i] = kllFloatsSV.getQuantile(ranks[i], inclusive);
    }
    return quantiles;
  }

  /**
   * Same as {@link #getQuantiles(int, QuantileSearchCriteria) getQuantiles(numEvenlySpaced, NON_INCLUSIVE)}
   * @param numEvenlySpaced number of evenly spaced normalied ranks
   * @return array of quantiles.
   * @see org.apache.datasketches.QuantileSearchCriteria QuantileSearchCriteria
   */
  public float[] getQuantiles(final int numEvenlySpaced) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.Util.evenlySpaced(0.0, 1.0, numEvenlySpaced), NON_INCLUSIVE);
  }

  /**
   * This is also a more efficient multiple-query version of getQuantile() and allows the caller to
   * specify the number of evenly spaced normalized ranks.
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param numEvenlySpaced an integer that specifies the number of evenly spaced normalized ranks.
   * This must be a positive integer greater than 0. A value of 1 will return the min value.
   * A value of 2 will return the min and the max value. A value of 3 will return the min,
   * the median and the max value, etc.
   *
   * @param inclusive if true, the normalized ranks are considered inclusive
   * @return array of quantiles.
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public float[] getQuantiles(final int numEvenlySpaced, final QuantileSearchCriteria inclusive) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.Util.evenlySpaced(0.0, 1.0, numEvenlySpaced), inclusive);
  }

  /**
   * Gets the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param rank the given normalized rank, a value in the interval [0.0,1.0].
   * @return the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public float getQuantileLowerBound(final double rank) {
    return getQuantile(max(0, rank - KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  /**
   * Gets the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param rank the given normalized rank
   * @return the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public float getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  /**
   * Same as {@link #getRank(float, QuantileSearchCriteria) getRank(value, NON_INCLUSIVE)}
   * @param value value to be ranked
   * @return normalized rank
   */
  public double getRank(final float value) {
    return getRank(value, NON_INCLUSIVE);
  }

  /**
   * Returns a normalized rank given a quantile value.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @param value to be ranked
   * @param inclusive if INCLUSIVE the given quantile value is included into the rank.
   * @return an approximate rank of the given value
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double getRank(final float value, final QuantileSearchCriteria inclusive) {
    if (this.isEmpty()) { return Double.NaN; }
    refreshSortedView();
    return kllFloatsSV.getRank(value, inclusive);
  }

  /**
   * Same as {@link #getRanks(float[], QuantileSearchCriteria) getRanks(values, NON_INCLUSIVE)}
   * @param values array of values to be ranked.
   * @return the array of normalized ranks.
   */
  public double[] getRanks(final float[] values) {
    return getRanks(values, NON_INCLUSIVE);
  }

  /**
   * Returns an array of normalized ranks corresponding to the given array of quantile values and the given
   * search criterion.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param values the given quantile values from which to obtain their corresponding ranks.
   * @param inclusive
   * @return an array of normalized ranks corresponding to the given array of quantile values.
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double[] getRanks(final float[] values, final QuantileSearchCriteria inclusive) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    final int len = values.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = kllFloatsSV.getRank(values[i], inclusive);
    }
    return ranks;
  }

  /**
   * @return the iterator for this class
   */
  public KllFloatsSketchIterator iterator() {
    return new KllFloatsSketchIterator(getFloatValuesArray(), getLevelsArray(), getNumLevels());
  }

  /**
   * Updates this sketch with the given data item.
   *
   * @param value an item from a stream of items. NaNs are ignored.
   */
  public void update(final float value) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    KllFloatsHelper.updateFloat(this, value);
    kllFloatsSV = null;
  }

  /**
   * Sorted view of the sketch.
   * Complexity: linear, single-pass merge of sorted levels plus sorting of the level 0.
   * @return sorted view object
   */
  public KllFloatsSketchSortedView getSortedView() {
    return kllFloatsSV = (kllFloatsSV == null) ? new KllFloatsSketchSortedView(this) : kllFloatsSV;
  }

  @Override //Artifact of inheritance
  double[] getDoubleItemsArray() { kllSketchThrow(MUST_NOT_CALL); return null; }

  @Override //Artifact of inheritance
  double getMaxDoubleValue() { kllSketchThrow(MUST_NOT_CALL); return Double.NaN; }

  @Override //Artifact of inheritance
  double getMinDoubleValue() { kllSketchThrow(MUST_NOT_CALL); return Double.NaN; }

  @Override //Artifact of inheritance
  void setDoubleItemsArray(final double[] doubleItems) { kllSketchThrow(MUST_NOT_CALL); }

  @Override //Artifact of inheritance
  void setDoubleItemsArrayAt(final int index, final double value) { kllSketchThrow(MUST_NOT_CALL); }

  @Override //Artifact of inheritance
  void setMaxDoubleValue(final double value) { kllSketchThrow(MUST_NOT_CALL); }

  @Override //Artifact of inheritance
  void setMinDoubleValue(final double value) { kllSketchThrow(MUST_NOT_CALL); }

  private final void refreshSortedView() {
    kllFloatsSV = (kllFloatsSV == null) ? new KllFloatsSketchSortedView(this) : kllFloatsSV;
  }

}
