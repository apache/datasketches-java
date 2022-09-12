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

import java.util.Objects;

import org.apache.datasketches.DoublesSortedView;
import org.apache.datasketches.QuantileSearchCriteria;
import org.apache.datasketches.QuantilesDoublesAPI;
import org.apache.datasketches.QuantilesDoublesSketchIterator;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This variation of the KllSketch implements primitive doubles.
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
public abstract class KllDoublesSketch extends KllSketch implements QuantilesDoublesAPI {
  private KllDoublesSketchSortedView kllDoublesSV = null;

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

  @Override
  public double getMaxQuantile() { return getMaxDoubleValue(); }

  @Override
  public double getMinQuantile() { return getMinDoubleValue(); }

  @Override
  public double[] getCDF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    return kllDoublesSV.getCDF(splitPoints, searchCrit);
  }

  @Override
  public double[] getPMF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    return kllDoublesSV.getPMF(splitPoints, searchCrit);
  }

  @Override
  public double getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return Float.NaN; }
    refreshSortedView();
    return kllDoublesSV.getQuantile(rank, searchCrit);
  }

  @Override
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

  @Override
  public double[] getQuantiles(final int numEvenlySpaced, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.Util.evenlySpaced(0.0, 1.0, numEvenlySpaced), searchCrit);
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.99.
   */
  @Override
  public double getQuantileLowerBound(final double rank) {
    return getQuantile(max(0, rank - KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.99.
   */
  @Override
  public double getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  @Override
  public double getRank(final double quantile, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return Double.NaN; }
    refreshSortedView();
    return kllDoublesSV.getRank(quantile, searchCrit);
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.99.
   */
  @Override
  public double getRankLowerBound(final double rank) {
    return max(0.0, rank - KllHelper.getNormalizedRankError(getMinK(), false));
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.99.
   */
  @Override
  public double getRankUpperBound(final double rank) {
    return min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false));
  }

  @Override
  public double[] getRanks(final double[] quantiles, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = kllDoublesSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }

  @Override
  public QuantilesDoublesSketchIterator iterator() {
    return new KllDoublesSketchIterator(getDoubleValuesArray(), getLevelsArray(), getNumLevels());
  }

  @Override
  public byte[] toByteArray() {
    return KllHelper.toCompactByteArrayImpl(this);
  }

  @Override
  public void update(final double value) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    KllDoublesHelper.updateDouble(this, value);
    kllDoublesSV = null;
  }

  @Override
  public DoublesSortedView getSortedView() {
    refreshSortedView();
    return kllDoublesSV;
  }

  void nullSortedView() { kllDoublesSV = null; }

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
