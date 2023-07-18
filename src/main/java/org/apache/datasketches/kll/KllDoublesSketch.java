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
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySerVer;
import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_READ_ONLY;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.THROWS_EMPTY;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.equallyWeightedRanks;

import java.util.Objects;

import org.apache.datasketches.common.SuppressFBWarnings;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.DoublesSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesDoublesAPI;
import org.apache.datasketches.quantilescommon.QuantilesDoublesSketchIterator;

/**
 * This variation of the KllSketch implements primitive doubles.
 *
 * @see org.apache.datasketches.kll.KllSketch
 */
public abstract class KllDoublesSketch extends KllSketch implements QuantilesDoublesAPI {
  private KllDoublesSketchSortedView kllDoublesSV = null;

  KllDoublesSketch(final WritableMemory wmem, final MemoryRequestServer memReqSvr) {
    super(SketchType.DOUBLES_SKETCH, wmem, memReqSvr);
  }

  /**
   * Factory heapify takes a compact sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem a compact Memory image of a sketch serialized by this sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based sketch based on the given Memory.
   */
  public static KllDoublesSketch heapify(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    return KllHeapDoublesSketch.heapifyImpl(srcMem);
  }

  /**
   * Create a new direct instance of this sketch with the default <em>k</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger <em>k</em> will have smaller error but the sketch will be larger (and slower).
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new direct instance of this sketch
   */
  public static KllDoublesSketch newDirectInstance(
      final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    return newDirectInstance(DEFAULT_K, dstMem, memReqSvr);
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
   * Create a new heap instance of this sketch with the default <em>k = 200</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @return new KllDoublesSketch on the Java heap.
   */
  public static KllDoublesSketch newHeapInstance() {
    return newHeapInstance(DEFAULT_K);
  }

  /**
   * Create a new heap instance of this sketch with a given parameter <em>k</em>.
   * <em>k</em> can be between 8, inclusive, and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @return new KllDoublesSketch on the Java heap.
   */
  public static KllDoublesSketch newHeapInstance(final int k) {
    return new KllHeapDoublesSketch(k, DEFAULT_M);
  }

  /**
   * Wrap a sketch around the given read only compact source Memory containing sketch data
   * that originated from this sketch.
   * @param srcMem the read only source Memory
   * @return instance of this sketch
   */
  public static KllDoublesSketch wrap(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, DOUBLES_SKETCH);
    if (getMemorySerVer(srcMem) == SERIAL_VERSION_UPDATABLE) {
      return new KllDirectDoublesSketch((WritableMemory) srcMem, null, memVal);
    } else {
      return new KllDirectCompactDoublesSketch(srcMem, memVal);
    }
  }

  /**
   * Wrap a sketch around the given source Writable Memory containing sketch data
   * that originated from this sketch.
   * @param srcMem a WritableMemory that contains data.
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return instance of this sketch
   */
  public static KllDoublesSketch writableWrap(
      final WritableMemory srcMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, DOUBLES_SKETCH);
    if (getMemorySerVer(srcMem) == SERIAL_VERSION_UPDATABLE && !srcMem.isReadOnly()) {
      Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
      return new KllDirectDoublesSketch(srcMem, memReqSvr, memVal);
    } else {
      return new KllDirectCompactDoublesSketch(srcMem, memVal);
    }
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

  @Override
  public double[] getCDF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    return kllDoublesSV.getCDF(splitPoints, searchCrit);
  }

  @Override
  public double getMaxItem() {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    return getMaxDoubleItem();
  }

  @Override
  public double getMinItem() {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    return getMinDoubleItem();
  }

  @Override
  public DoublesPartitionBoundaries getPartitionBoundaries(final int numEquallyWeighted,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    final double[] ranks = equallyWeightedRanks(numEquallyWeighted);
    final double[] boundaries = getQuantiles(ranks, searchCrit);
    boundaries[0] = getMinItem();
    boundaries[boundaries.length - 1] = getMaxItem();
    final DoublesPartitionBoundaries dpb = new DoublesPartitionBoundaries();
    dpb.N = this.getN();
    dpb.ranks = ranks;
    dpb.boundaries = boundaries;
    return dpb;
  }

  @Override
  public double[] getPMF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    return kllDoublesSV.getPMF(splitPoints, searchCrit);
  }

  @Override
  public double getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    return kllDoublesSV.getQuantile(rank, searchCrit);
  }

  @Override
  public double[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    final int len = ranks.length;
    final double[] quantiles = new double[len];
    for (int i = 0; i < len; i++) {
      quantiles[i] = kllDoublesSV.getQuantile(ranks[i], searchCrit);
    }
    return quantiles;
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
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
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
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = kllDoublesSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "OK in this case.")
  public DoublesSortedView getSortedView() {
    refreshSortedView();
    return kllDoublesSV;
  }

  @Override
  public QuantilesDoublesSketchIterator iterator() {
    return new KllDoublesSketchIterator(getDoubleItemsArray(), getLevelsArray(), getNumLevels());
  }

  @Override
  public final void merge(final KllSketch other) {
    final KllDoublesSketch othDblSk = (KllDoublesSketch)other;
    if (readOnly || sketchStructure != UPDATABLE) { kllSketchThrow(TGT_IS_READ_ONLY); }
    if (othDblSk.isEmpty()) { return; }
    KllDoublesHelper.mergeDoubleImpl(this, othDblSk);
    kllDoublesSV = null;
  }

  /**
   * {@inheritDoc}
   * <p>The parameter <i>k</i> will not change.</p>
   */
  @Override
  public final void reset() {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelZeroSorted(false);
    setLevelsArray(new int[] {k, k});
    setMinDoubleItem(Double.NaN);
    setMaxDoubleItem(Double.NaN);
    setDoubleItemsArray(new double[k]);
  }

  @Override
  public byte[] toByteArray() {
    return KllHelper.toCompactByteArrayImpl(this, null);
  }

  @Override
  public void update(final double item) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    KllDoublesHelper.updateDouble(this, item);
    kllDoublesSV = null;
  }

  //restricted

  @Override
  int getDataBlockBytes(final int numItemsAndMinMax) {
    return numItemsAndMinMax * Double.BYTES;
  }

  /**
   * @return full size of internal items array including garbage.
   */
  abstract double[] getDoubleItemsArray();

  abstract double getDoubleSingleItem();

  abstract double getMaxDoubleItem();

  abstract double getMinDoubleItem();

  @Override
  final int getTheSingleItemBytes() {
    return Double.BYTES;
  }

  private final void refreshSortedView() {
    kllDoublesSV = (kllDoublesSV == null) ? new KllDoublesSketchSortedView(this) : kllDoublesSV;
  }

  abstract void setDoubleItemsArray(double[] doubleItems);

  abstract void setDoubleItemsArrayAt(int index, double item);

  abstract void setMaxDoubleItem(double item);

  abstract void setMinDoubleItem(double item);

}
