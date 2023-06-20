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
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.THROWS_EMPTY;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.equallyWeightedRanks;

import java.util.Objects;

import org.apache.datasketches.common.SuppressFBWarnings;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.FloatsSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesFloatsAPI;
import org.apache.datasketches.quantilescommon.QuantilesFloatsSketchIterator;

/**
 * This variation of the KllSketch implements primitive floats.
 *
 * @see org.apache.datasketches.kll.KllSketch
 */
public abstract class KllFloatsSketch extends KllSketch implements QuantilesFloatsAPI {
  private KllFloatsSketchSortedView kllFloatsSV = null;

  KllFloatsSketch(final WritableMemory wmem, final MemoryRequestServer memReqSvr) {
    super(SketchType.FLOATS_SKETCH, wmem, memReqSvr);
  }

  /**
   * Factory heapify takes a compact sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem a compact Memory image of a sketch serialized by this sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based sketch based on the given Memory.
   */
  public static KllFloatsSketch heapify(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    return KllHeapFloatsSketch.heapifyImpl(srcMem);
  }

  /**
   * Create a new direct instance of this sketch with the default <em>k</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger <em>k</em> will have smaller error but the sketch will be larger (and slower).
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new direct instance of this sketch
   */
  public static KllFloatsSketch newDirectInstance(
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
  public static KllFloatsSketch newDirectInstance(
      final int k,
      final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(dstMem, "Parameter 'dstMem' must not be null");
    Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
    return KllDirectFloatsSketch.newDirectInstance(k, DEFAULT_M, dstMem, memReqSvr);
  }

  /**
   * Create a new heap instance of this sketch with the default <em>k = 200</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @return new KllFloatsSketch on the Java heap.
   */
  public static KllFloatsSketch newHeapInstance() {
    return newHeapInstance(DEFAULT_K);
  }

  /**
   * Create a new heap instance of this sketch with a given parameter <em>k</em>.
   * <em>k</em> can be between 8, inclusive, and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @return new KllFloatsSketch on the Java heap.
   */
  public static KllFloatsSketch newHeapInstance(final int k) {
    return new KllHeapFloatsSketch(k, DEFAULT_M);
  }

  /**
   * Wrap a sketch around the given read only compact source Memory containing sketch data
   * that originated from this sketch.
   * @param srcMem the read only source Memory
   * @return instance of this sketch
   */
  public static KllFloatsSketch wrap(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, FLOATS_SKETCH);
    if (getMemorySerVer(srcMem) == SERIAL_VERSION_UPDATABLE) {
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
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, FLOATS_SKETCH);
    if (getMemorySerVer(srcMem) == SERIAL_VERSION_UPDATABLE && !srcMem.isReadOnly()) {
        Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
      return new KllDirectFloatsSketch(srcMem, memReqSvr, memVal);
    } else {
      return new KllDirectCompactFloatsSketch(srcMem, memVal);
    }
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

  @Override
  public double[] getCDF(final float[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    return kllFloatsSV.getCDF(splitPoints, searchCrit);
  }

  @Override
  public float getMaxItem() {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    return getMaxFloatItem();
  }

  @Override
  public float getMinItem() {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    return getMinFloatItem();
  }

  @Override
  public FloatsPartitionBoundaries getPartitionBoundaries(final int numEquallyWeighted,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    final double[] ranks = equallyWeightedRanks(numEquallyWeighted);
    final float[] boundaries = getQuantiles(ranks, searchCrit);
    boundaries[0] = getMinItem();
    boundaries[boundaries.length - 1] = getMaxItem();
    final FloatsPartitionBoundaries fpb = new FloatsPartitionBoundaries();
    fpb.N = this.getN();
    fpb.ranks = ranks;
    fpb.boundaries = boundaries;
    return fpb;
  }

  @Override
  public double[] getPMF(final float[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    return kllFloatsSV.getPMF(splitPoints, searchCrit);
  }

  @Override
  public float getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    return kllFloatsSV.getQuantile(rank, searchCrit);
  }

  @Override
  public float[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    final int len = ranks.length;
    final float[] quantiles = new float[len];
    for (int i = 0; i < len; i++) {
      quantiles[i] = kllFloatsSV.getQuantile(ranks[i], searchCrit);
    }
    return quantiles;
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.99.
   */
  @Override
  public float getQuantileLowerBound(final double rank) {
    return getQuantile(max(0, rank - KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.99.
   */
  @Override
  public float getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  @Override
  public double getRank(final float quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    return kllFloatsSV.getRank(quantile, searchCrit);
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
  public double[] getRanks(final float[] quantiles, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = kllFloatsSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }
  
  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "OK in this case.")
  public FloatsSortedView getSortedView() {
    refreshSortedView();
    return kllFloatsSV;
  }

  @Override
  public QuantilesFloatsSketchIterator iterator() {
    return new KllFloatsSketchIterator(getFloatItemsArray(), getLevelsArray(), getNumLevels());
  }

  @Override
  public final void merge(final KllSketch other) {
    final KllFloatsSketch othFltSk = (KllFloatsSketch)other;
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    if (othFltSk.isEmpty()) { return; }
    KllFloatsHelper.mergeFloatImpl(this, othFltSk);
    kllFloatsSV = null;
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
    setMinFloatItem(Float.NaN);
    setMaxFloatItem(Float.NaN);
    setFloatItemsArray(new float[k]);
  }

  @Override
  public byte[] toByteArray() {
    return KllHelper.toCompactByteArrayImpl(this);
  }

  @Override
  public void update(final float item) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    KllFloatsHelper.updateFloat(this, item);
    kllFloatsSV = null;
  }

  //restricted

  @Override
  int getDataBlockBytes(final int numItemsAndMinMax) {
    return numItemsAndMinMax * Float.BYTES;
  }
  
  /**
   * @return full size of internal items array including garbage.
   */
  abstract float[] getFloatItemsArray();

  abstract float getFloatSingleItem();

  abstract float getMaxFloatItem();

  abstract float getMinFloatItem();
  
  final int getTheSingleItemBytes() {
    return Float.BYTES;
  }

  private final void refreshSortedView() {
    kllFloatsSV = (kllFloatsSV == null) ? new KllFloatsSketchSortedView(this) : kllFloatsSV;
  }

  abstract void setFloatItemsArray(float[] floatItems);

  abstract void setFloatItemsArrayAt(int index, float item);

  abstract void setMaxFloatItem(float item);

  abstract void setMinFloatItem(float item);

}
