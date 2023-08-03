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
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.ITEMS_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.equallyWeightedRanks;

import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.Objects;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesGenericAPI;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;

/**
 * This variation of the KllSketch implements generic data types. The user must provide
 * a suitable implementation of the <i>java.lang.Comparator</i> as well as an implementation of
 * the serializer / deserializer, <i>org.apache.datasketches.common.ArrayOfItemsSerDe</i>.
 * @param <T> The sketch data type.
 * @see org.apache.datasketches.kll.KllSketch
 */
@SuppressWarnings("unchecked")
public abstract class KllItemsSketch<T> extends KllSketch implements QuantilesGenericAPI<T> {
  private KllItemsSketchSortedView<T> kllItemsSV = null;
  final Comparator<? super T> comparator;
  final ArrayOfItemsSerDe<T> serDe;

  KllItemsSketch(
      final SketchStructure skStructure,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    super(ITEMS_SKETCH, skStructure);
    Objects.requireNonNull(comparator, "Comparator must not be null.");
    Objects.requireNonNull(serDe, "SerDe must not be null.");
    this.comparator = comparator;
    this.serDe = serDe;
  }

  //Factories for new heap instances.

  /**
   * Create a new heap instance of this sketch with the default <em>k = 200</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for an array of items, <i>T[]</i>.
   * @param <T> The sketch data type.
   * @return new KllItemsSketch on the Java heap.
   */
  public static <T> KllItemsSketch<T> newHeapInstance(
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
      final KllItemsSketch<T> itmSk =
          new KllHeapItemsSketch<T>(DEFAULT_K, DEFAULT_M, comparator, serDe);
    return itmSk;
  }

  /**
   * Create a new heap instance of this sketch with a given parameter <em>k</em>.
   * <em>k</em> can be between DEFAULT_M and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for items of type <i>T</i> and <i>T[]</i>.
   * @param <T> The sketch data type
   * @return new KllItemsSketch on the heap.
   */
  public static <T> KllItemsSketch<T> newHeapInstance(
      final int k,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    return new KllHeapItemsSketch<T>(k, DEFAULT_M, comparator, serDe);
  }

  // Factory to create an heap instance from a Memory image

  /**
   * Factory heapify takes a compact sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem a compact Memory image of a sketch serialized by this sketch and of the same type of T.
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for items of type <i>T</i> and <i>T[]</i>.
   * @param <T> The sketch data type
   * @return a heap-based sketch based on the given Memory.
   */
  public static <T> KllItemsSketch<T> heapify(
      final Memory srcMem,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    return new KllHeapItemsSketch<T>(srcMem, comparator, serDe);
  }

  //Factory to wrap a Read-Only Memory

  /**
   * Constructs a thin wrapper on the heap around a Memory (or WritableMemory) already initialized with a
   * validated sketch image of a type T consistent with the given comparator and serDe.
   * A reference to the Memory is kept in the sketch and must remain in scope consistent
   * with the temporal scope of this sketch. The amount of data kept on the heap is very small.
   * All of the item data originally collected by the given Memory sketch object remains in the
   * Memory object
   * @param srcMem the Memory object that this sketch will wrap.
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for items of type <i>T</i> and <i>T[]</i>.
   * @param <T> The sketch data type
   * @return a heap-base sketch that is a thin wrapper around the given srcMem.
   */
  public static <T> KllItemsSketch<T> wrap(
      final Memory srcMem,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, SketchType.ITEMS_SKETCH, serDe);
    return new KllDirectCompactItemsSketch<T>(memVal, comparator, serDe);
  }

  //END of Constructors

  @Override
  public double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return kllItemsSV.getCDF(splitPoints, searchCrit);
  }

  @Override
  public GenericPartitionBoundaries<T> getPartitionBoundaries(final int numEquallyWeighted,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    final double[] ranks = equallyWeightedRanks(numEquallyWeighted);
    final Object[] boundaries = getQuantiles(ranks, searchCrit);
    boundaries[0] = getMinItem();
    boundaries[boundaries.length - 1] = getMaxItem();
    final GenericPartitionBoundaries<T> gpb = new GenericPartitionBoundaries<T>();
    gpb.N = this.getN();
    gpb.ranks = ranks;
    gpb.boundaries = (T[])boundaries;
    return gpb;
  }

  @Override
  public double[] getPMF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return kllItemsSV.getPMF(splitPoints, searchCrit);
  }

  @Override
  public T getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return kllItemsSV.getQuantile(rank, searchCrit);
  }

  @Override
  public T[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    final int len = ranks.length;
    final T[] quantiles = (T[]) Array.newInstance(getMinItem().getClass(), len);
    for (int i = 0; i < len; i++) {
      quantiles[i] = kllItemsSV.getQuantile(ranks[i], searchCrit);
    }
    return quantiles;
  }

  @Override
  public T getQuantileLowerBound(final double rank) {
    return getQuantile(max(0, rank - KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  @Override
  public T getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  @Override
  public double getRank(final T quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return kllItemsSV.getRank(quantile, searchCrit);
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
  public double[] getRanks(final T[] quantiles, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = kllItemsSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }

  @Override
  public KllItemsSketchSortedView<T> getSortedView() {
    refreshSortedView();
    return kllItemsSV;
  }

  @Override
  public QuantilesGenericSketchIterator<T> iterator() {
    return new KllItemsSketchIterator<T>(
        getTotalItemsArray(), getLevelsArray(SketchStructure.UPDATABLE), getNumLevels());
  }

  @Override
  public final void merge(final KllSketch other) {
    if (readOnly || sketchStructure != UPDATABLE) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final KllItemsSketch<T> othItmSk = (KllItemsSketch<T>)other;
    if (othItmSk.isEmpty()) { return; }
    KllItemsHelper.mergeItemImpl(this, othItmSk, comparator);
    kllItemsSV = null;
  }

  @Override
  public void reset() {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelZeroSorted(false);
    setLevelsArray(new int[] {k, k});
    setMinItem(null);
    setMaxItem(null);
    setItemsArray(new Object[k]);
  }

  public byte[] toByteArray() {
    return KllHelper.toByteArray(this, false);
  }

  @Override
  public void update(final T item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    KllItemsHelper.updateItem(this, item, comparator);
    kllItemsSV = null;
  }

  //restricted

  @Override
  abstract byte[] getMinMaxByteArr();

  @Override
  abstract int getMinMaxSizeBytes();

  private final void refreshSortedView() {
    kllItemsSV = (kllItemsSV == null) ? new KllItemsSketchSortedView<T>(this) : kllItemsSV;
  }

  @Override
  abstract byte[] getRetainedItemsByteArr();

  @Override
  abstract int getRetainedItemsSizeBytes();

  //abstract Object[] getRetainedItemsArray();

  @Override
  ArrayOfItemsSerDe<T> getSerDe() { return serDe; }

  abstract Object getSingleItem();

  @Override
  abstract byte[] getSingleItemByteArr();

  @Override
  abstract int getSingleItemSizeBytes();

  /**
   * @return full size of internal items array including empty space at bottom.
   */
  abstract Object[] getTotalItemsArray();

  @Override
  abstract byte[] getTotalItemsByteArr();

  @Override
  int getTotalItemsNumBytes() {
    return serDe.sizeOf((T[]) getTotalItemsArray());
  }

  abstract void setItemsArray(Object[] ItemsArr);

  abstract void setItemsArrayAt(int index, Object item);

  abstract void setMaxItem(Object item);

  abstract void setMinItem(Object item);

  //This cannot be used on an empty sketch, i.e., getMaxItem must be valid.
  T[] copyRangeOfObjectArray(final Object[] srcArray, final int srcIndex, final int numItems) {
    final T[] tgtArr = (T[]) Array.newInstance(getMaxItem().getClass(), numItems);
    System.arraycopy(srcArray, srcIndex, tgtArr, 0, numItems);
    return tgtArr;
  }

}
