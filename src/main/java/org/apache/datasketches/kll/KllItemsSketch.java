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
import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_READ_ONLY;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.ITEMS_SKETCH;

import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.Objects;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.quantilescommon.GenericSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesGenericAPI;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;

@SuppressWarnings({"unused", "unchecked"})
public abstract class KllItemsSketch<T> extends KllSketch implements QuantilesGenericAPI<T> {
  //private KllItemsSketchSortedView<T> kllItemsSV = null;
  final Comparator<? super T> comparator;
  final ArrayOfItemsSerDe<T> serDe;

  KllItemsSketch(
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    super(ITEMS_SKETCH, SketchStructure.UPDATABLE);
    Objects.requireNonNull(comparator, "Comparator must not be null.");
    Objects.requireNonNull(serDe, "SerDe must not be null.");
    this.comparator = comparator;
    this.serDe = serDe;
  }

  /**
   * Create a new heap instance of this sketch with the default <em>k = 200</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * This will have a rank error of about 1.65%.
   * @param <T> The sketch data type
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for an array of items, <i>T[]</i>.
   * @return new KllItemsSketch on the heap.
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
   * @param <T> The sketch data type
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for items of type <i>T</i> and <i>T[]</i>.
   * @return new KllItemsSketch on the heap.
   */

  public static <T> KllItemsSketch<T> newHeapInstance(
      final int k,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
      final KllItemsSketch<T> itmSk =
          new KllHeapItemsSketch<T>(k, DEFAULT_M, comparator, serDe);
    return itmSk;
  }

  //END of Constructors

  @Override
  public double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GenericPartitionBoundaries<T> getPartitionBoundaries(final int numEquallyWeighted,
      final QuantileSearchCriteria searchCrit) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double[] getPMF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T getQuantileLowerBound(final double rank) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T getQuantileUpperBound(final double rank) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double getRank(final T quantile, final QuantileSearchCriteria searchCrit) {
    // TODO Auto-generated method stub
    return 0;
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GenericSortedView<T> getSortedView() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QuantilesGenericSketchIterator<T> iterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public final void merge(final KllSketch other) {
    if (readOnly || sketchStructure != UPDATABLE) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final KllItemsSketch<T> othItmSk = (KllItemsSketch<T>)other;
    if (othItmSk.isEmpty()) { return; }
    KllItemsHelper.mergeItemImpl(this, othItmSk, comparator);
    //kllFloatsSV = null;
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub
  }

  public byte[] toByteArray() {
    //return KllHelper.toCompactByteArrayImpl(this);
    return null;
  }

  @Override
  public void update(final T item) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    KllItemsHelper.updateItem(this, item, comparator);
    //kllFloatsSV = null;
  }

  //restricted

  @Override
  abstract byte[] getMinMaxByteArr();//

  @Override
  abstract int getMinMaxSizeBytes();//

  private final void refreshSortedView() {
    //TODO
  }

  @Override
  abstract byte[] getRetainedItemsByteArr();//

  @Override
  abstract int getRetainedItemsSizeBytes();//

  abstract Object[] getRetainedItemsArray();//

  @Override
  ArrayOfItemsSerDe<T> getSerDe() { return serDe; }

  abstract Object getSingleItem();//

  @Override
  abstract byte[] getSingleItemByteArr();//

  @Override
  abstract int getSingleItemSizeBytes();//

  /**
   * @return full size of internal items array including empty space at bottom.
   */
  abstract Object[] getTotalItemsArray();//

  @Override
  abstract byte[] getTotalItemsByteArr();

  @Override
  int getTotalItemsNumBytes() {
    return levelsArr[getNumLevels()] * Float.BYTES;
  }

  abstract void setItemsArray(Object[] ItemsArr);

  abstract void setItemsArrayAt(int index, Object item);

  abstract void setMaxItem(Object item);

  abstract void setMinItem(Object item);

  //This cannot be used on an empty sketch, i.e., getMaxItem must be valid.
  @SuppressWarnings("unchecked")
  T[] copyRangeOfObjectArray(final Object[] srcArray, final int srcIndex, final int numItems) {
    final T[] tgtArr = (T[]) Array.newInstance(getMaxItem().getClass(), numItems);
    System.arraycopy(srcArray, srcIndex, tgtArr, srcIndex, numItems);
    return tgtArr;
  }

}
