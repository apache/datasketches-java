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
import static org.apache.datasketches.kll.KllSketch.SketchType.ITEMS_SKETCH;

import java.util.Comparator;
import java.util.Objects;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.quantilescommon.GenericSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesGenericAPI;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;

@SuppressWarnings("unused")
public abstract class KllItemsSketch<T> extends KllSketch implements QuantilesGenericAPI<T> {
  private KllItemsSketchSortedView<T> kllItemsSV = null;
  final Comparator<? super T> comparator;
  private T maxItem;
  private T minItem;
  private final int k; // configured size of K.
  private final int m; // configured size of M.
  private long n;      // number of items input into this sketch.
  private int minK;    // dynamic minK for error estimation after merging with different k.
  private boolean isLevelZeroSorted;

  private Object[] itemsArr;


  KllItemsSketch(
      final int k,
      final int m,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    super(ITEMS_SKETCH, SketchStructure.UPDATABLE, null, null);
    Objects.requireNonNull(comparator, "Comparator must not be null.");
    Objects.requireNonNull(serDe, "SerDe must not be null.");
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    this.k = k;
    this.m = DEFAULT_M;
    this.n = 0;
    this.minK = k;
    this.isLevelZeroSorted = false;
    this.maxItem = null;
    this.minItem = null;
    this.itemsArr = new Object[k];
    super.levelsArr = new int[] {k, k};
    this.comparator = comparator;
    super.serDe = serDe;
  }

  /**
   * Create a new heap instance of this sketch with the default <em>k = 200</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * This will have a rank error of about 1.65%.
   * @param <T> The sketch data type
   * @param clazz the given class of T
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for an array of items, <i>T[]</i>.
   * @return new KllItemsSketch on the heap.
   */
  public static <T> KllItemsSketch<T> newHeapInstance(
      final Class<T> clazz,
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
   * @param clazz the given class of T
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for an array of items, <i>T[]</i>.
   * @return new KllItemsSketch on the heap.
   */

  public static <T> KllItemsSketch<T> newHeapInstance(
      final int k,
      final Class<T> clazz,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
      final KllItemsSketch<T> itmSk = new KllItemsSketch<T>(k, DEFAULT_M, clazz, comparator);
    return itmSk;
  }

  @Override
  public double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getK() {
    return k;
  }

  @Override
  public T getMaxItem() {
    return maxItem;
  }

  @Override
  public T getMinItem() {
    return minItem;
  }

  @Override
  public long getN() {
    return n;
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
  public T getQuantileLowerBound(final double rank) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
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

  @Override
  public double[] getRanks(final T[] quantiles, final QuantileSearchCriteria searchCrit) {
    // TODO Auto-generated method stub
    return null;
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
  public int getSerializedSizeBytes() {
    // TODO Auto-generated method stub
    return 0;
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
 // TODO Auto-generated method stub
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub
  }

  public byte[] toByteArray() {
    return KllHelper.toCompactByteArrayImpl(this);
  }

  @Override
  public void update(final T item) {
    // TODO Auto-generated method stub
  }

  //restricted

  @Override
  abstract byte[] getRetainedDataByteArr();

  @Override
  abstract int getRetainedDataSizeBytes();

  @Override
  abstract byte[] getMinMaxByteArr();

  @Override
  abstract int getMinMaxSizeBytes();

  @Override
  abstract byte[] getSingleItemByteArr();

  @Override
  abstract int getSingleItemSizeBytes();

  @Override
  int getM() {
    return m;
  }

  @Override
  int getMinK() {
    return minK;
  }

  abstract T getSingleItem();

  @Override
  void incN() {
    n++;
  }

  @Override
  void incNumLevels() {
    // TODO Auto-generated method stub
  }

  @Override
  boolean isLevelZeroSorted() {
    return false; //or isLevelZeroSorted_
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    isLevelZeroSorted = sorted;
  }

  @Override
  void setMinK(final int minK) {
    this.minK = minK;
  }

  @Override
  void setN(final long n) {
    this.n = n;
  }

  @Override
  void setNumLevels(final int numLevels) {
    // TODO Auto-generated method stub
  }

}
