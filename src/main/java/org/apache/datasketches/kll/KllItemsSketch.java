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
public class KllItemsSketch<T> extends KllSketch implements QuantilesGenericAPI<T> {
  private final int k_; // configured size of K.
  private final int m_; // configured size of M.
  private long n_;      // number of items input into this sketch.
  private int minK_;    // dynamic minK for error estimation after merging with different k.
  private boolean isLevelZeroSorted_;
  private T maxItem_;
  private T minItem_;
  private Object[] items_;
  private final Class<T> clazz_;
  private final Comparator<? super T> comparator_;
  final ArrayOfItemsSerDe<T> serDe_;

  KllItemsSketch(
      final int k,
      final int m,
      final Class<T> clazz,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    super(ITEMS_SKETCH, null, null);
    Objects.requireNonNull(clazz, "Class<T> must not be null.");
    Objects.requireNonNull(comparator, "Comparator must not be null.");
    Objects.requireNonNull(serDe, "Serializer/Deserializer must not be null.");
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    k_ = k;
    m_ = DEFAULT_M;
    n_ = 0;
    minK_ = k;
    isLevelZeroSorted_ = false;
    maxItem_ = null;
    minItem_ = null;
    items_ = new Object[k];
    levelsArr = new int[] {k, k};
    clazz_ = clazz;
    comparator_ = comparator;
    serDe_ = serDe;
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
      final KllItemsSketch<T> itmSk = new KllItemsSketch<T>(DEFAULT_K, DEFAULT_M, clazz, comparator, serDe);
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
      final KllItemsSketch<T> itmSk = new KllItemsSketch<T>(k, DEFAULT_M, clazz, comparator, serDe);
    return itmSk;
  }

  @Override
  public double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getK() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public T getMaxItem() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T getMinItem() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getN() {
    // TODO Auto-generated method stub
    return 0;
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

  @Override
  public void update(final T item) {
    // TODO Auto-generated method stub
  }

  //restricted

  @Override
  int getDataBlockBytes(final int numItemsAndMinMax) {
    return 0;
  }

  @Override
  int getM() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  int getMinK() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  int getTheSingleItemBytes() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  void incN() {
    // TODO Auto-generated method stub
  }

  @Override
  void incNumLevels() {
    // TODO Auto-generated method stub
  }

  @Override
  boolean isLevelZeroSorted() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    // TODO Auto-generated method stub
  }

  @Override
  void setMinK(final int minK) {
    // TODO Auto-generated method stub
  }

  @Override
  void setN(final long n) {
    // TODO Auto-generated method stub
  }

  @Override
  void setNumLevels(final int numLevels) {
    // TODO Auto-generated method stub
  }

}
