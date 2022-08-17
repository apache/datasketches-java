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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.GenericInequalitySearch.*;
import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.QuantileSearchCriteria.EXCLUSIVE_STRICT;
import static org.apache.datasketches.quantiles.Util.checkNormalizedRankBounds;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

import org.apache.datasketches.GenericInequalitySearch;
import org.apache.datasketches.GenericSortedView;
import org.apache.datasketches.InequalitySearch;
import org.apache.datasketches.QuantileSearchCriteria;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesStateException;

/**
 * The SortedView of the Classic Quantiles ItemsSketch.
 * @param <T> type of item
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
public final class ItemsSketchSortedView<T> implements GenericSortedView<T> {

  private final Object[] items; //array of size samples
  private final long[] cumWeights;
  private final long totalN;
  private final Comparator<? super T> comparator;

  /**
   * Construct from elements for testing.
   * @param items sorted array of itemss
   * @param cumWeights sorted, monotonically increasing cumulative weights.
   * @param totalN the total number of values presented to the sketch.
   */
  ItemsSketchSortedView(final T[] items, final long[] cumWeights, final long totalN,
      final Comparator<? super T> comparator) {
    this.items = items;
    this.cumWeights = cumWeights;
    this.totalN = totalN;
    this.comparator = comparator;
  }

  /**
   * Constructs this Sorted View given the sketch
   * @param sketch the given Classic Quantiles ItemsSketch
   */
  @SuppressWarnings("unchecked")
  ItemsSketchSortedView(final ItemsSketch<T> sketch) {
    this.totalN = sketch.getN();
    final int k = sketch.getK();
    final int numSamples = sketch.getRetainedItems();
    items = new Object[numSamples];
    cumWeights = new long[numSamples];
    comparator = sketch.getComparator();

    final Object[] combinedBuffer = sketch.getCombinedBuffer();
    final int baseBufferCount = sketch.getBaseBufferCount();

    // Populate from ItemsSketch:
    // copy over the "levels" and then the base buffer, all with appropriate weights
    populateFromItemsSketch(k, totalN, sketch.getBitPattern(), (T[]) combinedBuffer, baseBufferCount,
        numSamples, (T[]) items, cumWeights, sketch.getComparator());

    // Sort the first "numSamples" slots of the two arrays in tandem,
    // taking advantage of the already sorted blocks of length k
    ItemsMergeImpl.blockyTandemMergeSort((T[]) items, cumWeights, numSamples, k, sketch.getComparator());

    if (convertToCumulative(cumWeights) != totalN) {
      throw new SketchesStateException("Sorted View is misconfigured. TotalN does not match cumWeights.");
    }
  }


  @SuppressWarnings("unchecked")
  @Override
  public T getQuantile(final double normRank, final QuantileSearchCriteria searchCrit) {
    checkNormalizedRankBounds(normRank);
    final int len = cumWeights.length;
    final long naturalRank = (int)(normRank * totalN);
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, naturalRank, crit);
    if (index == -1) {
      if (searchCrit == EXCLUSIVE_STRICT) { return null; } //GT: normRank == 1.0;
      if (searchCrit == EXCLUSIVE) { return (T)items[len - 1]; }
    }
    return (T)items[index];
  }

  @SuppressWarnings("unchecked")
  @Override
  public double getRank(final T item, final QuantileSearchCriteria searchCrit) {
    final int len = items.length;
    final Inequality crit = (searchCrit == INCLUSIVE) ? Inequality.LE : Inequality.LT;
    final int index = GenericInequalitySearch.find((T[])items,  0, len - 1, item, crit, comparator);
    if (index == -1) {
      return 0; //LT: value <= minValue; LE: value < minValue
    }
    return (double)cumWeights[index] / totalN;
  }

  @Override
  public double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    checkSplitPoints(splitPoints);
    final int len = splitPoints.length + 1;
    final double[] buckets = new double[len];
    for (int i = 0; i < len - 1; i++) {
      buckets[i] = getRank(splitPoints[i], searchCrit);
    }
    buckets[len - 1] = 1.0;
    return buckets;
  }

  @Override
  public double[] getPMF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    final double[] buckets = getCDF(splitPoints, searchCrit);
    final int len = buckets.length;
    for (int i = len; i-- > 1; ) {
      buckets[i] -= buckets[i - 1];
    }
    return buckets;
  }

  @Override
  public long[] getCumulativeWeights() {
    return cumWeights;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T[] getItems() {
    return (T[])items;
  }

  @Override
  public ItemsSketchSortedViewIterator<T> iterator() {
    return new ItemsSketchSortedViewIterator<T>(items, cumWeights);
  }

  /**
   * Populate the arrays and registers from an ItemsSketch
   * @param <T> the data type
   * @param k K value of sketch
   * @param n The current size of the stream
   * @param bitPattern the bit pattern for valid log levels
   * @param combinedBuffer the combined buffer reference
   * @param baseBufferCount the count of the base buffer
   * @param numSamples Total samples in the sketch
   * @param itemsArr the consolidated array of all items from the sketch populated here
   * @param cumWtsArr the cumulative weights for each item from the sketch populated here
   * @param comparator the given comparator for data type T
   */
  private final static <T> void populateFromItemsSketch(
      final int k, final long n, final long bitPattern, final T[] combinedBuffer,
      final int baseBufferCount, final int numSamples, final T[] itemsArr, final long[] cumWtsArr,
      final Comparator<? super T> comparator) {
    long weight = 1;
    int nxt = 0;
    long bits = bitPattern;
    assert bits == (n / (2L * k)); // internal consistency check
    for (int lvl = 0; bits != 0L; lvl++, bits >>>= 1) {
      weight *= 2;
      if ((bits & 1L) > 0L) {
        final int offset = (2 + lvl) * k;
        for (int i = 0; i < k; i++) {
          itemsArr[nxt] = combinedBuffer[i + offset];
          cumWtsArr[nxt] = weight;
          nxt++;
        }
      }
    }

    weight = 1; //NOT a mistake! We just copied the highest level; now we need to copy the base buffer
    final int startOfBaseBufferBlock = nxt;

    // Copy BaseBuffer over, along with weight = 1
    for (int i = 0; i < baseBufferCount; i++) {
      itemsArr[nxt] = combinedBuffer[i];
      cumWtsArr[nxt] = weight;
      nxt++;
    }
    assert nxt == numSamples;

    // Must sort the items that came from the base buffer.
    // Don't need to sort the corresponding weights because they are all the same.
    Arrays.sort(itemsArr, startOfBaseBufferBlock, numSamples, comparator);
  }

  /**
   * Convert the individual weights into cumulative weights.
   * An array of {1,1,1,1} becomes {1,2,3,4}
   * @param array of actual weights from the sketch, none of the weights may be zero
   * @return total weight
   */
  private static long convertToCumulative(final long[] array) {
    long subtotal = 0;
    for (int i = 0; i < array.length; i++) {
      final long newSubtotal = subtotal + array[i];
      subtotal = array[i] = newSubtotal;
    }
    return subtotal;
  }

  private final void checkSplitPoints(final T[] items) {
    Objects.requireNonNull(items);
    Objects.requireNonNull(comparator);
    final int len = items.length -1;
    for (int i = 0; i < len; i++) {
      if (comparator.compare(items[i], items[i + 1]) < 0) { continue; }
      throw new SketchesArgumentException(
          "Values must be unique, monotonically increasing and not NaN.");
    }
  }
}
