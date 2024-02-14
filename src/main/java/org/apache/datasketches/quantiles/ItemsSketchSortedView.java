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

import static org.apache.datasketches.quantiles.ClassicUtil.getNormalizedRankError;
import static org.apache.datasketches.quantilescommon.GenericInequalitySearch.find;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesAPI.EMPTY_MSG;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.evenlySpacedDoubles;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.getNaturalRank;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.quantilescommon.GenericInequalitySearch.Inequality;
import org.apache.datasketches.quantilescommon.GenericPartitionBoundaries;
import org.apache.datasketches.quantilescommon.GenericSortedView;
import org.apache.datasketches.quantilescommon.GenericSortedViewIterator;
import org.apache.datasketches.quantilescommon.InequalitySearch;
import org.apache.datasketches.quantilescommon.PartitioningFeature;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesAPI;
import org.apache.datasketches.quantilescommon.QuantilesUtil;

/**
 * The SortedView of the Classic Quantiles ItemsSketch.
 * @param <T> The sketch data type
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
public class ItemsSketchSortedView<T> implements GenericSortedView<T>, PartitioningFeature<T> {
  private final T[] quantiles;
  private final long[] cumWeights; //comes in as individual weights, converted to cumulative natural weights
  private final long totalN;
  private final Comparator<? super T> comparator;
  private final T maxItem;
  private final T minItem;
  private final Class<T> clazz;
  private final int k;

  /**
   * Construct from elements for testing.
   * @param quantiles sorted array of quantiles
   * @param cumWeights sorted, monotonically increasing cumulative weights.
   * @param totalN the total number of items presented to the sketch.
   * @param comparator comparator for type T
   */
  @SuppressWarnings("unchecked")
  ItemsSketchSortedView(
      final T[] quantiles,
      final long[] cumWeights, //or Natural Ranks
      final long totalN,
      final Comparator<T> comparator,
      final T maxItem,
      final T minItem,
      final int k) {
    this.quantiles = quantiles;
    this.cumWeights = cumWeights;
    this.totalN = totalN;
    this.comparator = comparator;
    this.maxItem = maxItem;
    this.minItem = minItem;
    this.clazz = (Class<T>)quantiles[0].getClass();
    this.k = k;
  }

  /**
   * Constructs this Sorted View given the sketch
   * @param sketch the given Classic Quantiles ItemsSketch
   */
  @SuppressWarnings("unchecked")
  ItemsSketchSortedView(final ItemsSketch<T> sketch) {
    if (sketch.isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    this.totalN = sketch.getN();
    final int numQuantiles = sketch.getNumRetained();
    this.quantiles = (T[]) Array.newInstance(sketch.clazz, numQuantiles);
    this.minItem = sketch.minItem_;
    this.maxItem = sketch.maxItem_;
    this.cumWeights = new long[numQuantiles];
    this.comparator = sketch.getComparator();
    this.clazz = sketch.clazz;
    this.k = sketch.getK();

    final Object[] combinedBuffer = sketch.getCombinedBuffer();
    final int baseBufferCount = sketch.getBaseBufferCount();

    // Populate from ItemsSketch:
    // copy over the "levels" and then the base buffer, all with appropriate weights
    populateFromItemsSketch(k, totalN, sketch.getBitPattern(), (T[]) combinedBuffer, baseBufferCount,
        numQuantiles, quantiles, cumWeights, sketch.getComparator());

    // Sort the first "numSamples" slots of the two arrays in tandem,
    // taking advantage of the already sorted blocks of length k
    ItemsMergeImpl.blockyTandemMergeSort(quantiles, cumWeights, numQuantiles, k, sketch.getComparator());

    if (convertToCumulative(cumWeights) != totalN) {
      throw new SketchesStateException("Sorted View is misconfigured. TotalN does not match cumWeights.");
    }
  }

  //end of constructors

  @Override
  public double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(EMPTY_MSG); }
    GenericSortedView.validateItems(splitPoints, comparator);
    final int len = splitPoints.length + 1;
    final double[] buckets = new double[len];
    for (int i = 0; i < len - 1; i++) {
      buckets[i] = getRank(splitPoints[i], searchCrit);
    }
    buckets[len - 1] = 1.0;
    return buckets;
  }

  @Override
  public long[] getCumulativeWeights() {
    return cumWeights.clone();
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
    return totalN;
  }

  @Override
  @SuppressWarnings("unchecked")
  public GenericPartitionBoundaries<T> getPartitionBoundaries(final int numEquallySized,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    final long totalN = this.totalN;
    final double delta = getNormalizedRankError(k, true) * totalN;
    final int maxParts = (int) (totalN / Math.ceil(delta * 2) );
    final int svLen = cumWeights.length;
    if (numEquallySized > maxParts) {
      throw new SketchesArgumentException(QuantilesAPI.UNSUPPORTED_MSG
          + "Requested number of partitions is too large for this sized sketch. "
          + "It exceeds maximum number of partitions allowed by the error threshold for this size sketch."
          + "Requested Partitions: " + numEquallySized + " > " + maxParts);
    }
    if (numEquallySized > svLen / 2) {
      throw new SketchesArgumentException(QuantilesAPI.UNSUPPORTED_MSG
          + "Requested number of partitions is too large for this sized sketch. "
          + "It exceeds maximum number of retained items divided by 2."
          + "Requested Partitions: " + numEquallySized + " > "
          + "Retained Items / 2: " + (svLen / 2));
    }

    final double[] searchNormRanks = evenlySpacedDoubles(0, 1.0, numEquallySized + 1);
    final int partArrLen = searchNormRanks.length;
    final T[] partQuantiles = (T[]) Array.newInstance(clazz, partArrLen);
    final long[] partNatRanks = new long[partArrLen];
    final double[] partNormRanks = new double[partArrLen];
    //adjust ends
    int adjLen = svLen;
    final boolean adjLow = quantiles[0] != minItem;
    final boolean adjHigh = quantiles[svLen - 1] != maxItem;
    adjLen += adjLow ? 1 : 0;
    adjLen += adjHigh ? 1 : 0;

    final T[] adjQuantiles;
    final long[] adjCumWeights;
    if (adjLen > svLen) {
      adjQuantiles = (T[]) new Object[adjLen];
      adjCumWeights = new long[adjLen];
      final int offset = adjLow ? 1 : 0;
      System.arraycopy(quantiles, 0, adjQuantiles, offset, svLen);
      System.arraycopy(cumWeights,0, adjCumWeights, offset, svLen);
      if (adjLow) {
        adjQuantiles[0] = minItem;
        adjCumWeights[0] = 1;
      }
      if (adjHigh) {
        adjQuantiles[adjLen - 1] = maxItem;
        adjCumWeights[adjLen - 1] = cumWeights[svLen - 1];
        adjCumWeights[adjLen - 2] = cumWeights[svLen - 1] - 1;
      }
    } else {
      adjQuantiles = quantiles;
      adjCumWeights = cumWeights;
    }
    for (int i = 0; i < partArrLen; i++) {
      final int index = getQuantileIndex(searchNormRanks[i], adjCumWeights, searchCrit);
      partQuantiles[i] = adjQuantiles[index];
      final long cumWt = adjCumWeights[index];
      partNatRanks[i] = cumWt;
      partNormRanks[i] = (double)cumWt / totalN;
    }
    final GenericPartitionBoundaries<T> gpb = new GenericPartitionBoundaries<>(
        this.totalN,
        partQuantiles,
        partNatRanks,
        partNormRanks,
        getMaxItem(),
        getMinItem(),
        searchCrit);
    return gpb;
  }

  @Override
  public double[] getPMF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(EMPTY_MSG); }
    GenericSortedView.validateItems(splitPoints, comparator);
    final double[] buckets = getCDF(splitPoints, searchCrit);
    final int len = buckets.length;
    for (int i = len; i-- > 1; ) {
      buckets[i] -= buckets[i - 1];
    }
    return buckets;
  }

  @Override
  public T getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(EMPTY_MSG); }
    QuantilesUtil.checkNormalizedRankBounds(rank);
    final int index = getQuantileIndex(rank, cumWeights, searchCrit);
    return quantiles[index];
  }

  private int getQuantileIndex(final double normRank, final long[] cumWeights,
      final QuantileSearchCriteria searchCrit) {
    final int len = cumWeights.length;
    final double naturalRank = getNaturalRank(normRank, totalN, searchCrit);
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, naturalRank, crit);
    if (index == -1) { return len - 1; }
    return index;
  }

  @SuppressWarnings("unchecked")
  public T[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    final int len = ranks.length;
    final T[] quants = (T[]) Array.newInstance(clazz, len);
    for (int i = 0; i < len; i++) {
      quants[i] = getQuantile(ranks[i], searchCrit);
    }
    return quants;
  }

  @Override
  public T[] getQuantiles() {
    return quantiles.clone();
  }

  @Override
  public double getRank(final T quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(EMPTY_MSG); }
    final int len = quantiles.length;
    final Inequality crit = (searchCrit == INCLUSIVE) ? Inequality.LE : Inequality.LT;
    final int index = find(quantiles,  0, len - 1, quantile, crit, comparator);
    if (index == -1) {
      return 0; //EXCLUSIVE (LT) case: quantile <= minQuantile; INCLUSIVE (LE) case: quantile < minQuantile
    }
    return (double)cumWeights[index] / totalN;
  }

  @Override
  public boolean isEmpty() {
    return totalN == 0;
  }

  @Override
  public GenericSortedViewIterator<T> iterator() {
    return new GenericSortedViewIterator<>(quantiles, cumWeights);
  }

  //restricted methods

  /**
   * Populate the arrays and registers from an ItemsSketch
   * @param <T> the data type
   * @param k K parameter of sketch
   * @param n The current size of the stream
   * @param bitPattern the bit pattern for valid log levels
   * @param combinedBuffer the combined buffer reference
   * @param baseBufferCount the count of the base buffer
   * @param numQuantiles number of retained quantiles in the sketch
   * @param quantilesArr the consolidated array of all quantiles from the sketch
   * @param weightsArr the weights for each item from the sketch
   * @param comparator the given comparator for data type T
   */
  private final static <T> void populateFromItemsSketch(
      final int k, final long n, final long bitPattern, final T[] combinedBuffer,
      final int baseBufferCount, final int numQuantiles, final T[] quantilesArr, final long[] weightsArr,
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
          quantilesArr[nxt] = combinedBuffer[i + offset];
          weightsArr[nxt] = weight;
          nxt++;
        }
      }
    }

    weight = 1; //NOT a mistake! We just copied the highest level; now we need to copy the base buffer
    final int startOfBaseBufferBlock = nxt;

    // Copy BaseBuffer over, along with weight = 1
    for (int i = 0; i < baseBufferCount; i++) {
      quantilesArr[nxt] = combinedBuffer[i];
      weightsArr[nxt] = weight;
      nxt++;
    }
    assert nxt == numQuantiles;

    // Must sort the items that came from the base buffer.
    // Don't need to sort the corresponding weights because they are all the same.
    Arrays.sort(quantilesArr, startOfBaseBufferBlock, numQuantiles, comparator);
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

}
