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

import static org.apache.datasketches.quantiles.Util.checkFractionalRankBounds;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.datasketches.SketchesStateException;

/**
 * The Sorted View provides a view of the data retained by the sketch that would be cumbersome to get any other way.
 * One can iterate of the contents of the sketch, but the result is not sorted.
 * Trying to use getQuantiles would be very cumbersome since one doesn't know what ranks to use to supply the
 * getQuantiles method.  Even worse, suppose it is a large sketch that has retained 1000 values from a stream of
 * millions (or billions).  One would have to execute the getQuantiles method many thousands of times, and using
 * trial &amp; error, try to figure out what the sketch actually has retained.
 *
 * <p>The data from a Sorted view is an unbiased sample of the input stream that can be used for other kinds of
 * analysis not directly provided by the sketch.  A good example comparing two sketches using the Kolmogorov-Smirnov
 * test. One needs this sorted view for the test.</p>
 *
 * <p>This sorted view can also be used for multiple getRank and getQuantile queries once it has been created.
 * Because it takes some computational work to create this sorted view, it doesn't make sense to create this sorted view
 * just for single getRank queries.  For the first getQuantile queries, it must be created. But for all queries
 * after the first, assuming the sketch has not been updated, the getQuantile and getRank queries are very fast.</p>
 *
 * @param <T> type of item
 *
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
public final class ItemsSketchSortedView<T> {
  final long auxN_;
  final Object[] auxSamplesArr_; //array of size samples
  final long[] auxCumWtsArr_;

  /**
   * Constructs the Auxiliary structure from the ItemsSketch
   * @param qs an ItemsSketch
   */
  @SuppressWarnings("unchecked")
  ItemsSketchSortedView(final ItemsSketch<T> qs, final boolean cumulative, final boolean inclusive) {
    final int k = qs.getK();
    final long n = qs.getN();
    final long bitPattern = qs.getBitPattern();
    final Object[] combinedBuffer = qs.getCombinedBuffer();
    final int baseBufferCount = qs.getBaseBufferCount();
    final int numSamples = qs.getRetainedItems();

    final Object[] itemsArr = new Object[numSamples];
    final long[] cumWtsArr = new long[numSamples + 1]; /* the extra slot is very important */

    // Populate from ItemsSketch:
    // copy over the "levels" and then the base buffer, all with appropriate weights
    populateFromItemsSketch(k, n, bitPattern, (T[]) combinedBuffer, baseBufferCount,
        numSamples, (T[]) itemsArr, cumWtsArr, qs.getComparator());

    // Sort the first "numSamples" slots of the two arrays in tandem,
    // taking advantage of the already sorted blocks of length k
    ItemsMergeImpl.blockyTandemMergeSort((T[]) itemsArr, cumWtsArr, numSamples, k, qs.getComparator());

    // convert the item weights into totals of the weights preceding each item or including the item
    if (cumulative) {
      long subtot = 0;
      for (int i = 0; i < (numSamples + 1); i++) {
        final long newSubtot = subtot + cumWtsArr[i];
        cumWtsArr[i] = inclusive ? newSubtot : subtot;
        subtot = newSubtot;
      }

      assert subtot == n;
    }

    auxN_ = n;
    auxSamplesArr_ = itemsArr;
    auxCumWtsArr_ = cumWtsArr;
  }

  /**
   * Get the estimated quantile given a fractional rank.
   * @param rank the normalized rank where: 0 &le; rank &le; 1.0.
   * @return the estimated quantile
   */
  @SuppressWarnings("deprecation")
  public T getQuantile(final double rank) {
    checkFractionalRankBounds(rank);
    if (auxN_ <= 0) { return null; }
    if (auxCumWtsArr_[auxCumWtsArr_.length - 1] < auxN_) {
      throw new SketchesStateException("getQuantile must be used with cumulative view only");
    }
    final long pos = ClassicQuantilesHelper.posOfRank(rank, auxN_);
    return approximatelyAnswerPositionalQuery(pos);
  }

  /**
   * Assuming that there are n items in the true stream, this asks what
   * item would appear in position 0 &le; pos &lt; n of a hypothetical sorted
   * version of that stream.
   *
   * <p>Note that since that since the true stream is unavailable,
   * we don't actually answer the question for that stream, but rather for
   * a <i>different</i> stream of the same length, that could hypothetically
   * be reconstructed from the weighted samples in our sketch.
   * @param pos position
   * @return approximate answer
   */
  @SuppressWarnings({ "unchecked", "deprecation" })
  private T approximatelyAnswerPositionalQuery(final long pos) {
    assert 0 <= pos;
    assert pos < auxN_;
    final int index = ClassicQuantilesHelper.chunkContainingPos(auxCumWtsArr_, pos);
    return (T) this.auxSamplesArr_[index];
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
    cumWtsArr[numSamples] = 0;
  }

  public ItemsSketchSortedViewIterator<T> iterator() {
    return new ItemsSketchSortedViewIterator<T>(auxSamplesArr_, auxCumWtsArr_);
  }
}
