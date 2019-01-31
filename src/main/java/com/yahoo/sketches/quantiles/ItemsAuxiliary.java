/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.checkFractionalRankBounds;

import java.util.Arrays;
import java.util.Comparator;

import com.yahoo.sketches.QuantilesHelper;

/**
 * Auxiliary data structure for answering generic quantile queries
 *
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
final class ItemsAuxiliary<T> {
  final long auxN_;
  final Object[] auxSamplesArr_; //array of size samples
  final long[] auxCumWtsArr_;

  /**
   * Constructs the Auxiliary structure from the ItemsSketch
   * @param qs an Itemsketch
   */
  @SuppressWarnings("unchecked")
  ItemsAuxiliary(final ItemsSketch<T> qs) {
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

    // convert the item weights into totals of the weights preceding each item
    long subtot = 0;
    for (int i = 0; i < (numSamples + 1); i++ ) {
      final long newSubtot = subtot + cumWtsArr[i];
      cumWtsArr[i] = subtot;
      subtot = newSubtot;
    }

    assert subtot == n;

    auxN_ = n;
    auxSamplesArr_ = itemsArr;
    auxCumWtsArr_ = cumWtsArr;
  }

  /**
   * Get the estimated quantile given a fractional rank.
   * @param fRank the fractional rank where: 0 &le; fRank &le; 1.0.
   * @return the estimated quantile
   */
  T getQuantile(final double fRank) {
    checkFractionalRankBounds(fRank);
    if (auxN_ <= 0) { return null; }
    final long pos = QuantilesHelper.posOfPhi(fRank, auxN_);
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
  @SuppressWarnings("unchecked")
  private T approximatelyAnswerPositionalQuery(final long pos) {
    assert 0 <= pos;
    assert pos < auxN_;
    final int index = QuantilesHelper.chunkContainingPos(auxCumWtsArr_, pos);
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

} // end of class Auxiliary
