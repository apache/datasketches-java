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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.quantiles.ClassicUtil.MAX_PRELONGS;
import static org.apache.datasketches.quantiles.ClassicUtil.MIN_K;
import static org.apache.datasketches.quantiles.ClassicUtil.checkIsCompactMemory;
import static org.apache.datasketches.quantiles.ClassicUtil.checkK;
import static org.apache.datasketches.quantiles.ClassicUtil.computeNumLevelsNeeded;
import static org.apache.datasketches.quantiles.ClassicUtil.computeRetainedItems;
import static org.apache.datasketches.quantiles.DoublesSketchAccessor.BB_LVL_IDX;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.DoublesSketchSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesAPI;
import org.apache.datasketches.quantilescommon.QuantilesDoublesAPI;
import org.apache.datasketches.quantilescommon.QuantilesDoublesSketchIterator;

/**
 * This is an implementation of the Low Discrepancy Mergeable Quantiles Sketch, using doubles,
 * described in section 3.2 of the journal version of the paper "Mergeable Summaries"
 * by Agarwal, Cormode, Huang, Phillips, Wei, and Yi:
 *
 * <p>Reference: <a href="http://dblp.org/rec/html/journals/tods/AgarwalCHPWY13"></a></p>
 *
 * <p>A <i>k</i> of 128 produces a normalized, rank error of about 1.7%.
 * For example, the median returned from getQuantile(0.5) will be between the actual quantiles
 * from the hypothetically sorted array of input quantiles at normalized ranks of 0.483 and 0.517, with
 * a confidence of about 99%.</p>
 *
 * <pre>
Table Guide for DoublesSketch Size in Bytes and Approximate Error:
          K =&gt; |      16      32      64     128     256     512   1,024
    ~ Error =&gt; | 12.145%  6.359%  3.317%  1.725%  0.894%  0.463%  0.239%
             N | Size in Bytes -&gt;
------------------------------------------------------------------------
             0 |       8       8       8       8       8       8       8
             1 |      72      72      72      72      72      72      72
             3 |      72      72      72      72      72      72      72
             7 |     104     104     104     104     104     104     104
            15 |     168     168     168     168     168     168     168
            31 |     296     296     296     296     296     296     296
            63 |     424     552     552     552     552     552     552
           127 |     552     808   1,064   1,064   1,064   1,064   1,064
           255 |     680   1,064   1,576   2,088   2,088   2,088   2,088
           511 |     808   1,320   2,088   3,112   4,136   4,136   4,136
         1,023 |     936   1,576   2,600   4,136   6,184   8,232   8,232
         2,047 |   1,064   1,832   3,112   5,160   8,232  12,328  16,424
         4,095 |   1,192   2,088   3,624   6,184  10,280  16,424  24,616
         8,191 |   1,320   2,344   4,136   7,208  12,328  20,520  32,808
        16,383 |   1,448   2,600   4,648   8,232  14,376  24,616  41,000
        32,767 |   1,576   2,856   5,160   9,256  16,424  28,712  49,192
        65,535 |   1,704   3,112   5,672  10,280  18,472  32,808  57,384
       131,071 |   1,832   3,368   6,184  11,304  20,520  36,904  65,576
       262,143 |   1,960   3,624   6,696  12,328  22,568  41,000  73,768
       524,287 |   2,088   3,880   7,208  13,352  24,616  45,096  81,960
     1,048,575 |   2,216   4,136   7,720  14,376  26,664  49,192  90,152
     2,097,151 |   2,344   4,392   8,232  15,400  28,712  53,288  98,344
     4,194,303 |   2,472   4,648   8,744  16,424  30,760  57,384 106,536
     8,388,607 |   2,600   4,904   9,256  17,448  32,808  61,480 114,728
    16,777,215 |   2,728   5,160   9,768  18,472  34,856  65,576 122,920
    33,554,431 |   2,856   5,416  10,280  19,496  36,904  69,672 131,112
    67,108,863 |   2,984   5,672  10,792  20,520  38,952  73,768 139,304
   134,217,727 |   3,112   5,928  11,304  21,544  41,000  77,864 147,496
   268,435,455 |   3,240   6,184  11,816  22,568  43,048  81,960 155,688
   536,870,911 |   3,368   6,440  12,328  23,592  45,096  86,056 163,880
 1,073,741,823 |   3,496   6,696  12,840  24,616  47,144  90,152 172,072
 2,147,483,647 |   3,624   6,952  13,352  25,640  49,192  94,248 180,264
 4,294,967,295 |   3,752   7,208  13,864  26,664  51,240  98,344 188,456
 * </pre>
 *
 * @see QuantilesAPI
 */
public abstract class DoublesSketch implements QuantilesDoublesAPI {
  /**
   * Setting the seed makes the results of the sketch deterministic if the input quantiles are
   * received in exactly the same order. This is only useful when performing test comparisons,
   * otherwise is not recommended.
   */
  static Random rand = new Random();

  /**
   * Parameter that controls space usage of sketch and accuracy of estimates.
   */
  final int k_;

  DoublesSketchSortedView doublesSV = null;

  DoublesSketch(final int k) {
    checkK(k);
    k_ = k;
  }

  synchronized static void setRandom(final long seed) {
    DoublesSketch.rand = new Random(seed);
  }

  /**
   * Returns a new builder
   * @return a new builder
   */
  public static final DoublesSketchBuilder builder() {
    return new DoublesSketchBuilder();
  }

  /**
   * Heapify takes the sketch image in Memory and instantiates an on-heap Sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem a Memory image of a Sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based Sketch based on the given Memory
   */
  public static DoublesSketch heapify(final Memory srcMem) {
    if (checkIsCompactMemory(srcMem)) {
      return HeapCompactDoublesSketch.heapifyInstance(srcMem);
    }
    return UpdateDoublesSketch.heapify(srcMem);
  }

  /**
   * Wrap this sketch around the given Memory image of a DoublesSketch, compact or updatable.
   * A DirectUpdateDoublesSketch can only wrap an updatable array, and a
   * DirectCompactDoublesSketch can only wrap a compact array.
   *
   * @param srcMem the given Memory image of a DoublesSketch that may have data,
   * @return a sketch that wraps the given srcMem
   */
  public static DoublesSketch wrap(final Memory srcMem) {
    if (checkIsCompactMemory(srcMem)) {
      return DirectCompactDoublesSketch.wrapInstance(srcMem);
    }
    return DirectUpdateDoublesSketchR.wrapInstance(srcMem);
  }

  @Override
  public double[] getCDF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
  if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return doublesSV.getCDF(splitPoints, searchCrit);
  }

  @Override
  public abstract double getMaxItem();

  @Override
  public abstract double getMinItem();

  @Override
  public double[] getPMF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
  if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return doublesSV.getPMF(splitPoints, searchCrit);
  }

  @Override
  public double getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
  if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return doublesSV.getQuantile(rank, searchCrit);
  }

  @Override
  public double[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    final int len = ranks.length;
    final double[] quantiles = new double[len];
    for (int i = 0; i < len; i++) {
      quantiles[i] = doublesSV.getQuantile(ranks[i], searchCrit);
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
    return getQuantile(max(0, rank - getNormalizedRankError(k_, false)));
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.99.
   */
  @Override
  public double getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + getNormalizedRankError(k_, false)));
  }

  @Override
  public double getRank(final double quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return doublesSV.getRank(quantile, searchCrit);
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.99.
   */
  @Override
  public double getRankLowerBound(final double rank) {
    return max(0.0, rank - getNormalizedRankError(k_, false));
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.99.
   */
  @Override
  public double getRankUpperBound(final double rank) {
    return min(1.0, rank + getNormalizedRankError(k_, false));
  }

  @Override
  public double[] getRanks(final double[] quantiles, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = doublesSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }

  @Override
  public int getK() {
    return k_;
  }

  @Override
  public abstract long getN();

  @Override
  public double getNormalizedRankError(final boolean pmf) {
    return getNormalizedRankError(k_, pmf);
  }

  /**
   * Gets the normalized rank error given k and pmf.
   * Static method version of the <i>getNormalizedRankError(boolean)</i>.
   * The epsilon returned is a best fit to 99 percent confidence empirically measured max error
   * in thousands of trials.
   * @param k the configuration parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   */
  public static double getNormalizedRankError(final int k, final boolean pmf) {
    return ClassicUtil.getNormalizedRankError(k, pmf);
  }

  /**
   * Gets the approximate <em>k</em> to use given epsilon, the normalized rank error.
   * @param epsilon the normalized rank error between zero and one.
   * @param pmf if true, this function returns <em>k</em> assuming the input epsilon
   * is the desired "double-sided" epsilon for the getPMF() function. Otherwise, this function
   * returns <em>k</em> assuming the input epsilon is the desired "single-sided"
   * epsilon for all the other queries.
   * @return <i>k</i> given epsilon.
   */
  public static int getKFromEpsilon(final double epsilon, final boolean pmf) {
    return ClassicUtil.getKFromEpsilon(epsilon, pmf);
  }

  @Override
  public abstract boolean hasMemory();

  @Override
  public abstract boolean isDirect();

  @Override
  public boolean isEmpty() {
    return getN() == 0;
  }

  @Override
  public boolean isEstimationMode() {
    return getN() >= 2L * k_;
  }

  @Override
  public abstract boolean isReadOnly();

  /**
   * Returns true if the backing resource of <i>this</i> is identical with the backing resource
   * of <i>that</i>. The capacities must be the same.  If <i>this</i> is a region,
   * the region offset must also be the same.
   * @param that A different non-null object
   * @return true if the backing resource of <i>this</i> is the same as the backing resource
   * of <i>that</i>.
   */
  public boolean isSameResource(final Memory that) { //Overridden by direct sketches
    return false;
  }

  @Override
  public byte[] toByteArray() {
    if (isCompact()) {
      return toByteArray(true);
    }
    return toByteArray(false);
  }

  /**
   * Serialize this sketch in a byte array form.
   * @param compact if true the sketch will be serialized in compact form.
   * DirectCompactDoublesSketch can wrap() only a compact byte array;
   * DirectUpdateDoublesSketch can wrap() only a updatable byte array.
   * @return this sketch in a byte array form.
   */
  public byte[] toByteArray(final boolean compact) {
    return DoublesByteArrayImpl.toByteArray(this, compact, compact);
  }

  /**
   * Returns human readable summary information about this sketch.
   * Used for debugging.
   */
  @Override
  public String toString() {
    return toString(true, false);
  }

  /**
   * Returns human readable summary information about this sketch.
   * Used for debugging.
   * @param withLevels if true includes sketch levels array summary information
   * @param withLevelsAndItems if true include detail of levels array and items array together
   * @return human readable summary information about this sketch.
   */
  public String toString(final boolean withLevels, final boolean withLevelsAndItems) {
    return DoublesUtil.toString(withLevels, withLevelsAndItems, this);
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of a DoublesSketch.
   * Used for debugging.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of a DoublesSketch.
   */
  public static String toString(final byte[] byteArr) {
    return PreambleUtil.toString(byteArr, true);
  }

  /**
   * Returns a human readable string of the preamble of a Memory image of a DoublesSketch.
   * Used for debugging.
   * @param mem the given Memory
   * @return a human readable string of the preamble of a Memory image of a DoublesSketch.
   */
  public static String toString(final Memory mem) {
    return PreambleUtil.toString(mem, true);
  }

  /**
   * From an source sketch, create a new sketch that must have a smaller K.
   * The original sketch is not modified.
   *
   * @param srcSketch the sourcing sketch
   * @param smallerK the new sketch's K that must be smaller than this K.
   * It is required that this.getK() = smallerK * 2^(nonnegative integer).
   * @param dstMem the destination Memory.  It must not overlap the Memory of this sketch.
   * If null, a heap sketch will be returned, otherwise it will be off-heap.
   *
   * @return the new sketch.
   */
  public DoublesSketch downSample(final DoublesSketch srcSketch, final int smallerK,
        final WritableMemory dstMem) {
    return downSampleInternal(srcSketch, smallerK, dstMem);
  }

  @Override
  public int getNumRetained() {
    return computeRetainedItems(getK(), getN());
  }

  /**
   * Returns the current number of bytes this sketch would require to store in the compact Memory Format.
   * @return the current number of bytes this sketch would require to store in the compact Memory Format.
   */
  public int getCurrentCompactSerializedSizeBytes() {
    return getCompactSerialiedSizeBytes(getK(), getN());
  }

  /**
   * Returns the number of bytes a DoublesSketch would require to store in compact form
   * given <i>k</i> and <i>n</i>. The compact form is not updatable.
   * @param k the size configuration parameter for the sketch
   * @param n the number of quantiles input into the sketch
   * @return the number of bytes required to store this sketch in compact form.
   */
  public static int getCompactSerialiedSizeBytes(final int k, final long n) {
    if (n == 0) { return 8; }
    final int metaPreLongs = MAX_PRELONGS + 2; //plus min, max
    return metaPreLongs + computeRetainedItems(k, n) << 3;
  }

  @Override
  public int getSerializedSizeBytes() {
    if (isCompact()) { return getCurrentCompactSerializedSizeBytes(); }
    return getCurrentUpdatableSerializedSizeBytes();
  }

  /**
   * Returns the current number of bytes this sketch would require to store in the updatable Memory Format.
   * @return the current number of bytes this sketch would require to store in the updatable Memory Format.
   */
  public int getCurrentUpdatableSerializedSizeBytes() {
    return getUpdatableStorageBytes(getK(), getN());
  }

  /**
   * Returns the number of bytes a sketch would require to store in updatable form.
   * This uses roughly 2X the storage of the compact form
   * given <i>k</i> and <i>n</i>.
   * @param k the size configuration parameter for the sketch
   * @param n the number of quantiles input into the sketch
   * @return the number of bytes this sketch would require to store in updatable form.
   */
  public static int getUpdatableStorageBytes(final int k, final long n) {
    if (n == 0) { return 8; }
    final int metaPre = MAX_PRELONGS + 2; //plus min, max
    final int totLevels = computeNumLevelsNeeded(k, n);
    if (n <= k) {
      final int ceil = Math.max(ceilingPowerOf2((int)n), MIN_K * 2);
      return (metaPre + ceil) << 3;
    }
    return (metaPre + (2 + totLevels) * k) << 3;
  }

  /**
   * Puts the current sketch into the given Memory in compact form if there is sufficient space,
   * otherwise, it throws an error.
   *
   * @param dstMem the given memory.
   */
  public void putMemory(final WritableMemory dstMem) {
    putMemory(dstMem, true);
  }

  /**
   * Puts the current sketch into the given Memory if there is sufficient space, otherwise,
   * throws an error.
   *
   * @param dstMem the given memory.
   * @param compact if true, compacts and sorts the base buffer, which optimizes merge
   *                performance at the cost of slightly increased serialization time.
   */
  public void putMemory(final WritableMemory dstMem, final boolean compact) {
    if (hasMemory() && isCompact() == compact) {
      final Memory srcMem = getMemory();
      srcMem.copyTo(0, dstMem, 0, getSerializedSizeBytes());
    } else {
      final byte[] byteArr = toByteArray(compact);
      final int arrLen = byteArr.length;
      final long memCap = dstMem.getCapacity();
      if (memCap < arrLen) {
        throw new SketchesArgumentException(
           "Destination Memory not large enough: " + memCap + " < " + arrLen);
      }
      dstMem.putByteArray(0, byteArr, 0, arrLen);
    }
  }

  @Override
  public QuantilesDoublesSketchIterator iterator() {
    return new DoublesSketchIterator(this, getBitPattern());
  }

  /**
   * {@inheritDoc}
   *
   * <p>The parameter <i>k</i> will not change.</p>
   */
  @Override
  public abstract void reset();

  //Restricted

  /*
   * DoublesMergeImpl.downSamplingMergeInto requires the target sketch to implement update(), so
   * we ensure that the target is an UpdateSketch. The public API, on the other hand, just
   * specifies a DoublesSketch. This lets us be more specific about the type without changing the
   * public API.
   */
  UpdateDoublesSketch downSampleInternal(final DoublesSketch srcSketch, final int smallerK,
                                         final WritableMemory dstMem) {
    final UpdateDoublesSketch newSketch = dstMem == null
            ? HeapUpdateDoublesSketch.newInstance(smallerK)
            : DirectUpdateDoublesSketch.newInstance(smallerK, dstMem);
    if (srcSketch.isEmpty()) { return newSketch; }
    DoublesMergeImpl.downSamplingMergeInto(srcSketch, newSketch);
    return newSketch;
  }

  //Restricted abstract

  /**
   * Returns true if this sketch is compact
   * @return true if this sketch is compact
   */
  abstract boolean isCompact();

  /**
   * Returns the base buffer count
   * @return the base buffer count
   */
  abstract int getBaseBufferCount();

  /**
   * Returns the bit pattern for valid log levels
   * @return the bit pattern for valid log levels
   */
  abstract long getBitPattern();

  /**
   * Returns the capacity for the combined base buffer
   * @return the capacity for the combined base buffer
   */
  abstract int getCombinedBufferItemCapacity();

  /**
   * Returns the combined buffer, in non-compact form.
   * @return the combined buffer, in non-compact form.
   */
  abstract double[] getCombinedBuffer();

  /**
   * Gets the Memory if it exists, otherwise returns null.
   * @return the Memory if it exists, otherwise returns null.
   */
  abstract WritableMemory getMemory();

  //************SORTED VIEW****************************

  @Override
  public final DoublesSketchSortedView getSortedView() {
    return refreshSortedView();
  }

  private final DoublesSketchSortedView refreshSortedView() {
    return (doublesSV == null) ? (doublesSV = getSV()) : doublesSV;
  }

  private DoublesSketchSortedView getSV() {
    final long totalN = getN();
    if (isEmpty() || (totalN == 0)) { throw new SketchesArgumentException(EMPTY_MSG); }
    final int numQuantiles = getNumRetained();
    final double[] svQuantiles = new double[numQuantiles];
    final long[] svCumWeights = new long[numQuantiles];
    final DoublesSketchAccessor sketchAccessor = DoublesSketchAccessor.wrap(this);

    // Populate from DoublesSketch:
    //  copy over the "levels" and then the base buffer, all with appropriate weights
    populateFromDoublesSketch(getK(), totalN, getBitPattern(), sketchAccessor, svQuantiles, svCumWeights);

    // Sort the first "numSamples" slots of the two arrays in tandem,
    // taking advantage of the already sorted blocks of length k
    blockyTandemMergeSort(svQuantiles, svCumWeights, numQuantiles, getK());

    if (convertToCumulative(svCumWeights) != totalN) {
      throw new SketchesStateException("Sorted View is misconfigured. TotalN does not match cumWeights.");
    }
    return new DoublesSketchSortedView(svQuantiles, svCumWeights, this);
  }

  private final static void populateFromDoublesSketch(
      final int k, final long totalN, final long bitPattern,
      final DoublesSketchAccessor sketchAccessor,
      final double[] svQuantiles, final long[] svCumWeights) {
    long weight = 1;
    int index = 0;
    long bits = bitPattern;
    assert bits == (totalN / (2L * k)); // internal consistency check
    for (int lvl = 0; bits != 0L; lvl++, bits >>>= 1) {
      weight <<= 1; // X2
      if ((bits & 1L) > 0L) {
        sketchAccessor.setLevel(lvl);
        for (int i = 0; i < sketchAccessor.numItems(); i++) {
          svQuantiles[index] = sketchAccessor.get(i);
          svCumWeights[index] = weight;
          index++;
        }
      }
    }

    weight = 1; //NOT a mistake! We just copied the highest level; now we need to copy the base buffer
    final int startOfBaseBufferBlock = index;

    // Copy BaseBuffer over, along with weight = 1
    sketchAccessor.setLevel(BB_LVL_IDX);
    for (int i = 0; i < sketchAccessor.numItems(); i++) {
      svQuantiles[index] = sketchAccessor.get(i);
      svCumWeights[index] = weight;
      index++;
    }
    assert index == svQuantiles.length;

    // Must sort the items that came from the base buffer.
    // Don't need to sort the corresponding weights because they are all the same.
    final int numSamples = index;
    Arrays.sort(svQuantiles, startOfBaseBufferBlock, numSamples);
  }

  /**
   * blockyTandemMergeSort() is an implementation of top-down merge sort specialized
   * for the case where the input contains successive equal-length blocks
   * that have already been sorted, so that only the top part of the
   * merge tree remains to be executed. Also, two arrays are sorted in tandem,
   * as discussed below.
   * @param svQuantiles array of quantiles for sorted view
   * @param svCumWts array for the cumulative weights (but not yet cumulative) for sorted view
   * @param arrLen length of quantiles array and cumWts array
   * @param blkSize size of internal sorted blocks, equal to k
   */
  //used by this and UtilTest
  static void blockyTandemMergeSort(final double[] svQuantiles, final long[] svCumWts, final int arrLen,
      final int blkSize) {
    assert blkSize >= 1;
    if (arrLen <= blkSize) { return; }
    int numblks = arrLen / blkSize;
    if ((numblks * blkSize) < arrLen) { numblks += 1; }
    assert ((numblks * blkSize) >= arrLen);

    // duplication of the input arrays is preparation for the "ping-pong" copy reduction strategy.
    final double[] qSrc = Arrays.copyOf(svQuantiles, arrLen);
    final long[] cwSrc   = Arrays.copyOf(svCumWts, arrLen);

    blockyTandemMergeSortRecursion(qSrc, cwSrc,
                                   svQuantiles, svCumWts,
                                   0, numblks,
                                   blkSize, arrLen);
  }

  /**
   *  blockyTandemMergeSortRecursion() is called by blockyTandemMergeSort().
   *  In addition to performing the algorithm's top down recursion,
   *  it manages the buffer swapping that eliminates most copying.
   *  It also maps the input's pre-sorted blocks into the subarrays
   *  that are processed by tandemMerge().
   * @param qSrc source array of quantiles
   * @param cwSrc source weights array
   * @param qDst destination quantiles array
   * @param cwDst destination weights array
   * @param grpStart group start, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param grpLen group length, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param blkSize block size
   * @param arrLim array limit
   */
  private static void blockyTandemMergeSortRecursion(final double[] qSrc, final long[] cwSrc,
      final double[] qDst, final long[] cwDst, final int grpStart, final int grpLen,
      /* indices of blocks */ final int blkSize, final int arrLim) {
    // Important note: grpStart and grpLen do NOT refer to positions in the underlying array.
    // Instead, they refer to the pre-sorted blocks, such as block 0, block 1, etc.

    assert (grpLen > 0);
    if (grpLen == 1) { return; }
    final int grpLen1 = grpLen / 2;
    final int grpLen2 = grpLen - grpLen1;
    assert (grpLen1 >= 1);
    assert (grpLen2 >= grpLen1);

    final int grpStart1 = grpStart;
    final int grpStart2 = grpStart + grpLen1;

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(qDst, cwDst,
                           qSrc, cwSrc,
                           grpStart1, grpLen1, blkSize, arrLim);

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(qDst, cwDst,
                           qSrc, cwSrc,
                           grpStart2, grpLen2, blkSize, arrLim);

    // here we convert indices of blocks into positions in the underlying array.
    final int arrStart1 = grpStart1 * blkSize;
    final int arrStart2 = grpStart2 * blkSize;
    final int arrLen1   = grpLen1   * blkSize;
    int arrLen2         = grpLen2   * blkSize;

    // special case for the final block which might be shorter than blkSize.
    if ((arrStart2 + arrLen2) > arrLim) { arrLen2 = arrLim - arrStart2; }

    tandemMerge(qSrc, cwSrc,
                arrStart1, arrLen1,
                arrStart2, arrLen2,
                qDst, cwDst,
                arrStart1); // which will be arrStart3
  }

  /**
   *  Performs two merges in tandem. One of them provides the sort keys
   *  while the other one passively undergoes the same data motion.
   * @param qSrc quantiles source
   * @param cwSrc cumulative weights source
   * @param arrStart1 Array 1 start offset
   * @param arrLen1 Array 1 length
   * @param arrStart2 Array 2 start offset
   * @param arrLen2 Array 2 length
   * @param qDst quantiles destination
   * @param cwDst cumulative weights destination
   * @param arrStart3 Array 3 start offset
   */
  private static void tandemMerge(final double[] qSrc, final long[] cwSrc,
                                  final int arrStart1, final int arrLen1,
                                  final int arrStart2, final int arrLen2,
                                  final double[] qDst, final long[] cwDst,
                                  final int arrStart3) {
    final int arrStop1 = arrStart1 + arrLen1;
    final int arrStop2 = arrStart2 + arrLen2;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while ((i1 < arrStop1) && (i2 < arrStop2)) {
      if (qSrc[i2] < qSrc[i1]) {
        qDst[i3] = qSrc[i2];
        cwDst[i3] = cwSrc[i2];
        i2++;
      } else {
        qDst[i3] = qSrc[i1];
        cwDst[i3] = cwSrc[i1];
        i1++;
      }
      i3++;
    }

    if (i1 < arrStop1) {
      arraycopy(qSrc, i1, qDst, i3, arrStop1 - i1);
      arraycopy(cwSrc, i1, cwDst, i3, arrStop1 - i1);
    } else {
      assert i2 < arrStop2;
      arraycopy(qSrc, i2, qDst, i3, arrStop2 - i2);
      arraycopy(cwSrc, i2, cwDst, i3, arrStop2 - i2);
    }
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
