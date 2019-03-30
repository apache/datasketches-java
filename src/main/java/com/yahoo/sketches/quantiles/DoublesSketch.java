/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.quantiles.Util.checkIsCompactMemory;
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.util.Random;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.QuantilesHelper;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.kll.KllFloatsSketch;


/**
 * This is a stochastic streaming sketch that enables near-real time analysis of the
 * approximate distribution of real values from a very large stream in a single pass.
 * The analysis is obtained using a getQuantiles(*) function or its inverse functions the
 * Probability Mass Function from getPMF(*) and the Cumulative Distribution Function from getCDF(*).
 *
 * <p>Consider a large stream of one million values such as packet sizes coming into a network node.
 * The absolute rank of any specific size value is simply its index in the hypothetical sorted
 * array of values.
 * The normalized rank (or fractional rank) is the absolute rank divided by the stream size,
 * in this case one million.
 * The value corresponding to the normalized rank of 0.5 represents the 50th percentile or median
 * value of the distribution, or getQuantile(0.5).  Similarly, the 95th percentile is obtained from
 * getQuantile(0.95). Using the getQuantiles(0.0, 1.0) will return the min and max values seen by
 * the sketch.</p>
 *
 * <p>From the min and max values, for example, 1 and 1000 bytes,
 * you can obtain the PMF from getPMF(100, 500, 900) that will result in an array of
 * 4 fractional values such as {.4, .3, .2, .1}, which means that
 * <ul>
 * <li>40% of the values were &lt; 100,</li>
 * <li>30% of the values were &ge; 100 and &lt; 500,</li>
 * <li>20% of the values were &ge; 500 and &lt; 900, and</li>
 * <li>10% of the values were &ge; 900.</li>
 * </ul>
 * A frequency histogram can be obtained by simply multiplying these fractions by getN(),
 * which is the total count of values received.
 * The getCDF(*) works similarly, but produces the cumulative distribution instead.
 *
 * <p>The accuracy of this sketch is a function of the configured value <i>k</i>, which also affects
 * the overall size of the sketch. Accuracy of this quantile sketch is always with respect to
 * the normalized rank.  A <i>k</i> of 128 produces a normalized, rank error of about 1.7%.
 * For example, the median value returned from getQuantile(0.5) will be between the actual values
 * from the hypothetically sorted array of input values at normalized ranks of 0.483 and 0.517, with
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

 * <p>There is more documentation available on
 * <a href="https://datasketches.github.io">DataSketches.GitHub.io</a>.</p>
 *
 * <p>This is an implementation of the Low Discrepancy Mergeable Quantiles Sketch, using double
 * values, described in section 3.2 of the journal version of the paper "Mergeable Summaries"
 * by Agarwal, Cormode, Huang, Phillips, Wei, and Yi.
 * <a href="http://dblp.org/rec/html/journals/tods/AgarwalCHPWY13"></a></p>
 *
 * <p>This algorithm is independent of the distribution of values, which can be anywhere in the
 * range of the IEEE-754 64-bit doubles.
 *
 * <p>This algorithm intentionally inserts randomness into the sampling process for values that
 * ultimately get retained in the sketch. The results produced by this algorithm are not
 * deterministic. For example, if the same stream is inserted into two different instances of this
 * sketch, the answers obtained from the two sketches may not be be identical.</p>
 *
 * <p>Similarly, there may be directional inconsistencies. For example, the resulting array of
 * values obtained from getQuantiles(fractions[]) input into the reverse directional query
 * getPMF(splitPoints[]) may not result in the original fractional values.</p>
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 * @author Jon Malkin
 */
public abstract class DoublesSketch {
  static final int DOUBLES_SER_VER = 3;
  static final int MAX_PRELONGS = Family.QUANTILES.getMaxPreLongs();
  static final int MIN_K = 2;
  static final int MAX_K = 1 << 15;

  /**
   * Parameter that controls space usage of sketch and accuracy of estimates.
   */
  final int k_;

  /**
   * Setting the seed makes the results of the sketch deterministic if the input values are
   * received in exactly the same order. This is only useful when performing test comparisons,
   * otherwise is not recommended.
   */
  public static Random rand = new Random();

  DoublesSketch(final int k) {
    Util.checkK(k);
    k_ = k;
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
      return CompactDoublesSketch.heapify(srcMem);
    }
    return UpdateDoublesSketch.heapify(srcMem);
  }

  /**
   * Wrap this sketch around the given Memory image of a DoublesSketch, compact or non-compact.
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

  /**
   * This returns an approximation to the value of the data item
   * that would be preceded by the given fraction of a hypothetical sorted
   * version of the input stream so far.
   *
   * <p>We note that this method has a fairly large overhead (microseconds instead of nanoseconds)
   * so it should not be called multiple times to get different quantiles from the same
   * sketch. Instead use getQuantiles(), which pays the overhead only once.
   *
   * <p>If the sketch is empty this returns Double.NaN.
   *
   * @param fraction the specified fractional position in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * If fraction = 0.0, the true minimum value of the stream is returned.
   * If fraction = 1.0, the true maximum value of the stream is returned.
   *
   * @return the approximation to the value at the above fraction
   */
  public double getQuantile(final double fraction) {
    if (isEmpty()) { return Double.NaN; }
    if ((fraction < 0.0) || (fraction > 1.0)) {
      throw new SketchesArgumentException("Fraction cannot be less than zero or greater than 1.0");
    }
    if      (fraction == 0.0) { return getMinValue(); }
    else if (fraction == 1.0) { return getMaxValue(); }
    else {
      final DoublesAuxiliary aux = new DoublesAuxiliary(this);
      return aux.getQuantile(fraction);
    }
  }

  /**
   * Gets the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public double getQuantileUpperBound(final double fraction) {
    return getQuantile(min(1.0, fraction + Util.getNormalizedRankError(k_, false)));
  }

  /**
   * Gets the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public double getQuantileLowerBound(final double fraction) {
    return getQuantile(max(0, fraction - Util.getNormalizedRankError(k_, false)));
  }

  /**
   * This is a more efficient multiple-query version of getQuantile().
   *
   * <p>This returns an array that could have been generated by using getQuantile() with many
   * different fractional ranks, but would be very inefficient.
   * This method incurs the internal set-up overhead once and obtains multiple quantile values in
   * a single query.  It is strongly recommend that this method be used instead of multiple calls
   * to getQuantile().
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param fRanks the given array of fractional (or normalized) ranks in the hypothetical
   * sorted stream of all the input values seen so far.
   * These fRanks must all be in the interval [0.0, 1.0] inclusively.
   *
   * @return array of approximate quantiles of the given fRanks in the same order as in the given
   * fRanks array.
   */
  public double[] getQuantiles(final double[] fRanks) {
    if (isEmpty()) { return null; }
    DoublesAuxiliary aux = null;
    final double[] quantiles = new double[fRanks.length];
    for (int i = 0; i < fRanks.length; i++) {
      final double fRank = fRanks[i];
      if      (fRank == 0.0) { quantiles[i] = getMinValue(); }
      else if (fRank == 1.0) { quantiles[i] = getMaxValue(); }
      else {
        if (aux == null) {
          aux = new DoublesAuxiliary(this);
        }
        quantiles[i] = aux.getQuantile(fRank);
      }
    }
    return quantiles;
  }

  /**
   * This is also a more efficient multiple-query version of getQuantile() and allows the caller to
   * specify the number of evenly spaced fractional ranks.
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param evenlySpaced an integer that specifies the number of evenly spaced fractional ranks.
   * This must be a positive integer greater than 0. A value of 1 will return the min value.
   * A value of 2 will return the min and the max value. A value of 3 will return the min,
   * the median and the max value, etc.
   *
   * @return array of approximations to the given fractions in the same order as given fractions
   * array.
   */
  public double[] getQuantiles(final int evenlySpaced) {
    if (isEmpty()) { return null; }
    return getQuantiles(QuantilesHelper.getEvenlySpacedRanks(evenlySpaced));
  }

  /**
   * Returns an approximation to the normalized (fractional) rank of the given value from 0 to 1
   * inclusive.
   *
   * <p>The resulting approximation has a probabilistic guarantee that be obtained from the
   * getNormalizedRankError(false) function.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @param value to be ranked
   * @return an approximate rank of the given value
   */
  public double getRank(final double value) {
    if (isEmpty()) { return Double.NaN; }
    final DoublesSketchAccessor samples = DoublesSketchAccessor.wrap(this);
    long total = 0;
    int weight = 1;
    samples.setLevel(DoublesSketchAccessor.BB_LVL_IDX);
    for (int i = 0; i < samples.numItems(); i++) {
      if (samples.get(i) < value) {
        total += weight;
      }
    }
    long bitPattern = getBitPattern();
    for (int lvl = 0; bitPattern != 0L; lvl++, bitPattern >>>= 1) {
      weight *= 2;
      if ((bitPattern & 1L) > 0) { // level is not empty
        samples.setLevel(lvl);
        for (int i = 0; i < samples.numItems(); i++) {
          if (samples.get(i) < value) {
            total += weight;
          } else {
            break; // levels are sorted, no point comparing further
          }
        }
      }
    }
    return (double) total / getN();
  }

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of splitPoints (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that be obtained from the
   * getNormalizedRankError(true) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing double values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these splitpoints.
   *
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream values (the mass) that fall into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint, with the exception that the last interval will include maximum value.
   */
  public double[] getPMF(final double[] splitPoints) {
    if (isEmpty()) { return null; }
    return DoublesPmfCdfImpl.getPMFOrCDF(this, splitPoints, false);
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of splitPoint (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that be obtained from the
   * getNormalizedRankError(false) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing double values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these splitpoints.
   *
   * @return an array of m+1 double values, which are a consecutive approximation to the CDF
   * of the input stream given the splitPoints. The value at array position j of the returned
   * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
   * array.
   */
  public double[] getCDF(final double[] splitPoints) {
    if (isEmpty()) { return null; }
    return DoublesPmfCdfImpl.getPMFOrCDF(this, splitPoints, true);
  }

  /**
   * Returns the configured value of K
   * @return the configured value of K
   */
  public int getK() {
    return k_;
  }

  /**
   * Returns the min value of the stream.
   * If the sketch is empty this returns Double.NaN.
   *
   * @return the min value of the stream
   */
  public abstract double getMinValue();

  /**
   * Returns the max value of the stream.
   * If the sketch is empty this returns Double.NaN.
   *
   * @return the max value of the stream
   */
  public abstract double getMaxValue();

  /**
   * Returns the length of the input stream so far.
   * @return the length of the input stream so far
   */
  public abstract long getN();

  /**
   * Get the rank error normalized as a fraction between zero and one.
   * The error of this sketch is specified as a fraction of the normalized rank of the hypothetical
   * sorted stream of items presented to the sketch.
   *
   * <p>Suppose the sketch is presented with N values. The raw rank (0 to N-1) of an item
   * would be its index position in the sorted version of the input stream. If we divide the
   * raw rank by N, it becomes the normalized rank, which is between 0 and 1.0.
   *
   * <p>For example, choosing a K of 256 yields a normalized rank error of less than 1%.
   * The upper bound on the median value obtained by getQuantile(0.5) would be the value in the
   * hypothetical ordered stream of values at the normalized rank of 0.51.
   * The lower bound would be the value in the hypothetical ordered stream of values at the
   * normalized rank of 0.49.
   *
   * <p>The error of this sketch cannot be translated into an error (relative or absolute) of the
   * returned quantile values.
   *
   * @return the rank error normalized as a fraction between zero and one.
   * @deprecated replaced by {@link #getNormalizedRankError(boolean)}
   */
  @Deprecated
  public double getNormalizedRankError() {
    return Util.getNormalizedRankError(getK(), true);
  }

  /**
   * Gets the approximate rank error of this sketch normalized as a fraction between zero and one.
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, returns the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   */
  public double getNormalizedRankError(final boolean pmf) {
    return Util.getNormalizedRankError(k_, pmf);
  }

  /**
   * Static method version of {@link #getNormalizedRankError()}
   * @param k the configuration parameter of a DoublesSketch
   * @return the rank error normalized as a fraction between zero and one.
   * @deprecated replaced by {@link #getNormalizedRankError(int, boolean)}
   */
  @Deprecated
  public static double getNormalizedRankError(final int k) {
    return Util.getNormalizedRankError(k, true);
  }

  /**
   * Gets the normalized rank error given k and pmf.
   * Static method version of the {@link #getNormalizedRankError(boolean)}.
   * @param k the configuation parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @see KllFloatsSketch
   */
  public static double getNormalizedRankError(final int k, final boolean pmf) {
    return Util.getNormalizedRankError(k, pmf);
  }

  /**
   * Gets the approximate value of <em>k</em> to use given epsilon, the normalized rank error.
   * @param epsilon the normalized rank error between zero and one.
   * @param pmf if true, this function returns the value of <em>k</em> assuming the input epsilon
   * is the desired "double-sided" epsilon for the getPMF() function. Otherwise, this function
   * returns the value of <em>k</em> assuming the input epsilon is the desired "single-sided"
   * epsilon for all the other queries.
   * @return the value of <i>k</i> given a value of epsilon.
   * @see KllFloatsSketch
   */
  public static int getKFromEpsilon(final double epsilon, final boolean pmf) {
    return Util.getKFromEpsilon(epsilon, pmf);
  }

  /**
   * Returns true if this sketch is empty
   * @return true if this sketch is empty
   */
  public boolean isEmpty() {
    return getN() == 0;
  }

  /**
   * Returns true if this sketch is direct
   * @return true if this sketch is direct
   */
  public abstract boolean isDirect();

  /**
   * Returns true if this sketch is in estimation mode.
   * @return true if this sketch is in estimation mode.
   */
  public boolean isEstimationMode() {
    return getN() >= (2L * k_);
  }

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

  /**
   * Serialize this sketch to a byte array. An UpdateDoublesSketch will be serialized in
   * an unordered, non-compact form; a CompactDoublesSketch will be serialized in ordered,
   * compact form. A DirectUpdateDoublesSketch can only wrap a non-compact array, and a
   * DirectCompactDoublesSketch can only wrap a compact array.
   *
   * @return byte array of this sketch
   */
  public byte[] toByteArray() {
    if (isCompact()) {
      return toByteArray(true);
    }
    return toByteArray(false);
  }

  /**
   * Serialize this sketch in a byte array form.
   * @param compact if true the sketch will be serialized in compact form.
   *                DirectCompactDoublesSketch can wrap() only a compact byte array;
   *                DirectUpdateDoublesSketch can wrap() only a non-compact byte array.
   * @return this sketch in a byte array form.
   */
  public byte[] toByteArray(final boolean compact) {
    return DoublesByteArrayImpl.toByteArray(this, compact, compact);
  }

  /**
   * Returns summary information about this sketch.
   */
  @Override
  public String toString() {
    return toString(true, false);
  }

  /**
   * Returns summary information about this sketch. Used for debugging.
   * @param sketchSummary if true includes sketch summary
   * @param dataDetail if true includes data detail
   * @return summary information about the sketch.
   */
  public String toString(final boolean sketchSummary, final boolean dataDetail) {
    return DoublesUtil.toString(sketchSummary, dataDetail, this);
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of a DoublesSketch.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of a DoublesSketch.
   */
  public static String toString(final byte[] byteArr) {
    return PreambleUtil.toString(byteArr, true);
  }

  /**
   * Returns a human readable string of the preamble of a Memory image of a DoublesSketch.
   * @param mem the given Memory
   * @return a human readable string of the preamble of a Memory image of a DoublesSketch.
   */
  public static String toString(final Memory mem) {
    return PreambleUtil.toString(mem, true);
  }

  /**
   * From an source sketch, create a new sketch that must have a smaller value of K.
   * The original sketch is not modified.
   *
   * @param srcSketch the sourcing sketch
   * @param smallerK the new sketch's value of K that must be smaller than this value of K.
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

  /**
   * Computes the number of retained items (samples) in the sketch
   * @return the number of retained items (samples) in the sketch
   */
  public int getRetainedItems() {
    return Util.computeRetainedItems(getK(), getN());
  }

  /**
   * Returns the number of bytes this sketch would require to store in compact form, which is not
   * updatable.
   * @return the number of bytes this sketch would require to store in compact form.
   */
  public int getCompactStorageBytes() {
    return getCompactStorageBytes(getK(), getN());
  }

  /**
   * Returns the number of bytes a DoublesSketch would require to store in compact form
   * given the values of <i>k</i> and <i>n</i>. The compact form is not updatable.
   * @param k the size configuration parameter for the sketch
   * @param n the number of items input into the sketch
   * @return the number of bytes required to store this sketch in compact form.
   */
  public static int getCompactStorageBytes(final int k, final long n) {
    if (n == 0) { return 8; }
    final int metaPreLongs = DoublesSketch.MAX_PRELONGS + 2; //plus min, max
    return ((metaPreLongs + Util.computeRetainedItems(k, n)) << 3);
  }


  /**
   * Returns the number of bytes this sketch would require to store in native form: compact for
   * a CompactDoublesSketch, non-compact for an UpdateDoublesSketch.
   * @return the number of bytes this sketch would require to store in compact form.
   */
  public int getStorageBytes() {
    if (isCompact()) { return getCompactStorageBytes(); }
    return getUpdatableStorageBytes();
  }

  /**
   * Returns the number of bytes this sketch would require to store in updatable form.
   * This uses roughly 2X the storage of the compact form.
   * @return the number of bytes this sketch would require to store in updatable form.
   */
  public int getUpdatableStorageBytes() {
    return getUpdatableStorageBytes(getK(), getN());
  }

  /**
   * Returns the number of bytes a sketch would require to store in updatable form.
   * This uses roughly 2X the storage of the compact form
   * given the values of <i>k</i> and <i>n</i>.
   * @param k the size configuration parameter for the sketch
   * @param n the number of items input into the sketch
   * @return the number of bytes this sketch would require to store in updatable form.
   */
  public static int getUpdatableStorageBytes(final int k, final long n) {
    if (n == 0) { return 8; }
    final int metaPre = DoublesSketch.MAX_PRELONGS + 2; //plus min, max
    final int totLevels = Util.computeNumLevelsNeeded(k, n);
    if (n <= k) {
      final int ceil = Math.max(ceilingPowerOf2((int)n), DoublesSketch.MIN_K * 2);
      return (metaPre + ceil) << 3;
    }
    return (metaPre + ((2 + totLevels) * k)) << 3;
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
    if (isDirect() && (isCompact() == compact)) {
      final Memory srcMem = getMemory();
      srcMem.copyTo(0, dstMem, 0, getStorageBytes());
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

  public DoublesSketchIterator iterator() {
    return new DoublesSketchIterator(this, getBitPattern());
  }

  //Restricted

  /*
   * DoublesMergeImpl.downSamplingMergeInto requires the target sketch to implement update(), so
   * we ensure that the target is an UpdateSketch. The public API, on the other hand, just
   * specifies a DoublesSketch. This lets us be more specific about the type without changing the
   * public API.
   */
  UpdateDoublesSketch downSampleInternal(final DoublesSketch srcSketch, final int smallerK,
                                         final WritableMemory dstMem) {
    final UpdateDoublesSketch newSketch = (dstMem == null)
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
   * Returns the item capacity for the combined base buffer
   * @return the item capacity for the combined base buffer
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
}
