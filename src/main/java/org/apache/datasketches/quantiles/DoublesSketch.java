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
import static org.apache.datasketches.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.Util.ceilingIntPowerOf2;
import static org.apache.datasketches.quantiles.Util.checkIsCompactMemory;

import java.util.Random;

import org.apache.datasketches.Family;
import org.apache.datasketches.QuantileSearchCriteria;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

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
 * <a href="https://datasketches.apache.org">datasketches.apache.org</a>.</p>
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
   * Setting the seed makes the results of the sketch deterministic if the input values are
   * received in exactly the same order. This is only useful when performing test comparisons,
   * otherwise is not recommended.
   */
  static Random rand = new Random();

  /**
   * Parameter that controls space usage of sketch and accuracy of estimates.
   */
  final int k_;

  DoublesSketchSortedView classicQdsSV = null;

  DoublesSketch(final int k) {
    Util.checkK(k);
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
   * Same as {@link #getCDF(double[], QuantileSearchCriteria) getCDF(double[] splitPoints, false)}
   * @param splitPoints splitPoints
   * @return CDF
   */
  public double[] getCDF(final double[] splitPoints) {
    return getCDF(splitPoints, EXCLUSIVE);
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of splitPoint (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(false) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing double values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @param searchCrit if true the weight of the given value is included into the rank.
   * Otherwise the rank equals the sum of the weights of all values that are less than the given value
   *
   * @return an array of m+1 double values on the interval [0.0, 1.0),
   * which are a consecutive approximation to the CDF of the input stream given the splitPoints.
   * The value at array position j of the returned CDF array is the sum of the returned values
   * in positions 0 through j of the returned PMF array.
   */
  public double[] getCDF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    return classicQdsSV.getPmfOrCdf(splitPoints, true, searchCrit);
  }

  /**
   * Same as {@link #getPMF(double[], QuantileSearchCriteria) getPMF(double[] splitPoints, false)}
   * @param splitPoints splitPoints
   * @return PMF
   */
  public double[] getPMF(final double[] splitPoints) {
    return getPMF(splitPoints, EXCLUSIVE);
  }

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of splitPoints (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(true) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing double values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @param searchCrit  if INCLUSIVE, each interval within the distribution will include its top value and exclude its
   * bottom value. Otherwise, it will be the reverse.  The only exception is that the top interval will always include
   * the top value retained by the sketch.
   *
   * @return an array of m+1 doubles on the interval [0.0, 1.0),
   * each of which is an approximation to the fraction, or mass, of the total input stream values
   * that fall into that interval.
   */
  public double[] getPMF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    return classicQdsSV.getPmfOrCdf(splitPoints, false, searchCrit);
  }

  /**
   * Same as {@link #getQuantile(double, QuantileSearchCriteria) getQuantile(rank, EXCLUSIVE)}
   * @param rank  the given normalized rank, a value in the interval [0.0,1.0].
   * @return quantile
   * @see org.apache.datasketches.QuantileSearchCriteria QuantileSearchCriteria
   */
  public double getQuantile(final double rank) {
    return getQuantile(rank, EXCLUSIVE);
  }

  /**
   * Returns the quantile associated with the given rank.
   *
   * <p>If the sketch is empty this returns NaN.
   *
   * @param rank the given normalized rank, a value in the interval [0.0,1.0].
   * @param searchCrit is INCLUSIVE, the given rank includes all values &le; the value directly
   * corresponding to the given rank.
   * @return the quantile associated with the given rank.
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return Double.NaN; }
    refreshSortedView();
    return classicQdsSV.getQuantile(rank, searchCrit);
  }

  /**
   * Same as {@link #getQuantiles(double[], QuantileSearchCriteria) getQuantiles(ranks, EXCLUSIVE)}
   * @param ranks normalied ranks on the interval [0.0, 1.0].
   * @return quantiles
   * @see org.apache.datasketches.QuantileSearchCriteria QuantileSearchCriteria
   */
  public double[] getQuantiles(final double[] ranks) {
    return getQuantiles(ranks, EXCLUSIVE);
  }

  /**
   * Returns an array of quantiles from the given array of normalized ranks.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param ranks the given array of normalized ranks, each of which must be in the interval [0.0,1.0].
   * @param searchCrit if INCLUSIVE, the given ranks include all values &le; the value directly corresponding to each rank.
   * @return array of quantiles
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return null; }
    refreshSortedView();
    final int len = ranks.length;
    final double[] quantiles = new double[len];
    for (int i = 0; i < len; i++) {
      quantiles[i] = classicQdsSV.getQuantile(ranks[i], searchCrit);
    }
    return quantiles;
  }

  /**
   * Same as {@link #getQuantiles(int, QuantileSearchCriteria) getQuantiles(numEvenlySpaced, EXCLUSIVE)}
   * @param numEvenlySpaced number of evenly spaced normalied ranks
   * @return array of quantiles.
   * @see org.apache.datasketches.QuantileSearchCriteria QuantileSearchCriteria
   */
  public double[] getQuantiles(final int numEvenlySpaced) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.Util.evenlySpaced(0.0, 1.0, numEvenlySpaced), EXCLUSIVE);
  }

  /**
   * This is a version of getQuantiles() and allows the caller to
   * specify the number of evenly spaced normalized ranks.
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param numEvenlySpaced an integer that specifies the number of evenly spaced normalized ranks.
   * This must be a positive integer greater than 0. A value of 1 will return the min value.
   * A value of 2 will return the min and the max value. A value of 3 will return the min,
   * the median and the max value, etc.
   *
   * @param searchCrit if INCLUSIVE, the given ranks include all values &le; the value directly corresponding to each rank.
   * @return array of quantiles.
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double[] getQuantiles(final int numEvenlySpaced, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.Util.evenlySpaced(0.0, 1.0, numEvenlySpaced), searchCrit);
  }

  /**
   * Gets the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param rank the given normalized rank
   * @return the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public double getQuantileLowerBound(final double rank) {
    return getQuantile(max(0, rank - Util.getNormalizedRankError(k_, false)));
  }

  /**
   * Gets the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param rank the given normalized rank
   * @return the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public double getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + Util.getNormalizedRankError(k_, false)));
  }

  /**
   * Same as {@link #getRank(double, QuantileSearchCriteria) getRank(value, EXCLUSIVE)}
   * @param value value to be ranked
   * @return normalized rank
   */
  public double getRank(final double value) {
    return getRank(value, EXCLUSIVE);
  }

  /**
   * Returns a normalized rank given a quantile value.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @param value to be ranked
   * @param searchCrit if INCLUSIVE the given quantile value is included into the rank.
   * @return an approximate rank of the given value
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double getRank(final double value, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return Double.NaN; }
    refreshSortedView();
    return classicQdsSV.getRank(value, searchCrit);
  }

  /**
   * Same as {@link #getRanks(double[], QuantileSearchCriteria) getRanks(values, EXCLUSIVE)}
   * @param values array of values to be ranked.
   * @return the array of normalized ranks.
   */
  public double[] getRanks(final double[] values) {
    return getRanks(values, EXCLUSIVE);
  }

  /**
   * Returns an array of normalized ranks corresponding to the given array of quantile values and the given
   * search criterion.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param values the given quantile values from which to obtain their corresponding ranks.
   * @param searchCrit if INCLUSIVE, the given values include the rank directly corresponding to each value.
   * @return an array of normalized ranks corresponding to the given array of quantile values.
   * @see
   * <a href="https://datasketches.apache.org/api/java/snapshot/apidocs/org/apache/datasketches/kll/package-summary.html">
   * KLL package summary</a>
   * @see org.apache.datasketches.QuantileSearchCriteria
   */
  public double[] getRanks(final double[] values, final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    final int len = values.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = classicQdsSV.getRank(values[i], searchCrit);
    }
    return ranks;
  }

  /**
   * Returns the configured value of K
   * @return the configured value of K
   */
  public int getK() {
    return k_;
  }


  /**
   * Returns the length of the input stream so far.
   * @return the length of the input stream so far
   */
  public abstract long getN();

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
   * Gets the normalized rank error given k and pmf.
   * Static method version of the {@link #getNormalizedRankError(boolean)}.
   * @param k the configuration parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
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
    return getN() >= 2L * k_;
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
    return metaPreLongs + Util.computeRetainedItems(k, n) << 3;
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
      final int ceil = Math.max(ceilingIntPowerOf2((int)n), DoublesSketch.MIN_K * 2);
      return metaPre + ceil << 3;
    }
    return metaPre + (2 + totLevels) * k << 3;
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
    if (isDirect() && isCompact() == compact) {
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

  /**
   * @return the iterator for this class
   */
  public DoublesSketchIterator iterator() {
    return new DoublesSketchIterator(this, getBitPattern());
  }

  /**
   * Sorted view of the sketch.
   * Complexity: linear merge of sorted levels plus sorting of the level 0.
   * @return sorted view object
   */
  public DoublesSketchSortedView getSortedView() {
    return new DoublesSketchSortedView(this);
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
    final UpdateDoublesSketch newSketch = dstMem == null
            ? HeapUpdateDoublesSketch.newInstance(smallerK)
            : DirectUpdateDoublesSketch.newInstance(smallerK, dstMem);
    if (srcSketch.isEmpty()) { return newSketch; }
    DoublesMergeImpl.downSamplingMergeInto(srcSketch, newSketch);
    return newSketch;
  }

private final void refreshSortedView() {
  classicQdsSV = (classicQdsSV == null) ? new DoublesSketchSortedView(this) : classicQdsSV;
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
