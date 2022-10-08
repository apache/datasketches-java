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
import static org.apache.datasketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.quantiles.PreambleUtil.extractFlags;
import static org.apache.datasketches.quantiles.PreambleUtil.extractK;
import static org.apache.datasketches.quantiles.PreambleUtil.extractN;
import static org.apache.datasketches.quantiles.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.quantiles.PreambleUtil.extractSerVer;
import static org.apache.datasketches.quantiles.ClassicUtil.computeBaseBufferItems;
import static org.apache.datasketches.quantiles.ClassicUtil.computeBitPattern;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Random;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.GenericSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesAPI;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;

/**
 * This is an implementation of the Low Discrepancy Mergeable Quantiles Sketch, using generic items,
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
 * <p>The size of an ItemsSketch is very dependent on the size of the generic Items input into the sketch,
 * so there is no comparable size table as there is for the DoublesSketch.
 *
 * @see QuantilesAPI
 *
 * @param <T> The sketch data type
 */
public final class ItemsSketch<T> implements QuantilesAPI {

  final Class<T> clazz;

  private final Comparator<? super T> comparator_;

  final int k_;

  long n_;

  /**
   * The largest item ever seen in the stream.
   */
  T maxItem_;

  /**
   * The smallest item ever seen in the stream.
   */
  T minItem_;

  /**
   * In the initial on-heap version, equals combinedBuffer_.length.
   * May differ in later versions that grow space more aggressively.
   * Also, in the off-heap version, combinedBuffer_ won't even be a java array,
   * so it won't know its own length.
   */
  int combinedBufferItemCapacity_;

  /**
   * Number of items currently in base buffer.
   *
   * <p>Count = N % (2*K)
   */
  int baseBufferCount_;

  /**
   * Active levels expressed as a bit pattern.
   *
   * <p>Pattern = N / (2 * K)
   */
  long bitPattern_;

  /**
   * This single array contains the base buffer plus all levels some of which may not be used.
   * A level is of size K and is either full and sorted, or not used. A "not used" buffer may have
   * garbage. Whether a level buffer used or not is indicated by the bitPattern_.
   * The base buffer has length 2*K but might not be full and isn't necessarily sorted.
   * The base buffer precedes the level buffers.
   *
   * <p>The levels arrays require quite a bit of explanation, which we defer until later.
   */
  Object[] combinedBuffer_;

  ItemsSketchSortedView<T> classicQisSV = null;

  /**
   * Setting the seed makes the results of the sketch deterministic if the input items are
   * received in exactly the same order. This is only useful when performing test comparisons,
   * otherwise is not recommended.
   */
  public static final Random rand = new Random();

  private ItemsSketch(
      final int k,
      final Class<T> clazz,
      final Comparator<? super T> comparator) {
    Objects.requireNonNull(clazz, "Class<T> must not be null.");
    Objects.requireNonNull(comparator, "Comparator must not be null.");
    ClassicUtil.checkK(k);
    k_ = k;
    this.clazz = clazz;
    comparator_ = comparator;
  }

  /**
   * Obtains a new instance of an ItemsSketch using the DEFAULT_K.
   * @param <T> The sketch data type
   * @param clazz the given class of T
   * @param comparator to compare items
   * @return a GenericQuantileSketch
   */
  public static <T> ItemsSketch<T> getInstance(
      final Class<T> clazz,
      final Comparator<? super T> comparator) {
    return getInstance(clazz, PreambleUtil.DEFAULT_K, comparator);
  }

  /**
   * Obtains a new instance of an ItemsSketch.
   * @param clazz the given class of T
   * @param k Parameter that controls space usage of sketch and accuracy of estimates.
   * Must be greater than 2 and less than 65536 and a power of 2.
   * @param comparator to compare items
   * @param <T> The sketch data type
   * @return a GenericQuantileSketch
   */
  public static <T> ItemsSketch<T> getInstance(
      final Class<T> clazz,
      final int k,
      final Comparator<? super T> comparator) {
    final ItemsSketch<T> qs = new ItemsSketch<>(k, clazz, comparator);
    final int bufAlloc = 2 * Math.min(DoublesSketch.MIN_K, k); //the min is important
    qs.n_ = 0;
    qs.combinedBufferItemCapacity_ = bufAlloc;
    qs.combinedBuffer_ = new Object[bufAlloc];
    qs.baseBufferCount_ = 0;
    qs.bitPattern_ = 0;
    qs.minItem_ = null;
    qs.maxItem_ = null;
    return qs;
  }

  /**
   * Heapifies the given srcMem, which must be a Memory image of a ItemsSketch
   * @param clazz the given class of T
   * @param srcMem a Memory image of a sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param comparator to compare items
   * @param serDe an instance of ArrayOfItemsSerDe
   * @param <T> The sketch data type
   * @return a ItemsSketch on the Java heap.
   */
  public static <T> ItemsSketch<T> getInstance(
      final Class<T> clazz,
      final Memory srcMem,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    final long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) {
      throw new SketchesArgumentException("Memory too small: " + memCapBytes);
    }

    final int preambleLongs = extractPreLongs(srcMem);
    final int serVer = extractSerVer(srcMem);
    final int familyID = extractFamilyID(srcMem);
    final int flags = extractFlags(srcMem);
    final int k = extractK(srcMem);

    ItemsUtil.checkItemsSerVer(serVer);

    if (serVer == 3 && (flags & COMPACT_FLAG_MASK) == 0) {
      throw new SketchesArgumentException("Non-compact Memory images are not supported.");
    }

    final boolean empty = ClassicUtil.checkPreLongsFlagsCap(preambleLongs, flags, memCapBytes);
    ClassicUtil.checkFamilyID(familyID);

    final ItemsSketch<T> sk = getInstance(clazz, k, comparator); //checks k
    if (empty) { return sk; }

    //Not empty, must have valid preamble + min, max
    final long n = extractN(srcMem);

    //can't check memory capacity here, not enough information
    final int extra = 2; //for min, max
    final int numMemItems = ClassicUtil.computeRetainedItems(k, n) + extra;

    //set class members
    sk.n_ = n;
    sk.combinedBufferItemCapacity_ = ClassicUtil.computeCombinedBufferItemCapacity(k, n);
    sk.baseBufferCount_ = computeBaseBufferItems(k, n);
    sk.bitPattern_ = computeBitPattern(k, n);
    sk.combinedBuffer_ = new Object[sk.combinedBufferItemCapacity_];

    final int srcMemItemsOffsetBytes = preambleLongs * Long.BYTES;
    final Memory mReg = srcMem.region(srcMemItemsOffsetBytes,
        srcMem.getCapacity() - srcMemItemsOffsetBytes);
    final T[] itemsArray = serDe.deserializeFromMemory(mReg, numMemItems);
    sk.itemsArrayToCombinedBuffer(itemsArray);
    return sk;
  }

  /**
   * Returns a copy of the given sketch
   * @param <T> The sketch data type
   * @param sketch the given sketch
   * @return a copy of the given sketch
   */
  static <T> ItemsSketch<T> copy(final ItemsSketch<T> sketch) {
    final ItemsSketch<T> qsCopy = ItemsSketch.getInstance(sketch.clazz, sketch.k_, sketch.comparator_);
    qsCopy.n_ = sketch.n_;
    qsCopy.minItem_ = sketch.getMinItem();
    qsCopy.maxItem_ = sketch.getMaxItem();
    qsCopy.combinedBufferItemCapacity_ = sketch.getCombinedBufferAllocatedCount();
    qsCopy.baseBufferCount_ = sketch.getBaseBufferCount();
    qsCopy.bitPattern_ = sketch.getBitPattern();
    final Object[] combBuf = sketch.getCombinedBuffer();
    qsCopy.combinedBuffer_ = Arrays.copyOf(combBuf, combBuf.length);
    return qsCopy;
  }

  /**
   * This is equivalent to {@link #getCDF(Object[], QuantileSearchCriteria) getCDF(splitPoints, INCLUSIVE)}
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing items.
   * @return a CDF array of m+1 double ranks (or probabilities) on the interval [0.0, 1.0).
   */
  public double[] getCDF(final T[] splitPoints) {
    return getCDF(splitPoints, INCLUSIVE);
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF) of the input stream
   * as a monotonically increasing array of double ranks (or cumulative probabilities) on the interval [0.0, 1.0],
   * given a set of splitPoints.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(false) function.
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing items
   * (of the same type as the input items)
   * that divide the item input domain into <i>m+1</i> overlapping intervals.
   *
   * <p>The start of each interval is below the lowest item retained by the sketch
   * corresponding to a zero rank or zero probability, and the end of the interval
   * is the rank or cumulative probability corresponding to the split point.</p>
   *
   * <p>The <i>(m+1)th</i> interval represents 100% of the distribution represented by the sketch
   * and consistent with the definition of a cumulative probability distribution, thus the <i>(m+1)th</i>
   * rank or probability in the returned array is always 1.0.</p>
   *
   * <p>If a split point exactly equals a retained item of the sketch and the search criterion is:</p>
   *
   * <ul>
   * <li>INCLUSIVE, the resulting cumulative probability will include that item.</li>
   * <li>EXCLUSIVE, the resulting cumulative probability will not include the weight of that split point.</li>
   * </ul>
   *
   * <p>It is not recommended to include either the minimum or maximum items of the input stream.</p>
   *
   * @param searchCrit the desired search criteria.
   * @return a discrete CDF array of m+1 double ranks (or cumulative probabilities) on the interval [0.0, 1.0).
   */
  public double[] getCDF(
      final T[] splitPoints,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return null; }
    refreshSortedView();
    return classicQisSV.getCDF(splitPoints, searchCrit);
  }

  /**
   * @return the sketch item type
   */
  public Class<T> getSketchType() { return clazz; }

  /**
   * Returns the maximum item of the stream. This is provided for convenience, but may be different from the largest
   * item retained by the sketch algorithm.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @return the maximum item of the stream
   */
  public T getMaxItem() { return maxItem_; }

  /**
   * Returns the minimum quantile of the stream. This is provided for convenience, but is distinct from the smallest
   * quantile retained by the sketch algorithm.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @return the minimum quantile of the stream
   */
  public T getMinItem() { return minItem_; }

  /**
   * Same as {@link #getPMF(Object[], QuantileSearchCriteria) getPMF(splitPoints, INCLUSIVE)}
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing quantiles
   * @return a PDF array of m+1 probability masses as doubles on the interval [0.0, 1.0).
   */
  public double[] getPMF(final T[] splitPoints) {
    return getPMF(splitPoints, INCLUSIVE);
  }

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * as an array of double probability masses on the interval [0.0, 1.0],
   * given a set of splitPoints.
   * The sum of the probability masses in the returned array is always 1.0.
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(true) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing quantiles
   * that divide the real number line into <i>m+1</i> consecutive non-overlapping intervals.
   * It is not necessary to include either the minimum or maximum quantiles
   * of the input stream in these split points.
   *
   * <p>If searchCrit is INCLUSIVE, the definition of an "interval" is
   * exclusive of the left splitPoint and
   * inclusive of the right splitPoint.</p>
   *
   * <p>If searchCrit is EXCLUSIVE, the definition of an "interval" is
   * inclusive of the left splitPoint and
   * exclusive of the right splitPoint.</p>
   *
   *<p>The left "splitPoint" for the lowest interval is the
   * lowest quantile from the input stream retained by the sketch.
   * The right "splitPoint" for the highest interval is the
   * highest quantile from the input stream retained by the sketch.
   * </p>
   *
   * @param searchCrit if INCLUSIVE, the weight of a given splitPoint quantile,
   * if it also exists as retained quantile by the sketch,
   * is included into the interval below.
   * Otherwise, it is included into the interval above.
   *
   * @return a PDF array of m+1 probability masses as doubles on the interval [0.0, 1.0).
   */
  public double[] getPMF(
      final T[] splitPoints,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return null; }
    refreshSortedView();
    return classicQisSV.getPMF(splitPoints, searchCrit);
  }

  /**
   * Same as {@link #getQuantile(double, QuantileSearchCriteria) getQuantile(rank, INCLUSIVE)}
   * @param rank the given normalized rank, a double in the range [0.0, 1.0].
   * @return the approximate quantile given the normalized rank.
   */
  public T getQuantile(final double rank) {
    return getQuantile(rank, INCLUSIVE);
  }

  /**
   * Gets the approximate quantile of the given normalized rank and the given search criterion.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param rank the given normalized rank, a double in the range [0.0, 1.0].
   * @param searchCrit If INCLUSIVE, the given rank includes all quantiles &le;
   * the quantile directly corresponding to the given rank.
   * If EXCLUSIVE, he given rank includes all quantiles &lt;
   * the quantile directly corresponding to the given rank.
   * @return the approximate quantile given the normalized rank.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  public T getQuantile(
      final double rank,
      final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    return classicQisSV.getQuantile(rank, searchCrit);
  }

  /**
   * Same as {@link #getQuantiles(double[], QuantileSearchCriteria) getQuantiles(ranks, INCLUSIVE)}
   * @param ranks the given array of normalized ranks, each of which must be
   * in the interval [0.0,1.0].
   * @return an array of quantiles corresponding to the given array of normalized ranks.
   */
  public T[] getQuantiles(final double[] ranks) {
    return getQuantiles(ranks, INCLUSIVE);
  }

  /**
   * Gets an array of quantiles from the given array of normalized ranks.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param ranks the given array of normalized ranks, each of which must be
   * in the interval [0.0,1.0].
   * @param searchCrit if INCLUSIVE, the given ranks include all quantiles &le;
   * the quantile directly corresponding to each rank.
   * @return an array of quantiles corresponding to the given array of normalized ranks.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  @SuppressWarnings("unchecked")
  public T[] getQuantiles(
      final double[] ranks,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return null; }
    refreshSortedView();
    final int len = ranks.length;
    final T[] quantiles = (T[]) Array.newInstance(minItem_.getClass(), len);
    for (int i = 0; i < len; i++) {
      quantiles[i] = classicQisSV.getQuantile(ranks[i], searchCrit);
    }
    return quantiles;
  }

  /**
   * Same as {@link #getQuantiles(int, QuantileSearchCriteria) getQuantiles(numEvenlySpaced, INCLUSIVE)}
   * @param numEvenlySpaced number of evenly spaced normalied ranks
   * @return an array of quantiles that are evenly spaced by their ranks.
   */
  public T[] getQuantiles(final int numEvenlySpaced) {
    return getQuantiles(numEvenlySpaced, INCLUSIVE);
  }

  /**
   * This is a version of getQuantiles() where the caller only specifies the number of of desired evenly spaced,
   * normalized ranks, and returns an array of the corresponding quantiles.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param numEvenlySpaced an integer that specifies the number of evenly spaced normalized ranks.
   * This must be a positive integer greater than 0.
   * <ul><li>Let <i>Smallest</i> and <i>Largest</i> be the smallest and largest quantiles
   * retained by the sketch algorithm, respectively.
   * (This should not to be confused with {@link #getMinItem} and {@link #getMaxItem},
   * which are the smallest and largest quantiles of the stream.)</li>
   * <li>A 1 will return the Smallest quantile.</li>
   * <li>A 2 will return the Smallest and Largest quantiles.</li>
   * <li>A 3 will return the Smallest, the Median, and the Largest quantiles.</li>
   * <li>Etc.</li>
   * </ul>
   *
   * @param searchCrit if INCLUSIVE, the given ranks include all quantiles &le; the quantile directly corresponding to
   * each rank.
   * @return an array of quantiles that are evenly spaced by their ranks.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  public T[] getQuantiles(
      final int numEvenlySpaced,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.quantilescommon.QuantilesUtil.evenlySpaced(0.0, 1.0, numEvenlySpaced),
        searchCrit);
  }

  /**
   * Gets the lower bound of the quantile confidence interval in which the quantile of the
   * given rank exists.
   *
   * <p>Although it is possible to estimate the probablity that the true quantile
   * exists within the quantile confidence interval specified by the upper and lower quantile bounds,
   * it is not possible to guarantee the width of the quantile confidence interval
   * as an additive or multiplicative percent of the true quantile.</p>
   *
   * @param rank the given normalized rank
   * @return the lower bound of the quantile confidence interval in which the quantile of the
   * given rank exists.
   */
  public T getQuantileLowerBound(final double rank) {
    return getQuantile(max(0, rank - ClassicUtil.getNormalizedRankError(k_, false)));
  }

  /**
   * Gets the upper bound of the quantile confidence interval in which the true quantile of the
   * given rank exists.
   *
   * <p>Although it is possible to estimate the probablity that the true quantile
   * exists within the quantile confidence interval specified by the upper and lower quantile bounds,
   * it is not possible to guarantee the width of the quantile interval
   * as an additive or multiplicative percent of the true quantile.</p>
   *
   * @param rank the given normalized rank
   * @return the upper bound of the quantile confidence interval in which the true quantile of the
   * given rank exists.
   */
  public T getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + ClassicUtil.getNormalizedRankError(k_, false)));
  }

  /**
   * Same as {@link #getRank(Object, QuantileSearchCriteria) getRank(quantile, INCLUSIVE)}
   * @param quantile the given quantile
   * @return the normalized rank corresponding to the given quantile
   */
  public double getRank(final T quantile) {
    return getRank(quantile, INCLUSIVE);
  }

  /**
   * Gets the normalized rank corresponding to the given a quantile.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @param quantile the given quantile
   * @param searchCrit if INCLUSIVE the given quantile is included into the rank.
   * @return the normalized rank corresponding to the given quantile
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  public double getRank(
      final T quantile,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return Double.NaN; }
    refreshSortedView();
    return classicQisSV.getRank(quantile, searchCrit);
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.99.
   */
  @Override
  public double getRankLowerBound(final double rank) {
    return max(0.0, rank - ClassicUtil.getNormalizedRankError(k_, false));
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.99.
   */
  @Override
  public double getRankUpperBound(final double rank) {
    return min(1.0, rank + ClassicUtil.getNormalizedRankError(k_, false));
  }

  /**
   * Same as {@link #getRanks(Object[], QuantileSearchCriteria) getRanks(quantiles, INCLUSIVE)}
   * @param quantiles the given array of quantiles
   * @return an array of normalized ranks corresponding to the given array of quantiles.
   */
  public double[] getRanks(final T[] quantiles) {
    return getRanks(quantiles, INCLUSIVE);
  }

  /**
   * Gets an array of normalized ranks corresponding to the given array of quantiles and the given
   * search criterion.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param quantiles the given array of quantiles
   * @param searchCrit if INCLUSIVE, the given quantiles include the rank directly corresponding to each quantile.
   * @return an array of normalized ranks corresponding to the given array of quantiles.
   * @see org.apache.datasketches.quantilescommon.QuantileSearchCriteria
   */
  public double[] getRanks(
      final T[] quantiles,
      final QuantileSearchCriteria searchCrit) {
    if (this.isEmpty()) { return null; }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = classicQisSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }

  /**
   * Gets the iterator for this sketch, which is not sorted.
   * @return the iterator for this sketch
   */
  public QuantilesGenericSketchIterator<T> iterator() {
    return new ItemsSketchIterator<>(this, bitPattern_);
  }

  @Override
  public int getK() {
    return k_;
  }

  @Override
  public long getN() {
    return n_;
  }

  /**
   * Gets the approximate rank error of this sketch normalized as a fraction between zero and one.
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, returns the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   */
  public double getNormalizedRankError(final boolean pmf) {
    return ClassicUtil.getNormalizedRankError(k_, pmf);
  }

  /**
   * Gets the normalized rank error given k and pmf.
   * Static method version of the {@link #getNormalizedRankError(boolean)}.
   * @param k the configuation parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   */
  public static double getNormalizedRankError(
      final int k,
      final boolean pmf) {
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
  public static int getKFromEpsilon(
      final double epsilon,
      final boolean pmf) {
    return ClassicUtil.getKFromEpsilon(epsilon, pmf);
  }

  @Override
  public boolean hasMemory() {
    return false;
  }

  @Override
  public boolean isEmpty() {
   return getN() == 0;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public boolean isEstimationMode() {
    return getN() >= 2L * k_;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  /**
   * {@inheritDoc}
   * <p>The parameter <i>k</i> will not change.</p>
   */
  @Override
  public void reset() {
    n_ = 0;
    combinedBufferItemCapacity_ = 2 * Math.min(DoublesSketch.MIN_K, k_); //the min is important
    combinedBuffer_ = new Object[combinedBufferItemCapacity_];
    baseBufferCount_ = 0;
    bitPattern_ = 0;
    minItem_ = null;
    maxItem_ = null;
    classicQisSV = null;
  }

  /**
   * Serialize this sketch to a byte array form.
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return byte array of this sketch
   */
  public byte[] toByteArray(final ArrayOfItemsSerDe<T> serDe) {
    return toByteArray(false, serDe);
  }

  /**
   * Serialize this sketch to a byte array form.
   * @param ordered if true the base buffer will be ordered (default == false).
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return this sketch in a byte array form.
   */
  public byte[] toByteArray(
      final boolean ordered,
      final ArrayOfItemsSerDe<T> serDe) {
    return ItemsByteArrayImpl.toByteArray(this, ordered, serDe);
  }

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
  public String toString(
      final boolean sketchSummary,
      final boolean dataDetail) {
    return ItemsUtil.toString(sketchSummary, dataDetail, this);
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of an ItemsSketch.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of an ItemsSketch.
   */
  public static String toString(final byte[] byteArr) {
    return PreambleUtil.toString(byteArr, false);
  }

  /**
   * Returns a human readable string of the preamble of a Memory image of an ItemsSketch.
   * @param mem the given Memory
   * @return a human readable string of the preamble of a Memory image of an ItemsSketch.
   */
  public static String toString(final Memory mem) {
    return PreambleUtil.toString(mem, false);
  }

  /**
   * From an existing sketch, this creates a new sketch that can have a smaller K.
   * The original sketch is not modified.
   *
   * @param newK the new K that must be smaller than current K.
   * It is required that this.getK() = newK * 2^(nonnegative integer).
   * @return the new sketch.
   */
  public ItemsSketch<T> downSample(final int newK) {
    final ItemsSketch<T> newSketch = ItemsSketch.getInstance(clazz, newK, comparator_);
    ItemsMergeImpl.downSamplingMergeInto(this, newSketch);
    return newSketch;
  }

  @Override
  public int getNumRetained() {
    return ClassicUtil.computeRetainedItems(getK(), getN());
  }

  /**
   * Puts the current sketch into the given Memory if there is sufficient space.
   * Otherwise, throws an error.
   *
   * @param dstMem the given memory.
   * @param serDe an instance of ArrayOfItemsSerDe
   */
  public void putMemory(
      final WritableMemory dstMem,
      final ArrayOfItemsSerDe<T> serDe) {
    final byte[] byteArr = toByteArray(serDe);
    final long memCap = dstMem.getCapacity();
    if (memCap < byteArr.length) {
      throw new SketchesArgumentException(
          "Destination Memory not large enough: " + memCap + " < " + byteArr.length);
    }
    dstMem.putByteArray(0, byteArr, 0, byteArr.length);
  }

  /**
   * Gets the sorted view of this sketch
   * @return the sorted view of this sketch
   */
  public GenericSortedView<T> getSortedView() {
    return new ItemsSketchSortedView<T>(this);
  }

  /**
   * Updates this sketch with the given quantile
   * @param quantile from a stream of quantiles. Nulls are ignored.
   */
  public void update(final T quantile) {
    // this method only uses the base buffer part of the combined buffer

    if (quantile == null) { return; }
    if (maxItem_ == null || comparator_.compare(quantile, maxItem_) > 0) { maxItem_ = quantile; }
    if (minItem_ == null || comparator_.compare(quantile, minItem_) < 0) { minItem_ = quantile; }

    if (baseBufferCount_ + 1 > combinedBufferItemCapacity_) {
      ItemsSketch.growBaseBuffer(this);
    }
    combinedBuffer_[baseBufferCount_++] = quantile;
    n_++;
    if (baseBufferCount_ == 2 * k_) {
      ItemsUtil.processFullBaseBuffer(this);
    }
    classicQisSV = null;
  }

  // Restricted

  private final void refreshSortedView() {
    classicQisSV = (classicQisSV == null) ? new ItemsSketchSortedView<T>(this) : classicQisSV;
  }

  /**
   * Returns the base buffer count
   * @return the base buffer count
   */
  int getBaseBufferCount() {
    return baseBufferCount_;
  }

  /**
   * Returns the allocated count for the combined base buffer
   * @return the allocated count for the combined base buffer
   */
  int getCombinedBufferAllocatedCount() {
    return combinedBufferItemCapacity_;
  }

  /**
   * Returns the bit pattern for valid log levels
   * @return the bit pattern for valid log levels
   */
  long getBitPattern() {
    return bitPattern_;
  }

  /**
   * Returns the combined buffer reference
   * @return the combined buffer reference
   */
  Object[] getCombinedBuffer() {
    return combinedBuffer_;
  }

  Comparator<? super T> getComparator() {
    return comparator_;
  }

  /**
   * Loads the Combined Buffer, min and max from the given items array.
   * The Combined Buffer is always in non-compact form and must be pre-allocated.
   * @param itemsArray the given items array
   */
  private void itemsArrayToCombinedBuffer(final T[] itemsArray) {
    final int extra = 2; // space for min and max items

    //Load min, max
    minItem_ = itemsArray[0];
    maxItem_ = itemsArray[1];

    //Load base buffer
    System.arraycopy(itemsArray, extra, combinedBuffer_, 0, baseBufferCount_);

    //Load levels
    long bits = bitPattern_;
    if (bits > 0) {
      int index = extra + baseBufferCount_;
      for (int level = 0; bits != 0L; level++, bits >>>= 1) {
        if ((bits & 1L) > 0L) {
          System.arraycopy(itemsArray, index, combinedBuffer_, (2 + level) * k_, k_);
          index += k_;
        }
      }
    }
  }

  private static <T> void growBaseBuffer(final ItemsSketch<T> sketch) {
    final Object[] baseBuffer = sketch.getCombinedBuffer();
    final int oldSize = sketch.getCombinedBufferAllocatedCount();
    final int k = sketch.getK();
    assert oldSize < 2 * k;
    final int newSize = Math.max(Math.min(2 * k, 2 * oldSize), 1);
    sketch.combinedBufferItemCapacity_ = newSize;
    sketch.combinedBuffer_ = Arrays.copyOf(baseBuffer, newSize);
  }

}
