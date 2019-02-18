/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.kll;

import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.Math.round;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ByteArrayUtil;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.QuantilesHelper;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * Implementation of a very compact quantiles sketch with lazy compaction scheme
 * and nearly optimal accuracy per retained item.
 * See <a href="https://arxiv.org/abs/1603.05346v2">Optimal Quantile Approximation in Streams</a>.
 *
 * <p>This is a stochastic streaming sketch that enables near-real time analysis of the
 * approximate distribution of values from a very large stream in a single pass, requiring only
 * that the values are comparable.
 * The analysis is obtained using <i>getQuantile()</i> or <i>getQuantiles()</i> functions or the
 * inverse functions getRank(), getPMF() (Probability Mass Function), and getCDF()
 * (Cumulative Distribution Function).
 *
 * <p>Given an input stream of <i>N</i> numeric values, the <i>absolute rank</i> of any specific
 * value is defined as its index <i>(0 to N-1)</i> in the hypothetical sorted stream of all
 * <i>N</i> input values.
 *
 * <p>The <i>normalized rank</i> (<i>rank</i>) of any specific value is defined as its
 * <i>absolute rank</i> divided by <i>N</i>.
 * Thus, the <i>normalized rank</i> is a value between zero and one.
 * In the documentation and Javadocs for this sketch <i>absolute rank</i> is never used so any
 * reference to just <i>rank</i> should be interpreted to mean <i>normalized rank</i>.
 *
 * <p>This sketch is configured with a parameter <i>k</i>, which affects the size of the sketch
 * and its estimation error.
 *
 * <p>The estimation error is commonly called <i>epsilon</i> (or <i>eps</i>) and is a fraction
 * between zero and one. Larger values of <i>k</i> result in smaller values of epsilon.
 * Epsilon is always with respect to the rank and cannot be applied to the
 * corresponding values.
 *
 * <p>The relationship between the normalized rank and the corresponding values can be viewed
 * as a two dimensional monotonic plot with the normalized rank on one axis and the
 * corresponding values on the other axis. If the y-axis is specified as the value-axis and
 * the x-axis as the normalized rank, then <i>y = getQuantile(x)</i> is a monotonically
 * increasing function.
 *
 * <p>The functions <i>getQuantile(rank)</i> and getQuantiles(...) translate ranks into
 * corresponding values. The functions <i>getRank(value),
 * getCDF(...) (Cumulative Distribution Function), and getPMF(...)
 * (Probability Mass Function)</i> perform the opposite operation and translate values into ranks.
 *
 * <p>The <i>getPMF(...)</i> function has about 13 to 47% worse rank error (depending
 * on <i>k</i>) than the other queries because the mass of each "bin" of the PMF has
 * "double-sided" error from the upper and lower edges of the bin as a result of a subtraction,
 * as the errors from the two edges can sometimes add.
 *
 * <p>The default <i>k</i> of 200 yields a "single-sided" epsilon of about 1.33% and a
 * "double-sided" (PMF) epsilon of about 1.65%.
 *
 * <p>A <i>getQuantile(rank)</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>v = getQuantile(r)</i> where <i>r</i> is the rank between zero and one.</li>
 * <li>The value <i>v</i> will be a value from the input stream.</li>
 * <li>Let <i>trueRank</i> be the true rank of <i>v</i> derived from the hypothetical sorted
 * stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = getNormalizedRankError(false)</i>.</li>
 * <li>Then <i>r - eps &le; trueRank &le; r + eps</i> with a confidence of 99%. Note that the
 * error is on the rank, not the value.</li>
 * </ul>
 *
 * <p>A <i>getRank(value)</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>r = getRank(v)</i> where <i>v</i> is a value between the min and max values of
 * the input stream.</li>
 * <li>Let <i>trueRank</i> be the true rank of <i>v</i> derived from the hypothetical sorted
 * stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = getNormalizedRankError(false)</i>.</li>
 * <li>Then <i>r - eps &le; trueRank &le; r + eps</i> with a confidence of 99%.</li>
 * </ul>
 *
 * <p>A <i>getPMF()</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>{r1, r2, ..., r(m+1)} = getPMF(v1, v2, ..., vm)</i> where <i>v1, v2</i> are values
 * between the min and max values of the input stream.
 * <li>Let <i>mass<sub>i</sub> = estimated mass between v<sub>i</sub> and v<sub>i+1</sub></i>.</li>
 * <li>Let <i>trueMass</i> be the true mass between the values of <i>v<sub>i</sub>,
 * v<sub>i+1</sub></i> derived from the hypothetical sorted stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = getNormalizedRankError(true)</i>.</li>
 * <li>then <i>mass - eps &le; trueMass &le; mass + eps</i> with a confidence of 99%.</li>
 * <li>r(m+1) includes the mass of all points larger than vm.</li>
 * </ul>
 *
 * <p>A <i>getCDF(...)</i> query has the following guarantees;
 * <ul>
 * <li>Let <i>{r1, r2, ..., r(m+1)} = getCDF(v1, v2, ..., vm)</i> where <i>v1, v2</i> are values
 * between the min and max values of the input stream.
 * <li>Let <i>mass<sub>i</sub> = r<sub>i+1</sub> - r<sub>i</sub></i>.</li>
 * <li>Let <i>trueMass</i> be the true mass between the true ranks of <i>v<sub>i</sub>,
 * v<sub>i+1</sub></i> derived from the hypothetical sorted stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = getNormalizedRankError(true)</i>.</li>
 * <li>then <i>mass - eps &le; trueMass &le; mass + eps</i> with a confidence of 99%.</li>
 * <li>1 - r(m+1) includes the mass of all points larger than vm.</li>
 * </ul>
 *
 * <p>From the above, it might seem like we could make some estimates to bound the
 * <em>value</em> returned from a call to <em>getQuantile()</em>. The sketch, however, does not
 * let us derive error bounds or confidences around values. Because errors are independent, we
 * can approximately bracket a value as shown below, but there are no error estimates available.
 * Additionally, the interval may be quite large for certain distributions.
 * <ul>
 * <li>Let <i>v = getQuantile(r)</i>, the estimated quantile value of rank <i>r</i>.</li>
 * <li>Let <i>eps = getNormalizedRankError(false)</i>.</li>
 * <li>Let <i>v<sub>lo</sub></i> = estimated quantile value of rank <i>(r - eps)</i>.</li>
 * <li>Let <i>v<sub>hi</sub></i> = estimated quantile value of rank <i>(r + eps)</i>.</li>
 * <li>Then <i>v<sub>lo</sub> &le; v &le; v<sub>hi</sub></i>, with 99% confidence.</li>
 * </ul>
 *
 * @author Kevin Lang
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public class KllFloatsSketch {

  public static final int DEFAULT_K = 200;
  static final int DEFAULT_M = 8;
  static final int MIN_K = DEFAULT_M;
  static final int MAX_K = (1 << 16) - 1; // serialized as an unsigned short

  /* Serialized sketch layout:
   *  Adr:
   *      ||    7    |   6   |    5   |    4   |    3   |    2    |    1   |      0       |
   *  0   || unused  |   M   |--------K--------|  Flags |  FamID  | SerVer | PreambleInts |
   *      ||   15    |   14  |   13   |   12   |   11   |   10    |    9   |      8       |
   *  1   ||---------------------------------N_LONG---------------------------------------|
   *      ||   23    |   22  |   21   |   20   |   19   |    18   |   17   |      16      |
   *  2   ||---------------data----------------|--------|numLevels|-------min K-----------|
   */

  private static final int PREAMBLE_INTS_BYTE = 0;
  private static final int SER_VER_BYTE       = 1;
  private static final int FAMILY_BYTE        = 2;
  private static final int FLAGS_BYTE         = 3;
  private static final int K_SHORT            = 4;  // to 5
  private static final int M_BYTE             = 6;
  private static final int N_LONG             = 8;  // to 15
  private static final int MIN_K_SHORT        = 16;  // to 17
  private static final int NUM_LEVELS_BYTE    = 18;
  private static final int DATA_START         = 20;

  private static final int DATA_START_SINGLE_ITEM = 8;

  private static final byte serialVersionUID1 = 1;
  private static final byte serialVersionUID2 = 2;

  private enum Flags { IS_EMPTY, IS_LEVEL_ZERO_SORTED, IS_SINGLE_ITEM }

  private static final int PREAMBLE_INTS_SHORT = 2; // for empty and single item
  private static final int PREAMBLE_INTS_FULL = 5;

  /*
   * Data is stored in items_.
   * The data for level i lies in positions levels_[i] through levels_[i + 1] - 1 inclusive.
   * Hence levels_ must contain (numLevels_ + 1) indices.
   * The valid portion of items_ is completely packed, except for level 0.
   * Level 0 is filled from the top down.
   *
   * Invariants:
   * 1) After a compaction, or an update, or a merge, all levels are sorted except for level zero.
   * 2) After a compaction, (sum of capacities) - (sum of items) >= 1,
   *  so there is room for least 1 more item in level zero.
   * 3) There are no gaps except at the bottom, so if levels_[0] = 0,
   *  the sketch is exactly filled to capacity and must be compacted.
   */

  private final int k_;
  private final int m_; // minimum buffer "width"

  private int minK_; // for error estimation after merging with different k
  private long n_;
  private int numLevels_;
  private int[] levels_;
  private float[] items_;
  private float minValue_;
  private float maxValue_;
  private boolean isLevelZeroSorted_;

  private KllFloatsSketch(final Memory mem) {
    m_ = DEFAULT_M;
    k_ = mem.getShort(K_SHORT) & 0xffff;
    final int flags = mem.getByte(FLAGS_BYTE) & 0xff;
    final boolean isEmpty = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
    final boolean isSingleItem = (flags & (1 << Flags.IS_SINGLE_ITEM.ordinal())) > 0;
    if (isEmpty) {
      numLevels_ = 1;
      levels_ = new int[] {k_, k_};
      items_ = new float[k_];
      minValue_ = Float.NaN;
      maxValue_ = Float.NaN;
      isLevelZeroSorted_ = false;
      minK_ = k_;
    } else {
      if (isSingleItem) {
        n_ = 1;
        minK_ = k_;
        numLevels_ = 1;
      } else {
        n_ = mem.getLong(N_LONG);
        minK_ = mem.getShort(MIN_K_SHORT) & 0xffff;
        numLevels_ = mem.getByte(NUM_LEVELS_BYTE) & 0xff;
      }
      levels_ = new int[numLevels_ + 1];
      int offset = isSingleItem ? DATA_START_SINGLE_ITEM : DATA_START;
      final int capacity = KllHelper.computeTotalCapacity(k_, m_, numLevels_);
      if (isSingleItem) {
        levels_[0] = capacity - 1;
      } else {
        // the last integer in levels_ is not serialized because it can be derived
        mem.getIntArray(offset, levels_, 0, numLevels_);
        offset += numLevels_ * Integer.BYTES;
      }
      levels_[numLevels_] = capacity;
      if (!isSingleItem) {
        minValue_ = mem.getFloat(offset);
        offset += Float.BYTES;
        maxValue_ = mem.getFloat(offset);
        offset += Float.BYTES;
      }
      items_ = new float[capacity];
      mem.getFloatArray(offset, items_, levels_[0], getNumRetained());
      if (isSingleItem) {
        minValue_ = items_[levels_[0]];
        maxValue_ = items_[levels_[0]];
      }
      isLevelZeroSorted_ = (flags & (1 << Flags.IS_LEVEL_ZERO_SORTED.ordinal())) > 0;
    }
  }

  private KllFloatsSketch(final int k, final int m) {
    checkK(k);
    k_ = k;
    m_ = m;
    numLevels_ = 1;
    levels_ = new int[] {k, k};
    items_ = new float[k];
    minValue_ = Float.NaN;
    maxValue_ = Float.NaN;
    isLevelZeroSorted_ = false;
    minK_ = k;
  }

  /**
   * Constructor with the default <em>k</em> (rank error of about 1.65%)
   */
  public KllFloatsSketch() {
    this(DEFAULT_K);
  }

  /**
   * Constructor with a given parameter <em>k</em>. <em>k</em> can be any value between 8 and
   * 65535, inclusive. The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Higher values of K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates
   */
  public KllFloatsSketch(final int k) {
    this(k, DEFAULT_M);
  }

  /**
   * Returns the parameter k
   * @return parameter k
   */
  public int getK() {
    return k_;
  }

  /**
   * Returns the length of the input stream.
   * @return stream length
   */
  public long getN() {
    return n_;
  }

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  public boolean isEmpty() {
    return n_ == 0;
  }

  /**
   * Returns the number of retained items (samples) in the sketch.
   * @return the number of retained items (samples) in the sketch
   */
  public int getNumRetained() {
    return levels_[numLevels_] - levels_[0];
  }

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  public boolean isEstimationMode() {
    return numLevels_ > 1;
  }

  /**
   * Updates this sketch with the given data item.
   *
   * @param value an item from a stream of items. NaNs are ignored.
   */
  public void update(final float value) {
    if (Float.isNaN(value)) { return; }
    if (isEmpty()) {
      minValue_ = value;
      maxValue_ = value;
    } else {
      if (value < minValue_) { minValue_ = value; }
      if (value > maxValue_) { maxValue_ = value; }
    }
    if (levels_[0] == 0) {
      compressWhileUpdating();
    }
    n_++;
    isLevelZeroSorted_ = false;
    final int nextPos = levels_[0] - 1;
    assert levels_[0] >= 0;
    levels_[0] = nextPos;
    items_[nextPos] = value;
  }

  /**
   * Merges another sketch into this one.
   * @param other sketch to merge into this one
   */
  public void merge(final KllFloatsSketch other) {
    if ((other == null) || other.isEmpty()) { return; }
    if (m_ != other.m_) {
      throw new SketchesArgumentException("incompatible M: " + m_ + " and " + other.m_);
    }
    final long finalN = n_ + other.n_;
    for (int i = other.levels_[0]; i < other.levels_[1]; i++) {
      update(other.items_[i]);
    }
    if (other.numLevels_ >= 2) {
      mergeHigherLevels(other, finalN);
    }
    if (Float.isNaN(minValue_) || (other.minValue_ < minValue_)) { minValue_ = other.minValue_; }
    if (Float.isNaN(maxValue_) || (other.maxValue_ > maxValue_)) { maxValue_ = other.maxValue_; }
    n_ = finalN;
    assertCorrectTotalWeight();
    if (other.isEstimationMode()) {
      minK_ = min(minK_, other.minK_);
    }
  }

  /**
   * Returns the min value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the min value of the stream
   */
  public float getMinValue() {
    return minValue_;
  }

  /**
   * Returns the max value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the max value of the stream
   */
  public float getMaxValue() {
    return maxValue_;
  }

  /**
   * Returns an approximation to the value of the data item
   * that would be preceded by the given fraction of a hypothetical sorted
   * version of the input stream so far.
   *
   * <p>We note that this method has a fairly large overhead (microseconds instead of nanoseconds)
   * so it should not be called multiple times to get different quantiles from the same
   * sketch. Instead use getQuantiles(), which pays the overhead only once.
   *
   * <p>If the sketch is empty this returns NaN.
   *
   * @param fraction the specified fractional position in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * If fraction = 0.0, the true minimum value of the stream is returned.
   * If fraction = 1.0, the true maximum value of the stream is returned.
   *
   * @return the approximation to the value at the given fraction
   */
  public float getQuantile(final double fraction) {
    if (isEmpty()) { return Float.NaN; }
    if (fraction == 0.0) { return minValue_; }
    if (fraction == 1.0) { return maxValue_; }
    if ((fraction < 0.0) || (fraction > 1.0)) {
      throw new SketchesArgumentException("Fraction cannot be less than zero or greater than 1.0");
    }
    final KllFloatsQuantileCalculator quant = getQuantileCalculator();
    return quant.getQuantile(fraction);
  }

  /**
   * Gets the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public float getQuantileUpperBound(final double fraction) {
    return getQuantile(min(1.0, fraction + getNormalizedRankError(minK_, false)));
  }

  /**
   * Gets the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public float getQuantileLowerBound(final double fraction) {
    return getQuantile(max(0, fraction - getNormalizedRankError(minK_, false)));
  }

  /**
   * This is a more efficient multiple-query version of getQuantile().
   *
   * <p>This returns an array that could have been generated by using getQuantile() with many
   * different fractional ranks, but would be very inefficient.
   * This method incurs the internal set-up overhead once and obtains multiple quantile values in
   * a single query. It is strongly recommend that this method be used instead of multiple calls
   * to getQuantile().
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param fractions given array of fractional positions in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * These fractions must be in the interval [0.0, 1.0], inclusive.
   *
   * @return array of approximations to the given fractions in the same order as given fractions
   * array.
   */
  public float[] getQuantiles(final double[] fractions) {
    if (isEmpty()) { return null; }
    KllFloatsQuantileCalculator quant = null;
    final float[] quantiles = new float[fractions.length];
    for (int i = 0; i < fractions.length; i++) {
      final double fraction = fractions[i];
      if (fraction < 0.0 || fraction > 1.0) {
        throw new SketchesArgumentException("Fraction cannot be less than zero or greater than 1.0");
      }
      if      (fraction == 0.0) { quantiles[i] = minValue_; }
      else if (fraction == 1.0) { quantiles[i] = maxValue_; }
      else {
        if (quant == null) {
          quant = getQuantileCalculator();
        }
        quantiles[i] = quant.getQuantile(fraction);
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
   * @param numEvenlySpaced an integer that specifies the number of evenly spaced fractional ranks.
   * This must be a positive integer greater than 0. A value of 1 will return the min value.
   * A value of 2 will return the min and the max value. A value of 3 will return the min,
   * the median and the max value, etc.
   *
   * @return array of approximations to the given fractions in the same order as given fractions
   * array.
   */
  public float[] getQuantiles(final int numEvenlySpaced) {
    if (isEmpty()) { return null; }
    return getQuantiles(QuantilesHelper.getEvenlySpacedRanks(numEvenlySpaced));
  }

  /**
   * Returns an approximation to the normalized (fractional) rank of the given value from 0 to 1,
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
  public double getRank(final float value) {
    if (isEmpty()) { return Double.NaN; }
    int level = 0;
    int weight = 1;
    long total = 0;
    while (level < numLevels_) {
      final int fromIndex = levels_[level];
      final int toIndex = levels_[level + 1]; // exclusive
      for (int i = fromIndex; i < toIndex; i++) {
        if (items_[i] < value) {
          total += weight;
        } else if ((level > 0) || isLevelZeroSorted_) {
          break; // levels above 0 are sorted, no point comparing further
        }
      }
      level++;
      weight *= 2;
    }
    return (double) total / n_;
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
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing float values
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
  public double[] getPMF(final float[] splitPoints) {
    return getPmfOrCdf(splitPoints, false);
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
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing float values
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
  public double[] getCDF(final float[] splitPoints) {
    return getPmfOrCdf(splitPoints, true);
  }

  /**
   * Gets the approximate "double-sided" rank error for the <i>getPMF()</i> function of this
   * sketch normalized as a fraction between zero and one.
   *
   * @return the rank error normalized as a fraction between zero and one.
   * @deprecated replaced by {@link #getNormalizedRankError(boolean)}
   * @see KllFloatsSketch
   */
  @Deprecated
  public double getNormalizedRankError() {
    return getNormalizedRankError(true);
  }

  /**
   * Gets the approximate rank error of this sketch normalized as a fraction between zero and one.
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, returns the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @see KllFloatsSketch
   */
  public double getNormalizedRankError(final boolean pmf) {
    return getNormalizedRankError(minK_, pmf);
  }

  /**
   * Static method version of the double-sided {@link #getNormalizedRankError()} that
   * specifies <em>k</em>.
   * @param k the configuration parameter
   * @return the normalized "double-sided" rank error as a function of <em>k</em>.
   * @see KllFloatsSketch
   * @deprecated replaced by {@link #getNormalizedRankError(int, boolean)}
   */
  @Deprecated
  public static double getNormalizedRankError(final int k) {
    return getNormalizedRankError(k, true);
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
  // constants were derived as the best fit to 99 percentile empirically measured max error in
  // thousands of trials
  public static double getNormalizedRankError(final int k, final boolean pmf) {
    return pmf
        ? 2.446 / pow(k, 0.9433)
        : 2.296 / pow(k, 0.9723);
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
  // constants were derived as the best fit to 99 percentile empirically measured max error in
  // thousands of trials
  public static int getKFromEpsilon(final double epsilon, final boolean pmf) {
    //Ensure that eps is >= than the lowest possible eps given MAX_K and pmf=false.
    final double eps = max(epsilon, 4.7634E-5);
    final double kdbl = pmf
        ? exp(log(2.446 / eps) / 0.9433)
        : exp(log(2.296 / eps) / 0.9723);
    final double krnd = round(kdbl);
    final double del = abs(krnd - kdbl);
    final int k = (int) ((del < 1E-6) ? krnd : ceil(kdbl));
    return max(MIN_K, min(MAX_K, k));
  }

  /**
   * Returns the number of bytes this sketch would require to store.
   * @return the number of bytes this sketch would require to store.
   */
  public int getSerializedSizeBytes() {
    if (isEmpty()) { return N_LONG; }
    return getSerializedSizeBytes(numLevels_, getNumRetained());
  }

  /**
   * Returns upper bound on the serialized size of a sketch given a parameter <em>k</em> and stream
   * length. The resulting size is an overestimate to make sure actual sketches don't exceed it.
   * This method can be used if allocation of storage is necessary beforehand, but it is not
   * optimal.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @return upper bound on the serialized size
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n) {
    final int numLevels = KllHelper.ubOnNumLevels(n);
    final int maxNumItems = KllHelper.computeTotalCapacity(k, DEFAULT_M, numLevels);
    return getSerializedSizeBytes(numLevels, maxNumItems);
  }

  @Override
  public String toString() {
    return toString(false, false);
  }

  /**
   * Returns a summary of the sketch as a string.
   * @param withLevels if true include information about levels
   * @param withData if true include sketch data
   * @return string representation of sketch summary
   */
  public String toString(final boolean withLevels, final boolean withData) {
    final String epsPct = String.format("%.3f%%", getNormalizedRankError(false) * 100);
    final String epsPMFPct = String.format("%.3f%%", getNormalizedRankError(true) * 100);
    final StringBuilder sb = new StringBuilder();
    sb.append(Util.LS).append("### KLL sketch summary:").append(Util.LS);
    sb.append("   K                    : ").append(k_).append(Util.LS);
    sb.append("   min K                : ").append(minK_).append(Util.LS);
    sb.append("   M                    : ").append(m_).append(Util.LS);
    sb.append("   N                    : ").append(n_).append(Util.LS);
    sb.append("   Epsilon              : ").append(epsPct).append(Util.LS);
    sb.append("   Epsison PMF          : ").append(epsPMFPct).append(Util.LS);
    sb.append("   Empty                : ").append(isEmpty()).append(Util.LS);
    sb.append("   Estimation Mode      : ").append(isEstimationMode()).append(Util.LS);
    sb.append("   Levels               : ").append(numLevels_).append(Util.LS);
    sb.append("   Sorted               : ").append(isLevelZeroSorted_).append(Util.LS);
    sb.append("   Buffer Capacity Items: ").append(items_.length).append(Util.LS);
    sb.append("   Retained Items       : ").append(getNumRetained()).append(Util.LS);
    sb.append("   Storage Bytes        : ").append(getSerializedSizeBytes()).append(Util.LS);
    sb.append("   Min Value            : ").append(minValue_).append(Util.LS);
    sb.append("   Max Value            : ").append(maxValue_).append(Util.LS);
    sb.append("### End sketch summary").append(Util.LS);

    if (withLevels) {
      sb.append("### KLL sketch levels:").append(Util.LS)
      .append("   index: nominal capacity, actual size").append(Util.LS);
      for (int i = 0; i < numLevels_; i++) {
        sb.append("   ").append(i).append(": ")
        .append(KllHelper.levelCapacity(k_, numLevels_, i, m_))
        .append(", ").append(safeLevelSize(i)).append(Util.LS);
      }
      sb.append("### End sketch levels").append(Util.LS);
    }

    if (withData) {
      sb.append("### KLL sketch data:").append(Util.LS);
      int level = 0;
      while (level < numLevels_) {
        final int fromIndex = levels_[level];
        final int toIndex = levels_[level + 1]; // exclusive
        if (fromIndex < toIndex) {
          sb.append(" level ").append(level).append(":").append(Util.LS);
        }
        for (int i = fromIndex; i < toIndex; i++) {
          sb.append("   ").append(items_[i]).append(Util.LS);
        }
        level++;
      }
      sb.append("### End sketch data").append(Util.LS);
    }

    return sb.toString();
  }

  /**
   * Returns serialized sketch in a byte array form.
   * @return serialized sketch in a byte array form.
   */
  public byte[] toByteArray() {
    final byte[] bytes = new byte[getSerializedSizeBytes()];
    final boolean isSingleItem = n_ == 1;
    bytes[PREAMBLE_INTS_BYTE] = (byte) (isEmpty() || isSingleItem ? PREAMBLE_INTS_SHORT : PREAMBLE_INTS_FULL);
    bytes[SER_VER_BYTE] = isSingleItem ? serialVersionUID2 : serialVersionUID1;
    bytes[FAMILY_BYTE] = (byte) Family.KLL.getID();
    bytes[FLAGS_BYTE] = (byte) (
        (isEmpty() ? 1 << Flags.IS_EMPTY.ordinal() : 0)
      | (isLevelZeroSorted_ ? 1 << Flags.IS_LEVEL_ZERO_SORTED.ordinal() : 0)
      | (isSingleItem ? 1 << Flags.IS_SINGLE_ITEM.ordinal() : 0)
    );
    ByteArrayUtil.putShortLE(bytes, K_SHORT, (short) k_);
    bytes[M_BYTE] = (byte) m_;
    if (isEmpty()) { return bytes; }
    int offset = DATA_START_SINGLE_ITEM;
    if (!isSingleItem) {
      ByteArrayUtil.putLongLE(bytes, N_LONG, n_);
      ByteArrayUtil.putShortLE(bytes, MIN_K_SHORT, (short) minK_);
      bytes[NUM_LEVELS_BYTE] = (byte) numLevels_;
      offset = DATA_START;
      // the last integer in levels_ is not serialized because it can be derived
      for (int i = 0; i < numLevels_; i++) {
        ByteArrayUtil.putIntLE(bytes, offset, levels_[i]);
        offset += Integer.BYTES;
      }
      ByteArrayUtil.putFloatLE(bytes, offset, minValue_);
      offset += Float.BYTES;
      ByteArrayUtil.putFloatLE(bytes, offset, maxValue_);
      offset += Float.BYTES;
    }
    final int numItems = getNumRetained();
    for (int i = 0; i < numItems; i++) {
      ByteArrayUtil.putFloatLE(bytes, offset, items_[levels_[0] + i]);
      offset += Float.BYTES;
    }
    return bytes;
  }

  /**
   * Heapify takes the sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param mem a Memory image of a sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based sketch based on the given Memory
   */
  public static KllFloatsSketch heapify(final Memory mem) {
    final int preambleInts = mem.getByte(PREAMBLE_INTS_BYTE) & 0xff;
    final int serialVersion = mem.getByte(SER_VER_BYTE) & 0xff;
    final int family = mem.getByte(FAMILY_BYTE) & 0xff;
    final int flags = mem.getByte(FLAGS_BYTE) & 0xff;
    final int m = mem.getByte(M_BYTE) & 0xff;
    if (m != DEFAULT_M) {
      throw new SketchesArgumentException(
          "Possible corruption: M must be " + DEFAULT_M + ": " + m);
    }
    final boolean isEmpty = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
    final boolean isSingleItem = (flags & (1 << Flags.IS_SINGLE_ITEM.ordinal())) > 0;
    if (isEmpty || isSingleItem) {
      if (preambleInts != PREAMBLE_INTS_SHORT) {
        throw new SketchesArgumentException("Possible corruption: preambleInts must be "
            + PREAMBLE_INTS_SHORT + " for an empty or single item sketch: " + preambleInts);
      }
    } else {
      if (preambleInts != PREAMBLE_INTS_FULL) {
        throw new SketchesArgumentException("Possible corruption: preambleInts must be "
            + PREAMBLE_INTS_FULL + " for a sketch with more than one item: " + preambleInts);
      }
    }
    if ((serialVersion != serialVersionUID1) && (serialVersion != serialVersionUID2)) {
      throw new SketchesArgumentException(
          "Possible corruption: serial version mismatch: expected " + serialVersionUID1 + " or "
              + serialVersionUID2 + ", got " + serialVersion);
    }
    if (family != Family.KLL.getID()) {
      throw new SketchesArgumentException(
      "Possible corruption: family mismatch: expected " + Family.KLL.getID() + ", got " + family);
    }
    return new KllFloatsSketch(mem);
  }

  public KllFloatsSketchIterator iterator() {
    return new KllFloatsSketchIterator(items_, levels_, numLevels_);
  }

  /**
   * Checks the validity of the given value k
   * @param k must be greater than 7 and less than 65536.
   */
  static void checkK(final int k) {
    if ((k < MIN_K) || (k > MAX_K)) {
      throw new SketchesArgumentException(
          "K must be >= " + MIN_K + " and <= " + MAX_K + ": " + k);
    }
  }

  private KllFloatsQuantileCalculator getQuantileCalculator() {
    sortLevelZero(); // sort in the sketch to reuse if possible
    return new KllFloatsQuantileCalculator(items_, levels_, numLevels_, n_);
  }

  private double[] getPmfOrCdf(final float[] splitPoints, final boolean isCdf) {
    if (isEmpty()) { return null; }
    KllHelper.validateValues(splitPoints);
    final double[] buckets = new double[splitPoints.length + 1];
    int level = 0;
    int weight = 1;
    while (level < numLevels_) {
      final int fromIndex = levels_[level];
      final int toIndex = levels_[level + 1]; // exclusive
      if ((level == 0) && !isLevelZeroSorted_) {
        incrementBucketsUnsortedLevel(fromIndex, toIndex, weight, splitPoints, buckets);
      } else {
        incrementBucketsSortedLevel(fromIndex, toIndex, weight, splitPoints, buckets);
      }
      level++;
      weight *= 2;
    }
    // normalize and, if CDF, convert to cumulative
    if (isCdf) {
      double subtotal = 0;
      for (int i = 0; i < buckets.length; i++) {
        subtotal += buckets[i];
        buckets[i] = subtotal / n_;
      }
    } else {
      for (int i = 0; i < buckets.length; i++) {
        buckets[i] /= n_;
      }
    }
    return buckets;
  }

  private void incrementBucketsUnsortedLevel(final int fromIndex, final int toIndex,
      final int weight, final float[] splitPoints, final double[] buckets) {
    for (int i = fromIndex; i < toIndex; i++) {
      int j;
      for (j = 0; j < splitPoints.length; j++) {
        if (items_[i] < splitPoints[j]) {
          break;
        }
      }
      buckets[j] += weight;
    }
  }

  private void incrementBucketsSortedLevel(final int fromIndex, final int toIndex,
      final int weight, final float[] splitPoints, final double[] buckets) {
    int i = fromIndex;
    int j = 0;
    while ((i <  toIndex) && (j < splitPoints.length)) {
      if (items_[i] < splitPoints[j]) {
        buckets[j] += weight; // this sample goes into this bucket
        i++; // move on to next sample and see whether it also goes into this bucket
      } else {
        j++; // no more samples for this bucket
      }
    }
    // now either i == toIndex (we are out of samples), or
    // j == numSplitPoints (we are out of buckets, but there are more samples remaining)
    // we only need to do something in the latter case
    if (j == splitPoints.length) {
      buckets[j] += weight * (toIndex - i);
    }
  }

  // The following code is only valid in the special case of exactly reaching capacity while updating.
  // It cannot be used while merging, while reducing k, or anything else.
  private void compressWhileUpdating() {
    final int level = findLevelToCompact();

    // It is important to do add the new top level right here. Be aware that this operation
    // grows the buffer and shifts the data and also the boundaries of the data and grows the
    // levels array and increments numLevels_
    if (level == (numLevels_ - 1)) {
      addEmptyTopLevelToCompletelyFullSketch();
    }

    final int rawBeg = levels_[level];
    final int rawLim = levels_[level + 1];
    // +2 is OK because we already added a new top level if necessary
    final int popAbove = levels_[level + 2] - rawLim;
    final int rawPop = rawLim - rawBeg;
    final boolean oddPop = KllHelper.isOdd(rawPop);
    final int adjBeg = oddPop ? rawBeg + 1 : rawBeg;
    final int adjPop = oddPop ? rawPop - 1 : rawPop;
    final int halfAdjPop = adjPop / 2;

    // level zero might not be sorted, so we must sort it if we wish to compact it
    if (level == 0) {
      Arrays.sort(items_, adjBeg, adjBeg + adjPop);
    }
    if (popAbove == 0) {
      KllHelper.randomlyHalveUp(items_, adjBeg, adjPop);
    } else {
      KllHelper.randomlyHalveDown(items_, adjBeg, adjPop);
      KllHelper.mergeSortedArrays(items_, adjBeg, halfAdjPop, items_, rawLim, popAbove,
          items_, adjBeg + halfAdjPop);
    }
    levels_[level + 1] -= halfAdjPop; // adjust boundaries of the level above
    if (oddPop) {
      levels_[level] = levels_[level + 1] - 1; // the current level now contains one item
      items_[levels_[level]] = items_[rawBeg]; // namely this leftover guy
    } else {
      levels_[level] = levels_[level + 1]; // the current level is now empty
    }

    // verify that we freed up halfAdjPop array slots just below the current level
    assert levels_[level] == (rawBeg + halfAdjPop);

    // finally, we need to shift up the data in the levels below
    // so that the freed-up space can be used by level zero
    if (level > 0) {
      final int amount = rawBeg - levels_[0];
      System.arraycopy(items_, levels_[0], items_, levels_[0] + halfAdjPop, amount);
      for (int lvl = 0; lvl < level; lvl++) {
        levels_[lvl] += halfAdjPop;
      }
    }
  }

  private int findLevelToCompact() {
    int level = 0;
    while (true) {
      assert level < numLevels_;
      final int pop = levels_[level + 1] - levels_[level];
      final int cap = KllHelper.levelCapacity(k_, numLevels_, level, m_);
      if (pop >= cap) {
        return level;
      }
      level++;
    }
  }

  private void addEmptyTopLevelToCompletelyFullSketch() {
    final int curTotalCap = levels_[numLevels_];

    // make sure that we are following a certain growth scheme
    assert levels_[0] == 0;
    assert items_.length == curTotalCap;

    // note that merging MIGHT over-grow levels_, in which case we might not have to grow it here
    if (levels_.length < (numLevels_ + 2)) {
      levels_ = KllHelper.growIntArray(levels_, numLevels_ + 2);
    }

    final int deltaCap = KllHelper.levelCapacity(k_, numLevels_ + 1, 0, m_);
    final int newTotalCap = curTotalCap + deltaCap;

    final float[] newBuf = new float[newTotalCap];

    // copy (and shift) the current data into the new buffer
    System.arraycopy(items_, levels_[0], newBuf, levels_[0] + deltaCap, curTotalCap);
    items_ = newBuf;

    // this loop includes the old "extra" index at the top
    for (int i = 0; i <= numLevels_; i++) {
      levels_[i] += deltaCap;
    }

    assert levels_[numLevels_] == newTotalCap;

    numLevels_++;
    levels_[numLevels_] = newTotalCap; // initialize the new "extra" index at the top
  }

  private void sortLevelZero() {
    if (!isLevelZeroSorted_) {
      Arrays.sort(items_, levels_[0], levels_[1]);
      isLevelZeroSorted_ = true;
    }
  }

  private void mergeHigherLevels(final KllFloatsSketch other, final long finalN) {
    final int tmpSpaceNeeded = getNumRetained() + other.getNumRetainedAboveLevelZero();
    final float[] workbuf = new float[tmpSpaceNeeded];
    final int ub = KllHelper.ubOnNumLevels(finalN);
    final int[] worklevels = new int[ub + 2]; // ub+1 does not work
    final int[] outlevels  = new int[ub + 2];

    final int provisionalNumLevels = max(numLevels_, other.numLevels_);

    populateWorkArrays(other, workbuf, worklevels, provisionalNumLevels);

    // notice that workbuf is being used as both the input and output here
    final int[] result = KllHelper.generalCompress(k_, m_, provisionalNumLevels, workbuf,
        worklevels, workbuf, outlevels, isLevelZeroSorted_);
    final int finalNumLevels = result[0];
    final int finalCapacity = result[1];
    final int finalPop = result[2];

    assert (finalNumLevels <= ub); // can sometimes be much bigger

    // now we need to transfer the results back into the "self" sketch
    final float[] newbuf = finalCapacity == items_.length ? items_ : new float[finalCapacity];
    final int freeSpaceAtBottom = finalCapacity - finalPop;
    System.arraycopy(workbuf, outlevels[0], newbuf, freeSpaceAtBottom, finalPop);
    final int theShift = freeSpaceAtBottom - outlevels[0];

    if (levels_.length < (finalNumLevels + 1)) {
      levels_ = new int[finalNumLevels + 1];
    }

    for (int lvl = 0; lvl < (finalNumLevels + 1); lvl++) { // includes the "extra" index
      levels_[lvl] = outlevels[lvl] + theShift;
    }

    items_ = newbuf;
    numLevels_ = finalNumLevels;
  }

  private void populateWorkArrays(final KllFloatsSketch other, final float[] workbuf,
      final int[] worklevels, final int provisionalNumLevels) {
    worklevels[0] = 0;

    // Note: the level zero data from "other" was already inserted into "self"
    final int selfPopZero = safeLevelSize(0);
    System.arraycopy(items_, levels_[0], workbuf, worklevels[0], selfPopZero);
    worklevels[1] = worklevels[0] + selfPopZero;

    for (int lvl = 1; lvl < provisionalNumLevels; lvl++) {
      final int selfPop = safeLevelSize(lvl);
      final int otherPop = other.safeLevelSize(lvl);
      worklevels[lvl + 1] = worklevels[lvl] + selfPop + otherPop;

      if ((selfPop > 0) && (otherPop == 0)) {
        System.arraycopy(items_, levels_[lvl], workbuf, worklevels[lvl], selfPop);
      } else if ((selfPop == 0) && (otherPop > 0)) {
        System.arraycopy(other.items_, other.levels_[lvl], workbuf, worklevels[lvl], otherPop);
      } else if ((selfPop > 0) && (otherPop > 0)) {
        KllHelper.mergeSortedArrays(items_, levels_[lvl], selfPop, other.items_,
            other.levels_[lvl], otherPop, workbuf, worklevels[lvl]);
      }
    }
  }

  private int safeLevelSize(final int level) {
    if (level >= numLevels_) { return 0; }
    return levels_[level + 1] - levels_[level];
  }

 private int getNumRetainedAboveLevelZero() {
    if (numLevels_ == 1) { return 0; }
    return levels_[numLevels_] - levels_[1];
  }

  private void assertCorrectTotalWeight() {
    final long total = KllHelper.sumTheSampleWeights(numLevels_, levels_);
    assert total == n_;
  }

  private static int getSerializedSizeBytes(final int numLevels, final int numRetained) {
    if ((numLevels == 1) && (numRetained == 1)) {
      return DATA_START_SINGLE_ITEM + Float.BYTES;
    }
    // the last integer in levels_ is not serialized because it can be derived
    // + 2 for min and max
    return DATA_START + (numLevels * Integer.BYTES) + ((numRetained + 2) * Float.BYTES);
  }

  // for testing

  float[] getItems() {
    return items_;
  }

  int[] getLevels() {
    return levels_;
  }

  int getNumLevels() {
    return numLevels_;
  }

}
