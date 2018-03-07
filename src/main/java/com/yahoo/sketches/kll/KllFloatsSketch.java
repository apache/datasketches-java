package com.yahoo.sketches.kll;

import java.util.Arrays;
import java.util.Random;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ByteArrayUtil;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * Implementation of a very compact quantiles sketch with lazy compaction scheme
 * and nearly optimal accuracy per bit.
 * See https://arxiv.org/abs/1603.05346v2
 * 
 * <p>This is a stochastic streaming sketch that enables near-real time analysis of the
 * approximate distribution of real values from a very large stream in a single pass.
 * The analysis is obtained using getQuantile() or getQuantiles() functions or inverse functions
 * getRank(), Probability Mass Function from getPMF() and Cumulative Distribution Function from getCDF().
 * 
 * <p>The accuracy of this sketch is a function of the configured parameter <i>K</i>, which also affects
 * the overall size of the sketch. Accuracy of this quantile sketch is always with respect to
 * the normalized rank.
 * 
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
public class KllFloatsSketch {

  private static final Random random = new Random();

  public static final int DEFAULT_K = 200;
  public static final int DEFAULT_M = 8;
  public static final int MIN_K = 8;
  public static final int MAX_K = (1 << 16) - 1; // serialized as an unsigned short

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

  private static final byte serialVersionUID = 1;

  private enum Flags { IS_EMPTY, IS_LEVEL_ZERO_SORTED }

  private static final int PREAMBLE_INTS_EMPTY = 2;
  private static final int PREAMBLE_INTS_NONEMPTY = 5;

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

  /**
   * Constructor with the default K (rank error of about 1.3%)
   */
  public KllFloatsSketch() {
    this(DEFAULT_K);
  }

  /**
   * Constructor with a given parameter K
   * @param k parameter that controls size of the sketch and accuracy of estimates
   */
  public KllFloatsSketch(final int k) {
    this(k, DEFAULT_M);
  }

  /**
   * Returns the length of the input stream.
   * @return stream length
   */
  public long getN() {
    return n_;
  }

  /**
   * Returns true if this sketch is empty
   * @return empty flag
   */
  public boolean isEmpty() {
    return n_ == 0;
  }

  /**
   * Returns the number of retained items (samples) in the sketch
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
   * Updates this sketch with the given data item
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
    if (other == null || other.isEmpty()) { return; }
    if (m_ != other.m_) {
      throw new SketchesArgumentException("incompatible M: " + m_ + " and " + other.m_);
    }
    if (isEmpty()) {
      minValue_ = other.minValue_;
      maxValue_ = other.maxValue_;
    } else {
      if (other.minValue_ < minValue_) { minValue_ = other.minValue_; }
      if (other.maxValue_ > maxValue_) { maxValue_ = other.maxValue_; }
    }
    final long finalN = n_ + other.n_;
    if (other.numLevels_ >= 1) {
      for (int i = other.levels_[0]; i < other.levels_[1]; i++) {
        update(other.items_[i]);
      }
    }
    if (other.numLevels_ >= 2) {
      mergeHigherLevels(other, finalN);
    }
    n_ = finalN;
    assertCorrectTotalWeight();
    minK_ = Math.min(minK_, other.minK_);
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
   * This is a more efficient multiple-query version of getQuantile().
   *
   * <p>This returns an array that could have been generated by using getQuantile() with many different
   * fractional ranks, but would be very inefficient.
   * This method incurs the internal set-up overhead once and obtains multiple quantile values in
   * a single query. It is strongly recommend that this method be used instead of multiple calls
   * to getQuantile().
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param fractions given array of fractional positions in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * These fractions must be in the interval [0.0, 1.0] inclusive.
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
   * Returns an approximation to the normalized (fractional) rank of the given value from 0 to 1 inclusive.
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
        } else if (level > 0) {
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
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError() function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   *
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream values that fell into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint.
   */
  public double[] getPMF(final float[] splitPoints) {
    return getPmfOrCdf(splitPoints, false);
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of splitPoint (values).
   *
   * <p>More specifically, the value at array position j of the CDF is the
   * sum of the values in positions 0 through j of the PMF (rank of the splitPoint j).
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   *
   * @return an approximation to the CDF of the input stream given the splitPoints.
   */
  public double[] getCDF(final float[] splitPoints) {
    return getPmfOrCdf(splitPoints, true);
  }

  /**
   * Get the rank error normalized as a fraction between zero and one.
   * The error of this sketch is specified as a fraction of the normalized rank of the hypothetical
   * sorted stream of items presented to the sketch.
   *
   * <p>Suppose the sketch is presented with N values. The raw rank (0 to N-1) of an item
   * would be its index position in the sorted version of the input stream. If we divide the
   * raw rank by N, it becomes the normalized rank, which is between 0 and 1.0.
   *
   * <p>For example, choosing a K of 256 yields a normalized rank error of about 1.3%.
   * The upper bound on the median value obtained by getQuantile(0.5) would be the value in the
   * hypothetical ordered stream of values at the normalized rank of 0.513.
   * The lower bound would be the value in the hypothetical ordered stream of values at the
   * normalized rank of 0.487.
   *
   * <p>The returned error is for so-called "two-sided"queries corresponding to histogram bins of getPMF().
   * "One-sided" queries (getRank() and getCDF()) are expected to have slightly lower error (factor of 0.85 for
   * small K=16 to 0.75 for large K=4096).
   *
   * <p>The error of this sketch cannot be translated into an error (relative or absolute) of the
   * returned quantile values.
   *
   * @return the rank error normalized as a fraction between zero and one.
   */
  public double getNormalizedRankError() {
    return getNormalizedRankError(minK_);
  }

  /**
   * Static method version of {@link #getNormalizedRankError()}
   * @param k the configuration parameter
   * @return the rank error normalized as a fraction between zero and one.
   */
  // constants were derived as the best fit to 99 percentile empirically measured max error in thousands of trials
  public static double getNormalizedRankError(final int k) {
    return 2.446 / Math.pow(k, 0.943);
  }

  /**
   * Returns the number of bytes this sketch would require to store.
   * @return the number of bytes this sketch would require to store.
   */
  public int getSerializedSizeBytes() {
    if (isEmpty()) return N_LONG;
    return getSerializedSizeBytes(numLevels_, getNumRetained());
  }

  /**
   * Returns upper bound on the serialized size of a sketch given a parameter K and stream length.
   * The resulting size is an overestimate to make sure actual sketches don't exceed it.
   * This method can be used if allocation of storage is necessary beforehand, but it is not optimal. 
   * @param k
   * @param n
   * @return upper bound on the serialized size
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n) {
    final int numLevels = ubOnNumLevels(n);
    final int maxNumItems = computeTotalCapacity(k, DEFAULT_M, numLevels);
    return getSerializedSizeBytes(numLevels, maxNumItems);
  }

  @Override
  public String toString() {
    return toString(false, false);
  }

  /**
   * Returns a summary of the sketch as a string
   * @param withLevels if true include information about levels
   * @param withData if true include sketch data
   * @return string representation of sketch summary
   */
  public String toString(final boolean withLevels, final boolean withData) {
    final String epsilonPct = String.format("%.3f%%", getNormalizedRankError() * 100);
    final StringBuilder sb = new StringBuilder();
    sb.append(Util.LS).append("### KLL sketch summary:").append(Util.LS);
    sb.append("   K                    : ").append(k_).append(Util.LS);
    sb.append("   min K                : ").append(minK_).append(Util.LS);
    sb.append("   Normalized Rank Error: ").append(epsilonPct).append(Util.LS);
    sb.append("   M                    : ").append(m_).append(Util.LS);
    sb.append("   Empty                : ").append(isEmpty()).append(Util.LS);
    sb.append("   Estimation Mode      : ").append(isEstimationMode()).append(Util.LS);
    sb.append("   N                    : ").append(n_).append(Util.LS);
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
      .append("index: nominal capacity, actual size").append(Util.LS);
      for (int i = 0; i < numLevels_; i++) {
        sb.append("   ").append(i).append(": ").append(levelCapacity(k_, numLevels_, i, m_))
        .append(", ").append(safeLevelSize(i)).append(Util.LS);
      }
      sb.append("### End sketch levels").append(Util.LS);
    }

    // withData is not implemented yet

    return sb.toString();
  }

  /**
   * Returns serialized sketch in a byte array form.
   * @return serialized sketch in a byte array form.
   */
  public byte[] toByteArray() {
    final byte[] bytes = new byte[getSerializedSizeBytes()];
    bytes[PREAMBLE_INTS_BYTE] = (byte) (isEmpty() ? PREAMBLE_INTS_EMPTY : PREAMBLE_INTS_NONEMPTY);
    bytes[SER_VER_BYTE] = serialVersionUID;
    bytes[FAMILY_BYTE] = (byte) Family.KLL.getID();
    bytes[FLAGS_BYTE] = (byte) (
        (isEmpty() ? 1 << Flags.IS_EMPTY.ordinal() : 0)
      | (isLevelZeroSorted_ ? 1 << Flags.IS_LEVEL_ZERO_SORTED.ordinal() : 0)
    );
    ByteArrayUtil.putShort(bytes, K_SHORT, (short) k_);
    bytes[M_BYTE] = (byte) m_;
    if (isEmpty()) { return bytes; }
    ByteArrayUtil.putLong(bytes, N_LONG, n_);
    ByteArrayUtil.putShort(bytes, MIN_K_SHORT, (short) minK_);
    bytes[NUM_LEVELS_BYTE] = (byte) numLevels_;
    int offset = DATA_START;
    for (int i = 0; i < numLevels_ + 1; i++) {
      ByteArrayUtil.putInt(bytes, offset, levels_[i]);
      offset += Integer.BYTES;
    }
    ByteArrayUtil.putFloat(bytes, offset, minValue_);
    offset += Float.BYTES;
    ByteArrayUtil.putFloat(bytes, offset, maxValue_);
    offset += Float.BYTES;
    final int numItems = getNumRetained();
    for (int i = 0; i < numItems; i++) {
      ByteArrayUtil.putFloat(bytes, offset, items_[levels_[0] + i]);
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
      throw new SketchesArgumentException("Possible corruption: M must be " + DEFAULT_M + ": " + m);
    }
    final boolean isEmpty = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
    if (isEmpty) {
      if (preambleInts != PREAMBLE_INTS_EMPTY) {
        throw new SketchesArgumentException("Possible corruption: preambleInts must be "
            + PREAMBLE_INTS_EMPTY + " for an empty sketch: " + preambleInts);
      }
    } else {
      if (preambleInts != PREAMBLE_INTS_NONEMPTY) {
        throw new SketchesArgumentException("Possible corruption: preambleInts must be "
            + PREAMBLE_INTS_NONEMPTY + " for a non-empty sketch: " + preambleInts);
      }
    }
    if (serialVersion != serialVersionUID) {
      throw new SketchesArgumentException(
          "Possible corruption: serial version mismatch: expected " + serialVersionUID + ", got " + serialVersion);
    }
    if (family != Family.KLL.getID()) {
      throw new SketchesArgumentException(
          "Possible corruption: family mismatch: expected " + Family.KLL.getID() + ", got " + family);
    }
    return new KllFloatsSketch(mem);
  }

  private KllFloatsSketch(final Memory mem) {
    m_ = DEFAULT_M;
    k_ = mem.getShort(K_SHORT) & 0xffff;
    final int flags = mem.getByte(FLAGS_BYTE) & 0xff;
    final boolean isEmpty = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
    if (isEmpty) {
      numLevels_ = 1;
      levels_ = new int[] {k_, k_};
      items_ = new float[k_];
      minValue_ = Float.NaN;
      maxValue_ = Float.NaN;
      isLevelZeroSorted_ = false;
      minK_ = k_;
    } else {
      n_ = mem.getLong(N_LONG);
      minK_ = mem.getShort(MIN_K_SHORT) & 0xffff;
      numLevels_ = mem.getByte(NUM_LEVELS_BYTE) & 0xff;
      levels_ = new int[numLevels_ + 1];
      int offset = DATA_START;
      mem.getIntArray(offset, levels_, 0, numLevels_ + 1);
      offset += (numLevels_ + 1) * Integer.BYTES;
      minValue_ = mem.getFloat(offset);
      offset += Float.BYTES;
      maxValue_ = mem.getFloat(offset);
      offset += Float.BYTES;
      items_ = new float[computeTotalCapacity(k_, m_, numLevels_)];
      mem.getFloatArray(offset, items_, levels_[0], getNumRetained());
      isLevelZeroSorted_ = (flags & (1 << Flags.IS_LEVEL_ZERO_SORTED.ordinal())) > 0;
    }
  }

  private KllFloatsSketch(final int k, final int m) {
    if (k < MIN_K || k > MAX_K) {
      throw new SketchesArgumentException("K must be >= " + MIN_K + " and < " + MAX_K + ": " + k);
    }
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

  private KllFloatsQuantileCalculator getQuantileCalculator() {
    sortLevelZero(); // sort in the sketch to reuse if possible
    return new KllFloatsQuantileCalculator(items_, levels_, numLevels_, n_); 
  }

  private double[] getPmfOrCdf(final float[] splitPoints, boolean isCdf) {
    if (isEmpty()) { return null; }
    validateValues(splitPoints);
    final double[] buckets = new double[splitPoints.length + 1];
    int level = 0;
    int weight = 1;
    while (level < numLevels_) {
      final int fromIndex = levels_[level];
      final int toIndex = levels_[level + 1]; // exclusive
      if (level == 0 && !isLevelZeroSorted_) {
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

  /**
   * Checks the sequential validity of the given array of float values.
   * They must be unique, monotonically increasing and not NaN.
   * @param values the given array of values
   */
  private static final void validateValues(final float[] values) {
    for (int i = 0; i < values.length ; i++) {
      if (Float.isNaN(values[i])) {
        throw new SketchesArgumentException("Values must not be NaN");
      }
      if (i < values.length - 1 && values[i] >= values[i + 1]) {
        throw new SketchesArgumentException(
          "Values must be unique and monotonically increasing");
      }
    }
  }

  private void incrementBucketsUnsortedLevel(final int fromIndex, final int toIndex, final int weight, final float[] splitPoints, final double[] buckets) {
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

  private void incrementBucketsSortedLevel(final int fromIndex, final int toIndex, final int weight, final float[] splitPoints, final double[] buckets) {
    int i = fromIndex;
    int j = 0;
    while (i <  toIndex && j < splitPoints.length) {
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

    // It is important to do add the new top level right here. Be aware that this operation grows the buffer
    // and shifts the data and also the boundaries of the data and grows the levels array and increments numLevels_
    if (level == numLevels_ - 1) {
      addEmptyTopLevelToCompletelyFullSketch();
    }

    final int rawBeg = levels_[level];
    final int rawLim = levels_[level + 1];
    final int popAbove = levels_[level + 2] - rawLim; // +2 is OK because we already added a new top level if necessary
    final int rawPop = rawLim - rawBeg;
    final boolean oddPop = isOdd(rawPop);
    final int adjBeg = oddPop ? rawBeg + 1 : rawBeg;
    final int adjPop = oddPop ? rawPop - 1 : rawPop;
    final int halfAdjPop = adjPop / 2;

    // level zero might not be sorted, so we must sort it if we wish to compact it
    if (level == 0) {
      sortLevelZero();
    }
    if (popAbove == 0) {
      randomlyHalveUp(items_, adjBeg, adjPop);
    } else {
      randomlyHalveDown(items_, adjBeg, adjPop);
      mergeSortedArrays(items_, adjBeg, halfAdjPop, items_, rawLim, popAbove, items_, adjBeg + halfAdjPop);
    }
    levels_[level + 1] -= halfAdjPop; // adjust boundaries of the level above
    if (oddPop) {
      levels_[level] = levels_[level + 1] - 1; // the current level now contains one item
      items_[levels_[level]] = items_[rawBeg]; // namely this leftover guy
    } else {
      levels_[level] = levels_[level + 1]; // the current level is now empty
    }

    // verify that we freed up halfAdjPop array slots just below the current level
    assert levels_[level] == rawBeg + halfAdjPop;

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

  private static void mergeSortedArrays(final float[] bufA, final int startA, final int lenA, final float[] bufB,
      final int startB, final int lenB, final float[] bufC, final int startC) {
    final int lenC = lenA + lenB;
    final int limA = startA + lenA;
    final int limB = startB + lenB;
    final int limC = startC + lenC;

    int a = startA;
    int b = startB;

    for (int c = startC; c < limC; c++) {
      if (a == limA) {
        bufC[c] = bufB[b];
        b++;
      } else if (b == limB) {
        bufC[c] = bufA[a];
        a++;
      } else if (bufA[a] < bufB[b]) { 
        bufC[c] = bufA[a];
        a++;
      } else {
        bufC[c] = bufB[b];
        b++;
      }
    }
    assert a == limA;
    assert b == limB;
  }

  private static void randomlyHalveDown(final float[] buf, final int start, final int length) {
    assert isEven(length);
    final int half_length = length / 2;
    final int offset = random.nextInt(2);
    int j = start + offset;
    for (int i = start; i < start + half_length; i++) {
      buf[i] = buf[j];
      j += 2;
    }
  }

  private static void randomlyHalveUp(final float[] buf, final int start, final int length) {
    assert isEven(length);
    final int half_length = length / 2;
    final int offset = random.nextInt(2);
    int j = start + length - 1 - offset;
    for (int i = start + length - 1; i >= start + half_length; i--) {
      buf[i] = buf[j];
      j -= 2;
    }
  }

  private static boolean isEven(final int value) {
    return (value & 1) == 0;
  }

  private static boolean isOdd(final int value) {
    return (value & 1) > 0;
  }

  private int findLevelToCompact() {
    int level = 0;
    while (true) {
      assert level < numLevels_;
      final int pop = levels_[level + 1] - levels_[level];
      final int cap = levelCapacity(k_, numLevels_, level, m_);
      if (pop >= cap) {
        return level;
      }
      level++;
    }
  }

  private void addEmptyTopLevelToCompletelyFullSketch() {
    final int cur_total_cap = levels_[numLevels_];

    // make sure that we are following a certain growth scheme
    assert levels_[0] == 0; 
    assert items_.length == cur_total_cap;

    // note that merging MIGHT over-grow levels_, in which case we might not have to grow it here
    if (levels_.length < numLevels_ + 2) {
      levels_ = growIntArray(levels_, numLevels_ + 2);
    }

    final int deltaCap = levelCapacity(k_, numLevels_ + 1, 0, m_);
    final int newTotalCap = cur_total_cap + deltaCap;

    final float[] newBuf = new float[newTotalCap];

    // copy (and shift) the current data into the new buffer
    System.arraycopy(items_, levels_[0], newBuf, levels_[0] + deltaCap, cur_total_cap);
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
    int tmpSpaceNeeded = getNumRetained() + other.getNumRetainedAboveLevelZero();
    final float[] workbuf = new float[tmpSpaceNeeded];
    final int ub = ubOnNumLevels(finalN);
    final int[] worklevels = new int[ub + 2]; // ub+1 does not work
    final int[] outlevels  = new int[ub + 2];

    final int provisionalNumLevels = Math.max(numLevels_, other.numLevels_);

    populateWorkArrays(other, workbuf, worklevels, provisionalNumLevels);

    // notice that workbuf is being used as both the input and output here
    final int[] result = generalCompress(k_, m_, provisionalNumLevels, workbuf, worklevels, workbuf,
        outlevels, isLevelZeroSorted_);
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

    for (int lvl = 0; lvl < finalNumLevels + 1; lvl++) { // includes the "extra" index
      levels_[lvl] = outlevels[lvl] + theShift;
    }

    items_ = newbuf;
    numLevels_ = finalNumLevels;
  }

  private void populateWorkArrays(final KllFloatsSketch other, final float[] workbuf, final int[] worklevels, final int provisionalNumLevels) {
    worklevels[0] = 0;

    // Note: the level zero data from "other" was already inserted into "self"
    final int selfPopZero = safeLevelSize(0);
    System.arraycopy(items_, levels_[0], workbuf, worklevels[0], selfPopZero);
    worklevels[1] = worklevels[0] + selfPopZero;

    for (int lvl = 1; lvl < provisionalNumLevels; lvl++) {
      final int selfPop = safeLevelSize(lvl);
      final int otherPop = other.safeLevelSize(lvl);
      worklevels[lvl + 1] = worklevels[lvl] + selfPop + otherPop;

      if (selfPop > 0 && otherPop == 0) {
        System.arraycopy(items_, levels_[lvl], workbuf, worklevels[lvl], selfPop);
      } else if (selfPop == 0 && otherPop > 0) {
        System.arraycopy(other.items_, other.levels_[lvl], workbuf, worklevels[lvl], otherPop);
      } else if (selfPop > 0 && otherPop > 0) {
        mergeSortedArrays(items_, levels_[lvl], selfPop, other.items_, other.levels_[lvl], otherPop, workbuf, worklevels[lvl]);
      }
    }
  }

  private int safeLevelSize(final int level) {
    if (level >= numLevels_) { return 0; }
    return levels_[level + 1] - levels_[level];
  }

  private static int ubOnNumLevels(final long n) {
    if (n == 0) { return 1; }
    return 1 + floorOfLog2OfFraction(n, 1);
  }

  static int floorOfLog2OfFraction(final long numer, long denom) {
    if (denom > numer) { return 0; }
    int count = 0;
    while (true) {
      denom <<= 1;
      if (denom > numer) { return count; }
      count++;
    }
  }

 private int getNumRetainedAboveLevelZero() {
    if (numLevels_ == 1) { return 0; }
    return levels_[numLevels_] - levels_[1];
  }

  private static int[] growIntArray(final int[] oldArr, final int newLen) {
    final int oldLen = oldArr.length;
    assert newLen > oldLen;
    final int[] newArr = new int[newLen];
    System.arraycopy(oldArr, 0, newArr, 0, oldLen);
    return newArr;
  }

  private void assertCorrectTotalWeight() {
    long total = sumTheSampleWeights(numLevels_, levels_);
    assert total == n_;
  }

  private static long sumTheSampleWeights(final int num_levels, final int[] levels) {
    long total = 0;
    long weight = 1;
    for (int lvl = 0; lvl < num_levels; lvl++) {
      total += weight * (levels[lvl + 1] - levels[lvl]);
      weight *= 2;
    }
    return total;
  }

  private static int levelCapacity(final int k, final int numLevels, final int height, final int minWid) {
    assert height >= 0;
    assert height < numLevels;
    final int depth = numLevels - height - 1;
    return Math.max(minWid, intCapAux(k, depth));
  }

  static int intCapAux(final int k, final int depth) {
    assert (k <= (1 << 30));  
    assert (depth <= 60);
    if (depth <= 30) { return intCapAuxAux(k, depth); }
    final int half = depth / 2;
    final int rest = depth - half;
    final int tmp = intCapAuxAux(k, half);
    return intCapAuxAux(tmp, rest);
  }

  // 0 <= power <= 30
  private static final long[] powersOfThree = new long[] {1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
  1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
  3486784401L, 10460353203L, 31381059609L, 94143178827L, 282429536481L,
  847288609443L, 2541865828329L, 7625597484987L, 22876792454961L, 68630377364883L,
  205891132094649L};

  private static int intCapAuxAux(final int k, final int depth) {
    assert (k <= (1 << 30));
    assert (depth <= 30);
    final int twok = k << 1; // for rounding, we pre-multiply by 2
    final int tmp = (int) (((long) twok << depth) / powersOfThree[depth]);
    final int result = ((tmp + 1) >> 1); // then here we add 1 and divide by 2
    assert (result <= k);
    return result;
  }

  /*
     Here is what we do for each level:
     If it does not need to be compacted, then simply copy it over.

     Otherwise, it does need to be compacted, so...
       Copy zero or one guy over.
       If the level above is empty, halve up.
       Else the level above is nonempty, so...
            halve down, then merge up.
       Adjust the boundaries of the level above.

   * It can be proved that generalCompress returns a sketch that satisfies the space constraints no matter how much data is passed in
   * We are pretty sure that it works correctly when inBuf and outBuf are the same.
   * All levels except for level zero must be sorted before calling this, and will still be sorted afterwards.
   * Level zero is not required to be sorted before, and may not be sorted afterwards.

   * trashes inBuf and inLevels
   * modifies outBuf and outLevels

   * returns (finalNumLevels, finalCapacity, finalItemCount)
   */
  private static int[] generalCompress(final int k, final int m, final int numLevelsIn, final float[] inBuf,
      final int[] inLevels, final float[] outBuf, final int[] outLevels, final boolean isLevelZeroSorted) {
    assert numLevelsIn > 0; // things are too weird if zero levels are allowed
    int numLevels = numLevelsIn;
    int currentItemCount = inLevels[numLevels] - inLevels[0]; // decreases with each compaction
    int targetItemCount = computeTotalCapacity(k, m, numLevels); // increases if we add levels
    boolean doneYet = false;
    outLevels[0] = 0;
    int curLevel = -1;
    while (!doneYet) {
      curLevel++; // start out at level 0

      // If we are at the current top level, add an empty level above it for convenience,
      // but do not increment numLevels until later
      if (curLevel == numLevels - 1) {
        inLevels[curLevel + 2] = inLevels[curLevel + 1];
      }

      final int rawBeg = inLevels[curLevel];
      final int rawLim = inLevels[curLevel + 1];
      final int rawPop = rawLim - rawBeg;

      if (currentItemCount < targetItemCount || rawPop < levelCapacity(k, numLevels, curLevel, m)) {
        // copy level over as is
        // because inBuf and outBuf could be the same, make sure we are not moving data upwards!
        assert (rawBeg >= outLevels[curLevel]);
        System.arraycopy(inBuf, rawBeg, outBuf, outLevels[curLevel], rawPop);
        outLevels[curLevel + 1] = outLevels[curLevel] + rawPop;
      }
      else {
        // The sketch is too full AND this level is too full, so we compact it
        // Note: this can add a level and thus change the sketches capacities

        final int popAbove = inLevels[curLevel + 2] - rawLim;
        final boolean oddPop = isOdd(rawPop);
        final int adjBeg = oddPop ? 1 + rawBeg : rawBeg;
        final int adjPop = oddPop ? rawPop - 1 : rawPop;
        final int halfAdjPop = adjPop / 2;

        if (oddPop) { // copy one guy over
          outBuf[outLevels[curLevel]] = inBuf[rawBeg];
          outLevels[curLevel + 1] = outLevels[curLevel] + 1;
        } else { // copy zero guys over
          outLevels[curLevel + 1] = outLevels[curLevel];
        }

        // level zero might not be sorted, so we must sort it if we wish to compact it
        if (curLevel == 0 && !isLevelZeroSorted) {
          Arrays.sort(inBuf, adjBeg, adjBeg + adjPop);
        }

        if (popAbove == 0) { // Level above is empty, so halve up
          randomlyHalveUp(inBuf, adjBeg, adjPop);
        } else { // Level above is nonempty, so halve down, then merge up
          randomlyHalveDown(inBuf, adjBeg, adjPop);
          mergeSortedArrays(inBuf, adjBeg, halfAdjPop, inBuf, rawLim, popAbove, inBuf, adjBeg + halfAdjPop);
        }

        // track the fact that we just eliminated some data
        currentItemCount -= halfAdjPop;

        // Adjust the boundaries of the level above
        inLevels[curLevel + 1] = inLevels[curLevel + 1] - halfAdjPop;

        // Increment numLevels if we just compacted the old top level
        // This creates some more capacity (the size of the new bottom level)
        if (curLevel == numLevels - 1) {
          numLevels++;
          targetItemCount += levelCapacity(k, numLevels, 0, m);
        }

      } // end of code for compacting a level

      // determine whether we have processed all levels yet (including any new levels that we created)

      if (curLevel == numLevels - 1) { doneYet = true; }

    } // end of loop over levels

    assert outLevels[numLevels] - outLevels[0] == currentItemCount;

    return new int[] {numLevels, targetItemCount, currentItemCount};
  }

  private static int computeTotalCapacity(int k, int m, int numLevels) {
    int total = 0;
    for (int h = 0; h < numLevels; h++) {
      total += levelCapacity(k, numLevels, h, m);
    }
    return total;
  }

  private static int getSerializedSizeBytes(final int numLevels, final int numRetained) {
    return DATA_START + (numLevels + 1) * Integer.BYTES + (numRetained + 2) * Float.BYTES; // + 2 for min and max
  }

}
