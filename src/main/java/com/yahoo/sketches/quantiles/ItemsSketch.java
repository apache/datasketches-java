/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractN;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.Util.computeBaseBufferItems;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.QuantilesHelper;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * This is a stochastic streaming sketch that enables near-real time analysis of the
 * approximate distribution of comparable items from a very large stream in a single pass.
 * The analysis is obtained using a getQuantiles(*) function or its inverse functions the
 * Probability Mass Function from getPMF(*) and the Cumulative Distribution Function from getCDF(*).
 *
 * <p>The documentation for {@link DoublesSketch} applies here except that the size of an ItemsSketch
 * is very dependent on the Items input into the sketch, so there is no comparable size table as
 * for the DoublesSketch.
 *
 * <p>There is more documentation available on
 * <a href="https://datasketches.github.io">DataSketches.GitHub.io</a>.</p>
 *
 * @param <T> type of item
 *
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
public final class ItemsSketch<T> {

  private final Comparator<? super T> comparator_;

  /**
   * Parameter that controls space usage of sketch and accuracy of estimates.
   */
  final int k_;

  /**
   * Total number of data items in the stream so far. (Uniqueness plays no role in these sketches).
   */
  long n_;

  /**
   * The smallest value ever seen in the stream.
   */
  T minValue_;

  /**
   * The largest value ever seen in the stream.
   */
  T maxValue_;

  /**
   * In the initial on-heap version, equals combinedBuffer_.length.
   * May differ in later versions that grow space more aggressively.
   * Also, in the off-heap version, combinedBuffer_ won't even be a java array,
   * so it won't know its own length.
   */
  int combinedBufferItemCapacity_;

  /**
   * Number of samples currently in base buffer.
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

  /**
   * Setting the seed makes the results of the sketch deterministic if the input values are
   * received in exactly the same order. This is only useful when performing test comparisons,
   * otherwise is not recommended.
   */
  public static final Random rand = new Random();

  private ItemsSketch(final int k, final Comparator<? super T> comparator) {
    Util.checkK(k);
    k_ = k;
    comparator_ = comparator;
  }

  /**
   * Obtains a new instance of an ItemsSketch using the DEFAULT_K.
   * @param <T> type of item
   * @param comparator to compare items
   * @return a GenericQuantileSketch
   */
  public static <T> ItemsSketch<T> getInstance(final Comparator<? super T> comparator) {
    return getInstance(PreambleUtil.DEFAULT_K, comparator);
  }

  /**
   * Obtains a new instance of an ItemsSketch.
   * @param <T> type of item
   * @param k Parameter that controls space usage of sketch and accuracy of estimates.
   * Must be greater than 2 and less than 65536 and a power of 2.
   * @param comparator to compare items
   * @return a GenericQuantileSketch
   */
  public static <T> ItemsSketch<T> getInstance(final int k, final Comparator<? super T> comparator) {
    final ItemsSketch<T> qs = new ItemsSketch<>(k, comparator);
    final int bufAlloc = 2 * Math.min(DoublesSketch.MIN_K, k); //the min is important
    qs.n_ = 0;
    qs.combinedBufferItemCapacity_ = bufAlloc;
    qs.combinedBuffer_ = new Object[bufAlloc];
    qs.baseBufferCount_ = 0;
    qs.bitPattern_ = 0;
    qs.minValue_ = null;
    qs.maxValue_ = null;
    return qs;
  }

  /**
   * Heapifies the given srcMem, which must be a Memory image of a ItemsSketch
   * @param <T> type of item
   * @param srcMem a Memory image of a sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param comparator to compare items
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return a ItemsSketch on the Java heap.
   */
  public static <T> ItemsSketch<T> getInstance(final Memory srcMem,
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

    if ((serVer == 3) && ((flags & COMPACT_FLAG_MASK) == 0)) {
      throw new SketchesArgumentException("Non-compact Memory images are not supported.");
    }

    final boolean empty = Util.checkPreLongsFlagsCap(preambleLongs, flags, memCapBytes);
    Util.checkFamilyID(familyID);

    final ItemsSketch<T> qs = getInstance(k, comparator); //checks k
    if (empty) { return qs; }

    //Not empty, must have valid preamble + min, max
    final long n = extractN(srcMem);

    //can't check memory capacity here, not enough information
    final int extra = 2; //for min, max
    final int numMemItems = Util.computeRetainedItems(k, n) + extra;

    //set class members
    qs.n_ = n;
    qs.combinedBufferItemCapacity_ = Util.computeCombinedBufferItemCapacity(k, n);
    qs.baseBufferCount_ = computeBaseBufferItems(k, n);
    qs.bitPattern_ = computeBitPattern(k, n);
    qs.combinedBuffer_ = new Object[qs.combinedBufferItemCapacity_];

    final int srcMemItemsOffsetBytes = preambleLongs * Long.BYTES;
    final Memory mReg = srcMem.region(srcMemItemsOffsetBytes,
        srcMem.getCapacity() - srcMemItemsOffsetBytes);
    final T[] itemsArray = serDe.deserializeFromMemory(mReg, numMemItems);
    qs.itemsArrayToCombinedBuffer(itemsArray);
    return qs;
  }

  /**
   * Returns a copy of the given sketch
   * @param <T> the data type
   * @param sketch the given sketch
   * @return a copy of the given sketch
   */
  static <T> ItemsSketch<T> copy(final ItemsSketch<T> sketch) {
    final ItemsSketch<T> qsCopy = ItemsSketch.getInstance(sketch.k_, sketch.comparator_);
    qsCopy.n_ = sketch.n_;
    qsCopy.minValue_ = sketch.getMinValue();
    qsCopy.maxValue_ = sketch.getMaxValue();
    qsCopy.combinedBufferItemCapacity_ = sketch.getCombinedBufferAllocatedCount();
    qsCopy.baseBufferCount_ = sketch.getBaseBufferCount();
    qsCopy.bitPattern_ = sketch.getBitPattern();
    final Object[] combBuf = sketch.getCombinedBuffer();
    qsCopy.combinedBuffer_ = Arrays.copyOf(combBuf, combBuf.length);
    return qsCopy;
  }

  /**
   * Updates this sketch with the given double data item
   * @param dataItem an item from a stream of items. NaNs are ignored.
   */
  public void update(final T dataItem) {
    // this method only uses the base buffer part of the combined buffer

    if (dataItem == null) { return; }
    if ((maxValue_ == null) || (comparator_.compare(dataItem, maxValue_) > 0)) { maxValue_ = dataItem; }
    if ((minValue_ == null) || (comparator_.compare(dataItem, minValue_) < 0)) { minValue_ = dataItem; }

    if ((baseBufferCount_ + 1) > combinedBufferItemCapacity_) {
      ItemsSketch.growBaseBuffer(this);
    }
    combinedBuffer_[baseBufferCount_++] = dataItem;
    n_++;
    if (baseBufferCount_ == (2 * k_)) {
      ItemsUtil.processFullBaseBuffer(this);
    }
  }

  /**
   * This returns an approximation to the value of the data item
   * that would be preceded by the given fraction of a hypothetical sorted
   * version of the input stream so far.
   *
   * <p>We note that this method has a fairly large overhead (microseconds instead of nanoseconds)
   * so it should not be called multiple times to get different quantiles from the same
   * sketch. Instead use getQuantiles(). which pays the overhead only once.
   *
   * @param fraction the specified fractional position in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * If fraction = 0.0, the true minimum value of the stream is returned.
   * If fraction = 1.0, the true maximum value of the stream is returned.
   *
   * @return the approximation to the value at the above fraction
   */
  public T getQuantile(final double fraction) {
    if ((fraction < 0.0) || (fraction > 1.0)) {
      throw new SketchesArgumentException("Fraction cannot be less than zero or greater than 1.0");
    }
    if      (fraction == 0.0) { return minValue_; }
    else if (fraction == 1.0) { return maxValue_; }
    else {
      final ItemsAuxiliary<T> aux = constructAuxiliary();
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
  public T getQuantileUpperBound(final double fraction) {
    return getQuantile(min(1.0, fraction + Util.getNormalizedRankError(k_, false)));
  }

  /**
   * Gets the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public T getQuantileLowerBound(final double fraction) {
    return getQuantile(max(0, fraction - Util.getNormalizedRankError(k_, false)));
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
   * @param fRanks the given array of fractional (or normalized) ranks in the hypothetical
   * sorted stream of all the input values seen so far.
   * These fRanks must all be in the interval [0.0, 1.0] inclusively.
   *
   * @return array of approximate quantiles of the given fRanks in the same order as in the given
   * fRanks array.
   */
  public T[] getQuantiles(final double[] fRanks) {
    if (isEmpty()) { return null; }
    ItemsAuxiliary<T> aux = null;
    @SuppressWarnings("unchecked")
    final T[] quantiles = (T[]) Array.newInstance(minValue_.getClass(), fRanks.length);
    for (int i = 0; i < fRanks.length; i++) {
      final double fRank = fRanks[i];
      if      (fRank == 0.0) { quantiles[i] = minValue_; }
      else if (fRank == 1.0) { quantiles[i] = maxValue_; }
      else {
        if (aux == null) {
          aux = this.constructAuxiliary();
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
   * @param evenlySpaced an integer that specifies the number of evenly spaced fractional ranks.
   * This must be a positive integer greater than 0. A value of 1 will return the min value.
   * A value of 2 will return the min and the max value. A value of 3 will return the min,
   * the median and the max value, etc.
   *
   * @return array of approximations to the given fractions in the same order as given fractions
   * array.
   */
  public T[] getQuantiles(final int evenlySpaced) {
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
  @SuppressWarnings("unchecked")
  public double getRank(final T value) {
    if (isEmpty()) { return Double.NaN; }
    long total = 0;
    int weight = 1;
    for (int i = 0; i < baseBufferCount_; i++) {
      if (comparator_.compare((T) combinedBuffer_[i], value) < 0) {
        total += weight;
      }
    }
    long bitPattern = bitPattern_;
    for (int lvl = 0; bitPattern != 0L; lvl++, bitPattern >>>= 1) {
      weight *= 2;
      if ((bitPattern & 1L) > 0) { // level is not empty
        final int offset = (2 + lvl) * k_;
        for (int i = 0; i < k_; i++) {
          if (comparator_.compare((T) combinedBuffer_[i + offset], value) < 0) {
            total += weight;
          } else {
            break; // levels are sorted, no point comparing further
          }
        }
      }
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
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing item values
   * that divide the ordered space into <i>m+1</i> consecutive disjoint intervals.
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
  public double[] getPMF(final T[] splitPoints) {
    if (isEmpty()) { return null; }
    return ItemsPmfCdfImpl.getPMFOrCDF(this, splitPoints, false);
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of splitPoints (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that be obtained from the
   * getNormalizedRankError(false) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing item values
   * that divide the ordered space into <i>m+1</i> consecutive disjoint intervals.
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
  public double[] getCDF(final T[] splitPoints) {
    if (isEmpty()) { return null; }
    return ItemsPmfCdfImpl.getPMFOrCDF(this, splitPoints, true);
  }

  /**
   * Returns the configured value of K
   * @return the configured value of K
   */
  public int getK() {
    return k_;
  }

  /**
   * Returns the min value of the stream
   * @return the min value of the stream
   */
  public T getMinValue() {
    return minValue_;
  }

  /**
   * Returns the max value of the stream
   * @return the max value of the stream
   */
  public T getMaxValue() {
    return maxValue_;
  }

  /**
   * Returns the length of the input stream so far.
   * @return the length of the input stream so far
   */
  public long getN() {
    return n_;
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
   * <p>For example, choosing a K of 227 yields a normalized rank error of about 1%.
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
   * @param k the configuration parameter of a ItemsSketch
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

  @SuppressWarnings("static-method")
  public boolean isDirect() {
    return false;
  }

  public boolean isEstimationMode() {
    return getN() >= (2L * k_);
  }

  /**
   * Resets this sketch to a virgin state, but retains the original value of k.
   */
  public void reset() {
    n_ = 0;
    combinedBufferItemCapacity_ = 2 * Math.min(DoublesSketch.MIN_K, k_); //the min is important
    combinedBuffer_ = new Object[combinedBufferItemCapacity_];
    baseBufferCount_ = 0;
    bitPattern_ = 0;
    minValue_ = null;
    maxValue_ = null;
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
  public byte[] toByteArray(final boolean ordered, final ArrayOfItemsSerDe<T> serDe) {
    return ItemsByteArrayImpl.toByteArray(this, ordered, serDe);
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
   * From an existing sketch, this creates a new sketch that can have a smaller value of K.
   * The original sketch is not modified.
   *
   * @param newK the new value of K that must be smaller than current value of K.
   * It is required that this.getK() = newK * 2^(nonnegative integer).
   * @return the new sketch.
   */
  public ItemsSketch<T> downSample(final int newK) {
    final ItemsSketch<T> newSketch = ItemsSketch.getInstance(newK, comparator_);
    ItemsMergeImpl.downSamplingMergeInto(this, newSketch);
    return newSketch;
  }

  /**
   * Computes the number of retained entries (samples) in the sketch
   * @return the number of retained entries (samples) in the sketch
   */
  public int getRetainedItems() {
    return Util.computeRetainedItems(getK(), getN());
  }

  /**
   * Puts the current sketch into the given Memory if there is sufficient space.
   * Otherwise, throws an error.
   *
   * @param dstMem the given memory.
   * @param serDe an instance of ArrayOfItemsSerDe
   */
  public void putMemory(final WritableMemory dstMem, final ArrayOfItemsSerDe<T> serDe) {
    final byte[] byteArr = toByteArray(serDe);
    final long memCap = dstMem.getCapacity();
    if (memCap < byteArr.length) {
      throw new SketchesArgumentException(
          "Destination Memory not large enough: " + memCap + " < " + byteArr.length);
    }
    dstMem.putByteArray(0, byteArr, 0, byteArr.length);
  }

  public ItemsSketchIterator<T> iterator() {
    return new ItemsSketchIterator<>(this, bitPattern_);
  }

  // Restricted

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
    final int extra = 2; // space for min and max values

    //Load min, max
    minValue_ = itemsArray[0];
    maxValue_ = itemsArray[1];

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

  /**
   * Returns the Auxiliary data structure which is only used for getQuantile() and getQuantiles()
   * queries.
   * @return the Auxiliary data structure
   */
  private ItemsAuxiliary<T> constructAuxiliary() {
    return new ItemsAuxiliary<>(this);
  }

  private static <T> void growBaseBuffer(final ItemsSketch<T> sketch) {
    final Object[] baseBuffer = sketch.getCombinedBuffer();
    final int oldSize = sketch.getCombinedBufferAllocatedCount();
    final int k = sketch.getK();
    assert oldSize < (2 * k);
    final int newSize = Math.max(Math.min(2 * k, 2 * oldSize), 1);
    sketch.combinedBufferItemCapacity_ = newSize;
    sketch.combinedBuffer_ = Arrays.copyOf(baseBuffer, newSize);
  }

}
