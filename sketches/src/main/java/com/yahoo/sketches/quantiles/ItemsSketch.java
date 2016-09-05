/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import static com.yahoo.sketches.quantiles.PreambleUtil.*;
import static com.yahoo.sketches.quantiles.Util.computeBaseBufferItems;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;

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
 * @author Alex Saydakov
 */
public final class ItemsSketch<T> {

  /**
   * Parameter that controls space usage of sketch and accuracy of estimates.
   */
  protected final int k_;

  private final Comparator<? super T> comparator_;

  /**
   * Total number of data items in the stream so far. (Uniqueness plays no role in these sketches).
   */
  protected long n_;

  /**
   * The smallest value ever seen in the stream.
   */
  protected T minValue_;

  /**
   * The largest value ever seen in the stream.
   */
  protected T maxValue_;

  /**
   * In the initial on-heap version, equals combinedBuffer_.length.
   * May differ in later versions that grow space more aggressively.
   * Also, in the off-heap version, combinedBuffer_ won't even be a java array,
   * so it won't know its own length.
   */
  protected int combinedBufferItemCapacity_;

  /**
   * Number of samples currently in base buffer.
   * 
   * <p>Count = N % (2*K)
   */
  protected int baseBufferCount_;

  /**
   * Active levels expressed as a bit pattern.
   * 
   * <p>Pattern = N / (2 * K)
   */
  protected long bitPattern_;

  /**
   * This single array contains the base buffer plus all levels some of which may not be used.
   * A level is of size K and is either full and sorted, or not used. A "not used" buffer may have
   * garbage. Whether a level buffer used or not is indicated by the bitPattern_.
   * The base buffer has length 2*K but might not be full and isn't necessarily sorted.
   * The base buffer precedes the level buffers. 
   * 
   * <p>The levels arrays require quite a bit of explanation, which we defer until later.
   */
  protected Object[] combinedBuffer_;

  /**
   * Setting the seed makes the results of the sketch deterministic if the input values are
   * received in exactly the same order. This is only useful when performing test comparisons,
   * otherwise is not recommended.
   */
  public static final Random rand = new Random();

  /**
   * Default value for about 1.7% normalized rank accuracy
   */
  static final int DEFAULT_K = 128;

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
    return getInstance(DEFAULT_K, comparator);
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
    final ItemsSketch<T> qs = new ItemsSketch<T>(k, comparator);
    final int bufAlloc = Math.min(Util.MIN_BASE_BUF_SIZE, 2 * k); //the min is important
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
      final Comparator<? super T> comparator, final ArrayOfItemsSerDe<T> serDe) {
    final long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) {
      throw new SketchesArgumentException("Memory too small: " + memCapBytes);
    }
    final long pre0 = srcMem.getLong(0);
    final int preambleLongs = extractPreLongs(pre0);
    final int serVer = extractSerVer(pre0);
    final int familyID = extractFamilyID(pre0);
    final int flags = extractFlags(pre0);
    final int k = extractK(pre0);
    final short serDeId = extractSerDeId(pre0);

    if (serDeId != serDe.getId()) {
      throw new SketchesArgumentException(
          "Possible Corruption: serDeId incorrect: " + serDeId + " != " + serDe.getId());
    }

    final boolean empty = Util.checkPreLongsFlagsCap(preambleLongs, flags, memCapBytes);
    Util.checkFamilyID(familyID);
    Util.checkSerVer(serVer);

    final ItemsSketch<T> qs = getInstance(k, comparator);

    if (empty) return qs;

    //Not empty, must have valid preamble
    final long n = srcMem.getLong(N_LONG); //pre1
    final int retainedItems = Util.computeRetainedItems(k, n) + 2; // 2 for min and max

    //set class members
    qs.n_ = n;
    qs.combinedBufferItemCapacity_ = Util.computeCombBufItemCapacity(k, n);
    qs.baseBufferCount_ = computeBaseBufferItems(k, n);
    qs.bitPattern_ = computeBitPattern(k, n);
    qs.combinedBuffer_ = new Object[qs.combinedBufferItemCapacity_];
    
    final int srcMemItemsOffsetBytes = preambleLongs * Long.BYTES;
    final T[] validItems = serDe.deserializeFromMemory(
        new MemoryRegion(srcMem, srcMemItemsOffsetBytes, srcMem.getCapacity() - srcMemItemsOffsetBytes), retainedItems);
    qs.putValidItemsPlusMinAndMax(validItems);
    return qs;
  }

  /**
   * Returns a copy of the given sketch
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
    Object[] combBuf = sketch.getCombinedBuffer();
    qsCopy.combinedBuffer_ = Arrays.copyOf(combBuf, combBuf.length);
    return qsCopy;
  }

  /** 
   * Updates this sketch with the given double data item
   * @param dataItem an item from a stream of items.  NaNs are ignored.
   */
  public void update(final T dataItem) {
    // this method only uses the base buffer part of the combined buffer

    if (dataItem == null) return;
    if (maxValue_ == null || comparator_.compare(dataItem, maxValue_) > 0) { maxValue_ = dataItem; }
    if (minValue_ == null || comparator_.compare(dataItem, minValue_) < 0) { minValue_ = dataItem; }

    if (baseBufferCount_ + 1 > combinedBufferItemCapacity_) {
      ItemsUtil.growBaseBuffer(this);
    } 
    combinedBuffer_[baseBufferCount_++] = dataItem;
    n_++;
    if (baseBufferCount_ == 2 * k_) {
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
   * This is a more efficient multiple-query version of getQuantile().
   * 
   * <p>This returns an array that could have been generated by using getQuantile() with many different
   * fractional ranks, but would be very inefficient. 
   * This method incurs the internal set-up overhead once and obtains multiple quantile values in 
   * a single query.  It is strongly recommend that this method be used instead of multiple calls 
   * to getQuantile().
   * 
   * @param fractions given array of fractional positions in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * These fractions must be monotonic, in increasing order and in the interval 
   * [0.0, 1.0] inclusive.
   * 
   * @return array of approximations to the given fractions in the same order as given fractions 
   * array. Returns null if sketch is empty
   */
  public T[] getQuantiles(final double[] fractions) {
    if (isEmpty()) return null;
    Util.validateFractions(fractions);
    ItemsAuxiliary<T> aux = null;
    @SuppressWarnings("unchecked")
    final T[] answers = (T[]) Array.newInstance(minValue_.getClass(), fractions.length);
    for (int i = 0; i < fractions.length; i++) {
      final double fraction = fractions[i];
      if      (fraction == 0.0) { answers[i] = minValue_; }
      else if (fraction == 1.0) { answers[i] = maxValue_; }
      else {
        if (aux == null) aux = this.constructAuxiliary();
        answers[i] = aux.getQuantile(fraction);
      }
    }
    return answers;
  }

  /**
   * This is also a more efficient multiple-query version of getQuantile() and allows the caller to
   * specify the number of evenly spaced fractional ranks.
   * 
   * 
   * @param evenlySpaced an integer that specifies the number of evenly spaced fractional ranks. 
   * This must be a positive integer greater than 0. A value of 1 will return the min value. 
   * A value of 2 will return the min and the max value. A value of 3 will return the min, 
   * the median and the max value, etc.
   * 
   * @return array of approximations to the given fractions in the same order as given fractions 
   * array. 
   */
  public T[] getQuantiles(int evenlySpaced) {
    return getQuantiles(getEvenlySpaced(evenlySpaced));
  }

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream 
   * given a set of splitPoints (values).
   * 
   * <p>The resulting approximations have a probabilistic guarantee that be obtained from the 
   * getNormalizedRankError() function.
   * 
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing values
   * that divide the domain into <i>m+1</i> consecutive disjoint intervals.
   * 
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream values that fell into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint.
   */
  public double[] getPMF(final T[] splitPoints) {
    return getPMFOrCDF(splitPoints, false);
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the 
   * cumulative analog of the PMF, of the input stream given a set of splitPoints (values).
   * 
   * <p>More specifically, the value at array position j of the CDF is the
   * sum of the values in positions 0 through j of the PMF.
   * 
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing values
   * that divide the domain into <i>m+1</i> consecutive disjoint intervals.
   * 
   * @return an approximation to the CDF of the input stream given the splitPoints.
   */
  public double[] getCDF(final T[] splitPoints) {
    return getPMFOrCDF(splitPoints, true);
  }

  private double[] getPMFOrCDF(final T[] splitPoints, boolean isCDF) {
    long[] counters = ItemsUtil.internalBuildHistogram(splitPoints, this);
    int numCounters = counters.length;
    double[] result = new double[numCounters];
    double n = n_;
    long subtotal = 0;
    if (isCDF) {
      for (int j = 0; j < numCounters; j++) {
        long count = counters[j];
        subtotal += count;
        result[j] = subtotal / n; //normalize by n
      }
    } else { // PMF
      for (int j = 0; j < numCounters; j++) {
        long count = counters[j];
        subtotal += count;
        result[j] = count / n; //normalize by n
      }
    }
    assert subtotal == n; //internal consistency check
    return result;
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
   */
  public double getNormalizedRankError() {
    return getNormalizedRankError(getK());
  }

  /**
   * Static method version of {@link #getNormalizedRankError()}
   * @param k the configuration parameter of a ItemsSketch
   * @return the rank error normalized as a fraction between zero and one.
   */
  public static double getNormalizedRankError(int k) {
    return Util.EpsilonFromK.getAdjustedEpsilon(k);
  }

  /**
   * Returns true if this sketch is empty
   * @return true if this sketch is empty
   */
  public boolean isEmpty() {
   return getN() == 0; 
  }

  /**
   * Resets this sketch to a virgin state, but retains the original value of k.
   */
  public void reset() {
    n_ = 0;
    combinedBufferItemCapacity_ = Math.min(Util.MIN_BASE_BUF_SIZE, 2 * k_); //the min is important
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
  @SuppressWarnings("null")
  public byte[] toByteArray(final ArrayOfItemsSerDe<T> serDe) {
    final int preLongs, numOutBytes, flags;
    final boolean empty = isEmpty();
    byte[] itemsByteArr = null;
    T[] validItems = null;
    
    if (empty) {
      preLongs = 1;
      numOutBytes = Long.BYTES;
      flags = EMPTY_FLAG_MASK;
    } else {
      preLongs = 2;
      flags = 0;
      validItems = getValidItemsPlusMinAndMax();
      itemsByteArr = serDe.serializeToByteArray(validItems);
      numOutBytes = preLongs * Long.BYTES + itemsByteArr.length; //includes min and max
    }
    //build prelong 0
    long pre0 = 0L;
    pre0 = insertPreLongs(preLongs, pre0);
    pre0 = insertSerVer(SER_VER, pre0);
    pre0 = insertFamilyID(Family.QUANTILES.getID(), pre0);
    //other flags: bigEndian = false
    pre0 = insertFlags(flags, pre0);
    pre0 = insertK(k_, pre0);
    pre0 = insertSerDeId(serDe.getId(), pre0);

    final byte[] outArr = new byte[numOutBytes];
    final Memory memOut = new NativeMemory(outArr);
    if (empty) {
      memOut.putLong(0, pre0);
      return outArr;
    }
    memOut.putLong(0, pre0);
    memOut.putLong(N_LONG, n_);
    memOut.putByteArray(preLongs * Long.BYTES, itemsByteArr, 0, itemsByteArr.length);
    return outArr;
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
   * From an existing sketch, this creates a new sketch that can have a smaller value of K.
   * The original sketch is not modified.
   * 
   * @param newK the new value of K that must be smaller than current value of K.
   * It is required that this.getK() = newK * 2^(nonnegative integer).
   * @return the new sketch.
   */
  public ItemsSketch<T> downSample(final int newK) {
    final ItemsSketch<T> newSketch = ItemsSketch.getInstance(newK, comparator_);
    ItemsUtil.downSamplingMergeInto(this, newSketch);
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
  public void putMemory(final Memory dstMem, final ArrayOfItemsSerDe<T> serDe) {
    final byte[] byteArr = toByteArray(serDe);
    final long memCap = dstMem.getCapacity();
    if (memCap < byteArr.length) {
      throw new SketchesArgumentException(
          "Destination Memory not large enough: " + memCap + " < " + byteArr.length);
    }
    dstMem.putByteArray(0, byteArr, 0, byteArr.length);
  }

  // Restricted

  /**
   * Returns the base buffer count
   * @return the base buffer count
   */
  protected int getBaseBufferCount() {
    return baseBufferCount_;
  }

  /**
   * Returns the allocated count for the combined base buffer
   * @return the allocated count for the combined base buffer
   */
  protected int getCombinedBufferAllocatedCount() {
    return combinedBufferItemCapacity_;
  }

  /**
   * Returns the bit pattern for valid log levels
   * @return the bit pattern for valid log levels
   */
  protected long getBitPattern() {
    return bitPattern_;
  }

  private void putValidItemsPlusMinAndMax(T[] validItems) {
    int index = 0;
    minValue_ = validItems[index++];
    maxValue_ = validItems[index++];
    System.arraycopy(validItems, index, combinedBuffer_, 0, baseBufferCount_);
    index += baseBufferCount_;
    long bits = getBitPattern();
    for (int level = 0; bits != 0L; level++, bits >>>= 1) {
      if ((bits & 1L) > 0L) {
        System.arraycopy(validItems, index, combinedBuffer_, (2 + level) * k_, k_);
        index += k_;
      }
    }
  }

  /**
   * Returns the combined buffer reference
   * @return the combined buffer reference
   */
  protected Object[] getCombinedBuffer() {
    return combinedBuffer_;
  }

  private T[] getValidItemsPlusMinAndMax() {
    // 2 more for min and max values
    @SuppressWarnings("unchecked")
    final T[] validItems = (T[]) Array.newInstance(minValue_.getClass(), getRetainedItems() + 2);
    int index = 0;
    validItems[index++] = minValue_;
    validItems[index++] = maxValue_;
    System.arraycopy(combinedBuffer_, 0, validItems, index, baseBufferCount_);
    index += baseBufferCount_;
    long bits = getBitPattern();
    for (int level = 0; bits != 0L; level++, bits >>>= 1) {
      if ((bits & 1L) > 0L) {
        System.arraycopy(combinedBuffer_, (2 + level) * k_, validItems, index, k_);
        index += k_;
      }
    }
    return validItems;
  }

  /**
   * Returns the Auxiliary data structure which is only used for getQuantile() and getQuantiles() 
   * queries.
   * @return the Auxiliary data structure
   */
  private ItemsAuxiliary<T> constructAuxiliary() {
    return new ItemsAuxiliary<T>(this);
  }

  private static double[] getEvenlySpaced(final int n) {
    if (n <= 0) {
      throw new SketchesArgumentException("n must be > zero.");
    }
    final double[] fractions = new double[n];
    fractions[0] = 0.0;
    for (int i = 1; i < n; i++) {
      fractions[i] = (double) i / (n - 1);
    }
    if (n > 1) {
      fractions[n - 1] = 1.0;
    }
    return fractions;
  }

  Comparator<? super T> getComparator() {
    return comparator_;
  }

}
