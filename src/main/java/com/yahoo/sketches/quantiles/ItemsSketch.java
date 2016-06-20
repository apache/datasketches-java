/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.BUFFER_DOUBLES_ALLOC_INT;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.N_LONG;
import static com.yahoo.sketches.quantiles.PreambleUtil.PREAMBLE_LONGS;
import static com.yahoo.sketches.quantiles.PreambleUtil.SER_VER;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSketchType;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSketchType;
import static com.yahoo.sketches.quantiles.Util.computeBaseBufferCount;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.NativeMemory;


/**
 * This is a stochastic streaming sketch that enables near-real time analysis of the 
 * approximate distribution of values from a very large stream in a single pass. 
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
 * getQuantile(0.95).</p>
 * 
 * <p>If you have prior knowledge of the approximate range of values, for example, 1 to 1000 bytes,
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
Table Guide for QuantilesSketch Size in Bytes and Approximate Error:
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
 * <a href="http://datasketches.github.io">DataSketches.GitHub.io</a>.</p>
 * 
 * <p>This is an implementation of the Low Discrepancy Mergeable Quantiles Sketch, using arbitrary
 * comparable values, described in section 3.2 of the journal version of the paper
 * "Mergeable Summaries" by Agarwal, Cormode, Huang, Phillips, Wei, and Yi. 
 * <a href="http://dblp.org/rec/html/journals/tods/AgarwalCHPWY13"></a></p>
 * 
 * <p>This algorithm is independent of the distribution of values.
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
 * @param <T> type of item
 */
public class ItemsSketch<T> {

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
  protected int combinedBufferAllocatedCount_;

  /**
   * Number of samples currently in base buffer.
   * 
   * Count = N % (2*K)
   */
  protected int baseBufferCount_;

  /**
   * Active levels expressed as a bit pattern.
   * 
   * Pattern = N / (2 * K)
   */
  protected long bitPattern_;

  /**
   * This single array contains the base buffer plus all levels some of which may not be used.
   * A level is of size K and is either full and sorted, or not used. A "not used" buffer may have
   * garbage. Whether a level buffer used or not is indicated by the bitPattern_.
   * The base buffer has length 2*K but might not be full and isn't necessarily sorted.
   * The base buffer precedes the level buffers. 
   * 
   * The levels arrays require quite a bit of explanation, which we defer until later.
   */
  protected Object[] combinedBuffer_;

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
   * Obtains an instance of a GenericQuantileSketch.
   * @param <T> type of item
   * @param comparator to compare items 
   * @return a GenericQuantileSketch
   */
  public static <T> ItemsSketch<T> getInstance(final Comparator<? super T> comparator) {
    return getInstance(DEFAULT_K, comparator);
  }

  /**
   * Obtains an instance of a GenericQuantileSketch.
   * @param <T> type of item
   * @param k Parameter that controls space usage of sketch and accuracy of estimates. 
   * Must be greater than 0 and less than 65536.
   * It is recommended that <i>k</i> be a power of 2 to enable merging of sketches with
   * different values of <i>k</i>.
   * @param comparator to compare items
   * @return a GenericQuantileSketch
   */
  public static <T> ItemsSketch<T> getInstance(final int k, final Comparator<? super T> comparator) {
    final ItemsSketch<T> qs = new ItemsSketch<T>(k, comparator);
    final int bufAlloc = Math.min(Util.MIN_BASE_BUF_SIZE, 2 * k); //the min is important
    qs.n_ = 0;
    qs.combinedBufferAllocatedCount_ = bufAlloc;
    qs.combinedBuffer_ = new Object[bufAlloc];
    qs.baseBufferCount_ = 0;
    qs.bitPattern_ = 0;
    qs.minValue_ = null;
    qs.maxValue_ = null;
    return qs;
  }

  /**
   * Heapifies the given srcMem, which must be a Memory image of a GenericQuantilesSketch
   * @param <T> type of item
   * @param srcMem a Memory image of a sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param comparator to compare items
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return a GenericQuantilesSketch on the Java heap.
   */
  public static <T> ItemsSketch<T> getInstance(final Memory srcMem,
      final Comparator<? super T> comparator, final ArrayOfItemsSerDe<T> serDe) {
    final long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) {
      throw new IllegalArgumentException("Memory too small: " + memCapBytes);
    }
    final long pre0 = srcMem.getLong(0);
    final int preambleLongs = extractPreLongs(pre0);
    final int serVer = extractSerVer(pre0);
    final int familyID = extractFamilyID(pre0);
    final int flags = extractFlags(pre0);
    final int k = extractK(pre0);
    final byte type = extractSketchType(pre0);

    if (type != serDe.getType()) {
      throw new IllegalArgumentException(
          "Possible Corruption: Sketch Type incorrect: " + type + " != " + serDe.getType());
    }

    final boolean empty = Util.checkPreLongsFlagsCap(preambleLongs, flags, memCapBytes);
    Util.checkFamilyID(familyID);
    Util.checkSerVer(serVer);

    final ItemsSketch<T> qs = getInstance(k, comparator);

    if (empty) return qs;

    //Not empty, must have valid preamble
    final long[] remainderPreArr = new long[preambleLongs - 1];
    srcMem.getLongArray(Long.BYTES, remainderPreArr, 0, remainderPreArr.length);
  
    final long n = remainderPreArr[(N_LONG >> 3) - 1];
    final int numValidItems = (int) remainderPreArr[(BUFFER_DOUBLES_ALLOC_INT >> 3) - 1];
  
    //set class members
    qs.n_ = n;
    qs.combinedBufferAllocatedCount_ = Util.bufferElementCapacity(k, n);
    qs.baseBufferCount_ = computeBaseBufferCount(k, n);
    qs.bitPattern_ = computeBitPattern(k, n);
    qs.combinedBuffer_ = new Object[qs.combinedBufferAllocatedCount_];
    final int itemsOffset = preambleLongs * Long.BYTES;
    final T[] validItems = serDe.deserializeFromMemory(new MemoryRegion(srcMem, itemsOffset, srcMem.getCapacity() - itemsOffset), numValidItems);
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
    qsCopy.combinedBufferAllocatedCount_ = sketch.getCombinedBufferAllocatedCount();
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
  
    if (baseBufferCount_ + 1 > combinedBufferAllocatedCount_) {
      ItemsUtil.growBaseBuffer(this);
    } 
    combinedBuffer_[baseBufferCount_++] = dataItem;
    n_++;
    if (baseBufferCount_ == 2*k_) {
      ItemsUtil.processFullBaseBuffer(this);
    }
  }
  
  /**
   * This returns an approximation to the value of the data item
   * that would be preceded by the given fraction of a hypothetical sorted
   * version of the input stream so far.
   * 
   * <p>
   * We note that this method has a fairly large overhead (microseconds instead of nanoseconds)
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
      throw new IllegalArgumentException("Fraction cannot be less than zero or greater than 1.0");
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
   * <p>
   * This returns an array that could have been generated by using getQuantile() with many different
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
   * array. 
   */
  public T[] getQuantiles(final double[] fractions) {
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
   * The resulting approximations have a probabilistic guarantee that be obtained from the 
   * getNormalizedRankError() function.
   * 
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing doubles
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * 
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream values that fell into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint.
   */
  public double[] getPMF(final T[] splitPoints) {
    final long[] counters = ItemsUtil.internalBuildHistogram(splitPoints, this);
    final int numCounters = counters.length;
    final double[] result = new double[numCounters];
    final double n = n_;
    long subtotal = 0;
    for (int j = 0; j < numCounters; j++) { 
      final long count = counters[j];
      subtotal += count;
      result[j] = count / n; //normalize by n
    }
    assert subtotal == n; //internal consistency check
    return result;
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the 
   * cumulative analog of the PMF, of the input stream given a set of splitPoint (values).
   * <p>
   * More specifically, the value at array position j of the CDF is the
   * sum of the values in positions 0 through j of the PMF.
   * 
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing doubles
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * 
   * @return an approximation to the CDF of the input stream given the splitPoints.
   */
  public double[] getCDF(final T[] splitPoints) {
    final long[] counters = ItemsUtil.internalBuildHistogram(splitPoints, this);
    final int numCounters = counters.length;
    final double[] result = new double[numCounters];
    final double n = n_;
    long subtotal = 0;
    for (int j = 0; j < numCounters; j++) { 
      final long count = counters[j];
      subtotal += count;
      result[j] = subtotal / n; //normalize by n
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
   * @param k the configuration parameter of a QuantilesSketch
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
    combinedBufferAllocatedCount_ = Math.min(Util.MIN_BASE_BUF_SIZE, 2 * k_); //the min is important
    combinedBuffer_ = new Object[combinedBufferAllocatedCount_];
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
    final int preLongs, sizeBytes, flags;
    final boolean empty = isEmpty();
    byte[] bytes = null;
    T[] validItems = null;
    if (empty) {
      preLongs = 1;
      sizeBytes = Long.BYTES;
      flags = EMPTY_FLAG_MASK;
    } else {
      preLongs = PREAMBLE_LONGS;
      flags = 0;
      validItems = getValidItemsPlusMinAndMax();
      bytes = serDe.serializeToByteArray(validItems);
      sizeBytes = preLongs * Long.BYTES + bytes.length;
    }
    final byte[] outArr = new byte[sizeBytes];
    final Memory mem = new NativeMemory(outArr);

    //build first prelong
    long pre0 = 0L;
    pre0 = insertPreLongs(preLongs, pre0);
    pre0 = insertSerVer(SER_VER, pre0);
    pre0 = insertFamilyID(Family.QUANTILES.getID(), pre0);
    //other flags: bigEndian = readOnly = compact = ordered = false
    pre0 = insertFlags(flags, pre0);
    pre0 = insertK(k_, pre0);
    pre0 = insertSketchType(serDe.getType(), pre0);

    if (empty) {
      mem.putLong(0, pre0);
    } else {
      final long[] preArr = new long[preLongs];
      preArr[0] = pre0;
      preArr[1] = n_;
      preArr[2] = validItems.length;
      mem.putLongArray(0, preArr, 0, preArr.length);
      mem.putByteArray(PREAMBLE_LONGS * Long.BYTES, bytes, 0, bytes.length);
    }
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
  public int getRetainedEntries() {
    final int k =  getK();
    final long n = getN();
    final int bbCnt = Util.computeBaseBufferCount(k, n);
    final long bitPattern = Util.computeBitPattern(k, n);
    final int validLevels = Long.bitCount(bitPattern);
    return bbCnt + validLevels * k; 
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
      throw new IllegalArgumentException(
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
    return combinedBufferAllocatedCount_;
  }

  /**
   * Returns the bit pattern for valid log levels
   * @return the bit pattern for valid log levels
   */
  protected long getBitPattern() {
    return bitPattern_;
  }

  private void putValidItemsPlusMinAndMax(T[] validItems) {
    System.arraycopy(validItems, 0, combinedBuffer_, 0, baseBufferCount_);
    int index = baseBufferCount_;
    long bits = getBitPattern();
    for (int level = 0; bits != 0L; level++, bits >>>= 1) {
      if ((bits & 1L) > 0L) {
        System.arraycopy(validItems, index, combinedBuffer_, (2 + level) * k_, k_);
        index += k_;
      }
    }
    minValue_ = validItems[index++];
    maxValue_ = validItems[index];
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
    final T[] validItems = (T[]) Array.newInstance(minValue_.getClass(), getRetainedEntries() + 2);
    System.arraycopy(combinedBuffer_, 0, validItems, 0, baseBufferCount_);
    int index = baseBufferCount_;
    long bits = getBitPattern();
    for (int level = 0; bits != 0L; level++, bits >>>= 1) {
      if ((bits & 1L) > 0L) {
        System.arraycopy(combinedBuffer_, (2 + level) * k_, validItems, index, k_);
        index += k_;
      }
    }
    validItems[index++] = minValue_;
    validItems[index] = maxValue_;
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
      throw new IllegalArgumentException("n must be > zero.");
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
