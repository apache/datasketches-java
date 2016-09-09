/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import java.util.Random;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfDoublesSerDe;
import com.yahoo.sketches.SketchesArgumentException;


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
 * <a href="http://dblp.org/rec/html/journals/tods/AgarwalCHPWY13"></a></p> <!-- does not work with https -->
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
 */
public abstract class DoublesSketch {
  
  static final short ARRAY_OF_DOUBLES_SERDE_ID = new ArrayOfDoublesSerDe().getId();
  
  /**
   * Parameter that controls space usage of sketch and accuracy of estimates.
   */
  protected final int k_;

  /**
   * Setting the seed makes the results of the sketch deterministic if the input values are
   * received in exactly the same order. This is only useful when performing test comparisons,
   * otherwise is not recommended.
   */
  public static final Random rand = new Random();

  /**
   * Default value for about 1.7% normalized rank accuracy
   */
  public static final int DEFAULT_K = 128;
  
  DoublesSketch(int k) {
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
   * Updates this sketch with the given double data item
   * @param dataItem an item from a stream of items.  NaNs are ignored.
   */
  public abstract void update(double dataItem);
  
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
  public abstract double getQuantile(double fraction);
  
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
   * array. 
   */
  public abstract double[] getQuantiles(double[] fractions);
  
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
  public double[] getQuantiles(int evenlySpaced) {
    return getQuantiles(getEvenlySpaced(evenlySpaced));
  }
  
  static double[] getEvenlySpaced(int evenlySpaced) {
    int n = evenlySpaced;
    if (n <= 0) {
      throw new SketchesArgumentException("EvenlySpaced must be > zero.");
    }
    double[] fractions = new double[n];
    double frac = 0.0;
    fractions[0] = frac;
    for (int i = 1; i < n; i++) {
      frac = (double)i / (n - 1);
      fractions[i] = frac;
    }
    if (n > 1) {
      fractions[n - 1] = 1.0;
    }
    return fractions;
  }
  
  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream 
   * given a set of splitPoints (values).
   * 
   * <p>The resulting approximations have a probabilistic guarantee that be obtained from the 
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
  public abstract double[] getPMF(double[] splitPoints);
  
  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the 
   * cumulative analog of the PMF, of the input stream given a set of splitPoint (values).
   * 
   * <p>More specifically, the value at array position j of the CDF is the
   * sum of the values in positions 0 through j of the PMF.
   * 
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing doubles
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * 
   * @return an approximation to the CDF of the input stream given the splitPoints.
   */
  public abstract double[] getCDF(double[] splitPoints);
  
  /**
   * Returns the configured value of K
   * @return the configured value of K
   */
  public abstract int getK();

  /**
   * Returns the min value of the stream
   * @return the min value of the stream
   */
  public abstract double getMinValue();

  /**
   * Returns the max value of the stream
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
   * @param k the configuration parameter of a DoublesSketch
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
  public abstract void reset();

  /**
   * Serialize this sketch to a byte array form. 
   * This does not sort the base buffer.
   * @return byte array of this sketch
   */
  public byte[] toByteArray() {
    return toByteArray(false);
  }

  /**
   * Serialize this sketch to a byte array form. 
   * @param sort if true, this sorts the base buffer, which optimizes merge performance at
   * the cost of slightly increased serialization time. 
   * In real-time build-and-merge environments, this may not be desirable. 
   * @return byte array of this sketch
   */
  public abstract byte[] toByteArray(boolean sort);
  
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
  public abstract String toString(boolean sketchSummary, boolean dataDetail);
  

  /**
   * From an existing sketch, this creates a new sketch that can have a smaller value of K.
   * The original sketch is not modified.
   * 
   * @param smallerK the new sketch's value of K that must be smaller than this value of K.
   * It is required that this.getK() = smallerK * 2^(nonnegative integer).
   * @return the new sketch.
   */
  public abstract DoublesSketch downSample(int smallerK);

  /**
   * Heapify takes the sketch image in Memory and instantiates an on-heap Sketch. 
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem a Memory image of a Sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based Sketch based on the given Memory
   */
  public static DoublesSketch heapify(Memory srcMem) {
    return HeapDoublesSketch.getInstance(srcMem);
  }

  /**
   * Computes the number of retained items (samples) in the sketch
   * @return the number of retained items (samples) in the sketch
   */
  public int getRetainedItems() {
    return Util.computeRetainedItems(getK(), getN());
  }

  /**
   * Returns the number of bytes required to store this sketch as an array of bytes.
   * @return the number of bytes required to store this sketch as an array of bytes.
   */
  public int getStorageBytes() {
    if (isEmpty()) return 8;
    return 32 + Double.BYTES * Util.computeRetainedItems(getK(), getN());
  }

  /**
   * Puts the current sketch into the given Memory if there is sufficient space.
   * Otherwise, throws an error. This sorts the base buffer based on the given sort flag.
   * @param dstMem the given memory.
   * @param sort if true, this sorts the base buffer, which optimizes merge performance at
   * the cost of slightly increased serialization time. 
   * In real-time build-and-merge environments, this may not be desirable. 
   */
  public abstract void putMemory(Memory dstMem, boolean sort);
  
  /**
   * Puts the current sketch into the given Memory if there is sufficient space.
   * Otherwise, throws an error. This does not sort the base buffer.
   * 
   * @param dstMem the given memory.
   */
  public void putMemory(Memory dstMem) {
    putMemory(dstMem, false);
  }

  //Restricted abstract

  /**
   * Returns the base buffer count
   * @return the base buffer count
   */
  abstract int getBaseBufferCount();

  /**
   * Returns the bit pattern for valid log levels
   * @return the bit pattern for valid log levels
   */
  long getBitPattern() {
    return Util.computeBitPattern(k_, getN());
  }

  /**
   * Returns the item capacity for the combined base buffer
   * @return the item capacity for the combined base buffer
   */
  abstract int getCombinedBufferItemCapacity();

  /**
   * Returns the combined buffer reference
   * @return the combined buffer reference
   */
  abstract double[] getCombinedBuffer();

}
