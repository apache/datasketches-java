/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import java.util.Arrays;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.hashmaps.HashMapReverseEfficient;

import static com.yahoo.sketches.frequencies.PreambleUtil.SER_VER;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractLowerK;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractEmptyFlag;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractUpperK;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractInitialSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractBufferLength;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertLowerK;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertEmptyFlag;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertUpperK;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertBufferLength;

import static com.yahoo.sketches.frequencies.PreambleUtil.insertInitialSize;

/**
 * Implements frequent items sketch on the Java heap.
 * 
 * The frequent-items sketch is useful for keeping approximate counters for keys (map from key
 * (long) to value (long)). The sketch is initialized with a value k. The sketch will keep roughly k
 * counters when it is full size. More specifically, when k is a power of 2, a HashMap will be
 * created with 2*k cells, and the number of counters will typically oscillate between roughly .75*k
 * and 1.5*k. The space usage of the sketch is therefore proportional to k when it reaches full
 * size.
 * 
 * When the sketch is updated with a key and increment, the corresponding counter is incremented or,
 * if there is no counter for that key, a new counter is created. If the sketch reaches its maximal
 * allowed size, it decrements all of the counters (by an approximately computed median), and
 * removes any non-positive counters.
 * 
 * The logic of the frequent-items sketch is such that the stored counts and real counts are never
 * too different. More specifically, for any key KEY, the sketch can return an estimate of the true
 * frequency of KEY, along with upper and lower bounds on the frequency (that hold
 * deterministically). For our implementation, it is guaranteed that, with high probability over the
 * randomness of the implementation, the difference between the upper bound and the estimate is at
 * most (4/3)*(n/k), where n denotes the stream length (i.e, sum of all the item frequencies), and
 * similarly for the lower bound and the estimate. In practice, the difference is usually much
 * smaller.
 * 
 * Background: This code implements a variant of what is commonly known as the "Misra-Gries
 * algorithm" or "Frequent Items". Variants of it were discovered and rediscovered and redesigned
 * several times over the years. a) "Finding repeated elements", Misra, Gries, 1982 b)
 * "Frequency estimation of internet packet streams with limited space" Demaine, Lopez-Ortiz, Munro,
 * 2002 c) "A simple algorithm for finding frequent elements in streams and bags" Karp, Shenker,
 * Papadimitriou, 2003 d) "Efficient Computation of Frequent and Top-k Elements in Data Streams"
 * Metwally, Agrawal, Abbadi, 2006
 * 
 * @author Justin Thaler
 */
public class FrequentItems extends FrequencyEstimator {

  /**
   * We start by allocating a small data structure capable of explicitly storing very small streams
   * in full, and growing it as the stream grows. The following constant controls the size of the
   * initial data structure
   */
  static final int MIN_FREQUENT_ITEMS_SIZE = 4; // This is somewhat arbitrary
  
  /**
   * This is a constant large enough that computing the median of SAMPLE_SIZE
   * randomly selected entries from a list of numbers and outputting
   * the empirical median will give a constant-factor approximaion to the 
   * true median with high probability
   */
  static final int SAMPLE_SIZE = 256;

  /**
   * The current number of counters that the data structure can support
   */
  private int K;

  /**
   * The value of k passed to the constructor. Used to determine the maximum number of counters the
   * sketch can support, and remembered by the sketch for use in resetting to a virgin state.
   */
  private int k;

  /**
   * Initial number of counters supported by the data structure
   */
  private int initialSize;

  /**
   * Hash map mapping stored keys to approximate counts
   */
  private HashMapReverseEfficient counters;

  /**
   * The number of counters to be supported when sketch is full size
   */
  private int maxK;


  /**
   * Tracks the total number of decrements performed on sketch.
   */
  private long offset;

  /**
   * An upper bound on the error in any estimated count due to merging with other FrequentItems
   * sketches.
   */
  private long mergeError;

  /**
   * The sum of all frequencies of the stream so far.
   */
  private long streamLength = 0;

  /**
   * The maximum number of samples used to compute approximate median of counters when doing
   * decrement
   */
  private int sampleSize;


  // **CONSTRUCTOR**********************************************************
  /**
   * @param k Determines the accuracy of the estimates returned by the sketch.
   * @param initialCapacity determines the initial size of the sketch.
   * 
   *        The guarantee of the sketch is that with high probability, any returned estimate will
   *        have error at most (4/3)*(n/k), where n is the true sum of frequencies in the stream. In
   *        practice, the error is typically much smaller. The space usage of the sketch is
   *        proportional to k. If fewer than ~k different keys are inserted then the counts will be
   *        exact. More precisely, if k is a power of 2,then when the sketch reaches full size, the
   *        data structure's HashMap will contain 2*k cells. Assuming that the LOAD_FACTOR of the
   *        HashMap is set to 0.75, the number of cells of the hash table that are actually filled
   *        should oscillate between roughly .75*k and 1.5 * k.
   */
  public FrequentItems(int k, int initialCapacity) {

    if (k <= 0) throw new IllegalArgumentException("Received negative or zero value for k.");
    
    //set initial size of counters data structure so it can exactly store a stream with 
    //initialCapacity distinct elements

    this.K = initialCapacity;
    counters = new HashMapReverseEfficient(this.K);

    this.k = k;
    this.initialSize = initialCapacity;

    // set maxK to be the maximum number of counters that can be supported
    // by a HashMap with the appropriate number of cells (specifically,
    // 2*k cells if k is a power of 2) and a load that does not exceed
    // the designated load factor
    int maxHashMapLength = Integer.highestOneBit(4 * k - 1);
    this.maxK = (int) (maxHashMapLength * counters.LOAD_FACTOR);

    this.offset = 0;

    if (this.maxK < SAMPLE_SIZE) 
      this.sampleSize = this.maxK;
    else
      this.sampleSize = SAMPLE_SIZE;
  }

  public FrequentItems(int k) {
    this(k, MIN_FREQUENT_ITEMS_SIZE);
  }

  /**
   * @return the number of positive counters in the sketch.
   */
  public int nnz() {
    return counters.getSize();
  }

  @Override
  public long getEstimate(long key) {
    // the logic below returns the count of associated counter if key is tracked.
    // If the key is not tracked and fewer than maxK counters are in use, 0 is returned.
    // Otherwise, the minimum counter value is returned.
    if (counters.get(key) > 0)
      return counters.get(key) + offset;
    else
      return 0;
  }

  @Override
  public long getEstimateUpperBound(long key) {
    long estimate = getEstimate(key);
    if (estimate > 0)
      return estimate + mergeError;

    return mergeError + offset;

  }

  @Override
  public long getEstimateLowerBound(long key) {
    long estimate = getEstimate(key);
    
    long returnVal = estimate - offset - mergeError;
    return (returnVal > 0) ? returnVal : 0;
  }

  @Override
  public long getMaxError() {
    return offset + mergeError;
  }

  @Override
  public void update(long key) {
    update(key, 1);
  }

  @Override
  public void update(long key, long increment) {
    this.streamLength += increment;
    counters.adjust(key, increment);
    int size = this.nnz();

    // if the data structure needs to be grown
    if ((size >= this.K) && (this.K < this.maxK)) {
      // grow the size of the data structure
      int newSize = Math.max(Math.min(this.maxK, 2 * this.K), 1);
      this.K = newSize;
      HashMapReverseEfficient newTable = new HashMapReverseEfficient(newSize);
      long[] keys = this.counters.getKeys();
      long[] values = this.counters.getValues();
      
      assert(keys.length == size);
      for (int i = 0; i < size; i++) {
        newTable.adjust(keys[i], values[i]);
      }
      this.counters = newTable;
    }

    if (size > this.maxK) {
      purge();
      assert (this.nnz() <= this.maxK);
    }
  }

  /**
   * This function is called when a key is processed that is not currently assigned a counter, and
   * all the counters are in use. This function estimates the median of the counters in the sketch
   * via sampling, decrements all counts by this estimate, throws out all counters that are no
   * longer positive, and increments offset accordingly.
   */
  private void purge() {
    int limit = Math.min(this.sampleSize, nnz());

    long[] values = counters.ProtectedGetValues();
    int numSamples = 0;
    int i = 0;
    long[] samples = new long[limit];

    while (numSamples < limit) {
      if (counters.isActive(i)) {
        samples[numSamples] = values[i];
        numSamples++;
      }
      i++;
    }

    Arrays.sort(samples, 0, numSamples);
    long val = samples[limit / 2];
    counters.adjustAllValuesBy(-1 * val);
    counters.keepOnlyLargerThan(0);
    this.offset += val;
  }

  @Override
  public FrequencyEstimator merge(FrequencyEstimator other) {
    if (!(other instanceof FrequentItems))
      throw new IllegalArgumentException("FrequentItems can only merge with other FrequentItems");
    FrequentItems otherCasted = (FrequentItems) other;

    this.streamLength += otherCasted.streamLength;
    this.mergeError += otherCasted.getMaxError();

    long[] otherKeys = otherCasted.counters.getKeys();
    long[] otherValues = otherCasted.counters.getValues();

    for (int i = otherKeys.length; i-- > 0;) {
      this.update(otherKeys[i], otherValues[i]);
    }
    return this;
  }

  @Override
  public long[] getFrequentKeys(long threshold) {
    int count = 0;
    long[] keys = counters.ProtectedGetKey();

    // first, count the number of candidate frequent keys
    for (int i = counters.getLength(); i-- > 0;) {
      if (counters.isActive(i) && (getEstimate(keys[i]) >= threshold)) {
        count++;
      }
    }

    // allocate an array to store the candidate frequent keys, and then compute them
    long[] freqKeys = new long[count];
    count = 0;
    for (int i = counters.getLength(); i-- > 0;) {
      if (counters.isActive(i) && (getEstimateUpperBound(keys[i]) >= threshold)) {
        freqKeys[count] = keys[i];
        count++;
      }
    }
    return freqKeys;
  }


  @Override
  public int getK() {
    return this.K;
  }

  @Override
  public long getStreamLength() {
    return this.streamLength;
  }

  @Override
  public int getMaxK() {
    return this.maxK;
  }

  @Override
  public boolean isEmpty() {
    return nnz() == 0;
  }

  @Override
  public void reset() {
    this.K = this.initialSize;
    counters = new HashMapReverseEfficient(this.K);
    this.offset = 0;
    this.mergeError = 0;
    this.streamLength = 0;
  }

  /**
   * Returns the number of bytes required to store this sketch as an array of bytes.
   * 
   * @return the number of bytes required to store this sketch as an array of bytes.
   */
  public int getStorageBytes() {
    if (isEmpty())
      return 20;
    return 48 + 16 * nnz();
  }

  /**
   * Returns summary information about this sketch.
   * 
   * @return a string specifying the FrequentItems object
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(
        String.format("%d,%d,%d,%d,%d,%d,", k, mergeError, offset, streamLength, K, initialSize));
    // maxK, samplesize are deterministic functions of k, so we don't need them in the serialization
    sb.append(counters.hashMapReverseEfficientToString());
    return sb.toString();
  }

  /**
   * Turns a string specifying a FrequentItems object into a FrequentItems object.
   * 
   * @param string String specifying a FrequentItems object
   * @return a FrequentItems object corresponding to the string
   */
  public static FrequentItems StringToFrequentItems(String string) {
    String[] tokens = string.split(",");
    if (tokens.length < 6) {
      throw new IllegalArgumentException(
          "Tried to make FrequentItems out of string not long enough to specify relevant parameters.");
    }

    int k = Integer.parseInt(tokens[0]);
    long mergeError = Long.parseLong(tokens[1]);
    long offset = Long.parseLong(tokens[2]);
    long streamLength = Long.parseLong(tokens[3]);
    int K = Integer.parseInt(tokens[4]);
    int initialSize = Integer.parseInt(tokens[5]);

    FrequentItems sketch = new FrequentItems(k, K);
    sketch.mergeError = mergeError;
    sketch.offset = offset;
    sketch.streamLength = streamLength;
    sketch.initialSize = initialSize;

    sketch.counters = HashMapReverseEfficient.StringArrayToHashMapReverseEfficient(tokens, 6);
    return sketch;
  }

  // @formatter:off
  /**
   * @return byte array that looks as follows:
   * 
   * <pre>
   *  
   *      ||    7     |    6   |    5   |    4   |    3   |    2   |    1   |     0          |
   *  0   |||--------k---------------------------|--flag--| FamID  | SerVer | PreambleLongs |  
   *      ||    15    |   14   |   13   |   12   |   11   |   10   |    9   |     8          |
   *  1   ||---------------------------------mergeError--------------------------------------|
   *      ||    23    |   22   |   21   |   20   |   19   |   18   |   17   |    16          |
   *  2   ||---------------------------------offset------------------------------------------|      
   *      ||    31    |   30   |   29   |   28   |   27   |   26   |   25   |    24          |
   *  3   ||-----------------------------------streamLength----------------------------------| 
   *      ||    39    |   38   |   37   |   36   |   35   |   34   |   33   |    32          |
   *  4   ||------initialSize--------------------|-------------------K-----------------------| 
   *      ||    47    |   46   |   45   |   44   |   43   |   42   |   41   |   40           |
   *  5   ||------------(unused)-----------------|--------bufferlength-----------------------| 
   *      ||    55    |   54   |   53   |   52   |   51   |   50   |   49   |   48           |
   *  6   ||----------start of keys buffer, followed by values buffer------------------------|
   * </pre>
   **/
  // @formatter:on
  public byte[] toByteArray() {
    int preLongs, arrLongs;
    boolean empty = isEmpty();

    if (empty) {
      preLongs = 1;
      arrLongs = 1;
    } else {
      preLongs = 6;
      arrLongs = preLongs + 2 * nnz();
    }
    byte[] outArr = new byte[arrLongs << 3];
    NativeMemory mem = new NativeMemory(outArr);

    // build first prelong
    long pre0 = 0L;
    pre0 = insertPreLongs(preLongs, pre0);
    pre0 = insertSerVer(SER_VER, pre0);
    pre0 = insertFamilyID(10, pre0);
    if (empty)
      pre0 = insertEmptyFlag(1, pre0);
    else
      pre0 = insertEmptyFlag(0, pre0);
    pre0 = insertLowerK(this.k, pre0);

    if (empty) {
      mem.putLong(0, pre0);
    } else {
      long[] preArr = new long[6];
      preArr[0] = pre0;
      preArr[1] = this.mergeError;
      preArr[2] = this.offset;
      preArr[3] = this.streamLength;

      long pre1 = 0L;
      pre1 = insertUpperK(this.K, pre1);
      pre1 = insertInitialSize(this.initialSize, pre1);
      preArr[4] = pre1;

      long pre2 = 0L;
      pre2 = insertBufferLength(nnz(), pre2);
      preArr[5] = pre2;

      mem.putLongArray(0, preArr, 0, 6);
      mem.putLongArray(48, counters.getKeys(), 0, this.nnz());
      mem.putLongArray(48 + (this.nnz() << 3), counters.getValues(), 0, this.nnz());
    }
    return outArr;
  }

  public void putMemory(Memory dstMem) {
    byte[] byteArr = toByteArray();
    int arrLen = byteArr.length;
    long memCap = dstMem.getCapacity();
    if (memCap < arrLen) {
      throw new IllegalArgumentException(
          "Destination Memory not large enough: " + memCap + " < " + arrLen);
    }
    dstMem.putByteArray(0, byteArr, 0, arrLen);
  }

  /**
   * Heapifies the given srcMem, which must be a Memory image of a FrequentItems sketch
   * 
   * @param srcMem a Memory image of a sketch. <a href="{@docRoot}/resources/dictionary.html#mem"
   *        >See Memory</a>
   * @return a FrequentItems on the Java heap.
   */
  static FrequentItems getInstance(Memory srcMem) {
    long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) {
      throw new IllegalArgumentException("Memory too small: " + memCapBytes);
    }

    long pre0 = srcMem.getLong(0);
    int preambleLongs = extractPreLongs(pre0);

    assert ((preambleLongs == 1) || (preambleLongs == 6));
    int serVer = extractSerVer(pre0);
    assert (serVer == 1);
    int familyID = extractFamilyID(pre0);
    assert (familyID == 10);
    int emptyFlag = extractEmptyFlag(pre0);
    int k = extractLowerK(pre0);

    if (emptyFlag == 1)
      return new FrequentItems(k);

    // Not empty, must have valid preamble
    long[] remainderPreArr = new long[5];
    srcMem.getLongArray(8, remainderPreArr, 0, 5);

    long mergeError = remainderPreArr[0];
    long offset = remainderPreArr[1];
    long streamLength = remainderPreArr[2];
    long pre1 = remainderPreArr[3];
    long pre2 = remainderPreArr[4];

    int K = extractUpperK(pre1);
    int initialSize = extractInitialSize(pre1);
    int bufferLength = extractBufferLength(pre2);

    FrequentItems hfi = new FrequentItems(k, K);
    hfi.initialSize = initialSize;
    hfi.offset = offset;
    hfi.mergeError = mergeError;

    long[] keyArray = new long[bufferLength];
    long[] valueArray = new long[bufferLength];

    srcMem.getLongArray(48, keyArray, 0, bufferLength);
    srcMem.getLongArray(48 + 8 * bufferLength, valueArray, 0, bufferLength);

    for (int i = 0; i < bufferLength; i++) {
      hfi.update(keyArray[i], valueArray[i]);
    }
    hfi.streamLength = streamLength;
    return hfi;
  }
}
