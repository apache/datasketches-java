/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import java.util.Arrays;

import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

import static com.yahoo.sketches.frequencies.PreambleUtil.SER_VER;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractMaxMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractEmptyFlag;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractCurMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractInitialMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractBufferLength;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertMaxMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertEmptyFlag;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertCurMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertBufferLength;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertInitialMapSize;
//import static com.yahoo.sketches.Util.*;

/**
 * Implements frequent items sketch on the Java heap.
 * 
 * <p>The frequent-items sketch is useful for keeping approximate counters for keys that is 
 * implemented as a map (<i>long</i> key, <i>long</i> count).
 * 
 * The sketch is initialized with a value maxMapSize, which must be a power of 2. When at full
 * size, the sketch will maintain the counters in hash table, which is internally represented
 * with arrays of length maxMapSize. The total space usage of the sketch, at full size,
 * is 18*maxMapSize bytes, plus a small (constant) number of additional bytes.
 * 
 * At full size, hash table will keep at most k=maxMapSize * LOAD_FACTOR  counters, where 
 * LOAD_FACTOR is the maximum load that the hash table is configured to support.
 * Currently, LOAD_FACTOR is set to 3/4. If fewer than k different keys are inserted 
 * then the counts computed by the sketch will be exact.
 * 
 * When the sketch is updated with a key and increment, the corresponding counter is incremented or,
 * if there is no counter for that key, a new counter is created. If the hash table reaches capacity,
 * it decrements all of the counters (by an approximately computed median), and
 * removes any non-positive counters.
 * 
 * Hence, when the sketch is at full size, the number of counters maintained by the sketch will 
 * typically oscillate between roughly k and k/2. 
 * 
 * The logic of the frequent-items sketch is such that the stored counts and true counts are never
 * too different. More specifically, for any key KEY, the sketch can return an estimate of the true
 * frequency of KEY, along with upper and lower bounds on the frequency (that hold
 * deterministically). 
 * 
 * For this implementation, it is guaranteed that, with high probability over the
 * randomness of the implementation, the difference between the upper bound and the estimate is at
 * most 2n/k=(8/3)*(n/maxMapSize), where n denotes the stream length (i.e, sum of all the item frequencies), and
 * similarly for the lower bound and the estimate. In practice, the difference is usually much smaller.
 *
 * 
 * Background: This code implements a variant of what is commonly known as the "Misra-Gries
 * algorithm" or "Frequent Items". Variants of it were discovered and rediscovered and redesigned
 * several times over the years. a) "Finding repeated elements", Misra, Gries, 1982 b)
 * "Frequency estimation of internet packet streams with limited space" Demaine, Lopez-Ortiz, Munro,
 * 2002 c) "A simple algorithm for finding frequent elements in streams and bags" Karp, Shenker,
 * Papadimitriou, 2003 d) "Efficient Computation of Frequent and Top-k Elements in Data Streams"
 * Metwally, Agrawal, Abbadi, 2006
 * 
 * Uses HashMapReverseEfficient
 * 
 * @author Justin Thaler
 */
public class FrequentItems extends FrequencyEstimator {

  /**
   * We start by allocating a small data structure capable of explicitly storing very small streams
   * in full, and growing it as the stream grows. The following constant controls the size of the
   * initial data structure.
   */
  static final int MIN_HASHMAP_SIZE = 4; // This is somewhat arbitrary
  
  /**
   * This is a constant large enough that computing the median of SAMPLE_SIZE
   * randomly selected entries from a list of numbers and outputting
   * the empirical median will give a constant-factor approximation to the 
   * true median with high probability
   */
  static final int SAMPLE_SIZE = 256;

  /**
   * The current number of counters supported by the data structure.
   */
  private int curMapCap;
  
  /**
   * The number of counters to be supported when sketch is full size
   */
  private int maxMapCap;

  /**
   * Initial length of the arrays internal to the hash map supported by the data structure
   */
  private int initialMapSize;
  
  /**
   * Maximum length of the arrays internal to the hash map supported by the data structure.
   */
  private int maxMapSize;

  /**
   * Hash map mapping stored keys to approximate counts
   */
  private HashMapReverseEfficient hashMap;


  /**
   * Tracks the total of decremented counts performed.
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


  // **CONSTRUCTORS**********************************************************
  /**
   * Construct this sketch with parameter mapMapSize and initialMapSize
   * @param maxMapSize Determines the maximum length of the arrays internal to
   * the hash table maintained by the sketch. The larger maxMapSize, 
   * the more space the sketch uses, and the better the accuracy of the estimates returned 
   * by the sketch.
   * @param initialMapSize determines the initial length of the arrays internal to the hash table
   */
  FrequentItems(int maxMapSize, int initialMapSize) { 
    
    if (maxMapSize <= 0) {
      throw new IllegalArgumentException("maxMapSize cannot be negative or zero: "+maxMapSize);
    }
    if (initialMapSize <= 0) {
      throw new IllegalArgumentException("initialMapSize cannot be negative or zero: "+initialMapSize);
    }
    if (maxMapSize < initialMapSize) {
      throw new IllegalArgumentException("maxMapSize cannot be less than initialMapSize: "+maxMapSize);
    }
    if (!Util.isPowerOf2(maxMapSize))
      throw new IllegalArgumentException(
          "maxMapSize must be power of two: " + maxMapSize);
    if (!Util.isPowerOf2(initialMapSize))
      throw new IllegalArgumentException(
          "initialMapSize must be power of two: " + initialMapSize);
    
    //set initial size of counters data structure
    this.initialMapSize = Math.max(initialMapSize, MIN_HASHMAP_SIZE);
    hashMap = new HashMapReverseEfficient(initialMapSize);
    this.curMapCap = hashMap.getCapacity();
    
    this.maxMapSize = maxMapSize;
    this.maxMapCap = (int) (maxMapSize*hashMap.getLoadFactor());

    offset = 0;
    sampleSize = Math.min(SAMPLE_SIZE, maxMapCap);
  }
  
  /**
   * Construct this sketch with the parameter maxMapSize and the default initialMapSize
   * @param maxMapSize 
   */
  public FrequentItems(int maxMapSize) {
    this(maxMapSize, MIN_HASHMAP_SIZE);
  }

  /**
   * @return the number of active (positive) counters in the sketch.
   */
  public int getActiveCounters() {
    return hashMap.getNumActive();
  }

  @Override
  public long getEstimate(long key) {
    // If the key is tracked Estimate = curCount + offset; Otherwise it is 0.
    long curCount = hashMap.get(key);
    return (curCount > 0)? curCount + offset : 0;
  }

  @Override
  public long getUpperBound(long key) {
    // If key is tracked UB = curCount + offset + mergeError; Otherwise, it is 0.
    // If tracked:
    // If (mergeError == 0) UB = estimate = curCount + offset
    // If (mergeError >  0) UB = estimate + mergeError
    return hashMap.get(key) + getMaxError();
  }

  @Override
  public long getLowerBound(long key) {
    //The LB can never be negative.
    //If tracked:
    //If (mergeError == 0) LB = curCount.
    //If (mergeError >  0) LB = max(curCount - mergeError, 0)
    long returnVal = hashMap.get(key) - mergeError;
    return ((returnVal > 0)? returnVal : 0);
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
    if (increment == 0) return;
    if (increment < 0) throw new IllegalArgumentException("Increment may not be negative");
    this.streamLength += increment;
    hashMap.adjust(key, increment);
    int numActive = this.getActiveCounters();

    // if the data structure needs to be grown
    if ((numActive >= this.curMapCap) && (this.curMapCap < this.maxMapCap)) {
      // grow the size of the data structure
      int newSize = 2*hashMap.getLength();
      HashMapReverseEfficient newTable = new HashMapReverseEfficient(newSize);
      this.curMapCap = newTable.getCapacity();
      long[] keys = this.hashMap.getActiveKeys();
      long[] values = this.hashMap.getActiveValues();
      
      assert(keys.length == numActive);
      for (int i = 0; i < numActive; i++) {
        newTable.adjust(keys[i], values[i]);
      }
      this.hashMap = newTable;
    }

    //the +1 here is because, if we do not purge now, we might
    //wind up inserting a new item on the next update, and we 
    //don't want this to put us over capacity. (Going over capacity
    //by 1 is not a big deal, but we may as well be precise).
    if (numActive+1 > this.maxMapCap) {
      purge();
      assert (this.getActiveCounters() <= this.maxMapCap);
    }
  }

  /**
   * This function is called when a key is processed that is not currently assigned a counter, and
   * all the counters are in use. This function estimates the median of the counters in the sketch
   * via sampling, decrements all counts by this estimate, throws out all counters that are no
   * longer positive, and increments offset accordingly.
   */
  private void purge() {
    int limit = Math.min(this.sampleSize, getActiveCounters());

    long[] values = hashMap.getValues();
    int numSamples = 0;
    int i = 0;
    long[] samples = new long[limit];

    while (numSamples < limit) {
      if (hashMap.isActive(i)) {
        samples[numSamples] = values[i];
        numSamples++;
      }
      i++;
    }

    Arrays.sort(samples, 0, numSamples);
    long val = samples[limit / 2];
    hashMap.adjustAllValuesBy(-1 * val);
    hashMap.keepOnlyLargerThan(0);
    this.offset += val;
  }

  @Override
  public FrequencyEstimator merge(FrequencyEstimator other) {
    if (!(other instanceof FrequentItems))
      throw new IllegalArgumentException("FrequentItems can only merge with other FrequentItems");
    FrequentItems otherCasted = (FrequentItems) other;

    this.streamLength += otherCasted.streamLength;
    this.mergeError += otherCasted.getMaxError();

    long[] otherKeys = otherCasted.hashMap.getActiveKeys();
    long[] otherValues = otherCasted.hashMap.getActiveValues();

    for (int i = otherKeys.length; i-- > 0;) {
      this.update(otherKeys[i], otherValues[i]);
    }
    return this;
  }

  @Override
  public long[] getFrequentKeys(long threshold) {
    int count = 0;
    long[] keys = hashMap.getKeys();

    // first, count the number of candidate frequent keys
    for (int i = hashMap.getLength(); i-- > 0;) {
      if (hashMap.isActive(i) && (getEstimate(keys[i]) >= threshold)) {
        count++;
      }
    }

    // allocate an array to store the candidate frequent keys, and then compute them
    long[] freqKeys = new long[count];
    count = 0;
    for (int i = hashMap.getLength(); i-- > 0;) {
      if (hashMap.isActive(i) && (getUpperBound(keys[i]) >= threshold)) {
        freqKeys[count] = keys[i];
        count++;
      }
    }
    return freqKeys;
  }


  
  @Override
  public int getCurMapCap() {
    return this.curMapCap;
  }

  @Override
  public long getStreamLength() {
    return this.streamLength;
  }

  @Override
  public int getMaxMapCap() {
    return this.maxMapCap;
  }

  @Override
  public boolean isEmpty() {
    return getActiveCounters() == 0;
  }

  @Override
  public void reset() {
    hashMap = new HashMapReverseEfficient(this.initialMapSize);
    this.curMapCap = hashMap.getCapacity();
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
    return 48 + 16 * getActiveCounters();
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
        String.format("%d,%d,%d,%d,%d,%d,", maxMapSize, mergeError, offset, streamLength, hashMap.getLength(), initialMapSize));
    // maxMapCap, samplesize are deterministic functions of maxMapSize, so we don't need them in the serialization
    sb.append(hashMap.hashMapReverseEfficientToString());
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

    int maxMapSize = Integer.parseInt(tokens[0]);
    long mergeError = Long.parseLong(tokens[1]);
    long offset = Long.parseLong(tokens[2]);
    long streamLength = Long.parseLong(tokens[3]);
    int curMapSize = Integer.parseInt(tokens[4]);
    int initialMapSize = Integer.parseInt(tokens[5]);
    
    FrequentItems sketch = new FrequentItems(maxMapSize, curMapSize);
    sketch.mergeError = mergeError;
    sketch.offset = offset;
    sketch.streamLength = streamLength;
    sketch.initialMapSize = initialMapSize;

    sketch.hashMap = HashMapReverseEfficient.StringArrayToHashMapReverseEfficient(tokens, 6);
    return sketch;
  }

  public byte[] toByteArray() {
    int preLongs, arrLongs;
    boolean empty = isEmpty();

    if (empty) {
      preLongs = 1;
      arrLongs = 1;
    } else {
      preLongs = 6;
      arrLongs = preLongs + 2 * getActiveCounters();
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
    pre0 = insertMaxMapSize(this.maxMapSize, pre0);

    if (empty) {
      mem.putLong(0, pre0);
    } else {
      long[] preArr = new long[6];
      preArr[0] = pre0;
      preArr[1] = this.mergeError;
      preArr[2] = this.offset;
      preArr[3] = this.streamLength;

      long pre1 = 0L;
      pre1 = insertCurMapSize(this.hashMap.getLength(), pre1);
      pre1 = insertInitialMapSize(this.initialMapSize, pre1);
      preArr[4] = pre1;

      long pre2 = 0L;
      pre2 = insertBufferLength(getActiveCounters(), pre2);
      preArr[5] = pre2;

      mem.putLongArray(0, preArr, 0, 6);
      mem.putLongArray(48, hashMap.getActiveKeys(), 0, this.getActiveCounters());
      mem.putLongArray(48 + (this.getActiveCounters() << 3), hashMap.getActiveValues(), 0, this.getActiveCounters());
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
    int maxMapSize = extractMaxMapSize(pre0);

    if (emptyFlag == 1)
      return new FrequentItems(maxMapSize);

    // Not empty, must have valid preamble
    long[] remainderPreArr = new long[5];
    srcMem.getLongArray(8, remainderPreArr, 0, 5);

    long mergeError = remainderPreArr[0];
    long offset = remainderPreArr[1];
    long streamLength = remainderPreArr[2];
    long pre1 = remainderPreArr[3];
    long pre2 = remainderPreArr[4];

    int curMapSize = extractCurMapSize(pre1);
    int initialMapSize = extractInitialMapSize(pre1);
    int bufferLength = extractBufferLength(pre2);

    FrequentItems hfi = new FrequentItems(maxMapSize, curMapSize);
    hfi.initialMapSize = initialMapSize;
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
