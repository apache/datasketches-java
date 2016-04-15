/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.Util.*;
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

/**
 * This FrequentLongsSketch implements {@link FrequentLongsEstimator} with a specific algorithm.
 * 
 * <p>This sketch is useful for tracking approximate frequencies of long items that are members
 * of a multiset of such items. 
 * 
 * <p><b>Space Usage</b></p>
 * 
 * <p>The sketch is initialized with a maxMapSize that specifies the maximum physical length of the 
 * internal hash map of the form  (<i>long</i> item, <i>long</i> count). 
 * The maxMapSize must be a power of 2. </p>
 * 
 * <p>The hash map starts with a very small size (4), and grows as needed up to the 
 * specified maxMapSize. The LOAD_FACTOR for the hash map is internally set at 75%, 
 * which means at any time the capacity of (item, count) pairs is 75% * mapSize. 
 * The space usage of the sketch is 18 * mapSize bytes, plus a small constant
 * number of additional bytes. The space usage of this sketch will never exceed 18 * maxMapSize
 * bytes, plus a small constant number of additional bytes.</p>
 * 
 * <p><b>Maximum Capacity of the Sketch</b></p>
 * 
 * <p>The maximum capacity of (item, count) pairs of the sketch is maxMapCap = LOAD_FACTOR * maxMapSize.
 * Papers that describe the mathematical error properties of this type of algorithm often refer to 
 * this with the symbol <i>k</i>.</p>
 * 
 * <p><b>Updating the sketch with (item, count) pairs</b></p>
 * 
 * <p>If the item is found in the hash map, the mapped count field (the "counter") is incremented by
 * the incoming count, otherwise, a new counter "(item, count) pair" is created. 
 * If the number of tracked counters reaches the maximum capacity of the hash map the sketch 
 * decrements all of the counters (by an approximately computed median), and removes any 
 * non-positive counters.</p>
 * 
 * <p>Hence, when the sketch is at full size, the number of counters maintained by the sketch will 
 * typically oscillate between roughly maximum hash map capacity (maxMapCap) and maxMapCap/2, or
 * equivalently, k and k/2.</p>
 * 
 * <p><b>Accuracy</b></p>
 * 
 * <p>If fewer than LOAD_FACTOR * maxMapSize different items are inserted into the sketch the 
 * estimated frequencies returned by the sketch will be exact.
 * The logic of the frequent items sketch is such that the stored counts and true counts are never
 * too different. More specifically, for any <i>item</i>, the sketch can return an estimate of the 
 * true frequency of <i>item</i>, along with upper and lower bounds on the frequency (that hold
 * deterministically).</p>
 * 
 * <p>If the internal hash fuction had infinite precision and was perfectly uniform: Then,
 * for this implementation and for a specific active <i>item</i>, it is guaranteed that the difference 
 * between the Upper Bound and the Estimate is max(UB- Est) ~ 2n/k = (8/3)*(n/maxMapSize), where 
 * </i>n</i> denotes the stream length (i.e, sum of all the item counts). The behavior is similar
 * for the Lower Bound and the Estimate.
 * However, this implementation uses a deterministic hash function for performnace that performs 
 * well on real data, and in practice, the difference is usually much smaller.</p>
 * 
 * <p><b>Background</b></p>
 * 
 * <p>This code implements a variant of what is commonly known as the "Misra-Gries
 * algorithm". Variants of it were discovered and rediscovered and redesigned several times over 
 * the years:</p>
 * <ul><li>"Finding repeated elements", Misra, Gries, 1982</li>
 * <li>"Frequency estimation of internet packet streams with limited space" Demaine, Lopez-Ortiz, Munro,
 * 2002</li>
 * <li>"A simple algorithm for finding frequent elements in streams and bags" Karp, Shenker,
 * Papadimitriou, 2003</li>
 * <li>"Efficient Computation of Frequent and Top-k Elements in Data Streams" Metwally, Agrawal, 
 * Abbadi, 2006</li>
 * </ul>
 * 
 * @author Justin Thaler
 * @see FrequentLongsEstimator
 */
public class FrequentLongsSketch extends FrequentLongsEstimator {

  /**
   * We start by allocating a small data structure capable of explicitly storing very small streams
   * and then growing it as the stream grows. The following constant controls the size of the
   * initial data structure.
   */
  private static final int MIN_HASHMAP_SIZE = 4; // This is somewhat arbitrary
  
  /**
   * This is a constant large enough that computing the median of SAMPLE_SIZE
   * randomly selected entries from a list of numbers and outputting
   * the empirical median will give a constant-factor approximation to the 
   * true median with high probability
   */
  private static final int SAMPLE_SIZE = 256;
  
  private static final int IGNORE_TOKENS = 6;
  
  /**
   * The number of counters to be supported when sketch is full size
   */
  private final int maxMapCap;
  
  /**
   * The current number of counters supported by the data structure.
   */
  private int curMapCap;
  
  /**
   * Initial length of the arrays internal to the hash map supported by the data structure
   */
  private int initialMapSize;
  
  /**
   * Maximum length of the arrays internal to the hash map supported by the data structure.
   */
  private int maxMapSize;

  /**
   * Hash map mapping stored items to approximate counts
   */
  private ReversePurgeLongHashMap hashMap;

  /**
   * Tracks the total of decremented counts performed.
   */
  private long offset;

  /**
   * An upper bound on the error in any estimated count due to merging with other 
   * FrequentLongsSketches.
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


  // Constructors
  /**
   * Construct this sketch with the parameter maxMapSize and the default initialMapSize (4).
   * 
   * @param maxMapSize Determines the physical size of the internal hash map managed by this sketch
   * and must be a power of 2.  The maximum capacity of this internal hash map is 0.75 times 
   * maxMapSize. Both the ultimate accuracy and size of this sketch are a function of maxMapSize.
   */
  public FrequentLongsSketch(int maxMapSize) {
    this(maxMapSize, MIN_HASHMAP_SIZE);
  }
  
  /**
   * Construct this sketch with parameter mapMapSize and initialMapSize.
   * 
   * @param maxMapSize Determines the physical size of the internal hash map managed by this sketch
   * and must be a power of 2.  The maximum capacity of this internal hash map is 0.75 times 
   * maxMapSize. Both the ultimate accuracy and size of this sketch are a function of maxMapSize.
   * 
   * @param initialMapSize Determines the initial physical size of the internal hash map managed 
   * by this sketch and must be a power of 2.
   */
  FrequentLongsSketch(int maxMapSize, int initialMapSize) { 
    
    checkIfPowerOf2(maxMapSize, "maxMapSize");
    checkIfPowerOf2(initialMapSize, "initialMapSize");
    
    //set initial size of counters data structure
    this.initialMapSize = Math.max(initialMapSize, MIN_HASHMAP_SIZE);
    hashMap = new ReversePurgeLongHashMap(this.initialMapSize);
    this.curMapCap = hashMap.getCapacity();
    
    this.maxMapSize = maxMapSize;
    this.maxMapCap = (int) (maxMapSize*ReversePurgeLongHashMap.getLoadFactor());

    offset = 0;
    sampleSize = Math.min(SAMPLE_SIZE, maxMapCap);
  }
  
  //Factories
  
  /**
   * Returns a sketch instance of this class from the given srcMem, 
   * which must be a Memory representation of this sketch class.
   * 
   * @param srcMem a Memory representation of a sketch of this class. 
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a sketch instance of this class..
   */
  public static FrequentLongsSketch getInstance(Memory srcMem) {
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
      return new FrequentLongsSketch(maxMapSize);

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

    FrequentLongsSketch fi = new FrequentLongsSketch(maxMapSize, curMapSize);
    fi.initialMapSize = initialMapSize;
    fi.offset = offset;
    fi.mergeError = mergeError;

    long[] itemArray = new long[bufferLength];
    long[] countArray = new long[bufferLength];

    srcMem.getLongArray(48, countArray, 0, bufferLength);
    srcMem.getLongArray(48 + 8 * bufferLength, itemArray, 0, bufferLength);
    
    for (int i = 0; i < bufferLength; i++) {
      fi.update(itemArray[i], countArray[i]);
    }
    fi.streamLength = streamLength;
    return fi;
  }
  
  /**
   * Returns a sketch instance of this class from the given String, 
   * which must be a String representation of this sketch class.
   * 
   * @param string a String representation of a sketch of this class.
   * @return a sketch instance of this class.
   */
  public static FrequentLongsSketch getInstance(String string) {
    String[] tokens = string.split(",");
    if (tokens.length < 6) {
      throw new IllegalArgumentException(
          "String not long enough to specify required parameters.");
    }
    int maxMapSize = Integer.parseInt(tokens[0]);
    long mergeError = Long.parseLong(tokens[1]);
    long offset = Long.parseLong(tokens[2]);
    long streamLength = Long.parseLong(tokens[3]);
    int curMapSize = Integer.parseInt(tokens[4]);
    int initialMapSize = Integer.parseInt(tokens[5]);
    
    FrequentLongsSketch sketch = new FrequentLongsSketch(maxMapSize, curMapSize);
    sketch.mergeError = mergeError;
    sketch.offset = offset;
    sketch.streamLength = streamLength;
    sketch.initialMapSize = initialMapSize;
    
    sketch.hashMap = deserializeFromStringArray(tokens);
    return sketch;
  }
  
  //Serialization
  
  /**
   * Returns a String representation of this sketch
   * 
   * @return a String representation of this sketch
   */
  public String serializeToString() {
    StringBuilder sb = new StringBuilder();
    //start the string with 6 parameters of the sketch
    sb.append(
        String.format("%d,%d,%d,%d,%d,%d,", maxMapSize, mergeError, offset, streamLength, hashMap.getLength(), initialMapSize));
    // maxMapCap, samplesize are deterministic functions of maxMapSize, so we don't need them in the serialization
    //output the hashMap
    sb.append(hashMap.serializeToString());
    return sb.toString();
  }
  
  /**
   * Returns a byte array representation of this sketch
   * @return a byte array representation of this sketch
   */
  public byte[] serializeToByteArray() {
    int preLongs, arrLongs;
    boolean empty = isEmpty();

    if (empty) {
      preLongs = 1;
      arrLongs = 1;
    } else {
      preLongs = 6;
      arrLongs = preLongs + 2 * getActiveItems();
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
      pre2 = insertBufferLength(getActiveItems(), pre2);
      preArr[5] = pre2;

      mem.putLongArray(0, preArr, 0, 6);
      mem.putLongArray(48, hashMap.getActiveValues(), 0, this.getActiveItems());
      mem.putLongArray(48 + (this.getActiveItems() << 3), hashMap.getActiveKeys(), 0, this.getActiveItems());
    }
    return outArr;
  }
  
  /**
   * Puts this sketch into the given Memory as a byte array
   * @param dstMem the given destination Memory
   */
  public void serializeToMemory(Memory dstMem) {
    byte[] byteArr = serializeToByteArray();
    int arrLen = byteArr.length;
    long memCap = dstMem.getCapacity();
    if (memCap < arrLen) {
      throw new IllegalArgumentException(
          "Destination Memory not large enough: " + memCap + " < " + arrLen);
    }
    dstMem.putByteArray(0, byteArr, 0, arrLen);
  }
  
  //Override FrequencyEstimator
  
  @Override
  public void update(long item) {
    update(item, 1);
  }

  @Override
  public void update(long item, long count) {
    if (count == 0) return;
    if (count < 0) throw new IllegalArgumentException("Count may not be negative");
    this.streamLength += count;
    hashMap.adjust(item, count);
    int numActive = this.getActiveItems();

    // if the data structure needs to be grown
    if ((numActive >= this.curMapCap) && (this.curMapCap < this.maxMapCap)) {
      // grow the size of the data structure
      int newSize = 2*hashMap.getLength();
      ReversePurgeLongHashMap newTable = new ReversePurgeLongHashMap(newSize);
      this.curMapCap = newTable.getCapacity();
      long[] items = this.hashMap.getActiveKeys();
      long[] counters = this.hashMap.getActiveValues();
      
      assert(items.length == numActive);
      for (int i = 0; i < numActive; i++) {
        newTable.adjust(items[i], counters[i]);
      }
      this.hashMap = newTable;
    }

    //The reason for the +1 here is: If we do not purge now, we might wind up inserting a new 
    //item on the next update, and we don't want this to put us over capacity. 
    //(Going over capacity by 1 is not a big deal, but we may as well be precise).
    if (numActive+1 > this.maxMapCap) {
      offset += hashMap.purge(sampleSize);
      assert (this.getActiveItems() <= this.maxMapCap);
    }
  }

  @Override
  public FrequentLongsEstimator merge(FrequentLongsEstimator other) {
    if (other == null) return this;
    if (!(other instanceof FrequentLongsSketch)) {
      String s = this.getClass().getSimpleName();
      throw new IllegalArgumentException(s + " can only merge with other " + s);
    }
    FrequentLongsSketch otherCasted = (FrequentLongsSketch) other;
    if (otherCasted.isEmpty()) return this;
    
    this.streamLength += otherCasted.streamLength;
    this.mergeError += otherCasted.getMaximumError();

    long[] otherItems = otherCasted.hashMap.getActiveKeys();
    long[] otherCounters = otherCasted.hashMap.getActiveValues();

    for (int i = otherItems.length; i-- > 0;) {
      this.update(otherItems[i], otherCounters[i]);
    }
    return this;
  }

  @Override
  public long getEstimate(long item) {
    // If item is tracked:
    // Estimate = itemCount + offset; Otherwise it is 0.
    long itemCount = hashMap.get(item);
    return (itemCount > 0)? itemCount + offset : 0;
  }

  @Override
  public long getUpperBound(long item) {
    // UB = itemCount + offset + mergeError
    return hashMap.get(item) + getMaximumError();
  }

  @Override
  public long getLowerBound(long item) {
    //LB = max(itemCount - mergeError, 0)
    long returnVal = hashMap.get(item) - mergeError;
    return Math.max(returnVal, 0);
  }
  
  @Override
  public long[] getFrequentItems(long threshold, ErrorSpecification errorSpec) { 
    int count = 0;
    long[] items = hashMap.getKeys(); //ref to raw keys array
    int rawLen = items.length;
    int numActive = hashMap.getNumActive();
    
    // allocate an initial array to store the candidate frequent items
    long[] freqItems = new long[numActive];
    
    count = 0;
    if (errorSpec == ErrorSpecification.NO_FALSE_NEGATIVES) {
      for (int i = rawLen; i-- > 0;) {
        if (hashMap.isActive(i) && (getUpperBound(items[i]) >= threshold)) {
          freqItems[count] = items[i];
          count++;
        }
      }
    } else { //NO_FALSE_POSITIVES
      for (int i = rawLen; i-- > 0;) {
        if (hashMap.isActive(i) && (getLowerBound(items[i]) >= threshold)) {
          freqItems[count] = items[i];
          count++;
        }
      }
    }
    
    long[] outArr = new long[count];
    System.arraycopy(freqItems, 0, outArr, 0, count);
    return outArr;
  }

  @Override
  public int getCurrentMapCapacity() {
    return this.curMapCap;
  }

  @Override
  public long getMaximumError() {
    return offset + mergeError;
  }
  
  @Override
  public boolean isEmpty() {
    return getActiveItems() == 0;
  }
  
  @Override
  public long getStreamLength() {
    return this.streamLength;
  }
  
  @Override
  public int getMaximumMapCapacity() {
    return this.maxMapCap;
  }
  
  @Override
  public int getActiveItems() {
    return hashMap.getNumActive();
  }

  @Override
  public int getStorageBytes() {
    if (isEmpty())
      return 8;
    return 6 * 8 + 16 * getActiveItems();
  }
  
  @Override
  public void reset() {
    hashMap = new ReversePurgeLongHashMap(this.initialMapSize);
    this.curMapCap = hashMap.getCapacity();
    this.offset = 0;
    this.mergeError = 0;
    this.streamLength = 0;
  }
  
  /**
   * Deserializes an array of String tokens into a hash map object of this class.
   * 
   * @param tokens the given array of Strings tokens.
   * @param ignore specifies how many of the initial tokens to ignore. 
   * @return a hash map object of this class
   */
  static ReversePurgeLongHashMap deserializeFromStringArray(String[] tokens) {
    int ignore = IGNORE_TOKENS;
    int numActive = Integer.parseInt(tokens[ignore]); 
    int length = Integer.parseInt(tokens[ignore + 1]);
    ReversePurgeLongHashMap hashMap = new ReversePurgeLongHashMap(length);
    int j = 2 + ignore;
    for (int i = 0; i < numActive; i++) {
      long key = Long.parseLong(tokens[j++]);
      long value = Long.parseLong(tokens[j++]);
      hashMap.adjustOrPutValue(key, value, value);
    }
    return hashMap;
  }

}
