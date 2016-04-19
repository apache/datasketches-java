/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.Util.*;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
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

import java.lang.reflect.Array;

import static com.yahoo.sketches.frequencies.PreambleUtil.insertEmptyFlag;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertCurMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertBufferLength;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertInitialMapSize;

/**
 * <p>This sketch is useful for tracking approximate frequencies of items that are 
 * internally implemented as a hash map (<i>Object</i> item, <i>long</i> count).</p>
 * 
 * <p><b>Space Usage</b></p>
 * 
 * <p>The sketch is initialized with a maxMapSize that specifies the maximum length of the 
 * internal arrays used by the hash map. The maxMapSize must be a power of 2.</p>
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
 * <p>If the internal hash function had infinite precision and was perfectly uniform: Then,
 * for this implementation and for a specific active <i>item</i>, it is guaranteed that the difference 
 * between the Upper Bound and the Estimate is max(UB- Est) ~ 2n/k = (8/3)*(n/maxMapSize), where 
 * </i>n</i> denotes the stream length (i.e, sum of all the item counts). The behavior is similar
 * for the Lower Bound and the Estimate.
 * However, this implementation uses a deterministic hash function for performance that performs 
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
 */
public class FrequentItemsSketch<T> {

  public enum ErrorSpecification {NO_FALSE_POSITIVES, NO_FALSE_NEGATIVES}

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
  private ReversePurgeItemHashMap<T> hashMap;

  /**
   * Tracks the total of decremented counts performed.
   */
  private long offset;

  /**
   * An upper bound on the error in any estimated count due to merging with other 
   * FrequentItemsSketches.
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

  /**
   * Construct this sketch with the parameter maxMapSize and the default initialMapSize (4).
   * 
   * @param maxMapSize Determines the physical size of the internal hash map managed by this sketch
   * and must be a power of 2. The maximum capacity of this internal hash map is 0.75 times 
   * maxMapSize. Both the ultimate accuracy and size of this sketch are a function of maxMapSize.
   */
  public FrequentItemsSketch(final int maxMapSize) {
    this(maxMapSize, MIN_HASHMAP_SIZE);
  }

  /**
   * Construct this sketch with parameter mapMapSize and initialMapSize.
   * 
   * @param maxMapSize Determines the physical size of the internal hash map managed by this sketch
   * and must be a power of 2. The maximum capacity of this internal hash map is 0.75 times 
   * maxMapSize. Both the ultimate accuracy and size of this sketch are a function of maxMapSize.
   * 
   * @param initialMapSize Determines the initial physical size of the internal hash map managed 
   * by this sketch and must be a power of 2.
   */
  FrequentItemsSketch(final int maxMapSize, final int initialMapSize) { 

    checkIfPowerOf2(maxMapSize, "maxMapSize");
    checkIfPowerOf2(initialMapSize, "initialMapSize");

    //set initial size of counters data structure
    this.initialMapSize = Math.max(initialMapSize, MIN_HASHMAP_SIZE);
    hashMap = new ReversePurgeItemHashMap<T>(this.initialMapSize);
    this.curMapCap = hashMap.getCapacity();

    this.maxMapSize = maxMapSize;
    this.maxMapCap = (int) (maxMapSize*ReversePurgeItemHashMap.getLoadFactor());

    offset = 0;
    sampleSize = Math.min(SAMPLE_SIZE, maxMapCap);
  }

  /**
   * Returns a sketch instance of this class from the given srcMem, 
   * which must be a Memory representation of this sketch class.
   * 
   * @param srcMem a Memory representation of a sketch of this class. 
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return a sketch instance of this class..
   */
  public static <T> FrequentItemsSketch<T> getInstance(final Memory srcMem, final ArrayOfItemsSerDe<T> serDe) {
    final long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) {
      throw new IllegalArgumentException("Memory too small: " + memCapBytes);
    }

    final long pre0 = srcMem.getLong(0);
    final int preambleLongs = extractPreLongs(pre0);

    assert ((preambleLongs == 1) || (preambleLongs == 6));
    final int serVer = extractSerVer(pre0);
    assert (serVer == 1);
    final int familyID = extractFamilyID(pre0);
    assert (familyID == 10);
    final int emptyFlag = extractEmptyFlag(pre0);
    final int maxMapSize = extractMaxMapSize(pre0);

    if (emptyFlag == 1)
      return new FrequentItemsSketch<T>(maxMapSize);

    // Not empty, must have valid preamble
    final long[] remainderPreArr = new long[5];
    srcMem.getLongArray(8, remainderPreArr, 0, 5);

    final long mergeError = remainderPreArr[0];
    final long offset = remainderPreArr[1];
    final long streamLength = remainderPreArr[2];
    final long pre1 = remainderPreArr[3];
    final long pre2 = remainderPreArr[4];

    final int curMapSize = extractCurMapSize(pre1);
    final int initialMapSize = extractInitialMapSize(pre1);
    final int bufferLength = extractBufferLength(pre2);

    final FrequentItemsSketch<T> fi = new FrequentItemsSketch<T>(maxMapSize, curMapSize);
    fi.initialMapSize = initialMapSize;
    fi.offset = offset;
    fi.mergeError = mergeError;

    final long[] countArray = new long[bufferLength];
    srcMem.getLongArray(48, countArray, 0, bufferLength);

    final int itemsOffset = 48 + 8 * bufferLength;
    final T[] itemArray = serDe.deserializeFromMemory(new MemoryRegion(srcMem, itemsOffset, srcMem.getCapacity() - itemsOffset), bufferLength);

    for (int i = 0; i < bufferLength; i++) {
      fi.update(itemArray[i], countArray[i]);
    }
    fi.streamLength = streamLength;
    return fi;
  }

  /**
   * Returns a byte array representation of this sketch
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return a byte array representation of this sketch
   */
  public byte[] serializeToByteArray(final ArrayOfItemsSerDe<T> serDe) {
    int preLongs, arrLongs;
    boolean empty = isEmpty();

    if (empty) {
      preLongs = 1;
      arrLongs = 1;
    } else {
      preLongs = 6;
      arrLongs = preLongs + 2 * getNumActiveItems();
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
      pre2 = insertBufferLength(getNumActiveItems(), pre2);
      preArr[5] = pre2;

      mem.putLongArray(0, preArr, 0, 6);
      mem.putLongArray(48, hashMap.getActiveValues(), 0, this.getNumActiveItems());
      byte[] bytes = serDe.serializeToByteArray(hashMap.getActiveKeys());
      mem.putByteArray(48 + (this.getNumActiveItems() << 3), bytes, 0, bytes.length);
    }
    return outArr;
  }

  public void update(final T item) {
    update(item, 1);
  }

  public void update(final T item, final long count) {
    if (item == null || count == 0) return;
    if (count < 0) throw new IllegalArgumentException("Count may not be negative");
    this.streamLength += count;
    hashMap.adjust(item, count);
    final int numActive = this.getNumActiveItems();

    // if the data structure needs to be grown
    if ((numActive >= this.curMapCap) && (this.curMapCap < this.maxMapCap)) {
      // grow the size of the data structure
      final int newSize = 2*hashMap.getLength();
      final ReversePurgeItemHashMap<T> newTable = new ReversePurgeItemHashMap<T>(newSize);
      this.curMapCap = newTable.getCapacity();
      final T[] items = this.hashMap.getActiveKeys();
      final long[] counters = this.hashMap.getActiveValues();
      
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
      assert (this.getNumActiveItems() <= this.maxMapCap);
    }
  }

  public FrequentItemsSketch<T> merge(final FrequentItemsSketch<T> other) {
    long streamLength = this.streamLength;
    this.mergeError += other.getMaximumError();

    final T[] otherItems = other.hashMap.getActiveKeys();
    final long[] otherCounters = other.hashMap.getActiveValues();

    for (int i = otherItems.length; i-- > 0;) {
      this.update(otherItems[i], otherCounters[i]);
    }
    this.streamLength = streamLength + other.getStreamLength();
    return this;
  }

  public long getEstimate(final T item) {
    // If item is tracked:
    // Estimate = itemCount + offset; Otherwise it is 0.
    final long itemCount = hashMap.get(item);
    return (itemCount > 0) ? itemCount + offset : 0;
  }

  public long getUpperBound(final T item) {
    // UB = itemCount + offset + mergeError
    return hashMap.get(item) + getMaximumError();
  }

  public long getLowerBound(final T item) {
    //LB = max(itemCount - mergeError, 0)
    final long returnVal = hashMap.get(item) - mergeError;
    return Math.max(returnVal, 0);
  }

  @SuppressWarnings("unchecked")
  public T[] getFrequentItems(final long threshold, final ErrorSpecification errorSpec) { 
    final Object[] items = hashMap.getKeys(); //ref to raw keys array
    final int rawLen = items.length;
    int numActive = hashMap.getNumActive();

    // initial array to store the candidate frequent items
    T[] freqItems = null;

    int count = 0;
    if (errorSpec == ErrorSpecification.NO_FALSE_NEGATIVES) {
      for (int i = rawLen; i-- > 0;) {
        if (hashMap.isActive(i) && (getUpperBound((T) items[i]) >= threshold)) {
          if (freqItems == null) freqItems = (T[]) Array.newInstance(items[i].getClass(), numActive); 
          freqItems[count] = (T) items[i];
          count++;
        }
      }
    } else { //NO_FALSE_POSITIVES
      for (int i = rawLen; i-- > 0;) {
        if (hashMap.isActive(i) && (getLowerBound((T) items[i]) >= threshold)) {
          if (freqItems == null) freqItems = (T[]) Array.newInstance(items[i].getClass(), numActive); 
          freqItems[count] = (T) items[i];
          count++;
        }
      }
    }

    final T[] outArr = (T[]) Array.newInstance(freqItems.getClass().getComponentType(), count);
    System.arraycopy(freqItems, 0, outArr, 0, count);
    return outArr;
  }

  public int getCurrentMapCapacity() {
    return this.curMapCap;
  }

  public long getMaximumError() {
    return offset + mergeError;
  }

  public boolean isEmpty() {
    return getNumActiveItems() == 0;
  }

  public long getStreamLength() {
    return this.streamLength;
  }

  public int getMaximumMapCapacity() {
    return this.maxMapCap;
  }

  public int getNumActiveItems() {
    return hashMap.getNumActive();
  }

  public void reset() {
    hashMap = new ReversePurgeItemHashMap<T>(this.initialMapSize);
    this.curMapCap = hashMap.getCapacity();
    this.offset = 0;
    this.mergeError = 0;
    this.streamLength = 0;
  }

}
