/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.Util.toLog2;
import static com.yahoo.sketches.frequencies.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.frequencies.PreambleUtil.FREQ_SKETCH_TYPE;
import static com.yahoo.sketches.frequencies.PreambleUtil.SER_VER;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractActiveItems;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractLgCurMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractFlags;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractFreqSketchType;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractLgMaxMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertActiveItems;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertLgCurMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertFlags;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertFreqSketchType;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertLgMaxMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertSerVer;

import java.util.ArrayList;
import java.util.Comparator;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * <p>This sketch is useful for tracking approximate frequencies of long items with optional
 * associated counts (<i>long</i> item, <i>long</i> count) that are members of a multiset of 
 * such items. The frequency of an item is defined to be the sum of associated counts.</p>
 * 
 * <p>This implementation provides the following capabilities:</p>
 * <ol>
 * <li>Estimate the frequency of an item.</li>
 * <li>Return upper and lower bounds of any item, such that the true frequency is always between
 * the upper and lower bounds. </li>
 * <li>Return a global maximum error that holds for all items in the stream.</li>
 * <li>Return an array of frequent items that qualify either a NO_FALSE_POSITIVES or a 
 * NO_FALSE_NEGATIVES error type.</li>
 * <li>Merge itself with another sketch object created from this class.</li>
 * <li>Serialize and Deserialize to String or byte array.
 * </ol>
 * 
 * <p><b>Space Usage</b></p>
 * 
 * <p>The sketch is initialized with a maxMapSize that specifies the maximum physical length of the 
 * internal hash map of the form (<i>long</i> item, <i>long</i> count).
 * The maxMapSize must be a power of 2.</p>
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
 * for this implementation and for a specific active <i>item</i>, it is guaranteed that the 
 * difference between the Upper Bound and the Estimate is 
 * max(UB- Est) ~ 2n/k = (8/3)*(n/maxMapSize), where <i>n</i> denotes the stream length 
 * (i.e, sum of all the item counts). The behavior is similar for the Lower Bound and the Estimate.
 * However, this implementation uses a deterministic hash function for performance that performs 
 * well on real data, and in practice, the difference is usually much smaller.</p>
 * 
 * <p><b>Background</b></p>
 * 
 * <p>This code implements a variant of what is commonly known as the "Misra-Gries
 * algorithm". Variants of it were discovered and rediscovered and redesigned several times over 
 * the years:</p>
 * <ul><li>"Finding repeated elements", Misra, Gries, 1982</li>
 * <li>"Frequency estimation of Internet packet streams with limited space" Demaine, Lopez-Ortiz, 
 * Munro, 2002</li>
 * <li>"A simple algorithm for finding frequent elements in streams and bags" Karp, Shenker,
 * Papadimitriou, 2003</li>
 * <li>"Efficient Computation of Frequent and Top-k Elements in Data Streams" Metwally, Agrawal, 
 * Abbadi, 2006</li>
 * </ul>
 * 
 * @author Justin Thaler
 */
public class FrequentLongsSketch {

  public enum ErrorType {NO_FALSE_POSITIVES, NO_FALSE_NEGATIVES}

  /**
   * We start by allocating a small data structure capable of explicitly storing very small streams
   * and then growing it as the stream grows. The following constant controls the size of the
   * initial data structure.
   */
  private static final int LG_MIN_MAP_SIZE = 2; // This is somewhat arbitrary

  /**
   * This is a constant large enough that computing the median of SAMPLE_SIZE
   * randomly selected entries from a list of numbers and outputting
   * the empirical median will give a constant-factor approximation to the 
   * true median with high probability
   */
  private static final int SAMPLE_SIZE = 256;

  private static final int STR_PREAMBLE_TOKENS = 8;

  /**
   * Log2 Maximum length of the arrays internal to the hash map supported by the data structure.
   */
  private int lgMaxMapSize;

  /**
   * The current number of counters supported by the hash map.
   */
  private int curMapCap; //the threshold to purge

  /**
   * An upper bound on the error in any estimated count due to merging with other 
   * FrequentLongsSketches.
   */
  private long mergeError;

  /**
   * Tracks the total of decremented counts performed.
   */
  private long offset;

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
   * Hash map mapping stored items to approximate counts
   */
  private ReversePurgeLongHashMap hashMap;

  /**
   * Construct this sketch with the parameter maxMapSize and the default initialMapSize (4).
   * 
   * @param maxMapSize Determines the physical size of the internal hash map managed by this sketch
   * and must be a power of 2.  The maximum capacity of this internal hash map is 0.75 times 
   * maxMapSize. Both the ultimate accuracy and size of this sketch are a function of maxMapSize.
   */
  public FrequentLongsSketch(final int maxMapSize) {
    this(toLog2(maxMapSize, "maxMapSize"), LG_MIN_MAP_SIZE);
  }

  /**
   * Construct this sketch with parameter lgMapMapSize and lgCurMapSize. This internal constructor 
   * is used when deserializing the sketch.
   * 
   * @param lgMaxMapSize Log2 of the physical size of the internal hash map managed by this sketch.
   * The maximum capacity of this internal hash map is 0.75 times 2^lgMaxMapSize.
   * Both the ultimate accuracy and size of this sketch are a function of lgMaxMapSize.
   * 
   * @param lgCurMapSize Log2 of the starting (current) physical size of the internal hash map 
   * managed by this sketch.
   */
  FrequentLongsSketch(final int lgMaxMapSize, final int lgCurMapSize) {
    //set initial size of hash map
    this.lgMaxMapSize = lgMaxMapSize;
    final int lgCurMapSz = Math.max(lgCurMapSize, LG_MIN_MAP_SIZE);
    hashMap = new ReversePurgeLongHashMap(1 << lgCurMapSz);
    this.curMapCap = hashMap.getCapacity(); 
    final int maxMapCap = (int) ((1 << lgMaxMapSize)*ReversePurgeLongHashMap.getLoadFactor());
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
  public static FrequentLongsSketch getInstance(final Memory srcMem) {
    final long pre0 = PreambleUtil.getAndCheckPreLongs(srcMem);  //make sure we can get the preamble
    final int maxPreLongs = Family.FREQUENCY.getMaxPreLongs();

    final int preLongs = extractPreLongs(pre0);         //Byte 0
    final int serVer = extractSerVer(pre0);             //Byte 1
    final int familyID = extractFamilyID(pre0);         //Byte 2
    final int lgMaxMapSize = extractLgMaxMapSize(pre0); //Byte 3
    final int lgCurMapSize = extractLgCurMapSize(pre0); //Byte 4
    final boolean empty = (extractFlags(pre0) & EMPTY_FLAG_MASK) != 0; //Byte 5
    final int type = extractFreqSketchType(pre0);       //Byte 6

    // Checks
    final boolean preLongsEq1 = (preLongs == 1);        //Byte 0
    final boolean preLongsEqMax = (preLongs == maxPreLongs);
    if (!preLongsEq1 && !preLongsEqMax) {
      throw new IllegalArgumentException(
          "Possible Corruption: PreLongs must be 1 or "+maxPreLongs+": " + preLongs);
    }
    if (serVer != SER_VER) {                      //Byte 1
      throw new IllegalArgumentException(
          "Possible Corruption: Ser Ver must be "+SER_VER+": " + serVer);
    }
    final int actFamID = Family.FREQUENCY.getID();      //Byte 2
    if (familyID != actFamID) {
      throw new IllegalArgumentException(
          "Possible Corruption: FamilyID must be "+actFamID+": " + familyID);
    }
    if (empty ^ preLongsEq1) {                    //Byte 5 and Byte 0
      throw new IllegalArgumentException(
          "Possible Corruption: (PreLongs == 1) ^ Empty == True.");
    }
    if (type != FREQ_SKETCH_TYPE) {               //Byte 6
      throw new IllegalArgumentException(
          "Possible Corruption: Freq Sketch Type != 1: "+type);
    }

    if (empty) {
      return new FrequentLongsSketch(lgMaxMapSize, LG_MIN_MAP_SIZE);
    }
    //get full preamble
    final long[] preArr = new long[preLongs];
    srcMem.getLongArray(0, preArr, 0, preLongs);

    FrequentLongsSketch fls = new FrequentLongsSketch(lgMaxMapSize, lgCurMapSize);
    fls.streamLength = 0; //update after
    fls.offset = preArr[3];
    fls.mergeError = preArr[4];

    final int preBytes = preLongs << 3;
    final int activeItems = extractActiveItems(preArr[1]);
    //Get countArray
    final long[] countArray = new long[activeItems];
    srcMem.getLongArray(preBytes, countArray, 0, activeItems);
    //Get itemArray
    final int itemsOffset = preBytes + 8 * activeItems;
    final long[] itemArray = new long[activeItems];
    srcMem.getLongArray(itemsOffset, itemArray, 0, activeItems);
    //update the sketch
    for (int i = 0; i < activeItems; i++) {
      fls.update(itemArray[i], countArray[i]);
    }
    fls.streamLength = preArr[2]; //override count due to updating
    return fls;
  }

  /**
   * Returns a sketch instance of this class from the given String, 
   * which must be a String representation of this sketch class.
   * 
   * @param string a String representation of a sketch of this class.
   * @return a sketch instance of this class.
   */
  public static FrequentLongsSketch getInstance(final String string) {
    final String[] tokens = string.split(",");
    if (tokens.length < STR_PREAMBLE_TOKENS) {
      throw new IllegalArgumentException(
          "String not long enough to specify preamble.");
    }
    final int serVer  = Integer.parseInt(tokens[0]);
    final int famID   = Integer.parseInt(tokens[1]);
    final int lgMax   = Integer.parseInt(tokens[2]);
    final int flags   = Integer.parseInt(tokens[3]);
    final int type    = Integer.parseInt(tokens[4]);
    final long streamLength = Long.parseLong(tokens[5]);
    final long offset       = Long.parseLong(tokens[6]);
    final long mergeError   = Long.parseLong(tokens[7]);
    //should always get at least the next 2 from the map
    final int numActive = Integer.parseInt(tokens[8]);
    final int lgCur = Integer.numberOfTrailingZeros(Integer.parseInt(tokens[9]));

    //checks
    if (serVer != SER_VER) {
      throw new IllegalArgumentException("Possible Corruption: Bad SerVer: "+serVer);
    }
    Family.FREQUENCY.checkFamilyID(famID);
    final boolean empty = flags > 0;
    final boolean zeroStream = (streamLength == 0);
    if (empty ^ zeroStream) {
      throw new IllegalArgumentException(
          "Possible Corruption: (Empty ^ StreamLength=0) = true : Empty: "+empty + 
          ", strLen: "+streamLength);
    }
    if (type != FREQ_SKETCH_TYPE) {
      throw new IllegalArgumentException(
          "Possible Corruption: Sketch TYPE incorrect: "+type);
    }
    final int numTokens = tokens.length;
    if (numActive != (numTokens - STR_PREAMBLE_TOKENS -2)/2) {
      throw new IllegalArgumentException("Possible Corruption: Incorrect # of tokens: "+numTokens+
          ", numActive: "+numActive);
    }

    final FrequentLongsSketch sketch = new FrequentLongsSketch(lgMax, lgCur);
    sketch.streamLength = streamLength;
    sketch.mergeError = mergeError;
    sketch.offset = offset;
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
    final StringBuilder sb = new StringBuilder();
    //start the string with parameters of the sketch
    final int serVer = SER_VER; //0
    final int famID = Family.FREQUENCY.getID(); //1
    final int lgMaxMapSz = lgMaxMapSize; //2
    final int flags = (hashMap.getNumActive() == 0)? EMPTY_FLAG_MASK : 0; //3
    final int type = FREQ_SKETCH_TYPE; //4
    final String fmt = "%d,%d,%d,%d,%d,%d,%d,%d,";
    final String s = String.format(fmt, serVer, famID, lgMaxMapSz, flags, type, streamLength, offset, mergeError);
    sb.append(s);
    sb.append(hashMap.serializeToString()); //numActive, curMaplen, key[i], value[i], ...
    // maxMapCap, samplesize are deterministic functions of maxMapSize, 
    //  so we don't need them in the serialization
    return sb.toString();
  }

  /**
   * Returns a byte array representation of this sketch
   * @return a byte array representation of this sketch
   */
  public byte[] serializeToByteArray() {
    final int preLongs, outBytes;
    final boolean empty = isEmpty();
    final int activeItems = getNumActiveItems();
    if (empty) {
      preLongs = 1;
      outBytes = 8;
    } else {
      preLongs = Family.FREQUENCY.getMaxPreLongs();
      outBytes = (preLongs + 2 * activeItems) << 3;
    }
    final byte[] outArr = new byte[outBytes];
    final NativeMemory mem = new NativeMemory(outArr);

    // build first preLong empty or not
    long pre0 = 0L;
    pre0 = insertPreLongs(preLongs, pre0);         //Byte 0
    pre0 = insertSerVer(SER_VER, pre0);            //Byte 1
    pre0 = insertFamilyID(10, pre0);               //Byte 2
    pre0 = insertLgMaxMapSize(lgMaxMapSize, pre0); //Byte 3
    pre0 = insertLgCurMapSize(hashMap.getLgLength(), pre0); //Byte 4
    pre0 = (empty)? insertFlags(EMPTY_FLAG_MASK, pre0) : insertFlags(0, pre0); //Byte 5
    pre0 = insertFreqSketchType(FREQ_SKETCH_TYPE, pre0); //Byte 6

    if (empty) {
      mem.putLong(0, pre0);
    } else {
      final long pre = 0;
      final long[] preArr = new long[preLongs];
      preArr[0] = pre0;
      preArr[1] = insertActiveItems(activeItems, pre);
      preArr[2] = this.streamLength;
      preArr[3] = this.offset;
      preArr[4] = this.mergeError;
      mem.putLongArray(0, preArr, 0, preLongs);
      final int preBytes = preLongs << 3;
      mem.putLongArray(preBytes, hashMap.getActiveValues(), 0, activeItems);
   
      mem.putLongArray(preBytes + (activeItems << 3), hashMap.getActiveKeys(), 0, activeItems);
    }
    return outArr;
  }

  /**
   * Puts this sketch into the given Memory as a byte array
   * @param dstMem the given destination Memory
   */
  public void serializeToMemory(final Memory dstMem) {
    final byte[] byteArr = serializeToByteArray();
    final int arrLen = byteArr.length;
    final long memCap = dstMem.getCapacity();
    if (memCap < arrLen) {
      throw new IllegalArgumentException(
          "Destination Memory not large enough: " + memCap + " < " + arrLen);
    }
    dstMem.putByteArray(0, byteArr, 0, arrLen);
  }

  /**
   * Update this sketch with an item and a frequency count of one.
   * @param item for which the frequency should be increased. 
   */
  public void update(final long item) {
    update(item, 1);
  }

  /**
   * Update this sketch with a item and a positive frequency count. 
   * @param item for which the frequency should be increased. The item can be any long value and is 
   * only used by the sketch to determine uniqueness.
   * @param count the amount by which the frequency of the item should be increased. 
   * An count of zero is a no-op, and a negative count will throw an exception.
   */
  public void update(final long item, final long count) {
    if (count == 0) return;
    if (count < 0) throw new IllegalArgumentException("Count may not be negative");
    this.streamLength += count;
    hashMap.adjust(item, count);
    final int numActive = getNumActiveItems();

    if (hashMap.getLgLength() < lgMaxMapSize) { //below tgt size
      if (numActive >= curMapCap) {
        hashMap.resize(2 * hashMap.getLength());
        this.curMapCap = hashMap.getCapacity();
      }
    } else { //at tgt size
      //The reason for the +1 here is: If we do not purge now, we might wind up inserting a new 
      //item on the next update, and we don't want this to put us over capacity. 
      //(Going over capacity by 1 is not a big deal, but we may as well be precise).
      if (numActive+1 > curMapCap) {
        //need to purge and rebuild the map
        offset += hashMap.purge(sampleSize);
        if (getNumActiveItems() > getMaximumMapCapacity()) {
          throw new IllegalStateException("Map Purge did not reduce active items.");
        }
      }
    }
  }

  /**
   * This function merges the other sketch into this one. 
   * The other sketch may be of a different size.
   * 
   * @param other sketch of this class 
   * @return a sketch whose estimates are within the guarantees of the
   * largest error tolerance of the two merged sketches.
   */
  public FrequentLongsSketch merge(final FrequentLongsSketch other) {
    if (other == null) return this;
    if (other.isEmpty()) return this;

    final long streamLen = this.streamLength + other.streamLength;
    this.mergeError += other.getMaximumError();

    final ReversePurgeLongHashMap.Iterator iter = other.hashMap.iterator();
    while (iter.next()) {
      this.update(iter.getKey(), iter.getValue());
    }

    this.streamLength = streamLen;
    return this;
  }

  /**
   * Gets the estimate of the frequency of the given item. 
   * Note: The true frequency of a item would be the sum of the counts as a result of the two 
   * update functions.
   * 
   * @param item the given item
   * @return the estimate of the frequency of the given item
   */
  public long getEstimate(final long item) {
    // If item is tracked:
    // Estimate = itemCount + offset; Otherwise it is 0.
    final long itemCount = hashMap.get(item);
    return (itemCount > 0) ? itemCount + offset : 0;
  }

  /**
   * Gets the guaranteed upper bound frequency of the given item.
   * 
   * @param item the given item
   * @return the guaranteed upper bound frequency of the given item. That is, a number which is 
   * guaranteed to be no smaller than the real frequency.
   */
  public long getUpperBound(final long item) {
    // UB = itemCount + offset + mergeError
    return hashMap.get(item) + getMaximumError();
  }

  /**
   * Gets the guaranteed lower bound frequency of the given item, which can never be negative.
   * 
   * @param item the given item.
   * @return the guaranteed lower bound frequency of the given item. That is, a number which is 
   * guaranteed to be no larger than the real frequency.
   */
  public long getLowerBound(final long item) {
    //LB = max(itemCount - mergeError, 0)
    final long returnVal = hashMap.get(item) - mergeError;
    return Math.max(returnVal, 0);
  }

  /**
   * Returns an array of Rows that include frequent items, estimates, upper and lower bounds
   * given an ErrorCondition. 
   * 
   * The method first examines all active items in the sketch (items that have a counter).
   *  
   * <p>If <i>ErrorType = NO_FALSE_NEGATIVES</i>, this will include an item in the result list 
   * if getUpperBound(item) &gt; maxError. 
   * There will be no false negatives, i.e., no Type II error.
   * There may be items in the set with true frequencies less than the threshold (false positives).</p>
   * 
   * <p>If <i>ErrorType = NO_FALSE_POSITIVES</i>, this will include an item in the result list 
   * if getLowerBound(item) &gt; maxError. 
   * There will be no false positives, i.e., no Type I error.
   * There may be items omitted from the set with true frequencies greater than the threshold 
   * (false negatives).</p>
   * 
   * @param errorType determines whether no false positives or no false negatives are desired.
   * @return an array of frequent items
   */
  public Row[] getFrequentItems(final ErrorType errorType) { 
    return sortItems(getMaximumError(), errorType);
  }

  public class Row implements Comparable<Row> {
    final long item;
    final long est;
    final long ub;
    final long lb;

    Row(final long item, final long estimate, final long ub, final long lb) {
      this.item = item;
      this.est = estimate;
      this.ub = ub;
      this.lb = lb;
    }

    @Override
    public String toString() {
      return String.format("%d,%d,%d,%d", item, est, ub, lb);
    }

    @Override
    public int compareTo(final Row that) {
      return (this.est < that.est) ? -1 : (this.est > that.est) ? 1 : 0;
    }
  }

  Row[] sortItems(final long threshold, final ErrorType errorType) {
    final ArrayList<Row> rowList = new ArrayList<Row>();
    final ReversePurgeLongHashMap.Iterator iter = hashMap.iterator();
    if (errorType == ErrorType.NO_FALSE_NEGATIVES) {
      while (iter.next()) {
        final long ub = getUpperBound(iter.getKey());
        final long lb = getLowerBound(iter.getKey());
        if (ub >= threshold) {
          final Row row = new Row(iter.getKey(), iter.getValue(), ub, lb);
          rowList.add(row);
        }
      }
    } else { //NO_FALSE_POSITIVES
      while (iter.next()) {
        final long ub = getUpperBound(iter.getKey());
        final long lb = getLowerBound(iter.getKey());
        if (lb >= threshold) {
          final Row row = new Row(iter.getKey(), iter.getValue(), ub, lb);
          rowList.add(row);
        }
      }
    }

    rowList.sort(new Comparator<Row>() {
      @Override
      public int compare(final Row r1, final Row r2) {
        return r1.compareTo(r2);
      }
    });
    
    return rowList.toArray(new Row[rowList.size()]);
  }

  /**
   * Returns the current number of counters the sketch is configured to support.
   * 
   * @return the current number of counters the sketch is configured to support.
   */
  public int getCurrentMapCapacity() {
    return this.curMapCap;
  }

  /**
   * @return An upper bound on the maximum error of getEstimate(item) for any item. 
   * This is equivalent to the maximum distance between the upper bound and the lower bound for 
   * any item.
   */
  public long getMaximumError() {
    return offset + mergeError;
  }

  /**
   * Returns true if this sketch is empty
   * 
   * @return true if this sketch is empty
   */
  public boolean isEmpty() {
    return getNumActiveItems() == 0;
  }

  /**
   * Returns the sum of the frequencies in the stream seen so far by the sketch
   * 
   * @return the sum of the frequencies in the stream seen so far by the sketch
   */
  public long getStreamLength() {
    return this.streamLength;
  }

  /**
   * Returns the maximum number of counters the sketch is configured to support.
   * 
   * @return the maximum number of counters the sketch is configured to support.
   */
  public int getMaximumMapCapacity() {
    return (int) ((1 << lgMaxMapSize)*ReversePurgeLongHashMap.getLoadFactor());
  }

  /**
   * @return the number of active items in the sketch.
   */
  public int getNumActiveItems() {
    return hashMap.getNumActive();
  }

  /**
   * Returns the number of bytes required to store this sketch as an array of bytes.
   * 
   * @return the number of bytes required to store this sketch as an array of bytes.
   */
  public int getStorageBytes() {
    if (isEmpty())
      return 8;
    return 6 * 8 + 16 * getNumActiveItems();
  }

  /**
   * Resets this sketch to a virgin state.
   */
  public void reset() {
    hashMap = new ReversePurgeLongHashMap(1 << LG_MIN_MAP_SIZE);
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
    final int ignore = STR_PREAMBLE_TOKENS;
    final int numActive = Integer.parseInt(tokens[ignore]); 
    final int length = Integer.parseInt(tokens[ignore + 1]);
    final ReversePurgeLongHashMap hashMap = new ReversePurgeLongHashMap(length);
    int j = 2 + ignore;
    for (int i = 0; i < numActive; i++) {
      long key = Long.parseLong(tokens[j++]);
      long value = Long.parseLong(tokens[j++]);
      hashMap.adjustOrPutValue(key, value, value);
    }
    return hashMap;
  }

}
