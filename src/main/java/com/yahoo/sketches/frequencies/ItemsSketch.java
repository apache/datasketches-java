/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.isPowerOf2;
import static com.yahoo.sketches.Util.toLog2;
import static com.yahoo.sketches.frequencies.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.frequencies.PreambleUtil.SER_VER;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractActiveItems;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractFlags;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractLgCurMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractLgMaxMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertActiveItems;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertFlags;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertLgCurMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertLgMaxMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.frequencies.Util.LG_MIN_MAP_SIZE;
import static com.yahoo.sketches.frequencies.Util.SAMPLE_SIZE;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Comparator;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * <p>This sketch is useful for tracking approximate frequencies of items of type <i>&lt;T&gt;</i>
 * with optional associated counts (<i>&lt;T&gt;</i> item, <i>long</i> count) that are members of a
 * multiset of such items. The true frequency of an item is defined to be the sum of associated
 * counts.</p>
 *
 * <p>This implementation provides the following capabilities:</p>
 * <ul>
 * <li>Estimate the frequency of an item.</li>
 * <li>Return upper and lower bounds of any item, such that the true frequency is always
 * between the upper and lower bounds.</li>
 * <li>Return a global maximum error that holds for all items in the stream.</li>
 * <li>Return an array of frequent items that qualify either a NO_FALSE_POSITIVES or a
 * NO_FALSE_NEGATIVES error type.</li>
 * <li>Merge itself with another sketch object created from this class.</li>
 * <li>Serialize/Deserialize to/from a byte array.</li>
 * </ul>
 *
 * <p><b>Space Usage</b></p>
 *
 * <p>The sketch is initialized with a <i>maxMapSize</i> that specifies the maximum physical
 * length of the internal hash map of the form (<i>&lt;T&gt;</i> item, <i>long</i> count).
 * The <i>maxMapSize</i> must be a power of 2.</p>
 *
 * <p>The hash map starts at a very small size (8 entries), and grows as needed up to the
 * specified <i>maxMapSize</i>.</p>
 *
 * <p>Excluding external space required for the item objects, the internal memory space usage of
 * this sketch is 18 * <i>mapSize</i> bytes (assuming 8 bytes for each Java reference), plus a small
 * constant number of additional bytes. The internal memory space usage of this sketch will never
 * exceed 18 * <i>maxMapSize</i> bytes, plus a small constant number of additional bytes.</p>
 *
 * <p><b>Maximum Capacity of the Sketch</b></p>
 *
 * <p>The LOAD_FACTOR for the hash map is internally set at 75%,
 * which means at any time the map capacity of (item, count) pairs is <i>mapCap</i> = 0.75 *
 * <i>mapSize</i>.
 * The maximum capacity of (item, count) pairs of the sketch is <i>maxMapCap</i> = 0.75 *
 * <i>maxMapSize</i>.</p>
 *
 * <p><b>Updating the sketch with (item, count) pairs</b></p>
 *
 * <p>If the item is found in the hash map, the mapped count field (the "counter") is
 * incremented by the incoming count, otherwise, a new counter "(item, count) pair" is
 * created. If the number of tracked counters reaches the maximum capacity of the hash map
 * the sketch decrements all of the counters (by an approximately computed median), and
 * removes any non-positive counters.</p>
 *
 * <p><b>Accuracy</b></p>
 *
 * <p>If fewer than 0.75 * <i>maxMapSize</i> different items are inserted into the sketch the
 * estimated frequencies returned by the sketch will be exact.</p>
 *
 * <p>The logic of the frequent items sketch is such that the stored counts and true counts are
 * never too different.
 * More specifically, for any <i>item</i>, the sketch can return an estimate of the
 * true frequency of <i>item</i>, along with upper and lower bounds on the frequency
 * (that hold deterministically).</p>
 *
 * <p>For this implementation and for a specific active <i>item</i>, it is guaranteed that
 * the true frequency will be between the Upper Bound (UB) and the Lower Bound (LB) computed for
 * that <i>item</i>.  Specifically, <i>(UB- LB) &le; W * epsilon</i>, where <i>W</i> denotes the
 * sum of all item counts, and <i>epsilon = 3.5/M</i>, where <i>M</i> is the <i>maxMapSize</i>.</p>
 *
 * <p>This is a worst case guarantee that applies to arbitrary inputs.<sup>1</sup>
 * For inputs typically seen in practice <i>(UB-LB)</i> is usually much smaller.
 * </p>
 *
 * <p><b>Background</b></p>
 *
 * <p>This code implements a variant of what is commonly known as the "Misra-Gries
 * algorithm". Variants of it were discovered and rediscovered and redesigned several times
 * over the years:</p>
 * <ul><li>"Finding repeated elements", Misra, Gries, 1982</li>
 * <li>"Frequency estimation of Internet packet streams with limited space" Demaine,
 * Lopez-Ortiz, Munro, 2002</li>
 * <li>"A simple algorithm for finding frequent elements in streams and bags" Karp, Shenker,
 * Papadimitriou, 2003</li>
 * <li>"Efficient Computation of Frequent and Top-k Elements in Data Streams" Metwally,
 * Agrawal, Abbadi, 2006</li>
 * </ul>
 *
 * <sup>1</sup> For speed we do employ some randomization that introduces a small probability that
 * our proof of the worst-case bound might not apply to a given run.  However, we have ensured
 * that this probability is extremely small. For example, if the stream causes one table purge
 * (rebuild), our proof of the worst case bound applies with probability at least 1 - 1E-14.
 * If the stream causes 1E9 purges, our proof applies with probability at least 1 - 1E-5.
 *
 * @param <T> The type of item to be tracked by this sketch
 *
 * @author Justin Thaler
 * @author Alexander Saydakov
 */
public class ItemsSketch<T> {

  /**
   * Log2 Maximum length of the arrays internal to the hash map supported by the data
   * structure.
   */
  private int lgMaxMapSize;

  /**
   * The current number of counters supported by the hash map.
   */
  private int curMapCap; //the threshold to purge

  /**
   * Tracks the total of decremented counts.
   */
  private long offset;

  /**
   * The sum of all frequencies of the stream so far.
   */
  private long streamWeight = 0;

  /**
   * The maximum number of samples used to compute approximate median of counters when doing
   * decrement
   */
  private int sampleSize;

  /**
   * Hash map mapping stored items to approximate counts
   */
  private ReversePurgeItemHashMap<T> hashMap;

  /**
   * Construct this sketch with the parameter maxMapSize and the default initialMapSize (8).
   *
   * @param maxMapSize Determines the physical size of the internal hash map managed by this
   * sketch and must be a power of 2.  The maximum capacity of this internal hash map is
   * 0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are a
   * function of maxMapSize.
   */
  public ItemsSketch(final int maxMapSize) {
    this(toLog2(maxMapSize, "maxMapSize"), LG_MIN_MAP_SIZE);
  }

  /**
   * Construct this sketch with parameter lgMapMapSize and lgCurMapSize. This internal
   * constructor is used when deserializing the sketch.
   *
   * @param lgMaxMapSize Log2 of the physical size of the internal hash map managed by this
   * sketch. The maximum capacity of this internal hash map is 0.75 times 2^lgMaxMapSize.
   * Both the ultimate accuracy and size of this sketch are a function of lgMaxMapSize.
   *
   * @param lgCurMapSize Log2 of the starting (current) physical size of the internal hash
   * map managed by this sketch.
   */
  ItemsSketch(final int lgMaxMapSize, final int lgCurMapSize) {
    //set initial size of hash map
    this.lgMaxMapSize = Math.max(lgMaxMapSize, LG_MIN_MAP_SIZE);
    final int lgCurMapSz = Math.max(lgCurMapSize, LG_MIN_MAP_SIZE);
    hashMap = new ReversePurgeItemHashMap<>(1 << lgCurMapSz);
    this.curMapCap = hashMap.getCapacity();
    final int maxMapCap =
        (int) ((1 << lgMaxMapSize) * ReversePurgeItemHashMap.getLoadFactor());
    offset = 0;
    sampleSize = Math.min(SAMPLE_SIZE, maxMapCap);
  }

  /**
   * Returns a sketch instance of this class from the given srcMem,
   * which must be a Memory representation of this sketch class.
   *
   * @param <T> The type of item that this sketch will track
   * @param srcMem a Memory representation of a sketch of this class.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return a sketch instance of this class.
   */
  public static <T> ItemsSketch<T> getInstance(final Memory srcMem,
      final ArrayOfItemsSerDe<T> serDe) {
    final long pre0 = PreambleUtil.checkPreambleSize(srcMem); //make sure preamble will fit
    final int maxPreLongs = Family.FREQUENCY.getMaxPreLongs();

    final int preLongs = extractPreLongs(pre0);         //Byte 0
    final int serVer = extractSerVer(pre0);             //Byte 1
    final int familyID = extractFamilyID(pre0);         //Byte 2
    final int lgMaxMapSize = extractLgMaxMapSize(pre0); //Byte 3
    final int lgCurMapSize = extractLgCurMapSize(pre0); //Byte 4
    final boolean empty = (extractFlags(pre0) & EMPTY_FLAG_MASK) != 0; //Byte 5

    // Checks
    final boolean preLongsEq1 = (preLongs == 1);        //Byte 0
    final boolean preLongsEqMax = (preLongs == maxPreLongs);
    if (!preLongsEq1 && !preLongsEqMax) {
      throw new SketchesArgumentException(
          "Possible Corruption: PreLongs must be 1 or " + maxPreLongs + ": " + preLongs);
    }
    if (serVer != SER_VER) {                            //Byte 1
      throw new SketchesArgumentException(
          "Possible Corruption: Ser Ver must be " + SER_VER + ": " + serVer);
    }
    final int actFamID = Family.FREQUENCY.getID();      //Byte 2
    if (familyID != actFamID) {
      throw new SketchesArgumentException(
          "Possible Corruption: FamilyID must be " + actFamID + ": " + familyID);
    }
    if (empty ^ preLongsEq1) {                          //Byte 5 and Byte 0
      throw new SketchesArgumentException(
          "Possible Corruption: (PreLongs == 1) ^ Empty == True.");
    }

    if (empty) {
      return new ItemsSketch<>(lgMaxMapSize, LG_MIN_MAP_SIZE);
    }
    //get full preamble
    final long[] preArr = new long[preLongs];
    srcMem.getLongArray(0, preArr, 0, preLongs);

    final ItemsSketch<T> fis = new ItemsSketch<>(lgMaxMapSize, lgCurMapSize);
    fis.streamWeight = 0; //update after
    fis.offset = preArr[3];

    final int preBytes = preLongs << 3;
    final int activeItems = extractActiveItems(preArr[1]);
    //Get countArray
    final long[] countArray = new long[activeItems];
    srcMem.getLongArray(preBytes, countArray, 0, activeItems);
    //Get itemArray
    final int itemsOffset = preBytes + (8 * activeItems);
    final T[] itemArray = serDe.deserializeFromMemory(
        srcMem.region(itemsOffset, srcMem.getCapacity() - itemsOffset), activeItems);
    //update the sketch
    for (int i = 0; i < activeItems; i++) {
      fis.update(itemArray[i], countArray[i]);
    }
    fis.streamWeight = preArr[2]; //override streamWeight due to updating
    return fis;
  }

  /**
   * Returns the estimated <i>a priori</i> error given the maxMapSize for the sketch and the
   * estimatedTotalStreamWeight.
   * @param maxMapSize the planned map size to be used when constructing this sketch.
   * @param estimatedTotalStreamWeight the estimated total stream weight.
   * @return the estimated <i>a priori</i> error.
   */
  public static double getAprioriError(final int maxMapSize, final long estimatedTotalStreamWeight) {
    return getEpsilon(maxMapSize) * estimatedTotalStreamWeight;
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
   * Returns epsilon used to compute <i>a priori</i> error.
   * This is just the value <i>3.5 / maxMapSize</i>.
   * @param maxMapSize the planned map size to be used when constructing this sketch.
   * @return epsilon used to compute <i>a priori</i> error.
   */
  public static double getEpsilon(final int maxMapSize) {
    if (!isPowerOf2(maxMapSize)) {
      throw new SketchesArgumentException("maxMapSize is not a power of 2.");
    }
    return 3.5 / maxMapSize;
  }

  /**
   * Gets the estimate of the frequency of the given item.
   * Note: The true frequency of a item would be the sum of the counts as a result of the
   * two update functions.
   *
   * @param item the given item
   * @return the estimate of the frequency of the given item
   */
  public long getEstimate(final T item) {
    // If item is tracked:
    // Estimate = itemCount + offset; Otherwise it is 0.
    final long itemCount = hashMap.get(item);
    return (itemCount > 0) ? itemCount + offset : 0;
  }

  /**
   * Gets the guaranteed lower bound frequency of the given item, which can never be
   * negative.
   *
   * @param item the given item.
   * @return the guaranteed lower bound frequency of the given item. That is, a number which
   * is guaranteed to be no larger than the real frequency.
   */
  public long getLowerBound(final T item) {
    //LB = itemCount or 0
    return hashMap.get(item);
  }

  /**
   * Returns an array of Rows that include frequent items, estimates, upper and lower bounds
   * given a threshold and an ErrorCondition. If the threshold is lower than getMaximumError(),
   * then getMaximumError() will be used instead.
   *
   * <p>The method first examines all active items in the sketch (items that have a counter).
   *
   * <p>If <i>ErrorType = NO_FALSE_NEGATIVES</i>, this will include an item in the result
   * list if getUpperBound(item) &gt; threshold.
   * There will be no false negatives, i.e., no Type II error.
   * There may be items in the set with true frequencies less than the threshold
   * (false positives).</p>
   *
   * <p>If <i>ErrorType = NO_FALSE_POSITIVES</i>, this will include an item in the result
   * list if getLowerBound(item) &gt; threshold.
   * There will be no false positives, i.e., no Type I error.
   * There may be items omitted from the set with true frequencies greater than the
   * threshold (false negatives).</p>
   *
   * @param threshold to include items in the result list
   * @param errorType determines whether no false positives or no false negatives are
   * desired.
   * @return an array of frequent items
   */
  public Row<T>[] getFrequentItems(final long threshold, final ErrorType errorType) {
    return sortItems(threshold > getMaximumError() ? threshold : getMaximumError(), errorType);
  }

  /**
   * Returns an array of Rows that include frequent items, estimates, upper and lower bounds
   * given an ErrorCondition and the default threshold.
   * This is the same as getFrequentItems(getMaximumError(), errorType)
   *
   * @param errorType determines whether no false positives or no false negatives are
   * desired.
   * @return an array of frequent items
   */
  public Row<T>[] getFrequentItems(final ErrorType errorType) {
    return sortItems(getMaximumError(), errorType);
  }

  /**
   * @return An upper bound on the maximum error of getEstimate(item) for any item.
   * This is equivalent to the maximum distance between the upper bound and the lower bound
   * for any item.
   */
  public long getMaximumError() {
    return offset;
  }

  /**
   * Returns the maximum number of counters the sketch is configured to support.
   *
   * @return the maximum number of counters the sketch is configured to support.
   */
  public int getMaximumMapCapacity() {
    return (int) ((1 << lgMaxMapSize) * ReversePurgeLongHashMap.getLoadFactor());
  }

  /**
   * @return the number of active items in the sketch.
   */
  public int getNumActiveItems() {
    return hashMap.getNumActive();
  }

  /**
   * Returns the sum of the frequencies in the stream seen so far by the sketch
   *
   * @return the sum of the frequencies in the stream seen so far by the sketch
   */
  public long getStreamLength() {
    return this.streamWeight;
  }

  /**
   * Gets the guaranteed upper bound frequency of the given item.
   *
   * @param item the given item
   * @return the guaranteed upper bound frequency of the given item. That is, a number which
   * is guaranteed to be no smaller than the real frequency.
   */
  public long getUpperBound(final T item) {
    // UB = itemCount + offset
    return hashMap.get(item) + offset;
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
   * This function merges the other sketch into this one.
   * The other sketch may be of a different size.
   *
   * @param other sketch of this class
   * @return a sketch whose estimates are within the guarantees of the
   * largest error tolerance of the two merged sketches.
   */
  public ItemsSketch<T> merge(final ItemsSketch<T> other) {
    if (other == null) { return this; }
    if (other.isEmpty()) { return this; }

    final long streamLen = this.streamWeight + other.streamWeight; //capture before merge

    final ReversePurgeItemHashMap.Iterator<T> iter = other.hashMap.iterator();
    while (iter.next()) { //this may add to offset during rebuilds
      this.update(iter.getKey(), iter.getValue());
    }
    this.offset += other.offset;
    this.streamWeight = streamLen; //corrected streamWeight
    return this;
  }

  /**
   * Resets this sketch to a virgin state.
   */
  public void reset() {
    hashMap = new ReversePurgeItemHashMap<>(1 << LG_MIN_MAP_SIZE);
    this.curMapCap = hashMap.getCapacity();
    this.offset = 0;
    this.streamWeight = 0;
  }

  //Serialization

  /**
   * Returns a byte array representation of this sketch
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return a byte array representation of this sketch
   */
  @SuppressWarnings("null")
  public byte[] toByteArray(final ArrayOfItemsSerDe<T> serDe) {
    final int preLongs;
    final int outBytes;
    final boolean empty = isEmpty();
    final int activeItems = getNumActiveItems();
    byte[] bytes = null;
    if (empty) {
      preLongs = 1;
      outBytes = 8;
    } else {
      preLongs = Family.FREQUENCY.getMaxPreLongs();
      bytes = serDe.serializeToByteArray(hashMap.getActiveKeys());
      outBytes = ((preLongs + activeItems) << 3) + bytes.length;
    }
    final byte[] outArr = new byte[outBytes];
    final WritableMemory mem = WritableMemory.wrap(outArr);

    // build first preLong empty or not
    long pre0 = 0L;
    pre0 = insertPreLongs(preLongs, pre0);                  //Byte 0
    pre0 = insertSerVer(SER_VER, pre0);                     //Byte 1
    pre0 = insertFamilyID(Family.FREQUENCY.getID(), pre0);  //Byte 2
    pre0 = insertLgMaxMapSize(lgMaxMapSize, pre0);          //Byte 3
    pre0 = insertLgCurMapSize(hashMap.getLgLength(), pre0); //Byte 4
    pre0 = empty ? insertFlags(EMPTY_FLAG_MASK, pre0) : insertFlags(0, pre0); //Byte 5

    if (empty) {
      mem.putLong(0, pre0);
    } else {
      final long pre = 0;
      final long[] preArr = new long[preLongs];
      preArr[0] = pre0;
      preArr[1] = insertActiveItems(activeItems, pre);
      preArr[2] = this.streamWeight;
      preArr[3] = this.offset;
      mem.putLongArray(0, preArr, 0, preLongs);
      final int preBytes = preLongs << 3;
      mem.putLongArray(preBytes, hashMap.getActiveValues(), 0, activeItems);
      mem.putByteArray(preBytes + (this.getNumActiveItems() << 3), bytes, 0, bytes.length);
    }
    return outArr;
  }

  /**
   * Returns a human readable summary of this sketch.
   * @return a human readable summary of this sketch.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("FrequentItemsSketch<T>:").append(LS);
    sb.append("  Stream Length    : " + streamWeight).append(LS);
    sb.append("  Max Error Offset : " + offset).append(LS);
    sb.append(hashMap.toString());
    return sb.toString();
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of a ItemsSketch.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of a ItemsSketch.
   */
  public static String toString(final byte[] byteArr) {
    return toString(Memory.wrap(byteArr));
  }

  /**
   * Returns a human readable string of the preamble of a Memory image of a ItemsSketch.
   * @param mem the given Memory object
   * @return a human readable string of the preamble of a Memory image of a ItemsSketch.
   */
  public static String toString(final Memory mem) {
    return PreambleUtil.preambleToString(mem);
  }

  /**
   * Update this sketch with an item and a frequency count of one.
   * @param item for which the frequency should be increased.
   */
  public void update(final T item) {
    update(item, 1);
  }

  /**
   * Update this sketch with a item and a positive frequency count.
   * @param item for which the frequency should be increased. The item can be any long value and is
   * only used by the sketch to determine uniqueness.
   * @param count the amount by which the frequency of the item should be increased.
   * An count of zero is a no-op, and a negative count will throw an exception.
   */
  public void update(final T item, final long count) {
    if ((item == null) || (count == 0)) {
      return;
    }
    if (count < 0) {
      throw new SketchesArgumentException("Count may not be negative");
    }
    this.streamWeight += count;
    hashMap.adjustOrPutValue(item, count);

    if (getNumActiveItems() > curMapCap) { //over the threshold, we need to do something
      if (hashMap.getLgLength() < lgMaxMapSize) { //below tgt size, we can grow
        hashMap.resize(2 * hashMap.getLength());
        curMapCap = hashMap.getCapacity();
      } else { //At tgt size, must purge
        offset += hashMap.purge(sampleSize);
        if (getNumActiveItems() > getMaximumMapCapacity()) {
          throw new SketchesStateException("Purge did not reduce active items.");
        }
      }
    }
  }

  /**
   * Row class that defines the return values from a getFrequentItems query.
   * @param <T> type of item
   */
  public static class Row<T> implements Comparable<Row<T>> {
    final T item;
    final long est;
    final long ub;
    final long lb;
    private static final String FMT =  "  %12d%12d%12d %s";
    private static final String HFMT = "  %12s%12s%12s %s";

    Row(final T item, final long estimate, final long ub, final long lb) {
      this.item = item;
      this.est = estimate;
      this.ub = ub;
      this.lb = lb;
    }

    /**
     * @return item of type T
     */
    public T getItem() { return item; }

    /**
     * @return the estimate
     */
    public long getEstimate() { return est; }

    /**
     * @return the upper bound
     */
    public long getUpperBound() { return ub; }

    /**
     * @return return the lower bound
     */
    public long getLowerBound() { return lb; }

    /**
     * @return the descriptive row header
     */
    public static String getRowHeader() {
      return String.format(HFMT,"Est", "UB", "LB", "Item");
    }

    @Override
    public String toString() {
      return String.format(FMT,  est, ub, lb, item.toString());
    }

    /**
     * This compareTo is strictly limited to the Row.getEstimate() value and does not imply any
     * ordering whatsoever to the other elements of the row: item and upper and lower bounds.
     * Defined this way, this compareTo will be consistent with hashCode() and equals(Object).
     * @param that the other row to compare to.
     * @return a negative integer, zero, or a positive integer as this.getEstimate() is less than,
     * equal to, or greater than that.getEstimate().
     */
    @Override
    public int compareTo(final Row<T> that) {
      return (this.est < that.est) ? -1 : (this.est > that.est) ? 1 : 0;
    }

    /**
     * This hashCode is computed only from the Row.getEstimate() value.
     * Defined this way, this hashCode will be consistent with equals(Object):<br>
     * If (x.equals(y)) implies: x.hashCode() == y.hashCode().<br>
     * If (!x.equals(y)) does NOT imply: x.hashCode() != y.hashCode().
     * @return the hashCode computed from getEstimate().
     */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + (int) (est ^ (est >>> 32));
      return result;
    }

    /**
     * This equals is computed only from the Row.getEstimate() value and does not imply equality
     * of the other items within the row: item and upper and lower bounds.
     * Defined this way, this equals will be consistent with compareTo(Row).
     * @param obj the other row to determine equality with.
     * @return true if this.getEstimate() equals ((Row&lt;T&gt;)obj).getEstimate().
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) { return true; }
      if (obj == null) { return false; }
      if ( !(obj instanceof Row)) { return false; }
      final Row<T> that = (Row<T>) obj;
      if (est != that.est) { return false; }
      return true;
    }

  } //End of class Row<T>

  Row<T>[] sortItems(final long threshold, final ErrorType errorType) {
    final ArrayList<Row<T>> rowList = new ArrayList<>();
    final ReversePurgeItemHashMap.Iterator<T> iter = hashMap.iterator();
    if (errorType == ErrorType.NO_FALSE_NEGATIVES) {
      while (iter.next()) {
        final long est = getEstimate(iter.getKey());
        final long ub = getUpperBound(iter.getKey());
        final long lb = getLowerBound(iter.getKey());
        if (ub >= threshold) {
          final Row<T> row = new Row<>(iter.getKey(), est, ub, lb);
          rowList.add(row);
        }
      }
    } else { //NO_FALSE_POSITIVES
      while (iter.next()) {
        final long est = getEstimate(iter.getKey());
        final long ub = getUpperBound(iter.getKey());
        final long lb = getLowerBound(iter.getKey());
        if (lb >= threshold) {
          final Row<T> row = new Row<>(iter.getKey(), est, ub, lb);
          rowList.add(row);
        }
      }
    }

    // descending order
    rowList.sort(new Comparator<Row<T>>() {
      @Override
      public int compare(final Row<T> r1, final Row<T> r2) {
        return r2.compareTo(r1);
      }
    });

    @SuppressWarnings("unchecked")
    final Row<T>[] rowsArr =
      rowList.toArray((Row<T>[]) Array.newInstance(Row.class, rowList.size()));
    return rowsArr;
  }

}
