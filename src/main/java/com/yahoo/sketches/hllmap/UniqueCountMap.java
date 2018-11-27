/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * This is a real-time, key-value HLL mapping sketch that tracks approximate unique counts of
 * identifiers (the values) associated with each key. An example might be tracking the number of
 * unique user identifiers associated with each IP address. This map has been specifically designed
 * for the use-case where the number of keys is quite large (many millions) and the distribution of
 * identifiers per key is very skewed. A typical distribution where this works well is a
 * power-law distribution of identifiers per key of the form <i>y = Cx<sup>-&alpha;</sup></i>,
 * where <i>&alpha;</i> &lt; 0.5, and <i>C</i> is roughly <i>y<sub>max</sub></i>.
 * For example, with 100M keys, over 75% of the keys would have only
 * one identifier, 99% of the keys would have less than 20 identifiers, 99.9% would have less than
 * 200 identifiers, and a very tiny fraction might have identifiers in the thousands.
 *
 * <p>The space consumed by this map is quite sensitive to the actual distribution of identifiers
 * per key, so you should characterize and or experiment with your typical input streams.
 * Nonetheless, our experiments on live streams of over 100M keys required about 1.4GB of space.
 *
 * <p>Given such highly-skewed distributions, using this map is far more efficient space-wise than
 * the alternative of dedicating an HLL sketch per key. Based on our use cases, after
 * subtracting the space required for key storage, the average bytes per key required for unique
 * count estimation ({@link  #getAverageSketchMemoryPerKey()}) is about 10.
 *
 * <p>Internally, this map is implemented as a hierarchy of internal hash maps with progressively
 * increasing storage allocated for unique count estimation. As a key acquires more identifiers it
 * is "promoted" up to a higher internal map. The final map of keys is a map of compact HLL
 * sketches.
 *
 * <p>The unique values in all the internal maps, except the final HLL map, are stored in a special
 * form called a coupon. A coupon is a 16-bit value that fully describes a k=1024 HLL bin.
 * It contains 10 bits of address and a 6-bit HLL value.
 *
 * <p>All internal maps use a prime number size and Knuth's Open Addressing Double Hash (OADH)
 * search algorithm.
 *
 * <p>The internal base map holds all the keys and each key is associated with one 16-bit value.
 * Initially, the value is a single coupon. Once the key is promoted, this 16-bit field contains a
 * reference to the internal map where the key is still active.
 *
 * <p>The intermediate maps between the base map and the final HLL map are of two types.
 * The first few of these are called traverse maps where the coupons are
 * stored as unsorted arrays. After the traverse maps are the coupon hash maps, where the coupons
 * are stored in small OASH hash tables.
 *
 * <p>All the intermediate maps support deletes and can dynamically grow and shrink as required by
 * the input stream.
 *
 * <p>The sketch estimator algorithms are unbiased with a Relative Standard Error (RSE)
 * of about 2.6% with 68% confidence, or equivalently, about 5.2% with a 95% confidence.
 *
 * <p>In a parallel package in the sketches-misc repository, there are 2 classes  that can be used
 * from the command line to feed this mapping sketch piped from standard-in for experimental
 * evaluation. The first is ProcessIpStream, which processes simple IP/ID pairs and the second,
 * ProcessDistributionStream, which processes pairs that describe a distribution.
 * In this same package is the VariousMapRSETest class that was used to generate the error plots
 * for the web site. Please refer to the javadocs for those classes for more information.
 *
 * @author Lee Rhodes
 * @author Alexander Saydakov
 * @author Kevin Lang
 */
public class UniqueCountMap {
  private static final String LS = System.getProperty("line.separator");
  private static final int NUM_LEVELS = 10; // total of single coupon + traverse + coupon maps + hll
  private static final int NUM_TRAVERSE_MAPS = 3;
  private static final int HLL_K = 1024;
  private static final int INITIAL_NUM_ENTRIES = 1000003;
  private static final int MIN_INITIAL_NUM_ENTRIES = 157;
  private final int keySizeBytes_;

  /** TraverseCouponMap or HashCouponMap instances */
  private final Map[] maps_;

  /**
   * Constructs a UniqueCountMap with an initial capacity of one million entries.
   * @param keySizeBytes must be at least 4 bytes to have sufficient entropy.
   */
  public UniqueCountMap(final int keySizeBytes) {
    this(INITIAL_NUM_ENTRIES, keySizeBytes);
  }

  /**
   * Constructs a UniqueCountMap with a given initial number of entries.
   *
   * @param initialNumEntries The initial number of entries provides a tradeoff between
   * wasted space, if too high, and wasted time resizing the table, if too low.
   * @param keySizeBytes must be at least 4 bytes to have sufficient entropy
   */
  public UniqueCountMap(final int initialNumEntries, final int keySizeBytes) {
    checkConstructorKeySize(keySizeBytes);
    final int initEntries = Math.max(initialNumEntries, MIN_INITIAL_NUM_ENTRIES);
    keySizeBytes_ = keySizeBytes;
    maps_ = new Map[NUM_LEVELS]; // includes base level and top level
    maps_[0] = SingleCouponMap.getInstance(initEntries, keySizeBytes);
  }

  /**
   * Updates the map with a given key and identifier and returns the estimate of the number of
   * unique identifiers encountered so far for the given key.
   * @param key the given key
   * @param identifier the given identifier for unique counting associated with the key
   * @return the estimate of the number of unique identifiers encountered so far for the given key.
   */
  public double update(final byte[] key, final byte[] identifier) {
    if (key == null) { return Double.NaN; }
    checkMethodKeySize(key);
    if (identifier == null) { return getEstimate(key); }
    final short coupon = (short) Map.coupon16(identifier);

    final int baseMapIndex = maps_[0].findOrInsertKey(key);
    final double baseMapEstimate = maps_[0].update(baseMapIndex, coupon);
    if (baseMapEstimate > 0) { return baseMapEstimate; }
    final int level = -(int) baseMapEstimate; // base map is level 0
    if (level == 0) {
      return promote(key, coupon, maps_[0], baseMapIndex, level, baseMapIndex, 0);
    }

    final Map map = maps_[level];
    final int index = map.findOrInsertKey(key);
    final double estimate = map.update(index, coupon);
    if (estimate > 0) { return estimate; }
    return promote(key, coupon, map, index, level, baseMapIndex, -estimate);
  }

  /**
   * Retrieves the current estimate of unique count for a given key.
   * @param key given key
   * @return estimate of unique count so far
   */
  public double getEstimate(final byte[] key) {
    if (key == null) { return Double.NaN; }
    checkMethodKeySize(key);
    final double est = maps_[0].getEstimate(key);
    if (est >= 0.0) { return est; }
    //key has been promoted
    final int level = -(int)est;
    final Map map = maps_[level];
    return map.getEstimate(key);
  }

  /**
   * Returns the upper bound cardinality with respect to {@link #getEstimate(byte[])} associated
   * with the given key.
   * @param key the given key
   * @return the upper bound cardinality with respect to {@link #getEstimate(byte[])} associated
   * with the given key.
   */
  public double getUpperBound(final byte[] key) {
    if (key == null) { return Double.NaN; }
    checkMethodKeySize(key);
    final double est = maps_[0].getEstimate(key);
    if (est >= 0.0) { return est; }
    //key has been promoted
    final int level = -(int)est;
    final Map map = maps_[level];
    return map.getUpperBound(key);
  }

  /**
   * Returns the lower bound cardinality with respect to {@link #getEstimate(byte[])} associated
   * with the given key.
   * @param key the given key
   * @return the lower bound cardinality with respect to {@link #getEstimate(byte[])} associated
   * with the given key.
   */
  public double getLowerBound(final byte[] key) {
    if (key == null) { return Double.NaN; }
    checkMethodKeySize(key);
    final double est = maps_[0].getEstimate(key);
    if (est >= 0.0) { return est; }
    //key has been promoted
    final int level = -(int)est;
    final Map map = maps_[level];
    return map.getLowerBound(key);
  }

  /**
   * Returns the number of active, unique keys across all internal maps
   * @return the number of active, unique keys across all internal maps
   */
  public int getActiveEntries() {
    return maps_[0].getCurrentCountEntries();
  }

  /**
   * Returns total bytes used by all internal maps
   * @return total bytes used by all internal maps
   */
  public long getMemoryUsageBytes() {
    long total = 0;
    for (int i = 0; i < maps_.length; i++) {
      if (maps_[i] != null) {
        total += maps_[i].getMemoryUsageBytes();
      }
    }
    return total;
  }

  /**
   * Returns total bytes used for key storage
   * @return total bytes used for key storage
   */
  public long getKeyMemoryUsageBytes() {
    long total = 0;
    for (int i = 0; i < maps_.length; i++) {
      if (maps_[i] != null) {
        total += (long) (maps_[i].getActiveEntries()) * keySizeBytes_;
      }
    }
    return total;
  }

  /**
   * Returns the average memory storage per key that is dedicated to sketching the unique counts.
   * @return the average memory storage per key that is dedicated to sketching the unique counts.
   */
  public double getAverageSketchMemoryPerKey() {
    return (double) (getMemoryUsageBytes() - getKeyMemoryUsageBytes()) / getActiveEntries();
  }

  /**
   * Returns the number of active internal maps so far.
   * Only the base map is initialized in the constructor, so this method would return 1.
   * As more keys are promoted up to higher level maps, the return value would grow until the
   * last level HLL map is allocated.
   * @return the number of active levels so far
   */
  int getActiveMaps() {
    int levels = 0;
    final int iMapsLen = maps_.length;
    for (int i = 0; i < iMapsLen; i++) {
      if (maps_[i] != null) { levels++; }
    }
    return levels;
  }

  /**
   * Returns the base map
   * @return the base map
   */
  Map getBaseMap() {
    return maps_[0];
  }

  /**
   * Returns the top-level HllMap. It may be null.
   * @return the top-level HllMap.
   */
  Map getHllMap() {
    return maps_[maps_.length - 1];
  }

  /**
   * Returns a string with a human-readable summary of the UniqueCountMap and all the internal maps
   * @return human-readable summary
   */
  @Override
  public String toString() {
    final long totKeys = getActiveEntries();
    final long totMem = getMemoryUsageBytes();
    final long keyMem = getKeyMemoryUsageBytes();
    final double avgValMemPerKey = getAverageSketchMemoryPerKey();

    final String ksb = Map.fmtLong(keySizeBytes_);
    final String alvls  = Map.fmtLong(getActiveMaps());
    final String tKeys = Map.fmtLong(totKeys);
    final String tMem = Map.fmtLong(totMem);
    final String kMem = Map.fmtLong(keyMem);
    final String avgValMem = Map.fmtDouble(avgValMemPerKey);


    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = this.getClass().getSimpleName();
    sb.append("## ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("   Key Size Bytes             : ").append(ksb).append(LS);
    sb.append("   Active Map Levels          : ").append(alvls).append(LS);
    sb.append("   Total keys                 : ").append(tKeys).append(LS);
    sb.append("   Total Memory Bytes         : ").append(tMem).append(LS);
    sb.append("   Total Key Memory Bytes     : ").append(kMem).append(LS);
    sb.append("   Avg Sketch Memory Bytes/Key: ").append(avgValMem).append(LS);
    sb.append(LS);
    for (int i = 0; i < maps_.length; i++) {
      final Map cMap = maps_[i];
      if (cMap != null) {
        sb.append(cMap.toString());
        sb.append(LS);
      }
    }
    sb.append("## ").append("END UNIQUE COUNT MAP SUMMARY");
    sb.append(LS);
    return sb.toString();
  }

  private void setLevelInBaseMap(final int index, final int level) {
    ((SingleCouponMap) maps_[0]).setLevel(index, level);
  }

  private double promote(final byte[] key, final short coupon, final Map fromMap, final int fromIndex,
      final int fromLevel, final int baseMapIndex, final double estimate) {
    final Map newMap = getMapForLevel(fromLevel + 1);
    final int newMapIndex = newMap.findOrInsertKey(key);
    final CouponsIterator it = fromMap.getCouponsIterator(fromIndex);
    while (it.next()) {
      final double est = newMap.update(newMapIndex, it.getValue());
      assert est > 0;
    }
    fromMap.deleteKey(fromIndex);
    newMap.updateEstimate(newMapIndex, estimate);
    final double newEstimate = newMap.update(newMapIndex, coupon);
    setLevelInBaseMap(baseMapIndex, fromLevel + 1);
    assert newEstimate > 0; // this must be positive since we have just promoted
    return newEstimate;
  }

  private Map getMapForLevel(final int level) {
    if (maps_[level] == null) {
      final int newLevelCapacity = 1 << level;
      if (level <= NUM_TRAVERSE_MAPS) {
        maps_[level] = CouponTraverseMap.getInstance(keySizeBytes_, newLevelCapacity);
      } else if (level < (maps_.length - 1)) {
        maps_[level] = CouponHashMap.getInstance(keySizeBytes_, newLevelCapacity);
      } else {
        maps_[level] = HllMap.getInstance(keySizeBytes_, HLL_K);
      }
    }
    return maps_[level];
  }

  private static final void checkConstructorKeySize(final int keySizeBytes) {
    if (keySizeBytes < 4) {
      throw new SketchesArgumentException("KeySizeBytes must be >= 4: " + keySizeBytes);
    }
  }

  private final void checkMethodKeySize(final byte[] key) {
    if (key.length != keySizeBytes_) {
      throw new SketchesArgumentException("Key size must be " + keySizeBytes_ + " bytes.");
    }
  }

}
