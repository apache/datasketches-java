/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

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
 * Nonetheless, our experiments on live streams of over 100M keys required space less than 2GB.
 *
 * <p>Given such highly-skewed distributions, using this map is far more efficient space-wise than
 * the alternative of dedicating an HLL sketch per key. Based on our use cases, after
 * subtracting the space required for key storage, the average bytes per key required for unique
 * count estimation ({@link  #getAverageSketchMemoryPerKey()}) is about 10.
 *
 * <p>Internally, this map is implemented as a hierarchy of hash maps with progressively
 * increasing storage allocated for unique count estimation. As a key acquires more identifiers it
 * is "promoted" up to a higher level table. The final level is a map of keys to compact HLL
 * sketches.
 *
 * <p>The unique values in all the levels, except the final HLL map, are stored in a special form
 * called a coupon. A coupon is a 16-bit value that fully describes a k=1024 HLL bin. It contains
 * 10 bits of address and a 6-bit number, which represents the number of leading zeroes in a
 * 64-bit hash of the identifier, plus one to make it non-zero.
 *
 * <p>All hash tables use a prime number size and use Knuth's Open Addressing Double Hash (OADH)
 * search algorithm.
 *
 * <p>The base table holds all the keys and each key is associated with one 16-bit value. Initially,
 * the value is a single coupon. Once the key is promoted, this 16-bit field contains a reference
 * to the map table where the key is still active.
 *
 * <p>The intermediate map tables between the base level map and the final HLL map are of two types.
 * The first few of these are called traverse maps where the coupons are
 * stored as unsorted arrays. After the traverse maps are the coupon hash maps, where the coupons
 * are stored in small OASH hash tables.
 *
 * <p>All the intermediate maps support deletes and can dynamically grow and shrink as required by
 * the input stream.
 *
 * <p>The sketch estimator algorithms are unbiased with a Relative Standard Error (RSE)
 * of about 2.5% with 68% confidence, or equivalently, about 5% with a 95% confidence.
 *
 * <p>There are 2 classes in the test hierarchy that can be used from the command line to feed this
 * mapping sketch piped from standard-in for experimental evaluation.
 * The first is ProcessIpStream, which processes simple IP/ID pairs
 * and the second, ProcessDistributionStream, which processes pairs that describe a distribution.
 * Please refer to the javadocs for those classes for more information.
 *
 * @author Lee Rhodes
 * @author Alex Saydakov
 * @author Kevin Lang
 */
public class UniqueCountMap {
  private static final String LS = System.getProperty("line.separator");
  private static final int NUM_INTERMEDIATE_LEVELS = 8; // total of traverse + coupon map levels
  private static final int NUM_TRAVERSE_LEVELS = 3;
  private static final int HLL_K = 1024;
  private static final int INITIAL_NUM_ENTRIES = 1000003;
  private static final int MIN_INITIAL_NUM_ENTRIES = 157;
  private final int keySizeBytes_;

  private final SingleCouponMap baseLevelMap;

  /** TraverseCouponMap or HashCouponMap instances */
  private final CouponMap[] intermediateLevelMaps;

  private HllMap lastLevelMap;

  /**
   * Constructs a UniqueCountMap with an initial capacity of one million entries.
   * @param keySizeBytes must be at least 4 bytes to have enough entropy.
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
    checkTgtEntries(initialNumEntries);
    checkConstructorKeySize(keySizeBytes);

    keySizeBytes_ = keySizeBytes;
    baseLevelMap = SingleCouponMap.getInstance(initialNumEntries, keySizeBytes);
    intermediateLevelMaps = new CouponMap[NUM_INTERMEDIATE_LEVELS];
  }

  /**
   * Updates the map with a given key and identifier and returns the estimate of the number of
   * unique identifiers encountered so far for the given key.
   * @param key the given key
   * @param identifier the given identifier for unique counting associated with the key
   * @return the estimate of the number of unique identifiers encountered so far for the given key.
   */
  public double update(final byte[] key, final byte[] identifier) {
    if (key == null) return Double.NaN;
    checkMethodKeySize(key);
    if (identifier == null) return getEstimate(key);
    final short coupon = (short) Map.coupon16(identifier);

    final int baseLevelIndex = baseLevelMap.findOrInsertKey(key);
    if (baseLevelIndex < 0) {
      // this is a new key for the baseLevelMap. Set the coupon, keep the state bit clear.
      baseLevelMap.setCoupon(~baseLevelIndex, coupon, false);
      return 1;
    }
    final short baseLevelMapCoupon = baseLevelMap.getCoupon(baseLevelIndex);
    if (baseLevelMap.isCoupon(baseLevelIndex)) {
      if (baseLevelMapCoupon == coupon) return 1; //duplicate
      // promote from the base level
      baseLevelMap.setCoupon(baseLevelIndex, (short) 1, true); //set coupon = Level 1; state = 1
      CouponMap newMap = getIntermediateMap(1);
      final int index = newMap.findOrInsertKey(key);
      newMap.findOrInsertCoupon(index, baseLevelMapCoupon);
      final double estimate = newMap.findOrInsertCoupon(index, coupon);
      assert estimate > 0; // this must be positive since we have just promoted
      return estimate;
    }

    int level = baseLevelMapCoupon;
    if (level <= NUM_INTERMEDIATE_LEVELS) {
      final CouponMap map = intermediateLevelMaps[level - 1];
      final int index = map.findOrInsertKey(key);
      double estimate = map.findOrInsertCoupon(index, coupon);
      if (estimate > 0) return estimate;
      // promote to the next level
      level++;
      baseLevelMap.setCoupon(baseLevelIndex, (short) level, true); //set coupon = level number; state = 1
      if (level <= NUM_INTERMEDIATE_LEVELS) {
        final CouponMap newMap = getIntermediateMap(level);
        final int newMapIndex = newMap.findOrInsertKey(key);
        final CouponsIterator it = map.getCouponsIterator(key);
        while (it.next()) {
          final double est = newMap.findOrInsertCoupon(newMapIndex, it.getValue());
          assert est > 0;
        }
        newMap.updateEstimate(newMapIndex, -estimate);
        estimate = newMap.findOrInsertCoupon(newMapIndex, coupon);
      } else { // promoting to the last level
        if (lastLevelMap == null) {
          lastLevelMap = HllMap.getInstance(keySizeBytes_, HLL_K);
        }
        final CouponsIterator it = map.getCouponsIterator(key);
        final int lastLevelIndex = lastLevelMap.findOrInsertKey(key);
        while (it.next()) {
          lastLevelMap.findOrInsertCoupon(lastLevelIndex, it.getValue());
        }
        lastLevelMap.updateEstimate(lastLevelIndex, -estimate);
        estimate = lastLevelMap.findOrInsertCoupon(lastLevelIndex, coupon);
      }
      map.deleteKey(index);
      assert estimate > 0; // this must be positive since we have just promoted
      return estimate;
    }
    return lastLevelMap.update(key, coupon);
  }

  /**
   * Retrieves the current estimate of unique count for a given key.
   * @param key given key
   * @return estimate of unique count so far
   */
  public double getEstimate(final byte[] key) {
    if (key == null) return Double.NaN;
    checkMethodKeySize(key);
    double est = baseLevelMap.getEstimate(key);
    if (est >= 0.0) return est;
    //key has been promoted
    final int level = -(int)est;
    if (level <= NUM_INTERMEDIATE_LEVELS) {
      final Map map = intermediateLevelMaps[level - 1];
      return map.getEstimate(key);
    }
    return lastLevelMap.getEstimate(key);
  }

  /**
   * Returns the upper bound cardinality with respect to {@link #getEstimate(byte[])} associated
   * with the given key.
   * @param key the given key
   * @return the upper bound cardinality with respect to {@link #getEstimate(byte[])} associated
   * with the given key.
   */
  public double getUpperBound(final byte[] key) {
    if (key == null) return Double.NaN;
    checkMethodKeySize(key);
    double est = baseLevelMap.getEstimate(key);
    if (est >= 0.0) return est;
    //key has been promoted
    final int level = -(int)est;
    if (level <= NUM_INTERMEDIATE_LEVELS) {
      final Map map = intermediateLevelMaps[level - 1];
      return map.getUpperBound(key);
    }
    return lastLevelMap.getUpperBound(key);
  }

  /**
   * Returns the lower bound cardinality with respect to {@link #getEstimate(byte[])} associated
   * with the given key.
   * @param key the given key
   * @return the lower bound cardinality with respect to {@link #getEstimate(byte[])} associated
   * with the given key.
   */
  public double getLowerBound(final byte[] key) {
    if (key == null) return Double.NaN;
    checkMethodKeySize(key);
    double est = baseLevelMap.getEstimate(key);
    if (est >= 0.0) return est;
    //key has been promoted
    final int level = -(int)est;
    if (level <= NUM_INTERMEDIATE_LEVELS) {
      final Map map = intermediateLevelMaps[level - 1];
      return map.getLowerBound(key);
    }
    return lastLevelMap.getLowerBound(key);
  }

  /**
   * Returns the number of active, unique keys across all levels
   * @return the number of active, unique keys across all levels
   */
  public int getActiveEntries() {
    return baseLevelMap.getCurrentCountEntries();
  }

  /**
   * Returns total bytes used by all levels
   * @return total bytes used by all levels
   */
  public long getMemoryUsageBytes() {
    long total = baseLevelMap.getMemoryUsageBytes();
    for (int i = 0; i < intermediateLevelMaps.length; i++) {
      if (intermediateLevelMaps[i] != null) {
        total += intermediateLevelMaps[i].getMemoryUsageBytes();
      }
    }
    if (lastLevelMap != null) {
      total += lastLevelMap.getMemoryUsageBytes();
    }
    return total;
  }

  /**
   * Returns total bytes used for key storage
   * @return total bytes used for key storage
   */
  public long getKeyMemoryUsageBytes() {
    long total = baseLevelMap.getCurrentCountEntries() * keySizeBytes_;
    for (int i = 0; i < intermediateLevelMaps.length; i++) {
      if (intermediateLevelMaps[i] != null) {
        total += intermediateLevelMaps[i].getActiveEntries() * keySizeBytes_;
      }
    }
    if (lastLevelMap != null) {
      total += lastLevelMap.getCurrentCountEntries() * keySizeBytes_;
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
   * Returns the number of active levels so far.
   * Only the base level table is initialized in the constructor, so this method would return 1.
   * As more keys are promoted up to the next levels, the return value would grow until the
   * last level HLL table is allocated.
   * @return the number of active levels so far
   */
  public int getActiveLevels() {
    int levels = 1;
    int iMapsLen = intermediateLevelMaps.length;
    for (int i = 0; i < iMapsLen; i++) {
      if (intermediateLevelMaps[i] != null) levels++;
    }
    if (lastLevelMap != null) levels++;
    return levels;
  }

  /**
   * Returns a string with a human-readable summary of the UniqueCountMap and all the internal levels
   * @return human-readable summary
   */
  @Override
  public String toString() {
    final long totKeys = getActiveEntries();
    final long totMem = getMemoryUsageBytes();
    final long keyMem = getKeyMemoryUsageBytes();
    final double avgValMemPerKey = getAverageSketchMemoryPerKey();

    final String ksb = Map.fmtLong(keySizeBytes_);
    final String alvls  = Map.fmtLong(getActiveLevels());
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
    sb.append(baseLevelMap.toString());
    sb.append(LS);
    for (int i = 0; i < intermediateLevelMaps.length; i++) {
      final CouponMap cMap = intermediateLevelMaps[i];
      if (cMap != null) {
        sb.append(cMap.toString());
        sb.append(LS);
      }
    }
    if (lastLevelMap != null) {
      sb.append(lastLevelMap.toString());
      sb.append(LS);
    }
    sb.append("## ").append("END UNIQUE COUNT MAP SUMMARY");
    sb.append(LS);
    return sb.toString();
  }

  private CouponMap getIntermediateMap(final int level) {
    if (intermediateLevelMaps[level - 1] == null) {
      final int newLevelCapacity = 1 << level;
      if (level <= NUM_TRAVERSE_LEVELS) {
        intermediateLevelMaps[level - 1] = CouponTraverseMap.getInstance(keySizeBytes_, newLevelCapacity);
      } else {
        intermediateLevelMaps[level - 1] = CouponHashMap.getInstance(keySizeBytes_, newLevelCapacity);
      }
    }
    return intermediateLevelMaps[level - 1];
  }

  private static final void checkTgtEntries(final int initNumEntries) {
    if (initNumEntries < MIN_INITIAL_NUM_ENTRIES) {
      throw new SketchesArgumentException("initialNumEntries must be >= " + MIN_INITIAL_NUM_ENTRIES);
    }
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
