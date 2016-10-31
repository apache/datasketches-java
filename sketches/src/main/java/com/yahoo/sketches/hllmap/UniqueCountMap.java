/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static com.yahoo.sketches.hllmap.MapDistribution.HLL_INIT_NUM_ENTRIES;
import static com.yahoo.sketches.hllmap.MapDistribution.HLL_RESIZE_FACTOR;
import static com.yahoo.sketches.hllmap.MapDistribution.NUM_LEVELS;
import static com.yahoo.sketches.hllmap.MapDistribution.NUM_TRAVERSE_LEVELS;
import static com.yahoo.sketches.hllmap.Util.fmtLong;

/**
 * This map is to keep approximate unique counts of some ID associated with some other ID,
 * which serves as a key in the map.
 * Example: estimate the number of unique users per IP address.
 * The goal is to keep this data structure in memory in a space-efficient way, and return
 * estimate of unique count for a particular key upon each update.
 *
 * <p>This map is implemented as several levels of hash tables with progressively more expensive
 * entries in them as keys with more unique values get promoted up. The assumption is that
 * the distribution is highly skewed so that most of the keys have just one entry or just a few.
 *
 * <p>The unique values in all the levels, except the last one, are stored in a special form
 * based on a hash of the original value. We call this form a coupon. This is a 16-bit
 * value similar to an HLL sketch value with 10 bits of address and a 6-bit number, which
 * represents the number of leading zeroes in a 64-bit hash plus one to make it non-zero. 
 * 
 * <p>All hash tables here have prime size to reduce wasted space compared to powers of two.
 * Open addressing with the second hash is used to resolve collisions.
 * 
 * <p>The base table holds all the keys, so it doesn't need to support deletes. As a value, it
 * holds either one coupon or, once promoted, a level number to speed up the lookup.
 *
 * <p>Each next level can hold twice the number of coupons until a point when it becomes cheaper
 * to have an HLL sketch instead of the list of coupons. At this point the key is promoted to
 * the last level with HLL sketches.
 *
 * <p>Several levels above the base level are so-called traverse levels where the coupons are
 * stored as unsorted arrays. This is cheaper compared to more complicated containers
 * up to a point. The number of unique coupons is used as the estimate of unique count up
 * to this point. Coupon collisions are treated as duplicate coupons, so the number of coupons
 * slightly underestimates the unique count, which is another reason to switch to a more
 * complicated scheme on the next levels.
 * 
 * <p>Next levels use hash tables to store coupons for each key. These inner hash tables have
 * power of two sizes, and use linear probing for collision resolution. Historical Inverse
 * Probability (HIP) estimator is used from this point on.
 *
 * <p>All the intermediate level hash tables support deletes, can reuse slots from previously
 * deleted keys, and can shrink.
 *
 * <p>The last level is a hash table of HLL sketches. No deletes are needed at this point.
 *
 * <p>This approach provides unbiased unique count estimates with Relative Standard Error (RSE)
 * of about 2.5% (68% confidence) using HLL sketch on the last level with k=1024.
 */
public class UniqueCountMap {
  private static final String LS = System.getProperty("line.separator");
  private final int keySizeBytes_;
  private final int k_;

  // coupon is a 16-bit value similar to HLL sketch value: 10-bit address,
  // 6-bit number of leading zeroes in a 64-bit hash of the key + 1

  // prime size, double hash, no deletes, 1-bit state array
  // state: 0 - value is a coupon (if used), 1 - value is a level number
  // same growth rule as for the next levels
  private final SingleCouponMap baseLevelMap;

  // TraverseCouponMap or HashCouponMap instances
  private final CouponMap[] intermediateLevelMaps;

  // this map has a fixed slotSize (row size). No shrinking.
  // Similar growth algorithm to SingleCouponMap, maybe different constants.
  // needs to keep 3 double values per row for HIP estimator
  private HllMap lastLevelMap;

  /**
   * Constructs a UniqueCountMap. The initial number of entries provides a tradeoff between
   * wasted space, if too high, and wasted time resizing the table, if too low.
   * @param targetNumEntries initial size of the base table
   * @param keySizeBytes must be at least 4 bytes to have enough entropy
   * @param k parameter for last level HLL sketch (1024 is recommended)
   */
  public UniqueCountMap(final int targetNumEntries, final int keySizeBytes, final int k) {
    Util.checkK(k);
    Util.checkKeySizeBytes(keySizeBytes);
    k_ = k;
    keySizeBytes_ = keySizeBytes;
    baseLevelMap = SingleCouponMap.getInstance(targetNumEntries, keySizeBytes);
    intermediateLevelMaps = new CouponMap[NUM_LEVELS];
  }

  /**
   * Updates the map with a given key and value.
   * @param key given key
   * @param identifier given identifier for unique counting associated with the key
   * @return estimate of unique count so far including the current identifier
   */
  public double update(final byte[] key, final byte[] identifier) {
    if (key == null) return Double.NaN;
    if (key.length != keySizeBytes_) {
      throw new IllegalArgumentException("Key must be " + keySizeBytes_ + " bytes long");
    }
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
      CouponMap newMap = getIntermediateMapForLevel(1);
      final int index = newMap.findOrInsertKey(key);
      newMap.findOrInsertCoupon(index, baseLevelMapCoupon);
      final double estimate = newMap.findOrInsertCoupon(index, coupon);
      assert estimate > 0; // this must be positive since we have just promoted
      return estimate;
    }

    int level = baseLevelMapCoupon;
    if (level <= NUM_LEVELS) {
      final CouponMap map = intermediateLevelMaps[level - 1];
      final int index = map.findOrInsertKey(key);
      double estimate = map.findOrInsertCoupon(index, coupon);
      if (estimate > 0) return estimate;
      // promote to the next level
      level++;
      baseLevelMap.setCoupon(baseLevelIndex, (short) level, true); //set coupon = level number; state = 1
      if (level <= NUM_LEVELS) {
        final CouponMap newMap = getIntermediateMapForLevel(level);
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
          lastLevelMap = HllMap.getInstance(HLL_INIT_NUM_ENTRIES, keySizeBytes_, k_, HLL_RESIZE_FACTOR);
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

  private CouponMap getIntermediateMapForLevel(final int level) {
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

  /**
   * Retrieves the current estimate of unique count for a given key.
   * @param key given key
   * @return estimate of unique count so far
   */
  public double getEstimate(final byte[] key) {
    if (key == null) return Double.NaN;
    if (key.length != keySizeBytes_) throw new IllegalArgumentException("Key must be " + keySizeBytes_ + " bytes long");
    final int index = baseLevelMap.findKey(key);
    if (index < 0) return 0;
    if (baseLevelMap.isCoupon(index)) return 1;
    final short level = baseLevelMap.getCoupon(index);
    if (level <= NUM_LEVELS) {
      final Map map = intermediateLevelMaps[level - 1];
      return map.getEstimate(key);
    }
    return lastLevelMap.getEstimate(key);
  }

  /**
   * Returns total bytes used by all levels
   * @return memory used in bytes
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
    final String ksb = fmtLong(keySizeBytes_);
    final String hllk = fmtLong(k_);
    final String lvls  = fmtLong(getActiveLevels());
    final String mub = fmtLong(getMemoryUsageBytes());
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = this.getClass().getSimpleName();
    sb.append("## ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("   Key Size Bytes            : ").append(ksb).append(LS);
    sb.append("   HLL k                     : ").append(hllk).append(LS);
    sb.append("   Active Levels             : ").append(lvls).append(LS);
    sb.append("   Memory Usage Bytes        : ").append(mub).append(LS);
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
    sb.append("## ").append("END SKETCH SUMMARY");
    sb.append(LS);
    return sb.toString();
  }

}
