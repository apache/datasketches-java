/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static com.yahoo.sketches.hllmap.MapDistribution.COUPON_MAP_GROW_TRIGGER_FACTOR;
import static com.yahoo.sketches.hllmap.MapDistribution.COUPON_MAP_TARGET_FILL_FACTOR;

import static com.yahoo.sketches.hllmap.Util.fmtDouble;
import static com.yahoo.sketches.hllmap.Util.fmtLong;

import java.util.Arrays;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.hash.MurmurHash3;


// Always holds all keys.
// prime size, double hash, no deletes, 1-bit state array
// same growth algorithm as for the next levels, except no shrink. Constants may be specific.

class SingleCouponMap extends Map {
  public static final String LS = System.getProperty("line.separator");

  private final double entrySizeBytes_;

  private int tableEntries_;
  private int capacityEntries_;
  private int curCountEntries_;

  // Arrays
  private byte[] keysArr_;
  private short[] couponsArr_;

  // state: 0: empty or valid; empty if coupon is 0, otherwise valid.
  // state: 1: original coupon has been promoted, current coupon contains a table # instead.
  private byte[] stateArr_;

  private SingleCouponMap(final int keySizeBytes, final int tableEntries) {
    super(keySizeBytes);
    final double byteFraction = Math.ceil(tableEntries / 8.0) / tableEntries;
    entrySizeBytes_ = keySizeBytes + Short.BYTES + byteFraction;
  }

  static SingleCouponMap getInstance(final int tgtEntries, final int keySizeBytes) {
    Util.checkTgtEntries(tgtEntries); //optional
    final int tableEntries = Util.nextPrime(tgtEntries);

    final SingleCouponMap map = new SingleCouponMap(keySizeBytes, tableEntries);

    map.tableEntries_ = tableEntries;
    map.capacityEntries_ = (int)(tableEntries * COUPON_MAP_GROW_TRIGGER_FACTOR);
    map.curCountEntries_ = 0;

    map.keysArr_ = new byte[tableEntries * map.keySizeBytes_];
    map.couponsArr_ = new short[tableEntries];
    map.stateArr_ = new byte[(int) Math.ceil(tableEntries / 8.0)];
    return map;
  }

  @Override
  double update(final byte[] key, final int coupon) {
    final int entryIndex = findOrInsertKey(key);
    if (entryIndex < 0) { // insert
      setCoupon(~entryIndex, (short) coupon, false);
      return 1.0;
    }
    int coupon2 = couponsArr_[entryIndex];
    //depends on the fact that a valid coupon can never be a small number.
    if (coupon == coupon2) {
      return 1.0;
    }
    return -couponsArr_[entryIndex]; //indicates coupon contains table #
  }

  @Override
  double getEstimate(final byte[] key) {
    final int entryIndex = findKey(key);
    if (entryIndex < 0) return 0;
    if (isCoupon(entryIndex)) return 1;
    return ~couponsArr_[entryIndex]; //indicates coupon contains table #
  }

  /**
   * Returns entryIndex if the given key is found. The coupon may be valid or contain a table index.
   * If not found, returns one's complement entryIndex
   * of an empty slot for insertion, which may be over a deleted key.
   * @param key the given key
   * @return the entryIndex
   */
  int findKey(final byte[] key) {
    final long[] hash = MurmurHash3.hash(key, SEED);
    int entryIndex = getIndex(hash[0], tableEntries_);
    final int stride = getStride(hash[1], tableEntries_);
    final int loopIndex = entryIndex;

    do {
      if (couponsArr_[entryIndex] == 0) {
        return ~entryIndex; //empty
      }
      if (Map.arraysEqual(key, 0, keysArr_, entryIndex * keySizeBytes_, keySizeBytes_)) {
        return entryIndex;
      }
      entryIndex = (entryIndex + stride) % tableEntries_;
    } while (entryIndex != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

  int findOrInsertKey(final byte[] key) {
    int entryIndex = findKey(key);
    if (entryIndex < 0) {
      if (curCountEntries_ + 1 > capacityEntries_) {
        resize();
        entryIndex = findKey(key);
        assert entryIndex < 0;
      }
      //will return negative: was not found, inserted
      System.arraycopy(key, 0, keysArr_, ~entryIndex * keySizeBytes_, keySizeBytes_);
      curCountEntries_++;
    }
    return entryIndex;
  }

  boolean isCoupon(final int entryIndex) {
    return !isBitSet(stateArr_, entryIndex);
  }

  short getCoupon(final int entryIndex) {
    return couponsArr_[entryIndex];
  }

  void setCoupon(final int entryIndex, final short coupon, final boolean isLevel) {
    couponsArr_[entryIndex] = coupon;
    if (isLevel) {
      setBit(stateArr_, entryIndex);
    } else {
      clearBit(stateArr_, entryIndex);
    }
  }

  @Override
  double getEntrySizeBytes() {
    return entrySizeBytes_;
  }

  @Override
  int getTableEntries() {
    return tableEntries_;
  }

  @Override
  int getCapacityEntries() {
    return capacityEntries_;
  }

  @Override
  int getCurrentCountEntries() {
    return curCountEntries_;
  }

  @Override
  long getMemoryUsageBytes() {
    final long arrays = keysArr_.length
        + (long)couponsArr_.length * Short.BYTES
        + stateArr_.length;
    final long other = 4 * 4 + 8;
    return arrays + other;
  }

  @Override
  public String toString() {
    final String te = fmtLong(getTableEntries());
    final String ce = fmtLong(getCapacityEntries());
    final String cce = fmtLong(getCurrentCountEntries());
    final String esb = fmtDouble(getEntrySizeBytes());
    final String mub = fmtLong(getMemoryUsageBytes());

    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = this.getClass().getSimpleName();
    sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("    Max Coupons Per Entry     : ").append(1).append(LS);
    sb.append("    Capacity Coupons Per Entry: ").append(1).append(LS);
    sb.append("    Table Entries             : ").append(te).append(LS);
    sb.append("    Capacity Entries          : ").append(ce).append(LS);
    sb.append("    Current Count Entries     : ").append(cce).append(LS);
    sb.append("    Entry Size Bytes          : ").append(esb).append(LS);
    sb.append("    Memory Usage Bytes        : ").append(mub).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);
    return sb.toString();
  }

  private void resize() {
    final byte[] oldKeysArr = keysArr_;
    final short[] oldCouponsArr = couponsArr_;
    final byte[] oldStateArr = stateArr_;
    final int oldTableEntries = tableEntries_;
    tableEntries_ = Util.nextPrime((int) (curCountEntries_ / COUPON_MAP_TARGET_FILL_FACTOR));
    capacityEntries_ = (int)(tableEntries_ * COUPON_MAP_GROW_TRIGGER_FACTOR);
    keysArr_ = new byte[tableEntries_ * keySizeBytes_];
    couponsArr_ = new short[tableEntries_];
    stateArr_ = new byte[(int) Math.ceil(tableEntries_ / 8.0)];
    for (int i = 0; i < oldTableEntries; i++) {
      if (oldCouponsArr[i] != 0) {
        final byte[] key =
            Arrays.copyOfRange(oldKeysArr, i * keySizeBytes_, i * keySizeBytes_ + keySizeBytes_);
        insertEntry(key, oldCouponsArr[i], isBitSet(oldStateArr, i));
      }
    }
  }

  // for internal use during resize, so no resize check here
  private void insertEntry(final byte[] key, final int coupon, final boolean setStateOne) {
    final long[] hash = MurmurHash3.hash(key, SEED);
    int entryIndex = getIndex(hash[0], tableEntries_);
    final int stride = getStride(hash[1], tableEntries_);
    final int loopIndex = entryIndex;
    do {
      if (couponsArr_[entryIndex] == 0) {
        System.arraycopy(key, 0, keysArr_, entryIndex * keySizeBytes_, keySizeBytes_);
        setCoupon(entryIndex, (short)coupon, setStateOne);
        return;
      }
      entryIndex = (entryIndex + stride) % tableEntries_;
    } while (entryIndex != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

}
