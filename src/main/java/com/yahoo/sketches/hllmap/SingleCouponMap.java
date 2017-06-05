/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import java.util.Arrays;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.hash.MurmurHash3;

/**
 * Implements a key-value map where the value is a single coupon or a map reference.
 * This map holds all keys for all levels of the {@link UniqueCountMap}.
 * This map is implemented with a prime sized Open Address, Double Hash, with a 1-bit state array,
 * which indicates the contents of the value.
 *
 * @author Lee Rhodes
 * @author Alexander Saydakov
 * @author Kevin Lang
 */
final class SingleCouponMap extends Map {
  private static final double RSE = 0.408 / Math.sqrt(1024);

  private int tableEntries_;
  private int capacityEntries_;
  private int curCountEntries_;
  private double entrySizeBytes_;

  // Arrays
  private byte[] keysArr_;
  private short[] couponsArr_;

  /**
   * <ul><li>state: 0: empty or valid; empty if coupon is 0, otherwise valid.</li>
   * <li>state: 1: original coupon has been promoted, current coupon contains a table #
   * reference instead.</li>
   * </ul>
   */
  private byte[] stateArr_;

  private SingleCouponMap(final int keySizeBytes) {
    super(keySizeBytes);
  }

  static SingleCouponMap getInstance(final int initialNumEntries, final int keySizeBytes) {
    final int tableEntries = nextPrime(initialNumEntries);

    final SingleCouponMap map = new SingleCouponMap(keySizeBytes);
    map.tableEntries_ = tableEntries;
    map.capacityEntries_ = (int)(tableEntries * COUPON_MAP_GROW_TRIGGER_FACTOR);
    map.curCountEntries_ = 0;
    map.entrySizeBytes_ = updateEntrySizeBytes(tableEntries, keySizeBytes);

    map.keysArr_ = new byte[tableEntries * map.keySizeBytes_];
    map.couponsArr_ = new short[tableEntries];
    map.stateArr_ = new byte[(int) Math.ceil(tableEntries / 8.0)];
    return map;
  }

  @Override
  double update(final byte[] key, final short coupon) {
    final int entryIndex = findOrInsertKey(key);
    return update(entryIndex, coupon);
  }

  @Override
  double update(final int entryIndex, final short coupon) {
    if (couponsArr_[entryIndex] == 0) {
      couponsArr_[entryIndex] = coupon;
      return 1;
    }
    if (isCoupon(entryIndex)) {
      if (couponsArr_[entryIndex] == coupon) { //duplicate
        return 1;
      }
      return 0; // signal to promote
    }
    return -couponsArr_[entryIndex]; // negative level number
  }

  @Override
  double getEstimate(final byte[] key) {
    final int entryIndex = findKey(key);
    if (entryIndex < 0) { return 0; }
    if (isCoupon(entryIndex)) { return 1; }
    return -getCoupon(entryIndex); // negative: level #, zero: signal to promote
  }

  @Override
  double getUpperBound(final byte[] key) {
    return getEstimate(key) * (1 + RSE);
  }

  @Override
  double getLowerBound(final byte[] key) {
    return getEstimate(key) * (1 - RSE);
  }

  /**
   * Returns entryIndex if the given key is found. The coupon may be valid or contain a table index.
   * If not found, returns one's complement entryIndex
   * of an empty slot for insertion, which may be over a deleted key.
   * @param key the given key
   * @return the entryIndex
   */
  @Override
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

  @Override
  int findOrInsertKey(final byte[] key) {
    int entryIndex = findKey(key);
    if (entryIndex < 0) {
      if (curCountEntries_ + 1 > capacityEntries_) {
        resize();
        entryIndex = findKey(key);
        assert entryIndex < 0;
      }
      entryIndex = ~entryIndex;
      System.arraycopy(key, 0, keysArr_, entryIndex * keySizeBytes_, keySizeBytes_);
      curCountEntries_++;
    }
    return entryIndex;
  }

  @Override
  CouponsIterator getCouponsIterator(final int entryIndex) {
    return new CouponsIterator(couponsArr_, entryIndex, 1);
  }

  @Override
  int getMaxCouponsPerEntry() {
    return 1;
  }

  @Override
  int getCapacityCouponsPerEntry() {
    return 1;
  }

  @Override
  int getActiveEntries() {
    return curCountEntries_;
  }

  @Override
  int getDeletedEntries() {
    return 0;
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

  void setLevel(final int entryIndex, final int level) {
    couponsArr_[entryIndex] = (short) level;
    setBit(stateArr_, entryIndex);
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

  private void resize() {
    final byte[] oldKeysArr = keysArr_;
    final short[] oldCouponsArr = couponsArr_;
    final byte[] oldStateArr = stateArr_;
    final int oldTableEntries = tableEntries_;
    tableEntries_ = nextPrime((int) (curCountEntries_ / COUPON_MAP_TARGET_FILL_FACTOR));
    capacityEntries_ = (int)(tableEntries_ * COUPON_MAP_GROW_TRIGGER_FACTOR);
    keysArr_ = new byte[tableEntries_ * keySizeBytes_];
    couponsArr_ = new short[tableEntries_];
    stateArr_ = new byte[(int) Math.ceil(tableEntries_ / 8.0)];
    entrySizeBytes_ = updateEntrySizeBytes(tableEntries_, keySizeBytes_);
    //move the data
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

  private static final double updateEntrySizeBytes(final int tableEntries, final int keySizeBytes) {
    final double byteFraction = Math.ceil(tableEntries / 8.0) / tableEntries;
    return keySizeBytes + Short.BYTES + byteFraction;
  }

}
