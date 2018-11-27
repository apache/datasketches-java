/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static com.yahoo.sketches.Util.invPow2;

import java.util.Arrays;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.hash.MurmurHash3;

/**
 * Implements a key-value map where the value is a hash map of coupons.
 *
 * <p>The outer map is implemented as a prime-sized, Open Address, Double Hash, with deletes, so
 * this table can grow and shrink. Each entry row has a 1-byte count where 255 is a marker for
 * "dirty" and zero is empty.
 *
 * <p>The inner hash tables are implemented with linear probing or OASH and a load factor of 0.75.
 *
 * @author Lee Rhodes
 * @author Alexander Saydakov
 * @author Kevin Lang
 */
final class CouponHashMap extends Map {
  private static final double INNER_LOAD_FACTOR = 0.75;
  private static final byte DELETED_KEY_MARKER = (byte) 255;
  private static final int BYTE_MASK = 0XFF;
  private static final int COUPON_K = 1024;
  private static final double RSE = 0.408 / Math.sqrt(1024);

  private final int maxCouponsPerKey_;
  private final int capacityCouponsPerKey_;
  private final int entrySizeBytes_;

  private int tableEntries_;
  private int capacityEntries_;
  private int numActiveKeys_;
  private int numDeletedKeys_;

  //Arrays
  private byte[] keysArr_;
  private short[] couponsArr_;
  private byte[] curCountsArr_; //also acts as a stateArr: 0 empty, 255 deleted
  private float[] invPow2SumArr_;
  private float[] hipEstAccumArr_;

  private CouponHashMap(final int keySizeBytes, final int maxCouponsPerKey) {
    super(keySizeBytes);
    maxCouponsPerKey_ = maxCouponsPerKey;
    capacityCouponsPerKey_ = (int)(maxCouponsPerKey * INNER_LOAD_FACTOR);
    entrySizeBytes_ = keySizeBytes + (maxCouponsPerKey * Short.BYTES) + 1 + 4 + 4;
  }

  static CouponHashMap getInstance(final int keySizeBytes, final int maxCouponsPerKey) {
    checkMaxCouponsPerKey(maxCouponsPerKey);
    final int tableEntries = COUPON_MAP_MIN_NUM_ENTRIES;

    final CouponHashMap map = new CouponHashMap(keySizeBytes, maxCouponsPerKey);
    map.tableEntries_ = tableEntries;
    map.capacityEntries_ = (int)(tableEntries * COUPON_MAP_GROW_TRIGGER_FACTOR);
    map.numActiveKeys_ = 0;
    map.numDeletedKeys_ = 0;

    map.keysArr_ = new byte[tableEntries * keySizeBytes];
    map.couponsArr_ = new short[tableEntries * maxCouponsPerKey];
    map.curCountsArr_ = new byte[tableEntries];
    map.invPow2SumArr_ = new float[tableEntries];
    map.hipEstAccumArr_ = new float[tableEntries];
    return map;
  }

  @Override
  double update(final byte[] key, final short coupon) {
    final int entryIndex = findOrInsertKey(key);
    return update(entryIndex, coupon); //negative when time to promote
  }

  @Override
  double update(final int entryIndex, final short coupon) {
    final int couponMapArrEntryIndex = entryIndex * maxCouponsPerKey_;

    int innerCouponIndex = (coupon & 0xFFFF) % maxCouponsPerKey_;

    while (couponsArr_[couponMapArrEntryIndex + innerCouponIndex] != 0) {
      if (couponsArr_[couponMapArrEntryIndex + innerCouponIndex] == coupon) {
        return hipEstAccumArr_[entryIndex]; //duplicate, returns the estimate
      }
      innerCouponIndex = (innerCouponIndex + 1) % maxCouponsPerKey_; //linear search
    }
    if (((curCountsArr_[entryIndex] + 1) & BYTE_MASK) > capacityCouponsPerKey_) {
      //returns the negative estimate, as signal to promote
      return -hipEstAccumArr_[entryIndex];
    }

    couponsArr_[couponMapArrEntryIndex + innerCouponIndex] = coupon; //insert
    curCountsArr_[entryIndex]++;
    //hip +=  k/qt; qt -= 1/2^(val);
    hipEstAccumArr_[entryIndex] += COUPON_K / invPow2SumArr_[entryIndex];
    invPow2SumArr_[entryIndex] -= invPow2(coupon16Value(coupon));
    return hipEstAccumArr_[entryIndex]; //returns the estimate
  }

  @Override
  double getEstimate(final byte[] key) {
    final int index = findKey(key);
    if (index < 0) { return 0; }
    return hipEstAccumArr_[index];
  }

  @Override
  double getUpperBound(final byte[] key) {
    return getEstimate(key) * (1 + RSE);
  }

  @Override
  double getLowerBound(final byte[] key) {
    return getEstimate(key) * (1 - RSE);
  }

  @Override
  void updateEstimate(final int entryIndex, final double estimate) {
    if (entryIndex < 0) {
      throw new SketchesArgumentException("Key not found.");
    }
    hipEstAccumArr_[entryIndex] = (float) estimate;
  }

  /**
   * Returns entryIndex if the given key is found. If not found, returns one's complement index
   * of an empty slot for insertion, which may be over a deleted key.
   * @param key the given key
   * @return the entryIndex
   */
  @Override
  int findKey(final byte[] key) {
    final long[] hash = MurmurHash3.hash(key, SEED);
    int entryIndex = getIndex(hash[0], tableEntries_);
    int firstDeletedIndex = -1;
    final int loopIndex = entryIndex;
    do {
      if (curCountsArr_[entryIndex] == 0) {
        return firstDeletedIndex == -1 ? ~entryIndex : ~firstDeletedIndex; // found empty or deleted
      }
      if (curCountsArr_[entryIndex] == DELETED_KEY_MARKER) {
        if (firstDeletedIndex == -1) {
          firstDeletedIndex = entryIndex;
        }
      } else if (Map.arraysEqual(keysArr_, entryIndex * keySizeBytes_, key, 0, keySizeBytes_)) {
        return entryIndex; // found key
      }
      entryIndex = (entryIndex + getStride(hash[1], tableEntries_)) % tableEntries_;
    } while (entryIndex != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

  @Override
  int findOrInsertKey(final byte[] key) {
    int entryIndex = findKey(key);
    if (entryIndex < 0) { //key not found
      entryIndex = ~entryIndex;
      if (curCountsArr_[entryIndex] == DELETED_KEY_MARKER) { // reusing slot from a deleted key
        Arrays.fill(couponsArr_, entryIndex * maxCouponsPerKey_,
            (entryIndex + 1) *  maxCouponsPerKey_, (short) 0);
        curCountsArr_[entryIndex] = 0;
        numDeletedKeys_--;
      }
      if ((numActiveKeys_ + numDeletedKeys_) >= capacityEntries_) {
        resize();
        entryIndex = ~findKey(key);
        assert entryIndex >= 0;
      }
      //insert new key
      System.arraycopy(key, 0, keysArr_, entryIndex * keySizeBytes_, keySizeBytes_);
      //initialize HIP:  qt <- k; hip <- 0;
      invPow2SumArr_[entryIndex] = COUPON_K;
      hipEstAccumArr_[entryIndex] = 0;
      numActiveKeys_++;
    }
    return entryIndex;
  }

  @Override
  void deleteKey(final int entryIndex) {
    curCountsArr_[entryIndex] = DELETED_KEY_MARKER;
    numActiveKeys_--;
    numDeletedKeys_++;
    if ((numActiveKeys_ > COUPON_MAP_MIN_NUM_ENTRIES)
        && (numActiveKeys_ < (tableEntries_ * COUPON_MAP_SHRINK_TRIGGER_FACTOR))) {
      resize();
    }
  }

  @Override
  CouponsIterator getCouponsIterator(final int entryIndex) {
    return new CouponsIterator(couponsArr_, entryIndex * maxCouponsPerKey_, maxCouponsPerKey_);
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
    return numActiveKeys_ + numDeletedKeys_;
  }

  @Override
  long getMemoryUsageBytes() {
    final long arrays = keysArr_.length
        + ((long) couponsArr_.length * Short.BYTES)
        + curCountsArr_.length
        + ((long) invPow2SumArr_.length * Float.BYTES)
        + ((long) hipEstAccumArr_.length * Float.BYTES);
    final long other = 4 * 5;
    return arrays + other;
  }

  @Override
  int getActiveEntries() {
    return numActiveKeys_;
  }

  @Override
  int getDeletedEntries() {
    return numDeletedKeys_;
  }

  @Override
  int getMaxCouponsPerEntry() {
    return maxCouponsPerKey_;
  }

  @Override
  int getCapacityCouponsPerEntry() {
    return capacityCouponsPerKey_;
  }

  private static final void checkMaxCouponsPerKey(final int maxCouponsPerKey) {
    checkIfPowerOf2(maxCouponsPerKey, "maxCouponsPerKey");
    final int cpk = maxCouponsPerKey;
    if ((cpk < 16) || (cpk > 256)) {
      throw new SketchesArgumentException(
          "Required: 16 <= maxCouponsPerKey <= 256 : " + maxCouponsPerKey);
    }
  }

  private void resize() {
    final byte[] oldKeysArr = keysArr_;
    final short[] oldCouponMapArr = couponsArr_;
    final byte[] oldCurCountsArr = curCountsArr_;
    final float[] oldInvPow2SumArr = invPow2SumArr_;
    final float[] oldHipEstAccumArr = hipEstAccumArr_;
    final int oldNumEntries = tableEntries_;
    tableEntries_ = Math.max(
      nextPrime((int) (numActiveKeys_ / COUPON_MAP_TARGET_FILL_FACTOR)),
      COUPON_MAP_MIN_NUM_ENTRIES
    );
    capacityEntries_ = (int)(tableEntries_ * COUPON_MAP_GROW_TRIGGER_FACTOR);
    keysArr_ = new byte[tableEntries_ * keySizeBytes_];
    couponsArr_ = new short[tableEntries_ * maxCouponsPerKey_];
    curCountsArr_ = new byte[tableEntries_];
    invPow2SumArr_ = new float[tableEntries_];
    hipEstAccumArr_ = new float[tableEntries_];
    numActiveKeys_ = 0;
    numDeletedKeys_ = 0;
    for (int i = 0; i < oldNumEntries; i++) {
      if ((oldCurCountsArr[i] != 0) && (oldCurCountsArr[i] != DELETED_KEY_MARKER)) {
        //extract an old valid key
        final byte[] key =
            Arrays.copyOfRange(oldKeysArr, i * keySizeBytes_, (i * keySizeBytes_) + keySizeBytes_);
        //insert the key and get its index
        final int index = insertKey(key);
        //copy the coupons array into that index
        System.arraycopy(oldCouponMapArr, i * maxCouponsPerKey_, couponsArr_,
            index * maxCouponsPerKey_, maxCouponsPerKey_);
        //transfer the count
        curCountsArr_[index] = oldCurCountsArr[i];
        //transfer the HIP registers
        invPow2SumArr_[index] = oldInvPow2SumArr[i];
        hipEstAccumArr_[index] = oldHipEstAccumArr[i];
      }
    }
  }

  // for internal use by resize, no resize check and no deleted key check here
  // no changes to HIP
  private int insertKey(final byte[] key) {
    final long[] hash = MurmurHash3.hash(key, SEED);
    int entryIndex = getIndex(hash[0], tableEntries_);
    final int loopIndex = entryIndex;
    do {
      if (curCountsArr_[entryIndex] == 0) {
        System.arraycopy(key, 0, keysArr_, entryIndex * keySizeBytes_, keySizeBytes_);
        numActiveKeys_++;
        return entryIndex;
      }
      entryIndex = (entryIndex + getStride(hash[1], tableEntries_)) % tableEntries_;
    } while (entryIndex != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

}
