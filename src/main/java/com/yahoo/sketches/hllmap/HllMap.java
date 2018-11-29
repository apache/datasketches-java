/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static com.yahoo.sketches.Util.invPow2;
import static java.lang.Math.log;
import static java.lang.Math.sqrt;

import java.util.Arrays;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.hash.MurmurHash3;

/**
 * Implements a key-value map where the value is a compact HLL sketch of size k.
 * The HLL bins are compacted into 10 bins per long so that a 1024 bins are compacted into
 * 824 bytes, which is a 20% reduction in space. Higher density compressions are possible
 * (up to 50%), but the required code is much more complex and considerably slower.
 *
 * <p>Each entry row, associated with a key, also contains 3 double registers for accurately
 * tracking the HIP (Historical Inverse Probability) estimator. HLL implementations have multiple
 * estimators and the early estimators in this implementation are quite novel and provide superior
 * error performance over most other HLL implementations.
 *
 * @author Lee Rhodes
 * @author KevinLang
 * @author Alexander Saydakov
 */
final class HllMap extends Map {
  private static final double LOAD_FACTOR = 15.0 / 16.0;
  private static final int HLL_INIT_NUM_ENTRIES = 157;
  private static final float HLL_RESIZE_FACTOR = 2.0F;
  private static final double RSE = sqrt(log(2.0)) / 32.0;
  private final int k_;
  private final int hllArrLongs_; //# of longs required to store the HLL array

  private int tableEntries_;      //Full size of the table
  private int capacityEntries_;   //max capacity entries defined by Load factor
  private int curCountEntries_;   //current count of valid entries
  private float growthFactor_;    //e.g., 1.2 to 2.0
  private double entrySizeBytes_;

  //Arrays
  private byte[] keysArr_; //keys of zero are allowed
  private long[] arrOfHllArr_;
  private double[] invPow2SumHiArr_;
  private double[] invPow2SumLoArr_;
  private double[] hipEstAccumArr_;
  private byte[] stateArr_;

  /**
   * Private constructor used to set all finals
   * @param keySizeBytes size of key in bytes
   * @param k size of HLL sketch
   */
  private HllMap(final int keySizeBytes, final int k) {
    super(keySizeBytes);
    k_ = k;
    hllArrLongs_ = (k / 10) + 1;
  }

  static HllMap getInstance(final int keySizeBytes, final int k) {
    final int tableEntries = HLL_INIT_NUM_ENTRIES;

    final HllMap map = new HllMap(keySizeBytes, k);
    map.tableEntries_ = tableEntries;
    map.capacityEntries_ = (int)(tableEntries * LOAD_FACTOR);
    map.curCountEntries_ = 0;
    map.growthFactor_ = HLL_RESIZE_FACTOR;
    map.entrySizeBytes_ = updateEntrySizeBytes(map.tableEntries_, keySizeBytes, map.hllArrLongs_);

    map.keysArr_ = new byte[tableEntries * map.keySizeBytes_];
    map.arrOfHllArr_ = new long[tableEntries * map.hllArrLongs_];
    map.invPow2SumHiArr_ = new double[tableEntries];
    map.invPow2SumLoArr_ = new double[tableEntries];
    map.hipEstAccumArr_ = new double[tableEntries];
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
    updateHll(entryIndex, coupon); //update HLL array, updates HIP
    return hipEstAccumArr_[entryIndex];
  }

  @Override
  double getEstimate(final byte[] key) {
    if (key == null) { return Double.NaN; }
    final int entryIndex = findKey(key);
    if (entryIndex < 0) {
      return 0;
    }
    return hipEstAccumArr_[entryIndex];
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
    hipEstAccumArr_[entryIndex] = estimate;
  }

  /**
   * Returns the entry index for the given key given the array of keys, if found.
   * Otherwise, returns the one's complement of first empty entry found;
   * @param key the key to search for
   * @return the entry index of the given key, or the one's complement of the index if not found.
   */
  @Override
  final int findKey(final byte[] key) {
    final int keyLen = key.length;
    final long[] hash = MurmurHash3.hash(key, SEED);
    int entryIndex  = getIndex(hash[0], tableEntries_);
    final int stride = getStride(hash[1], tableEntries_);
    final int loopIndex = entryIndex;

    do {
      if (isBitClear(stateArr_, entryIndex)) { //check if slot is empty
        return ~entryIndex;
      }
      if (arraysEqual(key, 0, keysArr_, entryIndex * keyLen, keyLen)) { //check for key match
        return entryIndex;
      }
      entryIndex = (entryIndex + stride) % tableEntries_;
    } while (entryIndex != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

  @Override
  int findOrInsertKey(final byte[] key) {
    int entryIndex = findKey(key);
    if (entryIndex < 0) { //key not found, initialize new row
      entryIndex = ~entryIndex;
      System.arraycopy(key, 0, keysArr_, entryIndex * keySizeBytes_, keySizeBytes_);
      setBit(stateArr_, entryIndex);
      invPow2SumHiArr_[entryIndex] = k_;
      invPow2SumLoArr_[entryIndex] = 0;
      hipEstAccumArr_[entryIndex] = 0;
      curCountEntries_++;
      if (curCountEntries_ > capacityEntries_) {
        resize();
        entryIndex = findKey(key);
        assert entryIndex >= 0;
      }
    }
    return entryIndex;
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
        + ((long) arrOfHllArr_.length * Long.BYTES)
        + ((long) invPow2SumLoArr_.length * Double.BYTES)
        + ((long) invPow2SumHiArr_.length * Double.BYTES)
        + ((long) hipEstAccumArr_.length * Double.BYTES)
        + stateArr_.length;
    final long other = (5L * Integer.BYTES) + Float.BYTES + Double.BYTES;
    return arrays + other;
  }

  @Override
  CouponsIterator getCouponsIterator(final int index) {
    // not applicable
    return null;
  }

  @Override
  int getMaxCouponsPerEntry() {
    // not applicable
    return 0;
  }

  @Override
  int getCapacityCouponsPerEntry() {
    // not applicable
    return 0;
  }

  @Override
  int getActiveEntries() {
    return curCountEntries_;
  }

  @Override
  int getDeletedEntries() {
    return 0;
  }

  /**
   * Find the first empty slot for the given key.
   * Only used by resize, where it is known that the key does not exist in the table.
   * Throws an exception if no empty slots.
   * @param key the given key
   * @param tableEntries prime size of table
   * @param stateArr the valid bit array
   * @return the first empty slot for the given key
   */
  private static final int findEmpty(final byte[] key, final int tableEntries, final byte[] stateArr) {
    final long[] hash = MurmurHash3.hash(key, SEED);
    int entryIndex  = getIndex(hash[0], tableEntries);
    final int stride = getStride(hash[1], tableEntries);
    final int loopIndex = entryIndex;

    do {
      if (isBitClear(stateArr, entryIndex)) { //check if slot is empty
        return entryIndex;
      }
      entryIndex = (entryIndex + stride) % tableEntries;
    } while (entryIndex != loopIndex);
    throw new SketchesArgumentException("No empty slots.");
  }

  //This method is specifically tied to the HLL array layout
  private final boolean updateHll(final int entryIndex, final int coupon) {
    final int newValue = coupon16Value(coupon);

    final int hllIdx = coupon & (k_ - 1); //lower lgK bits
    final int longIdx = hllIdx / 10;
    final int shift = ((hllIdx % 10) * 6) & SIX_BIT_MASK;

    long hllLong = arrOfHllArr_[(entryIndex * hllArrLongs_) + longIdx];
    final int oldValue = (int)(hllLong >>> shift) & SIX_BIT_MASK;
    if (newValue <= oldValue) { return false; }
    // newValue > oldValue

    //update hipEstAccum BEFORE updating invPow2Sum
    final double invPow2Sum = invPow2SumHiArr_[entryIndex] + invPow2SumLoArr_[entryIndex];
    final double oneOverQ = k_ / invPow2Sum;
    hipEstAccumArr_[entryIndex] += oneOverQ;

    //update invPow2Sum
    if (oldValue < 32) { invPow2SumHiArr_[entryIndex] -= invPow2(oldValue); }
    else               { invPow2SumLoArr_[entryIndex] -= invPow2(oldValue); }
    if (newValue < 32) { invPow2SumHiArr_[entryIndex] += invPow2(newValue); }
    else               { invPow2SumLoArr_[entryIndex] += invPow2(newValue); }

    //insert the new value
    hllLong &= ~(0X3FL << shift);  //zero out the 6-bit field
    hllLong |=  ((long)newValue) << shift; //insert
    arrOfHllArr_[(entryIndex * hllArrLongs_) + longIdx] = hllLong;
    return true;
  }

  private final void resize() {
    final int newTableEntries = nextPrime((int)(tableEntries_ * growthFactor_));
    final int newCapacityEntries = (int)(newTableEntries * LOAD_FACTOR);

    final byte[] newKeysArr = new byte[newTableEntries * keySizeBytes_];
    final long[] newArrOfHllArr = new long[newTableEntries * hllArrLongs_];
    final double[] newInvPow2Sum1 = new double[newTableEntries];
    final double[] newInvPow2Sum2 = new double[newTableEntries];
    final double[] newHipEstAccum = new double[newTableEntries];
    final byte[] newStateArr = new byte[(int) Math.ceil(newTableEntries / 8.0)];

    for (int oldIndex = 0; oldIndex < tableEntries_; oldIndex++) {
      if (isBitClear(stateArr_, oldIndex)) { continue; }
      // extract an old key
      final byte[] key =
          Arrays.copyOfRange(keysArr_, oldIndex * keySizeBytes_, (oldIndex + 1) * keySizeBytes_);
      final int newIndex = findEmpty(key, newTableEntries, newStateArr);
      System.arraycopy(key, 0, newKeysArr, newIndex * keySizeBytes_, keySizeBytes_); //put key
      //put the rest of the row
      System.arraycopy(arrOfHllArr_, oldIndex * hllArrLongs_, newArrOfHllArr,
          newIndex * hllArrLongs_, hllArrLongs_);
      newInvPow2Sum1[newIndex] = invPow2SumHiArr_[oldIndex];
      newInvPow2Sum2[newIndex] = invPow2SumLoArr_[oldIndex];
      newHipEstAccum[newIndex] = hipEstAccumArr_[oldIndex];
      setBit(newStateArr, newIndex);
    }
    //restore into sketch
    tableEntries_ = newTableEntries;
    capacityEntries_ = newCapacityEntries;
    //curCountEntries_, growthFactor_  unchanged
    entrySizeBytes_ = updateEntrySizeBytes(tableEntries_, keySizeBytes_, hllArrLongs_);

    keysArr_ = newKeysArr;
    arrOfHllArr_ = newArrOfHllArr;
    invPow2SumHiArr_ = newInvPow2Sum1; //init to k
    invPow2SumLoArr_ = newInvPow2Sum2; //init to 0
    hipEstAccumArr_ = newHipEstAccum;  //init to 0
    stateArr_ = newStateArr;
  }

  private static final double updateEntrySizeBytes(final int tableEntries, final int keySizeBytes,
      final int hllArrLongs) {
    final double byteFraction = Math.ceil(tableEntries / 8.0) / tableEntries;
    return keySizeBytes + ((double) hllArrLongs * Long.BYTES) + (3.0 * Double.BYTES) + byteFraction;
  }

}
