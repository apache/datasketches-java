/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import com.yahoo.sketches.QuickSelect;

import static com.yahoo.sketches.Util.RESIZE_THRESHOLD;
import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.Util.ceilingPowerOf2;

/**
 * Top level class for hash table based implementation, which uses quick select algorithm
 * when the time comes to rebuild the hash table and throw away some entries.
 */
public abstract class ArrayOfDoublesQuickSelectSketch extends UpdatableArrayOfDoublesSketch {

  public static final byte serialVersionUID = 1;

  static final int LG_NOM_ENTRIES_BYTE = 16;
  static final int LG_CUR_CAPACITY_BYTE = 17;
  static final int LG_RESIZE_FACTOR_BYTE = 18;
  // 1 byte of padding for alignment
  static final int SAMPLING_P_FLOAT = 20;
  static final int RETAINED_ENTRIES_INT = 24;
  // 4 bytes of padding for alignment
  static final int ENTRIES_START = 32;

  protected static final int MIN_NOM_ENTRIES = 32;
  protected static final int DEFAULT_LG_RESIZE_FACTOR = 3;

  // these can be derived from other things, but are kept here for performance
  protected int rebuildThreshold_;
  protected int mask_;
  protected int lgCurrentCapacity_;

  protected ArrayOfDoublesQuickSelectSketch(int numValues, long seed) {
    super(numValues, seed);
  }

  protected abstract void updateValues(int index, double[] values);
  protected abstract void setNotEmpty();
  protected abstract void setIsEmpty(boolean isEmpty);
  protected abstract boolean isInSamplingMode();
  protected abstract int getResizeFactor();
  protected abstract int getCurrentCapacity();
  protected abstract void rebuild(int newCapacity);
  protected abstract long getKey(int index);
  protected abstract void setKey(int index, long key);
  protected abstract void setValues(int index, double[] values, boolean isCopyRequired);
  protected abstract void incrementCount();
  protected abstract void setThetaLong(long theta);
  protected abstract int insertKey(long key);
  protected abstract int findOrInsertKey(long key);
  protected abstract double[] find(long key);

  @Override
  public void trim() {
    if (getRetainedEntries() > getNominalEntries()) {
      updateTheta();
      rebuild();
    }
  }

  /**
   * @return maximum required storage bytes given nomEntries and numValues
   */
  public static int getMaxBytes(int nomEntries, int numValues) {
    return ENTRIES_START + (SIZE_OF_KEY_BYTES + SIZE_OF_VALUE_BYTES * numValues) * ceilingPowerOf2(nomEntries) * 2;
  }

  // non-public methods below

  // this is a special back door insert for merging
  // not sufficient by itself without keeping track of theta of another sketch
  void merge(long key, double[] values) {
    setNotEmpty();
    if (key < theta_) {
      int index = findOrInsertKey(key);
      if (index < 0) {
        incrementCount();
        setValues(~index, values, true);
      } else {
        updateValues(index, values);
      }
      rebuildIfNeeded();
    }
  }

  protected void rebuildIfNeeded() {
    if (getRetainedEntries() < rebuildThreshold_) return;
    if (getCurrentCapacity() > getNominalEntries()) {
      updateTheta();
      rebuild();
    } else {
      rebuild(getCurrentCapacity() * getResizeFactor());
    }
  }
  
  protected void rebuild() {
    rebuild(getCurrentCapacity());
  }

  protected void insert(long key, double[] values) {
    int index = insertKey(key);
    setValues(index, values, false);
    incrementCount();
  }

  protected void setRebuildThreshold() {
    if (getCurrentCapacity() > getNominalEntries()) {
      rebuildThreshold_ = (int) (getCurrentCapacity() * REBUILD_THRESHOLD);
    } else {
      rebuildThreshold_ = (int) (getCurrentCapacity() * RESIZE_THRESHOLD);
    }
  }

  @Override
  protected void insertOrIgnore(long key, double[] values) {
    if (values.length != getNumValues()) throw new IllegalArgumentException("input array of values must have " + getNumValues() + " elements, but has " + values.length);
    setNotEmpty();
    if (key == 0 || key >= theta_) return;
    int index = findOrInsertKey(key);
    if (index < 0) {
      incrementCount();
      setValues(~index, values, true);
    } else {
      updateValues(index, values);
    }
    rebuildIfNeeded();
  }

  protected void updateTheta() {
    long[] keys = new long[getRetainedEntries()];
    int i = 0;
    for (int j = 0; j < getCurrentCapacity(); j++) {
      long key = getKey(j); 
      if (key != 0) keys[i++] = key;
    }
    setThetaLong(QuickSelect.select(keys, 0, getRetainedEntries() - 1, getNominalEntries()));
  }

}
