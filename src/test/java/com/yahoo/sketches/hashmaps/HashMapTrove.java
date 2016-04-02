/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.hashmaps;

import gnu.trove.procedure.TLongLongProcedure;
import gnu.trove.function.TLongFunction;
import gnu.trove.map.hash.TLongLongHashMap;

/**
 * Only used for test comparisons
 */
public class HashMapTrove extends HashMap {

  TLongLongHashMap hashmap;

  public HashMapTrove(int capacity) {
    if (capacity <= 0)
      throw new IllegalArgumentException(
          "Received negative or zero value for as initial capacity.");
    this.capacity = capacity;
    hashmap = new TLongLongHashMap(capacity);
  }

  @Override
  public int getSize() {
    return hashmap.size();
  }

  @Override
  public void adjustOrPutValue(long key, long adjustAmount, long putAmount) {
    hashmap.adjustOrPutValue(key, adjustAmount, putAmount);
  }

  @Override
  public long get(long key) {
    return hashmap.get(key);
  }

  @Override
  public void keepOnlyLargerThan(long thresholdValue) {
    hashmap.retainEntries(new GreaterThenThreshold(thresholdValue));
  }

  @Override
  public void adjustAllValuesBy(long adjustAmount) {
    hashmap.transformValues(new AdjustAllValuesBy(adjustAmount));
  }

  @Override
  public long[] getKeys() {
    return hashmap.keys();
  }

  @Override
  public long[] getValues() {
    return hashmap.values();
  }

  @Override
  public boolean isActive(int probe) {
    return false;
  }

  private class GreaterThenThreshold implements TLongLongProcedure {
    long threshold;

    public GreaterThenThreshold(long threshold) {
      this.threshold = threshold;
    }

    @Override
    public boolean execute(long key, long value) {
      return (value > threshold);
    }
  }

  private class AdjustAllValuesBy implements TLongFunction {
    long adjustAmount;

    public AdjustAllValuesBy(long adjustAmount) {
      this.adjustAmount = adjustAmount;
    }

    @Override
    public long execute(long value) {
      return value + adjustAmount;
    }
  }

}
