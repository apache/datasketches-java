/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import com.yahoo.sketches.hash.MurmurHash3;

public class StressTestHashMap {

  public static void main(String[] args) {
    stress();
  }
  
  private static void stress() {
    for (int capacity = 2 << 5; capacity < 2 << 24; capacity *= 2) {
      int n = 10000000;

      long[] keys = new long[n];
      long[] values = new long[n];

      for (int i = 0; i < n; i++) {
        keys[i] = murmur(i);
        values[i] = (i < capacity / 2) ? n : 1;
      }

      ReversePurgeLongHashMap hashmap = new ReversePurgeLongHashMap(capacity);
      long timePerAdjust = timeOneHashMap(hashmap, keys, values, (int) (.75 * capacity));
      System.out.format("%s\t%d\t%d%n", hashmap.getClass().getSimpleName(), capacity, timePerAdjust);
    }
  }

  private static long timeOneHashMap(ReversePurgeLongHashMap hashMap, long[] keys, long[] values, int sizeToShift) {
    final long startTime = System.nanoTime();
    int n = keys.length;
    assert (n == values.length);
    for (int i = 0; i < n; i++) {
      hashMap.adjustOrPutValue(keys[i], values[i]);
      if (hashMap.getNumActive() == sizeToShift) {
        hashMap.adjustAllValuesBy(-1);
        hashMap.keepOnlyPositiveCounts();
      }
    }
    final long endTime = System.nanoTime();
    return (endTime - startTime) / n;
  }

  private static long murmur(long key) {
    long[] keyArr = { key };
    return MurmurHash3.hash(keyArr, 0)[0];
  }
}
