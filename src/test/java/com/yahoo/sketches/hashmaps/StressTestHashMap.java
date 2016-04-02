/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.hashmaps;

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

      for (int h = 0; h < 10; h++) {
        HashMap hashmap = newHashMap(capacity, h);
        if (hashmap == null)
          continue;

        long timePerAdjust = timeOneHashMap(hashmap, keys, values, capacity);

        System.out.format("%s\t%d\t%d\n", hashmap.getClass().getSimpleName(), capacity,
            timePerAdjust);
      }
    }
  }

  private static long timeOneHashMap(HashMap hashmap, long[] keys, long[] values, int sizeToShift) {
    final long startTime = System.nanoTime();
    int n = keys.length;
    assert (n == values.length);
    for (int i = 0; i < n; i++) {
      hashmap.adjust(keys[i], values[i]);
      if (hashmap.getSize() == sizeToShift) {
        hashmap.adjustAllValuesBy(-1);
        hashmap.keepOnlyLargerThan(0);
      }
    }
    final long endTime = System.nanoTime();
    return (endTime - startTime) / n;
  }

  static private HashMap newHashMap(int capacity, int i) {
    switch (i) {
      case 0:
        return new HashMapTrove(capacity);
      case 1:
        return new HashMapTroveRebuilds(capacity);
      case 2:
        return new HashMapLinearProbingWithRebuilds(capacity);
      case 3:
        return new HashMapDoubleHashingWithRebuilds(capacity);
      case 4:
        return new HashMapWithImplicitDeletes(capacity);
      case 5:
        return new HashMapWithEfficientDeletes(capacity);
      case 6:
        return new HashMapRobinHood(capacity);
      case 7:
        return new HashMapReverseEfficient(capacity);
      case 8:
        return new HashMapReverseEfficientOneArray(capacity);
    }
    return null;
  }

  private static long murmur(long key) {
    long[] keyArr = new long[1];
    keyArr[0] = key;
    return MurmurHash3.hash(keyArr, 0)[0];
  }
}
