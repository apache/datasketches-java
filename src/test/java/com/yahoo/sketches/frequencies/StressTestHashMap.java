/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import com.yahoo.sketches.frequencies.HashMap;
import com.yahoo.sketches.frequencies.HashMapReverseEfficient;
import com.yahoo.sketches.hash.MurmurHash3;

public class StressTestHashMap {

  public static void main(String[] args) {
    stress();
  }

  static private HashMap newHashMap(int capacity, int i) {
    HashMap hashMap = null;
    switch (i) {
      case 0: hashMap = new HashMapReverseEfficient(capacity); break;
      case 1: //hashMap = new HashMapTrove(capacity); break;
      case 2: //hashMap = new HashMapTroveRebuilds(capacity); break;
      case 3: //hashMap = new HashMapLinearProbingWithRebuilds(capacity); break;
      case 4: //hashMap = new HashMapDoubleHashingWithRebuilds(capacity); break;
      case 5: //hashMap = new HashMapWithImplicitDeletes(capacity); break;
      case 6: //hashMap = new HashMapWithEfficientDeletes(capacity); break;
      case 7: //hashMap = new HashMapRobinHood(capacity); break;
      case 8: //hashMap = new HashMapReverseEfficientOneArray(capacity); break;
      default:
    }
    return hashMap;
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

        long timePerAdjust = timeOneHashMap(hashmap, keys, values, (int) (.75 * capacity));

        System.out.format("%s\t%d\t%d\n", hashmap.getClass().getSimpleName(), capacity,
            timePerAdjust);
      }
    }
  }

  private static long timeOneHashMap(HashMap hashMap, long[] keys, long[] values, int sizeToShift) {
    final long startTime = System.nanoTime();
    int n = keys.length;
    assert (n == values.length);
    for (int i = 0; i < n; i++) {
      hashMap.adjust(keys[i], values[i]);
      if (hashMap.getNumActive() == sizeToShift) {
        hashMap.adjustAllValuesBy(-1);
        hashMap.keepOnlyLargerThan(0);
      }
    }
    final long endTime = System.nanoTime();
    return (endTime - startTime) / n;
  }

  private static long murmur(long key) {
    long[] keyArr = new long[1];
    keyArr[0] = key;
    return MurmurHash3.hash(keyArr, 0)[0];
  }
}
