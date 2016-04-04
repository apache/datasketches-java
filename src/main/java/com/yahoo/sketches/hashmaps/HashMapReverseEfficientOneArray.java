/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.hashmaps;

public class HashMapReverseEfficientOneArray extends HashMap {

  private final int KEY_OFFSET = 0;
  private final int VALUE_OFFSET = 1;
  private final int STATE_OFFSET = 2;
  private final int KVS_SIZE = 3;

  long[] kvsArray;
  private int kvsLength;

  public HashMapReverseEfficientOneArray(int capacity) {
    if (capacity <= 0)
      throw new IllegalArgumentException(
          "Received negative or zero value for as initial capacity.");
    this.capacity = capacity;
    // arraysLength is the smallest power of 2 greater than capacity/LOAD_FACTOR
    length = Integer.highestOneBit(2 * (int) (capacity / getLoadFactor()) - 1);
    arrayMask = length - 1;
    // super(capacity);
    kvsLength = 3 * getLength();
    this.kvsArray = new long[kvsLength];
  }

  @Override
  public boolean isActive(int probe) {
    return (kvsArray[probe * KVS_SIZE + STATE_OFFSET] > 0);
  }

  @Override
  public long[] getKeys() {
    if (size == 0)
      return null;
    long[] retrunedKeys = new long[size];
    int j = 0;
    for (int i = 0; i < length; i++)
      if (isActive(i)) {
        retrunedKeys[j] = kvsArray[i * KVS_SIZE + KEY_OFFSET];
        j++;
      }
    assert (j == size);
    return retrunedKeys;
  }

  @Override
  public long[] getValues() {
    if (size == 0)
      return null;
    long[] retrunedValues = new long[size];
    int j = 0;
    for (int i = 0; i < length; i++)
      if (isActive(i)) {
        retrunedValues[j] = kvsArray[i * KVS_SIZE + VALUE_OFFSET];
        j++;
      }
    assert (j == size);
    return retrunedValues;
  }


  @Override
  public long get(long key) {
    int probe = (int) hash(key) & arrayMask;
    int kvsProbe = probe * KVS_SIZE;
    while (kvsArray[kvsProbe + STATE_OFFSET] != 0 && kvsArray[kvsProbe + KEY_OFFSET] != key) {
      probe = (probe + 1) & arrayMask;
      kvsProbe = probe * KVS_SIZE;
    }
    return (kvsArray[kvsProbe + STATE_OFFSET] != 0) ? kvsArray[kvsProbe + VALUE_OFFSET] : 0;
  }

  @Override
  public void adjustAllValuesBy(long adjustAmount) {
    for (int kvsProbe = VALUE_OFFSET; kvsProbe < kvsLength; kvsProbe += KVS_SIZE)
      kvsArray[kvsProbe] += adjustAmount;
  }

  @Override
  public void adjustOrPutValue(long key, long adjustAmount, long putAmount) {
    int probe = (int) hash(key) & arrayMask;
    byte drift = 1;
    while (kvsArray[probe * KVS_SIZE + STATE_OFFSET] != 0
        && kvsArray[probe * KVS_SIZE + KEY_OFFSET] != key) {
      probe = (probe + 1) & arrayMask;
      drift++;
    }
    int kvsProbe = probe * KVS_SIZE;
    if (kvsArray[probe * KVS_SIZE + STATE_OFFSET] == 0) {
      // adding the key to the table the value
      assert (size < capacity);
      // kvsProbe = probe*KVS_SIZE;
      kvsArray[kvsProbe + KEY_OFFSET] = key;
      kvsArray[kvsProbe + VALUE_OFFSET] = putAmount;
      kvsArray[kvsProbe + STATE_OFFSET] = drift;
      size++;
    } else {
      // adjusting the value of an existing key
      assert (kvsArray[kvsProbe + KEY_OFFSET] == key);
      kvsArray[kvsProbe + VALUE_OFFSET] += adjustAmount;
    }
  }


  @Override
  public void keepOnlyLargerThan(long thresholdValue) {
    int firstProbe = length - 1;
    while (kvsArray[firstProbe * KVS_SIZE + STATE_OFFSET] > 0)
      firstProbe--;

    for (int probe = firstProbe; probe-- > 0;) {
      int kvsProbe = probe * KVS_SIZE;
      if (kvsArray[kvsProbe + STATE_OFFSET] > 0
          && kvsArray[kvsProbe + VALUE_OFFSET] <= thresholdValue) {
        hashDelete(probe);
        size--;
      }
    }
    for (int probe = length; probe-- > firstProbe;) {
      int kvsProbe = probe * KVS_SIZE;
      if (kvsArray[kvsProbe + STATE_OFFSET] > 0
          && kvsArray[kvsProbe + VALUE_OFFSET] <= thresholdValue) {
        hashDelete(probe);
        size--;
      }
    }
  }


  @Override
  public void print() {
    for (int probe = 0; probe < keys.length; probe++) {
      System.out.format("%3d: (%4d,%4d,%3d)\n", probe, kvsArray[probe * KVS_SIZE + STATE_OFFSET],
          kvsArray[probe * KVS_SIZE + KEY_OFFSET], kvsArray[probe * KVS_SIZE + VALUE_OFFSET]);
    }
    System.out.format("=====================\n");
  }

  private void hashDelete(int deleteProbe) {
    // Looks ahead in the table to search for another
    // item to move to this location
    // if none are found, the status is changed
    int kvsDeleteProbe = deleteProbe * KVS_SIZE;
    kvsArray[kvsDeleteProbe + STATE_OFFSET] = 0;
    byte drift = 1;
    int probe = (deleteProbe + drift) & arrayMask;
    // advance until you find a free location replacing locations as needed
    int kvsProbe = probe * KVS_SIZE;
    while (kvsArray[kvsProbe + STATE_OFFSET] != 0) {
      if (kvsArray[kvsProbe + STATE_OFFSET] > drift) {
        // move current element
        kvsArray[kvsDeleteProbe + KEY_OFFSET] = kvsArray[kvsProbe + KEY_OFFSET];
        kvsArray[kvsDeleteProbe + VALUE_OFFSET] = kvsArray[kvsProbe + VALUE_OFFSET];
        kvsArray[kvsDeleteProbe + STATE_OFFSET] =
            (byte) (kvsArray[kvsProbe + STATE_OFFSET] - drift);
        // marking this location as deleted
        kvsArray[kvsProbe + STATE_OFFSET] = 0;
        drift = 0;
        deleteProbe = probe;
      }
      probe = (probe + 1) & arrayMask;
      kvsProbe = probe * KVS_SIZE;
      kvsDeleteProbe = deleteProbe * KVS_SIZE;
      drift++;
    }
  }

}
