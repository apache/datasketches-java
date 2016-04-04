/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */


package com.yahoo.sketches.hashmaps;

/**
 * Implements a linear-probing based hash table. Supports a purge operation that removes all keys in
 * the table whose associated values are below a threshold. This purge operation is done starting at
 * the ``back'' of the array and moving toward the front.
 */
public class HashMapReverseEfficient extends HashMap {


  /**
   * Constructs a hash table
   * 
   * @param capacity
   */
  public HashMapReverseEfficient(int capacity) {
    super(capacity);
  }

  @Override
  public boolean isActive(int probe) {
    return (states[probe] > 0);
  }

  @Override
  public long get(long key) {
    int probe = hashProbe(key);
    if (states[probe] > 0) {
      assert (keys[probe] == key);
      return values[probe];
    }
    return 0;
  }

  @Override
  public void adjustOrPutValue(long key, long adjustAmount, long putAmount) {
    int probe = (int) hash(key) & arrayMask;
    int drift = 1;
    while (states[probe] != 0 && keys[probe] != key) {
      probe = (probe + 1) & arrayMask;
      drift++;
      assert (drift < 512);
    }

    if (states[probe] == 0) {
      // adding the key to the table the value
      assert (size <= capacity);
      keys[probe] = key;
      values[probe] = putAmount;
      states[probe] = (short) drift;
      size++;
      assert (size <= .8 * length);
    } else {
      // adjusting the value of an existing key
      assert (keys[probe] == key);
      values[probe] += adjustAmount;
    }
  }

  @Override
  public void keepOnlyLargerThan(long thresholdValue) {
    int firstProbe = length - 1;
    while (states[firstProbe] > 0)
      firstProbe--;

    for (int probe = firstProbe; probe-- > 0;) {
      if (states[probe] > 0 && values[probe] <= thresholdValue) {
        hashDelete(probe);
        size--;
      }
    }
    for (int probe = length; probe-- > firstProbe;) {
      if (states[probe] > 0 && values[probe] <= thresholdValue) {
        hashDelete(probe);
        size--;
      }
    }
  }

  private int hashProbe(long key) {
    int probe = (int) hash(key) & arrayMask;
    while (states[probe] > 0 && keys[probe] != key)
      probe = (probe + 1) & arrayMask;
    return probe;
  }

  private void hashDelete(int deleteProbe) {
    // Looks ahead in the table to search for another
    // item to move to this location
    // if none are found, the status is changed
    states[deleteProbe] = 0;
    int drift = 1;
    int probe = (deleteProbe + drift) & arrayMask;
    // advance until you find a free location replacing locations as needed
    while (states[probe] != 0) {
      if (states[probe] > drift) {
        // move current element
        keys[deleteProbe] = keys[probe];
        values[deleteProbe] = values[probe];
        states[deleteProbe] = (byte) (states[probe] - drift);
        // marking this location as deleted
        states[probe] = 0;
        drift = 0;
        deleteProbe = probe;
      }
      probe = (probe + 1) & arrayMask;
      drift++;
      assert (drift < 512);
    }
  }

  /**
   * Turns the HashMapReverseEfficient object into a string listing properties of the table and all
   * the (key, value) pairs that the table contains.
   * 
   * @return a string specifying the full contents of the hash map
   */
  public String hashMapReverseEfficientToString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%d,%d,", size, capacity));

    for (int i = 0; i < keys.length; i++) {
      if (states[i] != 0) {
        sb.append(String.format("%d,%d,", keys[i], values[i]));
      }
    }
    return sb.toString();
  }

  /**
   * Turns a string specifying a HashMapReverseEfficient object into a HashMapReverseEfficient
   * object.
   * 
   * @param string String specifying a HashMapReverseEfficient object
   * @return a HashMapReverseEfficient Object containing all (key, value) pairs in string
   */
  public static HashMapReverseEfficient StringToHashMapReverseEfficient(String string) {
    String[] tokens = string.split(",");
    if (tokens.length < 2) {
      throw new IllegalArgumentException(
          "Tried to make HashMapReviseEfficient out of string not long enough to specify length and capacity.");
    }

    int size = Integer.parseInt(tokens[0]);
    int capacity = Integer.parseInt(tokens[1]);
    HashMapReverseEfficient table = new HashMapReverseEfficient(capacity);
    int j = 2;
    for (int i = 0; i < size; i++) {
      long key = Long.parseLong(tokens[j++]);
      long value = Long.parseLong(tokens[j++]);
      table.adjustOrPutValue(key, value, value);
    }
    return table;
  }

  /**
   * Turns an array of strings specifying a HashMapReverseEfficient object into a
   * HashMapReverseEfficient object.
   * 
   * @param tokens Array of strings specifying a HashMapReverseEfficient object
   * @param ignore specifies how many of the initial strings in tokens to ignore
   * @return a HashMapReverseEfficient object corresponding to the array of strings
   */
  public static HashMapReverseEfficient StringArrayToHashMapReverseEfficient(String[] tokens,
      int ignore) {
    if (ignore < 0) {
      throw new IllegalArgumentException(
          "ignore parameter is negative in StringArrayToHashMapReverseEfficient.");
    }
    if (tokens.length < 2) {
      throw new IllegalArgumentException(
          "Tried to make HashMapReviseEfficient out of string not long enough to specify length and capacity.");
    }

    int size = Integer.parseInt(tokens[ignore]);
    int capacity = Integer.parseInt(tokens[ignore + 1]);
    HashMapReverseEfficient table = new HashMapReverseEfficient(capacity);
    int j = 2 + ignore;
    for (int i = 0; i < size; i++) {
      long key = Long.parseLong(tokens[j++]);
      long value = Long.parseLong(tokens[j++]);
      table.adjustOrPutValue(key, value, value);
    }
    return table;
  }


}
