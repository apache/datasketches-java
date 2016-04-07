/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

/**
 * Implements a linear-probing based hash table. Supports a purge operation that removes all keys in
 * the table whose associated values are below a threshold. This purge operation is done starting at
 * the ``back'' of the array and moving toward the front.
 */
public class HashMapReverseEfficient extends HashMap {

  /**
   * Constructs a hash table
   * 
   * @param mapSize
   */
  public HashMapReverseEfficient(int mapSize) {
    super(mapSize);
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
      assert (numActive <= loadThreshold);
      keys[probe] = key;
      values[probe] = putAmount;
      states[probe] = (short) drift;
      numActive++;
      assert (numActive <= .8 * length);
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
        numActive--;
      }
    }
    for (int probe = length; probe-- > firstProbe;) {
      if (states[probe] > 0 && values[probe] <= thresholdValue) {
        hashDelete(probe);
        numActive--;
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
    sb.append(String.format("%d,%d,", numActive, length));

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
          "Tried to make HashMapReverseEfficient out of string not long enough to specify length and capacity.");
    }

    int numActive = Integer.parseInt(tokens[0]);
    int length = Integer.parseInt(tokens[1]);
    HashMapReverseEfficient table = new HashMapReverseEfficient(length);
    int j = 2;
    for (int i = 0; i < numActive; i++) {
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

    int numActive = Integer.parseInt(tokens[ignore]);
    int length = Integer.parseInt(tokens[ignore + 1]);
    HashMapReverseEfficient table = new HashMapReverseEfficient(length);
    int j = 2 + ignore;
    for (int i = 0; i < numActive; i++) {
      long key = Long.parseLong(tokens[j++]);
      long value = Long.parseLong(tokens[j++]);
      table.adjustOrPutValue(key, value, value);
    }
    return table;
  }

}
