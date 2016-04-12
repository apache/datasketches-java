/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.Util.*;

/**
 * Abstract class for a hashmap data structure, which stores (key, value) pairs, and supports the
 * following non-standard operations: decrement all values by a given amount, and purge all (key,
 * value) pairs whose key is below a specified threshold.
 * 
 * @author Edo Liberty
 * @author Justin Thaler
 */
public abstract class LongLongHashMap {
  public final static double LOAD_FACTOR = 0.75;
  protected final int loadThreshold;
  protected final int length;
  protected final int arrayMask;
  protected int numActive = 0;
  protected long[] keys;
  protected long[] values;
  protected short[] states;

  /**
   * Constructor will create arrays of length mapSize, which must be a power of two.
   * This restriction was made to ensure fast hashing.
   * The protected variable this.loadThreshold is then set to the largest value that 
   * will not overload the hash table.
   * @param mapSize determines the number of cells in the arrays underlying the HashMap implementation.
   * Must be a power of 2. The hash table will be expected to store LOAD_FACT * mapSize (key, value) pairs.
   * 
   */
  public LongLongHashMap(int mapSize) {
    if (mapSize <= 0)
      throw new IllegalArgumentException(
          "Initial mapSize cannot be negative or zero: " + mapSize); //TODO fix here??
    this.length = mapSize;
    this.loadThreshold = (int) (length * LOAD_FACTOR);
    this.arrayMask = length - 1;
    this.keys = new long[length];
    this.values = new long[length];
    this.states = new short[length];
  }

  /**
   * Increments the value mapped to the key if the key is present in the map. Otherwise,
   * the key is inserted with the putAmount.
   * 
   * @param key the key of the value to increment
   * @param adjustAmount the amount by which to increment the value
   * @param putAmount the value put into the map if the key is not present
   */
  public abstract void adjustOrPutValue(long key, long adjustAmount, long putAmount);

  /**
   * Increments the primitive value mapped to the key if the key is present in the map. Otherwise,
   * the key is inserted with the value.
   * 
   * @param key the key of the value to increment
   * @param value the value increment by, or to put into the map if the key is not initial present
   */
  public void adjust(long key, long value) {
    adjustOrPutValue(key, value, value);
  }

  /**
   * Gets the current value with the given key
   * @param key the given key
   * @return the positive value the key corresponds to or zero if if the key is not found in the
   * hash map.
   */
  public abstract long get(long key);

  /**
   * @param adjustAmount value by which to shift all values. Only keys corresponding to positive
   * values are retained.
   */
  public void adjustAllValuesBy(long adjustAmount) {
    for (int i = length; i-- > 0;)
      values[i] += adjustAmount;
  }

  /**
   * Processes the map arrays and retains only keys with positive counts.
   */
  public abstract void keepOnlyPositiveCounts();

  /**
   * @param probe location in the hash table array
   * @return true if the cell in the array contains an active key
   */
  public abstract boolean isActive(int probe);

  /**
   * @return an array containing the active keys in the hash map.
   */
  public long[] getActiveKeys() {
    if (numActive == 0)
      return null;
    long[] returnedKeys = new long[numActive];
    int j = 0;
    for (int i = 0; i < length; i++)
      if (isActive(i)) {
        returnedKeys[j] = keys[i];
        j++;
      }
    assert (j == numActive) : "j: "+j+" != numActive: "+numActive;
    return returnedKeys;
  }

  /**
   * @return an array containing the values corresponding. to the active keys in the hash
   */
  public long[] getActiveValues() {
    if (numActive == 0)
      return null;
    long[] returnedValues = new long[numActive];
    int j = 0;
    for (int i = 0; i < length; i++)
      if (isActive(i)) {
        returnedValues[j] = values[i];
        j++;
      }
    assert (j == numActive);
    return returnedValues;
  }

  /**
   * @return the raw array of keys. Do NOT modify this array!
   */
  long[] getKeys() {
    return keys;
  }

  /**
   * @return the raw array of values. Do NOT modify this array!
   */
  long[] getValues() {
    return values;
  }

  /**
   * @return length of hash table internal arrays
   */
  public int getLength() {
    return length;
  }

  /**
   * @return capacity of hash table internal arrays (i.e., max number of keys that can be stored)
   */
  public int getCapacity() {
    return loadThreshold;
  }

  /**
   * @return number of populated keys
   */
  public int getNumActive() {
    return numActive;
  }

  /**
   * Returns the hash table as a human readable string.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("HashMap").append(LS);
    sb.append("Index: States,       Keys,     Values").append(LS);
    for (int i = 0; i < keys.length; i++) {
      sb.append(String.format("%5d: %6d, %10d, %10d\n", i, states[i], keys[i], values[i]));
    }
    sb.append(LS);
    return sb.toString();
  }

  /**
   * @return the load factor of the hash table, i.e, the ratio between the capacity and the array
   * length
   */
  protected double getLoadFactor() {
    return LOAD_FACTOR;
  }

  /**
   * @param key to be hashed
   * @return an index into the hash table This hash function is taken from the internals of the
   * Trove open source library.
   */
  protected long hash(long key) {
    key ^= key >>> 33;
    key *= 0xff51afd7ed558ccdL;
    key ^= key >>> 33;
    key *= 0xc4ceb9fe1a85ec53L;
    key ^= key >>> 33;
    return key;
  }

}
