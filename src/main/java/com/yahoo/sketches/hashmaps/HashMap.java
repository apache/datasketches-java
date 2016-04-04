/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.hashmaps;

/**
 * @author Edo Liberty
 * @author Justin Thaler
 */

/**
 * Abstract class for a hashmap data structure, which stores (key, value) pairs, and supports the
 * following non-standard operations: decrement all values by a given amount, and purge all (key,
 * value) pairs whose key is below a specified threshold.
 * 
 */
public abstract class HashMap {

  // The load factor is decided upon by the abstract class.
  // This cannot be modified by inheriting classes!
  final public double LOAD_FACTOR = 0.75;

  protected int capacity;
  protected int length;
  protected int arrayMask;
  protected int size = 0;
  protected long[] keys;
  protected long[] values;
  protected short[] states;

  public HashMap() {}

  /**
   * @param capacity Determines the number of (key, value) pairs the hashmap is expected to store.
   *        Constructor will create arrays of size capacity/LOAD_FACTOR, rounded up to a power of 2.
   *        The size of the hash table is set to be a power of two for fast hashing. The protected
   *        variable this.capacity is then set to the largest value that will not overload the hash
   *        table.
   */
  public HashMap(int capacity) {
    if (capacity <= 0)
      throw new IllegalArgumentException(
          "Received negative or zero value for as initial capacity.");
    length = Integer.highestOneBit(2 * (int) (capacity / LOAD_FACTOR) - 1);
    this.capacity = (int) (length * LOAD_FACTOR);
    arrayMask = length - 1;
    keys = new long[length];
    values = new long[length];
    states = new short[length];
  }

  /**
   * Increments the primitive value mapped to the key if the key is present in the map. Otherwise,
   * the key is inserted with the putAmount.
   * 
   * @param key the key of the value to increment
   * @param adjustAmount the amount by which to increment the value
   * @param putAmount the value put into the map if the key is not initial present
   */
  abstract public void adjustOrPutValue(long key, long adjustAmount, long putAmount);


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
   * @param key the key to look for
   * @return the positive value the key corresponds to or zero if if the key is not found in the
   *         hash map.
   */
  abstract public long get(long key);

  /**
   * @param adjustAmount value by which to shift all values. Only keys corresponding to positive
   *        values are retained.
   */
  public void adjustAllValuesBy(long adjustAmount) {
    for (int i = length; i-- > 0;)
      values[i] += adjustAmount;
  }

  /**
   * @param thresholdValue value by which to shift all values. Only keys corresponding to positive
   *        values are retained.
   */
  abstract public void keepOnlyLargerThan(long thresholdValue);

  /**
   * @param probe location in the hash table array
   * @return true if the cell in the array contains an active key
   */
  abstract public boolean isActive(int probe);

  /**
   * @return an array containing the active keys in the hash map.
   */
  public long[] getKeys() {
    if (size == 0)
      return null;
    long[] returnedKeys = new long[size];
    int j = 0;
    for (int i = 0; i < length; i++)
      if (isActive(i)) {
        returnedKeys[j] = keys[i];
        j++;
      }
    assert (j == size);
    return returnedKeys;
  }

  /**
   * @return an array containing the values corresponding. to the active keys in the hash
   */
  public long[] getValues() {
    if (size == 0)
      return null;
    long[] returnedValues = new long[size];
    int j = 0;
    for (int i = 0; i < length; i++)
      if (isActive(i)) {
        returnedValues[j] = values[i];
        j++;
      }
    assert (j == size);
    return returnedValues;
  }

  /**
   * @return the raw array of keys. Do NOT modify this array!
   */
  public long[] ProtectedGetKey() {
    return keys;
  }

  /**
   * @return the raw array of values. Do NOT modify this array!
   */
  public long[] ProtectedGetValues() {
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
    return capacity;
  }

  /**
   * @return number of populated keys
   */
  public int getSize() {
    return size;
  }

  /**
   * prints the hash table
   */
  public void print() {
    for (int i = 0; i < keys.length; i++) {
      System.out.format("%3d: (%4d,%4d,%3d)\n", i, states[i], keys[i], values[i]);
    }
    System.out.format("=====================\n");
  }


  /**
   * @return the load factor of the hash table, i.e, the ratio between the capacity and the array
   *         length
   */
  protected double getLoadFactor() {
    return LOAD_FACTOR;
  }

  /**
   * @param key to be hashed
   * @return an index into the hash table This hash function is taken from the internals of the
   *         Trove open source library.
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
