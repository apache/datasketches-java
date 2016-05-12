/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.frequencies.Util.hash;
import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.toLog2;

import com.yahoo.sketches.QuickSelect;

/**
 * Implements a linear-probing based hash map of (key, value) pairs and is distinguished by a 
 * "reverse" purge operation that removes all keys in the map whose associated values are &le; 0 
 * and is performed in reverse, starting at the "back" of the array and moving toward the front.
 * 
 * @author Edo Liberty
 * @author Justin Thaler
 */
class ReversePurgeLongHashMap {
  private static final double LOAD_FACTOR = 0.75;
  private static final int DRIFT_LIMIT = 1024; //used only in stress testing
  private int lgLength;
  private int loadThreshold;
  private long[] keys;
  private long[] values;
  private short[] states;
  private int numActive = 0;

  /**
   * Constructor will create arrays of length mapSize, which must be a power of two.
   * This restriction was made to ensure fast hashing.
   * The protected variable this.loadThreshold is then set to the largest value that 
   * will not overload the hash table.
   * 
   * @param mapSize This determines the number of cells in the arrays underlying the 
   * HashMap implementation and must be a power of 2. 
   * The hash table will be expected to store LOAD_FACTOR * mapSize (key, value) pairs.
   */
  ReversePurgeLongHashMap(final int mapSize) {
    lgLength = toLog2(mapSize, "mapSize");
    this.loadThreshold = (int) (mapSize * LOAD_FACTOR);
    this.keys = new long[mapSize];
    this.values = new long[mapSize];
    this.states = new short[mapSize];
  }

  /**
   * Returns an instance of this class from the given String,
   * which must be a String representation of this class.
   * 
   * @param string a String representation of this class.
   * @return an instance of this class.
   */
  static ReversePurgeLongHashMap getInstance(final String string) {
    String[] tokens = string.split(",");
    if (tokens.length < 2) {
      throw new IllegalArgumentException(
          "String not long enough to specify length and capacity.");
    }
    final int numActive = Integer.parseInt(tokens[0]);
    final int length = Integer.parseInt(tokens[1]);
    final ReversePurgeLongHashMap table = new ReversePurgeLongHashMap(length);
    int j = 2;
    for (int i = 0; i < numActive; i++) {
      final long key = Long.parseLong(tokens[j++]);
      final long value = Long.parseLong(tokens[j++]);
      table.adjustOrPutValue(key, value);
    }
    return table;
  }
  
  //Serialization
  
  /**
   * Returns a String representation of this hash map.
   * 
   * @return a String representation of this hash map.
   */
  String serializeToString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(String.format("%d,%d,", numActive, keys.length));

    for (int i = 0; i < keys.length; i++) {
      if (states[i] != 0) {
        sb.append(String.format("%d,%d,", keys[i], values[i]));
      }
    }
    return sb.toString();
  }
  
  /**
   * @param probe location in the hash table array
   * @return true if the cell in the array contains an active key
   */
  boolean isActive(final int probe) {
    return (states[probe] > 0);
  }

  /**
   * Gets the current value with the given key
   * @param key the given key
   * @return the positive value the key corresponds to or zero if if the key is not found in the
   * hash map.
   */
  long get(final long key) {
    final int probe = hashProbe(key);
    if (states[probe] > 0) {
      assert (keys[probe] == key);
      return values[probe];
    }
    return 0;
  }
  
  /**
   * Increments the value mapped to the key if the key is present in the map. Otherwise,
   * the key is inserted with the putAmount.
   * 
   * @param key the key of the value to increment
   * @param adjustAmount the amount by which to increment the value
   */
  void adjustOrPutValue(final long key, final long adjustAmount) {
    final int arrayMask = keys.length - 1;
    int probe = (int) hash(key) & arrayMask;
    int drift = 1;
    while (states[probe] != 0 && keys[probe] != key) {
      probe = (probe + 1) & arrayMask;
      drift++;
      //only used for theoretical analysis
      assert (drift < DRIFT_LIMIT) : "drift: " + drift + " >= DRIFT_LIMIT";
    }

    if (states[probe] == 0) {
      // adding the key to the table the value
      assert (numActive <= loadThreshold): "numActive: "+numActive+" > loadThreshold: "+loadThreshold;
      keys[probe] = key;
      values[probe] = adjustAmount;
      states[probe] = (short) drift;
      numActive++;
    } else {
      // adjusting the value of an existing key
      assert (keys[probe] == key);
      values[probe] += adjustAmount;
    }
  }
  
  /**
   * Processes the map arrays and retains only keys with positive counts.
   */
  void keepOnlyPositiveCounts() {
    // Starting from the back, find the first empty cell, 
    //  which establishes the high end of a cluster.
    int firstProbe = keys.length - 1;
    while (states[firstProbe] > 0) { 
      firstProbe--;
    }
    // firstProbe keeps track of this point.
    // When we find the next non-empty cell, we know we are at the high end of a cluster
    // Work towards the front; delete any non-positive entries.
    for (int probe = firstProbe; probe-- > 0;) {
      if (states[probe] > 0 && values[probe] <= 0) {
        hashDelete(probe); //does the work of deletion and moving higher items towards the front.
        numActive--;
      }
    }
    //now work on the first cluster that was skipped.
    for (int probe = keys.length; probe-- > firstProbe;) {
      if (states[probe] > 0 && values[probe] <= 0) {
        hashDelete(probe);
        numActive--;
      }
    }
  }

  /**
   * @param adjustAmount value by which to shift all values. Only keys corresponding to positive
   * values are retained.
   */
  void adjustAllValuesBy(final long adjustAmount) {
    for (int i = keys.length; i-- > 0;)
      values[i] += adjustAmount;
  }

  /**
   * @return an array containing the active keys in the hash map.
   */
  long[] getActiveKeys() {
    if (numActive == 0) return null;
    final long[] returnedKeys = new long[numActive];
    int j = 0;
    for (int i = 0; i < keys.length; i++)
      if (isActive(i)) {
        returnedKeys[j] = keys[i];
        j++;
      }
    assert (j == numActive) : "j: " + j + " != numActive: " + numActive;
    return returnedKeys;
  }

  /**
   * @return an array containing the values corresponding. to the active keys in the hash
   */
  long[] getActiveValues() {
    if (numActive == 0) return null;
    final long[] returnedValues = new long[numActive];
    int j = 0;
    for (int i = 0; i < values.length; i++) {
      if (isActive(i)) {
        returnedValues[j] = values[i];
        j++;
      }
    }
    assert (j == numActive);
    return returnedValues;
  }

  // assume newSize is power of 2
  void resize(final int newSize) {
    final long[] oldKeys = keys;
    final long[] oldValues = values;
    final short[] oldStates = states;
    keys = new long[newSize];
    values = new long[newSize];
    states = new short[newSize];
    loadThreshold = (int) (newSize * LOAD_FACTOR);
    lgLength = Integer.numberOfTrailingZeros(newSize);
    numActive = 0;
    for (int i = 0; i < oldKeys.length; i++) {
      if (oldStates[i] > 0) {
        adjustOrPutValue(oldKeys[i], oldValues[i]);
      }
    }
  }

  /**
   * @return length of hash table internal arrays
   */
  int getLength() {
    return keys.length;
  }

  int getLgLength() {
    return lgLength;
  }

  /**
   * @return capacity of hash table internal arrays (i.e., max number of keys that can be stored)
   */
  int getCapacity() {
    return loadThreshold;
  }

  /**
   * @return number of populated keys
   */
  int getNumActive() {
    return numActive;
  }

  /**
   * Returns the hash table as a human readable string.
   */
  @Override
  public String toString() {
    String fmt  = "  %12d:%11d%12d %d";
    String hfmt = "  %12s:%11s%12s %s";
    final StringBuilder sb = new StringBuilder();
    sb.append("ReversePurgeLongHashMap:").append(LS);
    sb.append(String.format(hfmt, "Index","States","Values","Keys")).append(LS);

    for (int i = 0; i < keys.length; i++) {
      if (states[i] <= 0) continue;
      sb.append(String.format(fmt, i, states[i], values[i], keys[i])).append(LS);
    }
    return sb.toString();
  }

  /**
   * @return the load factor of the hash table, i.e, the ratio between the capacity and the array
   * length
   */
  static double getLoadFactor() {
    return LOAD_FACTOR;
  }

  /**
   * This function is called when a key is processed that is not currently assigned a counter, and
   * all the counters are in use. This function estimates the median of the counters in the sketch
   * via sampling, decrements all counts by this estimate, throws out all counters that are no
   * longer positive, and increments offset accordingly.
   * @param sampleSize number of samples
   * @return the median value
   */
  long purge(final int sampleSize) {
    final int limit = Math.min(sampleSize, getNumActive());

    int numSamples = 0;
    int i = 0;
    final long[] samples = new long[limit];

    while (numSamples < limit) {
      if (isActive(i)) {
        samples[numSamples] = values[i];
        numSamples++;
      }
      i++;
    }

    final long val = QuickSelect.select(samples, 0, numSamples - 1, limit / 2);
    adjustAllValuesBy(-1 * val);
    keepOnlyPositiveCounts();
    return val;
  }

  private void hashDelete(int deleteProbe) {
    // Looks ahead in the table to search for another
    // item to move to this location
    // if none are found, the status is changed
    states[deleteProbe] = 0; //mark as empty
    int drift = 1;
    final int arrayMask = keys.length - 1;
    int probe = (deleteProbe + drift) & arrayMask; //map length must be a power of 2
    // advance until you find a free location replacing locations as needed
    while (states[probe] != 0) {
      if (states[probe] > drift) {
        // move current element
        keys[deleteProbe] = keys[probe];
        values[deleteProbe] = values[probe];
        states[deleteProbe] = (short) (states[probe] - drift);
        // marking this location as deleted
        states[probe] = 0;
        drift = 0;
        deleteProbe = probe;
      }
      probe = (probe + 1) & arrayMask;
      drift++;
      //only used for theoretical analysis
      assert (drift < DRIFT_LIMIT) : "drift: " + drift + " >= DRIFT_LIMIT";
    }
  }
  
  private int hashProbe(final long key) {
    final int arrayMask = keys.length - 1;
    int probe = (int) hash(key) & arrayMask;
    while (states[probe] > 0 && keys[probe] != key)
      probe = (probe + 1) & arrayMask;
    return probe;
  }

  Iterator iterator() {
    return new Iterator(keys, values, states);
  }

  class Iterator {
    private final long[] iKeys;
    private final long[] iValues;
    private final short[] iStates;
    private int i;

    Iterator(final long[] keys, final long[] values, final short[] states) {
      iKeys = keys;
      iValues = values;
      iStates = states;
      i = -1;
    }

    boolean next() {
      i++;
      while (i < iKeys.length) {
        if (iStates[i] > 0) return true;
        i++;
      }
      return false;
    }

    long getKey() {
      return iKeys[i];
    }

    long getValue() {
      return iValues[i];
    }
  }
}
