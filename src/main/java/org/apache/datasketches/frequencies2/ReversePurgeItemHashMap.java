/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.frequencies2;

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.exactLog2OfInt;
import static org.apache.datasketches.frequencies2.Util.hash;

import java.lang.reflect.Array;

import org.apache.datasketches.common.QuickSelect;

/**
 * Implements a linear-probing based hash map of (key, value) pairs and is distinguished by a
 * "reverse" purge operation that removes all keys in the map whose associated values are &le; 0
 * and is performed in reverse, starting at the "back" of the array and moving toward the front.
 *
 * @param <T> The type of item to be tracked by this sketch
 *
 * @author Edo Liberty
 * @author Justin Thaler
 * @author Alexander Saydakov
 */
class ReversePurgeItemHashMap<T> {
  private static final double LOAD_FACTOR = 0.75;
  private static final int DRIFT_LIMIT = 1024; //used only in stress testing
  private int lgLength;
  protected int loadThreshold;
  protected Object[] keys;
  protected long[] values;
  protected short[] states;
  protected int numActive = 0;

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
  ReversePurgeItemHashMap(final int mapSize) {
    lgLength = exactLog2OfInt(mapSize, "mapSize");
    loadThreshold = (int) (mapSize * LOAD_FACTOR);
    keys = new Object[mapSize];
    values = new long[mapSize];
    states = new short[mapSize];
  }

  /**
   * @param probe location in the hash table array
   * @return true if the cell in the array contains an active key
   */
  boolean isActive(final int probe) {
    return states[probe] > 0;
  }

  /**
   * Gets the current value with the given key
   * @param key the given key
   * @return the positive value the key corresponds to or zero if the key is not found in the
   * hash map.
   */
  long get(final T key) {
    if (key == null) { return 0; }
    final int probe = hashProbe(key);
    if (states[probe] > 0) {
      assert keys[probe].equals(key);
      return values[probe];
    }
    return 0;
  }

  /**
   * Increments the value mapped to the key if the key is present in the map. Otherwise,
   * the key is inserted with the adjustAmount.
   *
   * @param key the key of the value to increment
   * @param adjustAmount the amount by which to increment the value
   */
  void adjustOrPutValue(final T key, final long adjustAmount) {
    final int arrayMask = keys.length - 1;
    int probe = (int) hash(key.hashCode()) & arrayMask;
    int drift = 1;
    while ((states[probe] != 0) && !keys[probe].equals(key)) {
      probe = (probe + 1) & arrayMask;
      drift++;
      //only used for theoretical analysis
      assert drift < DRIFT_LIMIT : "drift: " + drift + " >= DRIFT_LIMIT";
    }

    if (states[probe] == 0) {
      // adding the key to the table the value
      assert numActive <= loadThreshold
        : "numActive: " + numActive + " > loadThreshold: " + loadThreshold;
      keys[probe] = key;
      values[probe] = adjustAmount;
      states[probe] = (short) drift;
      numActive++;
    } else {
      // adjusting the value of an existing key
      assert keys[probe].equals(key);
      values[probe] += adjustAmount;
    }
  }

  /**
   * Processes the map arrays and retains only keys with positive counts.
   */
  void keepOnlyPositiveCounts() {
    // Starting from the back, find the first empty cell,
    //  which establishes the high end of a cluster.
    int firstProbe = states.length - 1;
    while (states[firstProbe] > 0) {
      firstProbe--;
    }
    // firstProbe keeps track of this point.
    // When we find the next non-empty cell, we know we are at the high end of a cluster
    // Work towards the front; delete any non-positive entries.
    for (int probe = firstProbe; probe-- > 0;) {
      if ((states[probe] > 0) && (values[probe] <= 0)) {
        hashDelete(probe); //does the work of deletion and moving higher items towards the front.
        numActive--;
      }
    }
    //now work on the first cluster that was skipped.
    for (int probe = states.length; probe-- > firstProbe;) {
      if ((states[probe] > 0) && (values[probe] <= 0)) {
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
    for (int i = values.length; i-- > 0;) {
      values[i] += adjustAmount;
    }
  }

  /**
   * @return an array containing the active keys in the hash map.
   */
  @SuppressWarnings("unchecked")
  T[] getActiveKeys() {
    if (numActive == 0) { return null; }
    T[] returnedKeys = null;
    int j = 0;
    for (int i = 0; i < keys.length; i++) {
      if (isActive(i)) {
        if (returnedKeys == null) {
          returnedKeys = (T[]) Array.newInstance(keys[i].getClass(), numActive);
        }
        returnedKeys[j] = (T) keys[i];
        j++;
      }
    }
    assert j == numActive : "j: " + j + " != numActive: " + numActive;
    return returnedKeys;
  }

  /**
   * @return an array containing the values corresponding to the active keys in the hash
   */
  long[] getActiveValues() {
    if (numActive == 0) { return null; }
    final long[] returnedValues = new long[numActive];
    int j = 0;
    for (int i = 0; i < values.length; i++) {
      if (isActive(i)) {
        returnedValues[j] = values[i];
        j++;
      }
    }
    assert j == numActive;
    return returnedValues;
  }

  // assume newSize is power of 2
  @SuppressWarnings("unchecked")
  void resize(final int newSize) {
    final Object[] oldKeys = keys;
    final long[] oldValues = values;
    final short[] oldStates = states;
    keys = new Object[newSize];
    values = new long[newSize];
    states = new short[newSize];
    loadThreshold = (int) (newSize * LOAD_FACTOR);
    lgLength = Integer.numberOfTrailingZeros(newSize);
    numActive = 0;
    for (int i = 0; i < oldKeys.length; i++) {
      if (oldStates[i] > 0) {
        adjustOrPutValue((T) oldKeys[i], oldValues[i]);
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
    final String fmt  = "  %12d:%11d%12d %s";
    final String hfmt = "  %12s:%11s%12s %s";
    final StringBuilder sb = new StringBuilder();
    sb.append("ReversePurgeItemHashMap").append(LS);
    sb.append(String.format(hfmt, "Index","States","Values","Keys")).append(LS);

    for (int i = 0; i < keys.length; i++) {
      if (states[i] <= 0) { continue; }
      sb.append(String.format(fmt, i, states[i], values[i], keys[i].toString()));
      sb.append(LS);
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
      assert drift < DRIFT_LIMIT : "drift: " + drift + " >= DRIFT_LIMIT";
    }
  }

  private int hashProbe(final T key) {
    final int arrayMask = keys.length - 1;
    int probe = (int) hash(key.hashCode()) & arrayMask;
    while ((states[probe] > 0) && !keys[probe].equals(key)) {
      probe = (probe + 1) & arrayMask;
    }
    return probe;
  }

  Iterator<T> iterator() {
    return new Iterator<>(keys, values, states, numActive);
  }

  // This iterator uses strides based on golden ratio to avoid clustering during merge
  static class Iterator<T> {
    private static final double GOLDEN_RATIO_RECIPROCAL = (Math.sqrt(5) - 1) / 2;

    private final Object[] keys_;
    private final long[] values_;
    private final short[] states_;
    private final int numActive_;
    private final int stride_;
    private final int mask_;
    private int i_;
    private int count_;

    Iterator(final Object[] keys, final long[] values, final short[] states, final int numActive) {
      keys_ = keys;
      values_ = values;
      states_ = states;
      numActive_ = numActive;
      stride_ = (int) (keys.length * GOLDEN_RATIO_RECIPROCAL) | 1;
      mask_ = keys.length - 1;
      i_ = -stride_;
      count_ = 0;
    }

    boolean next() {
      i_ = (i_ + stride_) & mask_;
      while (count_ < numActive_) {
        if (states_[i_] > 0) {
          count_++;
          return true;
        }
        i_ = (i_ + stride_) & mask_;
      }
      return false;
    }

    @SuppressWarnings("unchecked")
    T getKey() {
      return (T) keys_[i_];
    }

    long getValue() {
      return values_[i_];
    }
  }

}
