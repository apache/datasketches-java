/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import java.util.Arrays;

import com.yahoo.sketches.Util;

/**
 * Implements a linear-probing based hash map with a "reverse" purge. 
 * The purge operation removes all keys in the map whose associated values are below a threshold
 * and is done in reverse, starting at the "back" of the array and moving toward the front.
 */
public class ReversePurgeHashMap extends LongLongHashMap {

  /**
   * Constructs a hash table
   * 
   * @param mapSize The size of this hash map that must be a power of 2.
   */
  public ReversePurgeHashMap(int mapSize) {
    super(mapSize);
    if (!Util.isPowerOf2(mapSize))
      throw new IllegalArgumentException(
          "Initial mapSize must be power of two: " + mapSize);
  }
  
  /**
   * Deserializes a String into an hash map object of this class.
   * 
   * @param string the given String representing a hash map object of this class.
   * @return a hash map object of this class.
   */
  public static ReversePurgeHashMap deserializeFromString(String string) {
    String[] tokens = string.split(",");
    if (tokens.length < 2) {
      throw new IllegalArgumentException(
          "String not long enough to specify length and capacity.");
    }

    int numActive = Integer.parseInt(tokens[0]);
    int length = Integer.parseInt(tokens[1]);
    ReversePurgeHashMap table = new ReversePurgeHashMap(length);
    int j = 2;
    for (int i = 0; i < numActive; i++) {
      long key = Long.parseLong(tokens[j++]);
      long value = Long.parseLong(tokens[j++]);
      table.adjustOrPutValue(key, value, value);
    }
    return table;
  }
  
  /**
   * Returns a String representation of this hash map.
   * 
   * @return a String representation of this hash map.
   */
  public String serializeToString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%d,%d,", numActive, length));

    for (int i = 0; i < keys.length; i++) {
      if (states[i] != 0) {
        sb.append(String.format("%d,%d,", keys[i], values[i]));
      }
    }
    return sb.toString();
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
      assert (drift < 512) : "drift: " + drift + " >= 512";
    }

    if (states[probe] == 0) {
      // adding the key to the table the value
      assert (numActive <= loadThreshold): "numActive: "+numActive+" > loadThreshold: "+loadThreshold;
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
  
  /**
   * This function is called when a key is processed that is not currently assigned a counter, and
   * all the counters are in use. This function estimates the median of the counters in the sketch
   * via sampling, decrements all counts by this estimate, throws out all counters that are no
   * longer positive, and increments offset accordingly.
   */
  long purge(int sampleSize) {
    int limit = Math.min(sampleSize, getNumActive());

    long[] myValues = getValues();
    int numSamples = 0;
    int i = 0;
    long[] samples = new long[limit];

    while (numSamples < limit) {
      if (isActive(i)) {
        samples[numSamples] = myValues[i];
        numSamples++;
      }
      i++;
    }

    Arrays.sort(samples, 0, numSamples);
    long val = samples[limit / 2];
    adjustAllValuesBy(-1 * val);
    keepOnlyPositiveCounts();
    return val;
  }
  
  @Override
  public void keepOnlyPositiveCounts() {
    // Starting from the back, find the first empty cell, 
    //  which establishes the high end of a cluster.
    int firstProbe = length - 1;
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
    for (int probe = length; probe-- > firstProbe;) {
      if (states[probe] > 0 && values[probe] <= 0) {
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
    states[deleteProbe] = 0; //mark as empty
    int drift = 1; 
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
      assert (drift < 512);
    }
  }

}
