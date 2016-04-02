/*
 * Copyright 2015, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */
package com.yahoo.sketches.frequencies;

/**
 * @author Edo Liberty, Justin Thaler
 */

/**
 * Abstract class for a FrequencyEstimator algorithm. Supports the ability to process a data stream
 * of (item, increment) pairs, where item is an identifier, specified as a long, and increment is a
 * non-negative integer that is also specified as a long. The frequency of an identifier is defined
 * to be the sum of associated the increments. Any FrequencyEstimator algorithm must be able to: 1)
 * estimate the frequency of an identifier, 2) return upper and lower bounds on the frequency
 * (depending on the implementation, these bounds may hold deterministically, or with high
 * probability), 3) return an upper bound on the maximum error in any estimate (which also might
 * hold deterministically or with high probability, depending on implementation) 4) Return an array
 * of keys whose frequencies might be above a certain threshold (specifically, the threshold is
 * 1/errorTolerance + 1) 5) merge itself with another FrequencyEstimator algorithm from the same
 * instantiation of the abstract class.
 * 
 */
public abstract class FrequencyEstimator {

  /**
   * @param key for which the frequency should be increased. The frequency of a key is equal to the
   *        number of times the function increment(key) was called.
   */
  abstract public void update(long key);

  /**
   * @param key for which the frequency should be increased.
   * @param value by which the frequency of the key should be increased. The value must by
   *        non-negative.
   */
  abstract public void update(long key, long value);

  /**
   * @param key for which an estimate of the frequency is required. The exact frequency of a key is
   *        the number of times the function increment(key) was executed.
   * @return an estimate of the frequency
   */
  abstract public long getEstimate(long key);

  /**
   * @param key the key for which the frequency lower bound is needed.
   * @return a lower bound on the frequency. That is, a number which is guaranteed to be no larger
   *         than the real frequency.
   */
  abstract public long getEstimateLowerBound(long key);

  /**
   * @param key the key for which the frequency upper bound is needed.
   * @return an upper bound on the frequency. That is, a number which is guaranteed to be no smaller
   *         than the real frequency.
   */
  abstract public long getEstimateUpperBound(long key);

  /**
   * @return An upper bound on the maximum error of getEstimate(key) for any key. This upper bound
   *         may only hold for each key with probability failure_prob.
   */
  abstract public long getMaxError();


  /**
   * @param threshold This function is guaranteed to return an array that contains a superset of all
   *        keys with frequency above the threshold.
   * @return an array of keys containing a superset of all keys whose frequencies are are least the
   *         error tolerance.
   */
  abstract public long[] getFrequentKeys(long threshold);

  /**
   * This function merges two FrequencyEstimator sketches, potentially of different sizes.
   * 
   * @param other another FrequenciesEstimator of the same class
   * @return a pointer to a FrequencyEstimator whose estimates are within the guarantees of the
   *         largest error tolerance of the two merged sketches. This method does not create a new
   *         sketch. The sketch whose function is executed is changed and a reference to it is
   *         returned.
   */
  abstract public FrequencyEstimator merge(FrequencyEstimator other);

  /**
   * Returns the current number of counters the sketch is configured to support.
   * 
   * @return the current number of counters the sketch is configured to support.
   */
  abstract public int getK();

  /**
   * Returns the maximum number of counters the sketch will ever be configured to support.
   * 
   * @return the maximum number of counters the sketch will ever be configured to support.
   */
  abstract public int getMaxK();

  /**
   * Returns true if this sketch is empty
   * 
   * @return true if this sketch is empty
   */
  abstract public boolean isEmpty();

  /**
   * Returns the sum of the frequencies in the stream seen so far by the sketch
   * 
   * @return the sum of the frequencies in the stream seen so far by the sketch
   */
  abstract public long getStreamLength();

  /**
   * Resets this sketch to a virgin state, but retains the original value of the error parameter
   */
  public abstract void reset();

}
