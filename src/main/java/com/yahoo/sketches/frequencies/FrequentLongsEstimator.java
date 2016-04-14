/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

/**
 * Abstract base class for algorithms that estimate frequency of long items. 
 * All classes that extend this class support the ability to process a data stream of 
 * (<i>long</i> item, <i>long</i> count) pairs, 
 * where item is an identifier that must identify some long item uniquely and count 
 * is a non-negative integer. 
 * The frequency of an identifier is defined to be the sum of associated counts.
 * <p>Any FrequencyEstimator algorithm must be able to: 
 * <ol>
 * <li>Estimate the frequency of an identifier.</li> 
 * <li>Return upper and lower bounds on the estimate. Depending on the implementation, 
 * these bounds may hold deterministically, or with high probability.</li>
 * <li>Return an upper bound on the maximum error in any estimate, which also might
 * hold deterministically or with high probability, depending on the implementation.</li>
 * <li>Return an array of items whose frequencies might be above a certain threshold, specifically, 
 * the threshold is 1/errorTolerance + 1.</li>
 * <li>Merge itself with another FrequencyEstimator object created from this class.</li>
 * </ol>
 * 
 * @author Edo Liberty
 * @author Justin Thaler
 */
public abstract class FrequentLongsEstimator {
  public enum ErrorSpecification {NO_FALSE_POSITIVES, NO_FALSE_NEGATIVES}

  /**
   * Update this sketch with an item and a frequency count of one.
   * @param item for which the frequency should be increased. 
   */
  public abstract void update(long item);

  /**
   * Update this sketch with a item and a positive frequency count. 
   * @param item for which the frequency should be increased. The item can be any long value and is 
   * only used by the sketch to determine uniqueness.
   * @param count the amount by which the frequency of the item should be increased. 
   * An count of zero is a no-op, and a negative count will throw an exception.
   */
  public abstract void update(long item, long count);

  /**
   * This function merges two FrequencyEstimator sketches, potentially of different sizes.
   * 
   * @param other another FrequenciesEstimator of the same class
   * @return a pointer to a FrequencyEstimator whose estimates are within the guarantees of the
   * largest error tolerance of the two merged sketches. This method does not create a new
   * sketch. The sketch whose function is executed is changed and a reference to it is
   * returned.
   */
  public abstract FrequentLongsEstimator merge(FrequentLongsEstimator other);
  
  /**
   * Gets the estimate of the frequency of the given item. 
   * Note: The true frequency of a item would be the sum of the counts as a result of the two 
   * update functions.
   * 
   * @param item the given item
   * @return the estimate of the frequency of the given item
   */
  public abstract long getEstimate(long item);

  /**
   * Gets the guaranteed upper bound frequency of the given item.
   * 
   * @param item the given item
   * @return the guaranteed upper bound frequency of the given item. That is, a number which is 
   * guaranteed to be no smaller than the real frequency.
   */
  public abstract long getUpperBound(long item);
  
  /**
   * Gets the guaranteed lower bound frequency of the given item, which can never be negative.
   * 
   * @param item the given item.
   * @return the guaranteed lower bound frequency of the given item. That is, a number which is 
   * guaranteed to be no larger than the real frequency.
   */
  public abstract long getLowerBound(long item);

  /**
   * Returns an array of frequent items given a threshold frequency count and an ErrorCondition. 
   * Note: if the given threshold is less than getMaxError() the items that are returned have no
   * guarantees.
   * 
   * The method first examines all active items in the sketch (items that have a counter).
   *  
   * <p>If <i>errorSpec = NO_FALSE_NEGATIVES</i>, this will include a item in the result list 
   * if getUpperBound(item) > threshold. 
   * There will be no false negatives, i.e., no Type II error.
   * There may be items in the set with true frequencies less than the threshold (false positives).</p>
   * 
   * <p>If <i>errorSpec = NO_FALSE_POSITIVES</i>, this will include a item in the result list 
   * if getLowerBound(item) > threshold. 
   * There will be no false positives, i.e., no Type I error.
   * There may be items ommitted from the set with true frequencies greater than the threshold 
   * (false negatives).</p>
   * 
   * @param threshold the given frequency threshold that should be greater than getMaxError().
   * @param errorSpec determines whether no false positives or no false negatives are desired.
   * @return an array of frequent items
   */
  public abstract long[] getFrequentItems(long threshold, ErrorSpecification errorSpec);

  /**
   * Returns the current number of counters the sketch is configured to support.
   * 
   * @return the current number of counters the sketch is configured to support.
   */
  public abstract int getCurrentMapCapacity();
  
  /**
   * @return An upper bound on the maximum error of getEstimate(item) for any item. 
   * This is equivalent to the maximum distance between the upper bound and the lower bound for 
   * any item.
   */
  public abstract long getMaximumError();

  /**
   * Returns true if this sketch is empty
   * 
   * @return true if this sketch is empty
   */
  public abstract boolean isEmpty();

  /**
   * Returns the sum of the frequencies in the stream seen so far by the sketch
   * 
   * @return the sum of the frequencies in the stream seen so far by the sketch
   */
  public abstract long getStreamLength();
  
  /**
   * Returns the maximum number of counters the sketch is configured to support.
   * 
   * @return the maximum number of counters the sketch is configured to support.
   */
  public abstract int getMaximumMapCapacity();
  
  /**
   * @return the number of active items in the sketch.
   */
  public abstract int getActiveItems();
  
  /**
   * Returns the number of bytes required to store this sketch as an array of bytes.
   * 
   * @return the number of bytes required to store this sketch as an array of bytes.
   */
  public abstract int getStorageBytes();
  
  /**
   * Resets this sketch to a virgin state, but retains the original value of the error parameter
   */
  public abstract void reset();

}
