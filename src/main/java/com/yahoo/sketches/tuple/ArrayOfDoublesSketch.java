/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

/**
 * This is a base class for a specialized version of a tuple sketch, where an array of double values is associated with each key.
 * A primitive array is used here, as opposed to a generic Summary object, for performance reasons.
 */
public abstract class ArrayOfDoublesSketch {

  // The concept of being empty is about representing an empty set.
  // So a sketch can be non-empty, and have no entries.
  // For example, as a result of a sampling, when some data was presented to the sketch, but no entries were retained.
  static enum Flags { IS_BIG_ENDIAN, IS_IN_SAMPLING_MODE, IS_EMPTY, HAS_ENTRIES }
  static final int SIZE_OF_KEY_BYTES = 8;
  static final int SIZE_OF_VALUE_BYTES = 8;
  
  // common part of serialized layout
  static final int PREAMBLE_LONGS_BYTE = 0; // not used, always 1
  static final int SERIAL_VERSION_BYTE = 1;
  static final int FAMILY_ID_BYTE = 2;
  static final int SKETCH_TYPE_BYTE = 3;
  static final int FLAGS_BYTE = 4;
  static final int NUM_VALUES_BYTE = 5;
  static final int SEED_HASH_SHORT = 6;
  static final int THETA_LONG = 8;

  protected final int numValues_;

  protected long theta_;
  protected boolean isEmpty_ = true;

  protected ArrayOfDoublesSketch(int numValues) {
    numValues_ = numValues;
  }

  /**
   * Estimates the cardinality of the set (number of unique values presented to the sketch)
   * @return best estimate of the number of unique values
   */
  public double getEstimate() {
    if (!isEstimationMode()) return getRetainedEntries();
    return getRetainedEntries() / getTheta();
  }

  /**
   * Gets the approximate upper error bound given the specified number of Standard Deviations. 
   * This will return getEstimate() if isEmpty() is true.
   * 
   * @param numStdDev <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the upper bound.
   */
  public double getUpperBound(int numStdDev) {
    if (!isEstimationMode()) return getRetainedEntries();
    return Util.upperBound(getEstimate(), getTheta(), numStdDev);
  }

  /**
   * Gets the approximate lower error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   * 
   * @param numStdDev <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the lower bound.
   */
  public double getLowerBound(int numStdDev) {
    if (!isEstimationMode()) return getRetainedEntries();
    return Util.lowerBound(getEstimate(), getTheta(), numStdDev);
  }

  /**
   * <a href="{@docRoot}/resources/dictionary.html#empty">See Empty</a>
   * @return true if empty.
   */
  public boolean isEmpty() {
    return isEmpty_;
  }

  /**
   * @return number of double values associated with each key
   */
  public int getNumValues() {
    return numValues_;
  }

  /**
   * Returns true if the sketch is Estimation Mode (as opposed to Exact Mode).
   * This is true if theta &lt; 1.0 AND isEmpty() is false.
   * @return true if the sketch is in estimation mode.
   */
  public boolean isEstimationMode() {
    return ((theta_ < Long.MAX_VALUE) && !isEmpty());
  }

  /**
   * Gets the value of theta as a double between zero and one
   * @return the value of theta as a double
   */
  public double getTheta() {
    return theta_ / (double) Long.MAX_VALUE;
  }

  /**
   * @return number of retained entries
   */
  public abstract int getRetainedEntries();

  /**
   * @return serialized representation of the sketch
   */
  public abstract byte[] toByteArray();

  /**
   * @return array of arrays of double values in the sketch
   */
  public abstract double[][] getValues();

  /**
   * @return the value of theta as a long
   */
  long getThetaLong() {
    return theta_;
  }

  abstract short getSeedHash();

  /**
   * @return iterator over the sketch
   */
  abstract ArrayOfDoublesSketchIterator iterator();

}
