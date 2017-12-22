/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.LS;

import com.yahoo.sketches.BinomialBoundsN;

/**
 * This is an equivalent to com.yahoo.sketches.theta.Sketch with
 * addition of a user-defined Summary object associated with every unique entry
 * in the sketch.
 * @param <S> Type of Summary
 */
public abstract class Sketch<S extends Summary> {

  protected static final byte PREAMBLE_LONGS = 1;

  long[] keys_;
  S[] summaries_;
  long theta_;
  boolean isEmpty_ = true;

  Sketch() {}

  /**
   * Estimates the cardinality of the set (number of unique values presented to the sketch)
   * @return best estimate of the number of unique values
   */
  public double getEstimate() {
    if (!isEstimationMode()) { return getRetainedEntries(); }
    return getRetainedEntries() / getTheta();
  }

  /**
   * Gets the approximate upper error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the upper bound.
   */
  public double getUpperBound(final int numStdDev) {
    if (!isEstimationMode()) { return getRetainedEntries(); }
    return BinomialBoundsN.getUpperBound(getRetainedEntries(), getTheta(), numStdDev, isEmpty_);
  }

  /**
   * Gets the approximate lower error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the lower bound.
   */
  public double getLowerBound(final int numStdDev) {
    if (!isEstimationMode()) { return getRetainedEntries(); }
    return BinomialBoundsN.getLowerBound(getRetainedEntries(), getTheta(), numStdDev, isEmpty_);
  }

  /**
   * <a href="{@docRoot}/resources/dictionary.html#empty">See Empty</a>
   * @return true if empty.
   */
  public boolean isEmpty() {
    return isEmpty_;
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
   * @return number of retained entries
   */
  public abstract int getRetainedEntries();

  /**
   * Gets the value of theta as a double between zero and one
   * @return the value of theta as a double
   */
  public double getTheta() {
    return theta_ / (double) Long.MAX_VALUE;
  }

  /**
   * This is to serialize an instance to a byte array.
   * @return serialized representation of the sketch
   */
  public abstract byte[] toByteArray();

  /**
   * Returns a SketchIterator
   * @return a SketchIterator
   */
  public SketchIterator<S> iterator() {
    return new SketchIterator<S>(keys_, summaries_);
  }

  long getThetaLong() {
    return theta_;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("### ").append(this.getClass().getSimpleName()).append(" SUMMARY: ").append(LS);
    sb.append("   Estimate                : ").append(getEstimate()).append(LS);
    sb.append("   Upper Bound, 95% conf   : ").append(getUpperBound(2)).append(LS);
    sb.append("   Lower Bound, 95% conf   : ").append(getLowerBound(2)).append(LS);
    sb.append("   Theta (double)          : ").append(this.getTheta()).append(LS);
    sb.append("   Theta (long)            : ").append(this.getThetaLong()).append(LS);
    sb.append("   EstMode?                : ").append(isEstimationMode()).append(LS);
    sb.append("   Empty?                  : ").append(isEmpty()).append(LS);
    sb.append("   Retained Entries        : ").append(this.getRetainedEntries()).append(LS);
    if (this instanceof UpdatableSketch) {
      @SuppressWarnings("rawtypes")
      final UpdatableSketch updatable = (UpdatableSketch) this;
      sb.append("   Nominal Entries (k)     : ").append(updatable.getNominalEntries()).append(LS);
      sb.append("   Current Capacity        : ").append(updatable.getCurrentCapacity()).append(LS);
      sb.append("   Resize Factor           : ").append(updatable.getResizeFactor().getValue()).append(LS);
      sb.append("   Sampling Probability (p): ").append(updatable.getSamplingProbability()).append(LS);
    }
    sb.append("### END SKETCH SUMMARY").append(LS);
    return sb.toString();
  }

}
