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

package org.apache.datasketches.tuple;

import static org.apache.datasketches.common.Util.LS;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.thetacommon.BinomialBoundsN;

/**
 * The top-level class for all Tuple sketches. This class is never constructed directly.
 * Use the UpdatableTupleSketchBuilder() methods to create UpdatableTupleSketches.
 * This is similar to {@link org.apache.datasketches.theta.ThetaSketch ThetaSketch} with
 * addition of a user-defined Summary object associated with every unique entry
 * in the sketch.
 * @param <S> Type of Summary
 */
@SuppressWarnings("deprecation")
public abstract class TupleSketch<S extends Summary> {

  protected static final byte PREAMBLE_LONGS = 1;

  long thetaLong_;
  boolean empty_ = true;
  protected SummaryFactory<S> summaryFactory_ = null;

  TupleSketch(final long thetaLong, final boolean empty, final SummaryFactory<S> summaryFactory) {
    this.thetaLong_ = thetaLong;
    this.empty_ = empty;
    this.summaryFactory_ = summaryFactory;
  }

  /**
   * Converts this TupleSketch to a CompactTupleSketch on the Java heap.
   *
   * <p>If this sketch is already in compact form this operation returns <i>this</i>.
   *
   * @return this sketch as a CompactTupleSketch on the Java heap.
   */
  public abstract CompactTupleSketch<S> compact();

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
    return BinomialBoundsN.getUpperBound(getRetainedEntries(), getTheta(), numStdDev, empty_);
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
    return BinomialBoundsN.getLowerBound(getRetainedEntries(), getTheta(), numStdDev, empty_);
  }

  /**
   * Gets the estimate of the true distinct population of subset tuples represented by the count
   * of entries in a subset of the total retained entries of the sketch.
   * @param numSubsetEntries number of entries for a chosen subset of the sketch.
   * @return the estimate of the true distinct population of subset tuples represented by the count
   * of entries in a subset of the total retained entries of the sketch.
   */
  public double getEstimate(final int numSubsetEntries) {
    if (!isEstimationMode()) { return numSubsetEntries; }
    return numSubsetEntries / getTheta();
  }

  /**
   * Gets the estimate of the lower bound of the true distinct population represented by the count
   * of entries in a subset of the total retained entries of the sketch.
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param numSubsetEntries number of entries for a chosen subset of the sketch.
   * @return the estimate of the lower bound of the true distinct population represented by the count
   * of entries in a subset of the total retained entries of the sketch.
   */
  public double getLowerBound(final int numStdDev, final int numSubsetEntries) {
    if (!isEstimationMode()) { return numSubsetEntries; }
    return BinomialBoundsN.getLowerBound(numSubsetEntries, getTheta(), numStdDev, isEmpty());
  }

  /**
   * Gets the estimate of the upper bound of the true distinct population represented by the count
   * of entries in a subset of the total retained entries of the sketch.
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param numSubsetEntries number of entries for a chosen subset of the sketch.
   * @return the estimate of the upper bound of the true distinct population represented by the count
   * of entries in a subset of the total retained entries of the sketch.
   */
  public double getUpperBound(final int numStdDev, final int numSubsetEntries) {
    if (!isEstimationMode()) { return numSubsetEntries; }
    return BinomialBoundsN.getUpperBound(numSubsetEntries, getTheta(), numStdDev, isEmpty());
  }

  /**
   * <a href="{@docRoot}/resources/dictionary.html#empty">See Empty</a>
   * @return true if empty.
   */
  public boolean isEmpty() {
    return empty_;
  }

  /**
   * Returns true if the sketch is Estimation Mode (as opposed to Exact Mode).
   * This is true if theta &lt; 1.0 AND isEmpty() is false.
   * @return true if the sketch is in estimation mode.
   */
  public boolean isEstimationMode() {
    return thetaLong_ < Long.MAX_VALUE && !isEmpty();
  }

  /**
   * Returns number of retained entries
   * @return number of retained entries
   */
  public abstract int getRetainedEntries();

  /**
   * Gets the number of hash values less than the given theta expressed as a long.
   * @param thetaLong the given theta as a long in the range (zero, <i>Long.MAX_VALUE</i>].
   * @return the number of hash values less than the given thetaLong.
   */
  public abstract int getCountLessThanThetaLong(final long thetaLong);

  /**
   * Gets the Summary Factory class of type S
   * @return the Summary Factory class of type S
   */
  public SummaryFactory<S> getSummaryFactory() {
    return summaryFactory_;
  }

  /**
   * Gets the value of theta as a double between zero and one
   * @return the value of theta as a double
   */
  public double getTheta() {
    return getThetaLong() / (double) Long.MAX_VALUE;
  }

  /**
   * Serialize this sketch to a byte array.
   *
   * <p>As of 3.0.0, serializing an UpdatableTupleSketch is deprecated.
   * This capability will be removed in a future release.
   * Serializing a CompactTupleSketch is not deprecated.</p>
   * @return serialized representation of this sketch.
   */
  public abstract byte[] toByteArray();

  /**
   * Returns a SketchIterator
   * @return a SketchIterator
   */
  public abstract TupleSketchIterator<S> iterator();

  /**
   * Returns Theta as a long
   * @return Theta as a long
   */
  public long getThetaLong() {
    return isEmpty() ? Long.MAX_VALUE : thetaLong_;
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
    if (this instanceof UpdatableTupleSketch) {
      @SuppressWarnings("rawtypes")
      final UpdatableTupleSketch updatable = (UpdatableTupleSketch) this;
      sb.append("   Nominal Entries (k)     : ").append(updatable.getNominalEntries()).append(LS);
      sb.append("   Current Capacity        : ").append(updatable.getCurrentCapacity()).append(LS);
      sb.append("   Resize Factor           : ").append(updatable.getResizeFactor().getValue()).append(LS);
      sb.append("   Sampling Probability (p): ").append(updatable.getSamplingProbability()).append(LS);
    }
    sb.append("### END SKETCH SUMMARY").append(LS);
    return sb.toString();
  }

  /**
   * Instantiate an UpdatableTupleSketch from a given MemorySegment on the heap,
   * @param <U> Type of update value
   * @param <S> Type of Summary
   * @param seg MemorySegment object representing an UpdatableTupleSketch
   * @param deserializer instance of SummaryDeserializer
   * @param summaryFactory instance of SummaryFactory
   * @return UpdatableTupleSketch created from its MemorySegment representation
   */
  public static <U, S extends UpdatableSummary<U>> UpdatableTupleSketch<U, S> heapifyUpdatableSketch(
      final MemorySegment seg,
      final SummaryDeserializer<S> deserializer,
      final SummaryFactory<S> summaryFactory) {
    return new UpdatableTupleSketch<>(seg, deserializer, summaryFactory);
  }

  /**
   * Instantiate a TupleSketch from a given MemorySegment.
   * @param <S> Type of Summary
   * @param seg MemorySegment object representing a TupleSketch
   * @param deserializer instance of SummaryDeserializer
   * @return TupleSketch created from its MemorySegment representation
   */
  public static <S extends Summary> TupleSketch<S> heapifySketch(
      final MemorySegment seg,
      final SummaryDeserializer<S> deserializer) {
    final SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(seg);
    if (sketchType == SerializerDeserializer.SketchType.QuickSelectSketch) {
      return new QuickSelectSketch<>(seg, deserializer, null);
    }
    return new CompactTupleSketch<>(seg, deserializer);
  }

  /**
   * Creates an empty CompactTupleSketch.
   * @param <S> Type of Summary
   * @return an empty instance of a CompactTupleSketch
   */
  public static <S extends Summary> TupleSketch<S> createEmptySketch() {
    return new CompactTupleSketch<>(null, null, Long.MAX_VALUE, true);
  }

}
