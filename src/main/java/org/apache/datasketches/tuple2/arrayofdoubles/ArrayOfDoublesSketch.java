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

package org.apache.datasketches.tuple2.arrayofdoubles;

import static org.apache.datasketches.common.Util.LS;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.thetacommon2.BinomialBoundsN;
import org.apache.datasketches.thetacommon2.ThetaUtil;
import org.apache.datasketches.tuple2.SerializerDeserializer;

/**
 * The base class for the tuple sketch of type ArrayOfDoubles, where an array of double values
 * is associated with each key.
 * A primitive array of doubles is used here, as opposed to a generic Summary object,
 * for improved performance.
 */
public abstract class ArrayOfDoublesSketch {

  // The concept of being empty is about representing an empty set.
  // So a sketch can be non-empty, and have no entries.
  // For example, as a result of a sampling, when some data was presented to the sketch, but no
  //  entries were retained.
  static enum Flags { IS_BIG_ENDIAN, IS_IN_SAMPLING_MODE, IS_EMPTY, HAS_ENTRIES }

  static final int SIZE_OF_KEY_BYTES = Long.BYTES;
  static final int SIZE_OF_VALUE_BYTES = Double.BYTES;

  // Common Layout of first 16 bytes and Empty AoDCompactSketch:
  // Long || Start Byte Adr:
  // Adr:
  //      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
  //  0   ||    Seed Hash    | #Dbls  |  Flags | SkType | FamID  | SerVer |  Preamble_Longs    |
  //      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
  //  1   ||-------------------------Theta Long------------------------------------------------|

  static final int PREAMBLE_LONGS_BYTE = 0; // not used, always 1
  static final int SERIAL_VERSION_BYTE = 1;
  static final int FAMILY_ID_BYTE = 2;
  static final int SKETCH_TYPE_BYTE = 3;
  static final int FLAGS_BYTE = 4;
  static final int NUM_VALUES_BYTE = 5;
  static final int SEED_HASH_SHORT = 6;
  static final int THETA_LONG = 8;

  final int numValues_;

  long thetaLong_;
  boolean isEmpty_ = true;

  ArrayOfDoublesSketch(final int numValues) {
    numValues_ = numValues;
  }

  /**
   * Heapify the given MemorySegment as an ArrayOfDoublesSketch
   * @param seg the given MemorySegment
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch heapify(final MemorySegment seg) {
    return heapify(seg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given MemorySegment and seed as a ArrayOfDoublesSketch
   * @param seg the given MemorySegment
   * @param seed the given seed
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch heapify(final MemorySegment seg, final long seed) {
    final SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(seg);
    if (sketchType == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      return new HeapArrayOfDoublesQuickSelectSketch(seg, seed);
    }
    return new HeapArrayOfDoublesCompactSketch(seg, seed);
  }

  /**
   * Wrap the given MemorySegment as an ArrayOfDoublesSketch.
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param seg the given MemorySegment
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch wrap(final MemorySegment seg) {
    return wrap(seg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given MemorySegment and seed as a ArrayOfDoublesSketch.
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param seg the given MemorySegment
   * @param seed the given seed
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch wrap(final MemorySegment seg, final long seed) {
    final SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(seg);
    if (sketchType == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      return new DirectArrayOfDoublesQuickSelectSketchR(seg, seed);
    }
    return new DirectArrayOfDoublesCompactSketch(seg, seed);
  }

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
   * Returns true if this sketch's data structure is backed by MemorySegment.
   * @return true if this sketch's data structure is backed by MemorySegment.
   */
  public abstract boolean hasMemorySegment();

  /**
   * Returns the MemorySegment object if it exists, otherwise null.
   * @return the MemorySegment object if it exists, otherwise null.
   */
  abstract MemorySegment getMemorySegment();

  /**
   * <a href="{@docRoot}/resources/dictionary.html#empty">See Empty</a>
   * @return true if empty.
   */
  public boolean isEmpty() {
    return isEmpty_;
  }

  /**
   * Returns number of double values associated with each key
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
    return ((thetaLong_ < Long.MAX_VALUE) && !isEmpty());
  }

  /**
   * Gets the value of theta as a double between zero and one
   * @return the value of theta as a double
   */
  public double getTheta() {
    return getThetaLong() / (double) Long.MAX_VALUE;
  }

  /**
   * Returns number of retained entries
   * @return number of retained entries
   */
  public abstract int getRetainedEntries();

  /**
   * Returns the maximum number of bytes for this sketch when serialized.
   * @return the maximum number of bytes for this sketch when serialized.
   */
  public abstract int getMaxBytes();

  /**
   * For compact sketches this is the same as getMaxBytes().
   * @return the current number of bytes for this sketch when serialized.
   */
  public abstract int getCurrentBytes();

  /**
   * Returns serialized representation of the sketch
   * @return serialized representation of the sketch
   */
  public abstract byte[] toByteArray();

  /**
   * Returns array of arrays of double values in the sketch
   * @return array of arrays of double values in the sketch
   */
  public abstract double[][] getValues();

  abstract double[] getValuesAsOneDimension();

  abstract long[] getKeys();

  /**
   * Returns the value of theta as a long
   * @return the value of theta as a long
   */
  long getThetaLong() {
    return isEmpty() ? Long.MAX_VALUE : thetaLong_;
  }

  abstract short getSeedHash();

  /**
   * Returns an iterator over the sketch
   * @return an iterator over the sketch
   */
  public abstract ArrayOfDoublesSketchIterator iterator();

  /**
   * Returns this sketch in compact form, which is immutable.
   * @return this sketch in compact form, which is immutable.
   */
  public ArrayOfDoublesCompactSketch compact() {
    return compact(null);
  }

  /**
   * Returns this sketch in compact form, which is immutable.
   * @param dstSeg the destination MemorySegment
   * @return this sketch in compact form, which is immutable.
   */
  public abstract ArrayOfDoublesCompactSketch compact(MemorySegment dstSeg);

  @Override
  public String toString() {
    final int seedHash = Short.toUnsignedInt(getSeedHash());
    final StringBuilder sb = new StringBuilder();
    sb.append("### ").append(this.getClass().getSimpleName()).append(" SUMMARY: ").append(LS);
    sb.append("   Estimate                : ").append(getEstimate()).append(LS);
    sb.append("   Upper Bound, 95% conf   : ").append(getUpperBound(2)).append(LS);
    sb.append("   Lower Bound, 95% conf   : ").append(getLowerBound(2)).append(LS);
    sb.append("   Theta (double)          : ").append(getTheta()).append(LS);
    sb.append("   Theta (long)            : ").append(getThetaLong()).append(LS);
    sb.append("   EstMode?                : ").append(isEstimationMode()).append(LS);
    sb.append("   Empty?                  : ").append(isEmpty()).append(LS);
    sb.append("   Retained Entries        : ").append(getRetainedEntries()).append(LS);
    if (this instanceof ArrayOfDoublesUpdatableSketch) {
      final ArrayOfDoublesUpdatableSketch updatable = (ArrayOfDoublesUpdatableSketch) this;
      sb.append("   Nominal Entries (k)     : ").append(updatable.getNominalEntries()).append(LS);
      sb.append("   Current Capacity        : ").append(updatable.getCurrentCapacity()).append(LS);
      sb.append("   Resize Factor           : ").append(updatable.getResizeFactor().getValue()).append(LS);
      sb.append("   Sampling Probability (p): ").append(updatable.getSamplingProbability()).append(LS);
    }
    sb.append("   Seed Hash               : ")
      .append(Integer.toHexString(seedHash)).append(" | ").append(seedHash).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);
    return sb.toString();
  }

}
