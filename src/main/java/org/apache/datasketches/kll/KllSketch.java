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

package org.apache.datasketches.kll;

import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.N_LONG_ADR;
import static org.apache.datasketches.kll.KllSketch.Error.SRC_MUST_BE_DOUBLE;
import static org.apache.datasketches.kll.KllSketch.Error.SRC_MUST_BE_FLOAT;
import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_READ_ONLY;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;

import java.util.Random;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/*
 * Sampled stream data (floats or doubles) is stored as an array or as part of a Memory object.
 * This array is partitioned into sections called levels and the indices into the array of items
 * are tracked by a small integer array called levels or levels array.
 * The data for level i lies in positions levelsArray[i] through levelsArray[i + 1] - 1 inclusive.
 * Hence, the levelsArray must contain (numLevels + 1) indices.
 * The valid portion of items array is completely packed and sorted, except for level 0,
 * which is filled from the top down. Any items below the index levelsArray[0] is garbage and will be
 * overwritten by subsequent updates.
 *
 * Invariants:
 * 1) After a compaction, or an update, or a merge, every level is sorted except for level zero.
 * 2) After a compaction, (sum of capacities) - (sum of items) >= 1,
 *  so there is room for least 1 more item in level zero.
 * 3) There are no gaps except at the bottom, so if levels_[0] = 0,
 *  the sketch is exactly filled to capacity and must be compacted or the itemsArray and levelsArray
 *  must be expanded to include more levels.
 * 4) Sum of weights of all retained items == N.
 * 5) Current total item capacity = itemsArray.length = levelsArray[numLevels].
 */

/**
 * This class is the root of the KLL sketch class hierarchy. It includes the public API that is independent
 * of either sketch type (float or double) and independent of whether the sketch is targeted for use on the
 * heap or Direct (off-heap).
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
public abstract class KllSketch {

  /**
   * Used to define the variable type of the current instance of this class.
   */
  public enum SketchType { FLOATS_SKETCH, DOUBLES_SKETCH }

  enum Error {
    TGT_IS_READ_ONLY("Given sketch Memory is immutable, cannot write."),
    SRC_MUST_BE_DOUBLE("Given sketch must be of type Double."),
    SRC_MUST_BE_FLOAT("Given sketch must be of type Float."),
    MUST_NOT_CALL("This is an artifact of inheritance and should never be called."),
    SINGLE_ITEM_IMPROPER_CALL("Improper method use for single-item sketch"),
    MRS_MUST_NOT_BE_NULL("MemoryRequestServer cannot be null."),
    NOT_SINGLE_ITEM("Sketch is not single item."),
    MUST_NOT_BE_UPDATABLE_FORMAT("Given Memory object must not be in updatableFormat.");

    private String msg;

    private Error(final String msg) {
      this.msg = msg;
    }

    final static void kllSketchThrow(final Error errType) {
      throw new SketchesArgumentException(errType.getMessage());
    }

    private String getMessage() {
      return msg;
    }
  }

  /**
   * The default value of K
   */
  public static final int DEFAULT_K = 200;

  /**
   * The maximum value of K
   */
  public static final int MAX_K = (1 << 16) - 1; // serialized as an unsigned short

  /**
   * The default value of M. The parameter <i>m</i> is the minimum level size in number of items.
   * Currently, the public default is 8, but this can be overridden using Package Private methods to
   * 2, 4, 6 or 8, and the sketch works just fine.  The value 8 was chosen as a compromise between speed and size.
   * Choosing smaller values of <i>m</i> less than 8 will make the sketch slower.
   */
  static final int DEFAULT_M = 8;
  static final int MAX_M = 8; //The maximum value of M
  static final int MIN_M = 2; //The minimum value of M
  static final Random random = new Random();
  final SketchType sketchType;
  final boolean updatableMemFormat;
  final MemoryRequestServer memReqSvr;
  final boolean readOnly;
  int[] levelsArr;
  WritableMemory wmem;

  /**
   * Constructor for on-heap and off-heap.
   * If both wmem and memReqSvr are null, this is a heap constructor.
   * If wmem != null and wmem is not readOnly, then memReqSvr must not be null.
   * If wmem was derived from an original Memory instance via a cast, it will be readOnly.
   * @param sketchType either DOUBLE_SKETCH or FLOAT_SKETCH
   * @param wmem  the current WritableMemory or null
   * @param memReqSvr the given MemoryRequestServer or null
   */
  KllSketch(final SketchType sketchType, final WritableMemory wmem, final MemoryRequestServer memReqSvr) {
   this.sketchType = sketchType;
   this.wmem = wmem;
   if (wmem != null) {
     this.updatableMemFormat = KllPreambleUtil.getMemoryUpdatableFormatFlag(wmem);
     this.readOnly = wmem.isReadOnly() || !updatableMemFormat;
     if (readOnly) {
       this.memReqSvr = null;
     } else {
       if (memReqSvr == null) { kllSketchThrow(Error.MRS_MUST_NOT_BE_NULL); }
       this.memReqSvr = memReqSvr;
     }
   } else { //wmem is null, heap case
     this.updatableMemFormat = false;
     this.memReqSvr = null;
     this.readOnly = false;
   }
  }

  /**
   * Gets the approximate value of <em>k</em> to use given epsilon, the normalized rank error.
   * @param epsilon the normalized rank error between zero and one.
   * @param pmf if true, this function returns the value of <em>k</em> assuming the input epsilon
   * is the desired "double-sided" epsilon for the getPMF() function. Otherwise, this function
   * returns the value of <em>k</em> assuming the input epsilon is the desired "single-sided"
   * epsilon for all the other queries.
   *
   * <p>Please refer to the documentation in the package-info:<br>
   * {@link org.apache.datasketches.kll}</p>
   * @return the value of <i>k</i> given a value of epsilon.
   */
  public static int getKFromEpsilon(final double epsilon, final boolean pmf) {
    return KllHelper.getKFromEpsilon(epsilon, pmf);
  }

  /**
   * Returns upper bound on the compact serialized size of a FloatsSketch given a parameter
   * <em>k</em> and stream length. This method can be used if allocation of storage
   * is necessary beforehand.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @return upper bound on the compact serialized size
   * @deprecated Instead use getMaxSerializedSizeBytes(int, long, boolean)
   * from the descendants of this class, or
   * getMaxSerializedSizeBytes(int, long, SketchType, boolean) from this class.
   * Version 3.2.0
   */
  @Deprecated
  public static int getMaxSerializedSizeBytes(final int k, final long n) {
    final KllHelper.GrowthStats gStats =
        KllHelper.getGrowthSchemeForGivenN(k, DEFAULT_M, n, FLOATS_SKETCH, false);
    return gStats.compactBytes;
  }

  /**
   * Returns upper bound on the serialized size of a KllSketch given the following parameters.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @param sketchType either DOUBLES_SKETCH or FLOATS_SKETCH
   * @param updatableMemFormat true if updatable Memory format, otherwise the standard compact format.
   * @return upper bound on the serialized size of a KllSketch.
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n,
      final SketchType sketchType, final boolean updatableMemFormat) {
    final KllHelper.GrowthStats gStats =
        KllHelper.getGrowthSchemeForGivenN(k, DEFAULT_M, n, sketchType, false);
    return updatableMemFormat ? gStats.updatableBytes : gStats.compactBytes;
  }

  /**
   * Gets the normalized rank error given k and pmf.
   * Static method version of the <i>getNormalizedRankError(boolean)</i>.
   * @param k the configuration parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   */
  public static double getNormalizedRankError(final int k, final boolean pmf) {
    return KllHelper.getNormalizedRankError(k, pmf);
  }

  //numItems can be either numRetained, or current max capacity at given K and numLevels.
  static int getCurrentSerializedSizeBytes(final int numLevels, final int numItems,
      final SketchType sketchType, final boolean updatableMemFormat) {
    final int typeBytes = (sketchType == DOUBLES_SKETCH) ? Double.BYTES : Float.BYTES;
    int levelsBytes = 0;
    if (updatableMemFormat) {
      levelsBytes = (numLevels + 1) * Integer.BYTES;
    } else {
      if (numItems == 0) { return N_LONG_ADR; }
      if (numItems == 1) { return DATA_START_ADR_SINGLE_ITEM + typeBytes; }
      levelsBytes = numLevels * Integer.BYTES;
    }
    return DATA_START_ADR + levelsBytes + (numItems + 2) * typeBytes; //+2 is for min & max
  }

  /**
   * Returns the current number of bytes this sketch would require to store in the compact Memory Format.
   * @return the current number of bytes this sketch would require to store in the compact Memory Format.
   */
  public final int getCurrentCompactSerializedSizeBytes() {
    return getCurrentSerializedSizeBytes(getNumLevels(), getNumRetained(), sketchType, false);
  }

  /**
   * Returns the current number of bytes this sketch would require to store in the updatable Memory Format.
   * @return the current number of bytes this sketch would require to store in the updatable Memory Format.
   */
  public final int getCurrentUpdatableSerializedSizeBytes() {
    final int itemCap = KllHelper.computeTotalItemCapacity(getK(), getM(), getNumLevels());
    return getCurrentSerializedSizeBytes(getNumLevels(), itemCap, sketchType, true);
  }

  /**
   * Returns the user configured parameter k
   * @return the user configured parameter k
   */
  public abstract int getK();

  /**
   * Returns the length of the input stream in items.
   * @return stream length
   */
  public abstract long getN();

  /**
   * Gets the approximate rank error of this sketch normalized as a fraction between zero and one.
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * The epsilon value returned is a best fit to 99 percentile empirically measured max error in
   * thousands of trials
   * @return if pmf is true, returns the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   *
   * <p>Please refer to the documentation in the package-info:<br>
   * {@link org.apache.datasketches.kll}</p>
   */
  public final double getNormalizedRankError(final boolean pmf) {
    return getNormalizedRankError(getMinK(), pmf);
  }

  /**
   * Returns the number of retained items (samples) in the sketch.
   * @return the number of retained items (samples) in the sketch
   */
  public final int getNumRetained() {
    return levelsArr[getNumLevels()] - levelsArr[0];
  }

  /**
   * Returns the current number of bytes this Sketch would require if serialized.
   * @return the number of bytes this sketch would require if serialized.
   */
  public int getSerializedSizeBytes() {
    return (updatableMemFormat)
        ? getCurrentUpdatableSerializedSizeBytes()
        : getCurrentCompactSerializedSizeBytes();
  }

  /**
   * This returns the WritableMemory for Direct type sketches,
   * otherwise returns null.
   * @return the WritableMemory for Direct type sketches, otherwise null.
   */
  WritableMemory getWritableMemory() {
    return wmem;
  }

  /**
   * Returns true if this sketch's data structure is backed by Memory or WritableMemory.
   * @return true if this sketch's data structure is backed by Memory or WritableMemory.
   */
  public boolean hasMemory() {
    return (wmem != null);
  }

  /**
   * Returns true if the backing resource is direct, i.e., actually allocated in off-heap memory.
   * This is the case for off-heap memory and memory mapped files.
   * This backing resource could be either Memory(read-only) or WritableMemory.
   * However, if the backing Memory or WritabelMemory resource is allocated on-heap,
   * this will return false.
   * @return true if the backing resource is off-heap memory.
   */
  public boolean isDirect() {
    return wmem.isDirect();
  }

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  public final boolean isEmpty() {
    return getN() == 0;
  }

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  public final boolean isEstimationMode() {
    return getNumLevels() > 1;
  }

  /**
   * Returns true if the backing WritableMemory is in updatable format.
   * @return true if the backing WritableMemory is in updatable format.
   */
  public final boolean isMemoryUpdatableFormat() {
    return hasMemory() && updatableMemFormat;
  }

  /**
   * Returns true if this sketch is read only.
   * @return true if this sketch is read only.
   */
  public final boolean isReadOnly() {
    return readOnly;
  }

  /**
   * Returns true if the backing resource of <i>this</i> is identical with the backing resource
   * of <i>that</i>. The capacities must be the same.  If <i>this</i> is a region,
   * the region offset must also be the same.
   * @param that A different non-null object
   * @return true if the backing resource of <i>this</i> is the same as the backing resource
   * of <i>that</i>.
   */
  public final boolean isSameResource(final Memory that) {
    return (wmem != null) && wmem.isSameResource(that);
  }

  /**
   * Merges another sketch into this one.
   * Attempting to merge a KllDoublesSketch with a KllFloatsSketch will
   * throw an exception.
   * @param other sketch to merge into this one
   */
  public final void merge(final KllSketch other) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    if (sketchType == DOUBLES_SKETCH) {
      if (!other.isDoublesSketch()) { kllSketchThrow(SRC_MUST_BE_DOUBLE); }
      KllDoublesHelper.mergeDoubleImpl(this, other);
    } else {
      if (!other.isFloatsSketch()) { kllSketchThrow(SRC_MUST_BE_FLOAT); }
      KllFloatsHelper.mergeFloatImpl(this, other);
    }
  }

  /**
   * This resets the current sketch back to zero entries.
   * It retains key parameters such as <i>k</i> and
   * <i>SketchType (double or float)</i>.
   */
  public final void reset() {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelZeroSorted(false);
    setLevelsArray(new int[] {k, k});
    if (sketchType == DOUBLES_SKETCH) {
      setMinDoubleValue(Double.NaN);
      setMaxDoubleValue(Double.NaN);
      setDoubleItemsArray(new double[k]);
    } else {
      setMinFloatValue(Float.NaN);
      setMaxFloatValue(Float.NaN);
      setFloatItemsArray(new float[k]);
    }
  }

  /**
   * Returns serialized sketch in a compact byte array form.
   * @return serialized sketch in a compact byte array form.
   */
  public byte[] toByteArray() {
    return KllHelper.toCompactByteArrayImpl(this);
  }

  @Override
  public final String toString() {
    return toString(false, false);
  }

  /**
   * Returns a summary of the sketch as a string.
   * @param withLevels if true include information about levels
   * @param withData if true include sketch data
   * @return string representation of sketch summary
   */
  public String toString(final boolean withLevels, final boolean withData) {
    return KllHelper.toStringImpl(this, withLevels, withData);
  }

  /**
   * @return full size of internal items array including garbage.
   */
  abstract double[] getDoubleItemsArray();

  abstract double getDoubleSingleItem();

  /**
   * @return full size of internal items array including garbage.
   */
  abstract float[] getFloatItemsArray();

  abstract float getFloatSingleItem();

  final int[] getLevelsArray() {
    return levelsArr;
  }

  /**
   * Returns the configured parameter <i>m</i>, which is the minimum level size in number of items.
   * Currently, the public default is 8, but this can be overridden using Package Private methods to
   * 2, 4, 6 or 8, and the sketch works just fine.  The value 8 was chosen as a compromise between speed and size.
   * Choosing smaller values of <i>m</i> will make the sketch much slower.
   * @return the configured parameter m
   */
  abstract int getM();

  abstract double getMaxDoubleValue();

  abstract float getMaxFloatValue();

  abstract double getMinDoubleValue();

  abstract float getMinFloatValue();

  /**
   * MinK is the value of K that results from a merge with a sketch configured with a value of K lower than
   * the k of this sketch. This value is then used in computing the estimated upper and lower bounds of error.
   * @return The minimum K as a result of merging with lower values of k.
   */
  abstract int getMinK();

  final int getNumLevels() {
    return levelsArr.length - 1;
  }

  abstract void incN();

  abstract void incNumLevels();

  final boolean isCompactSingleItem() {
    return hasMemory() && !updatableMemFormat && (getN() == 1);
  }

  boolean isDoublesSketch() { return sketchType == DOUBLES_SKETCH; }

  boolean isFloatsSketch() { return sketchType == FLOATS_SKETCH; }

  abstract boolean isLevelZeroSorted();

  /**
   * First determine that this is a singleItem sketch before calling this.
   * @return the value of the single item
   */
  boolean isSingleItem() { return getN() == 1; }

  abstract void setDoubleItemsArray(double[] floatItems);

  abstract void setDoubleItemsArrayAt(int index, double value);

  abstract void setFloatItemsArray(float[] floatItems);

  abstract void setFloatItemsArrayAt(int index, float value);

  final void setLevelsArray(final int[] levelsArr) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    this.levelsArr = levelsArr;
    if (wmem != null) {
      wmem.putIntArray(DATA_START_ADR, this.levelsArr, 0, levelsArr.length);
    }
  }

  final void setLevelsArrayAt(final int index, final int value) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    this.levelsArr[index] = value;
    if (wmem != null) {
      final int offset = DATA_START_ADR + index * Integer.BYTES;
      wmem.putInt(offset, value);
    }
  }

  abstract void setLevelZeroSorted(boolean sorted);

  abstract void setMaxDoubleValue(double value);

  abstract void setMaxFloatValue(float value);

  abstract void setMinDoubleValue(double value);

  abstract void setMinFloatValue(float value);

  abstract void setMinK(int minK);

  abstract void setN(long n);

  abstract void setNumLevels(int numLevels);

}
