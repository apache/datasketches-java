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

import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.N_LONG_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_FLOATS_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_ITEMS_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_LONGS_SKETCH;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.quantilescommon.QuantilesAPI;

/*
 * Sampled stream data (floats, doubles, or items) is stored as a heap array called itemsArr or as part of a
 * MemorySegment object.
 * This array is partitioned into sections called levels and the indices into the array of items
 * are tracked by a small integer array called levelsArr.
 * The data for level i lies in positions levelsArr[i] through levelsArr[i + 1] - 1 inclusive.
 * Hence, the levelsArr must contain (numLevels + 1) elements.
 * The valid portion of the itemsArr is completely packed and sorted, except for level 0,
 * which is filled from the top down. Any items below the index levelsArr[0] is free space and will be
 * overwritten by subsequent updates.
 *
 * Invariants:
 * 1) After a compaction, update, or a merge, every level is sorted except for level zero.
 * 2) After a compaction, (sum of level capacities) - (number of valid items) >= 1,
 *  so there is room for least 1 more quantile in level zero.
 * 3) There are no gaps except at the bottom, so if levelsArr[0] = 0,
 *  the sketch is exactly filled to capacity and must be compacted or the itemsArr and levelsArr
 *  must be expanded to include more levels.
 * 4) Sum of weights of all retained, valid items = N.
 * 5) Current total item capacity = itemsArr.length = levelsArr[numLevels].
 */

/**
 * This class is the root of the KLL sketch class hierarchy. It includes the public API that is independent
 * of either sketch type (e.g., float, double, long or generic item) and independent of whether the sketch is targeted
 * for use on the Java heap or off-heap.
 *
 * <p>KLL is an implementation of a very compact quantiles sketch with lazy compaction scheme
 * and nearly optimal accuracy per retained quantile.</p>
 *
 * <p>Reference <a href="https://arxiv.org/abs/1603.05346v2">Optimal Quantile Approximation in Streams</a>.</p>
 *
 * <p>The default <i>k</i> of 200 yields a "single-sided" epsilon of about 1.33% and a
 * "double-sided" (PMF) epsilon of about 1.65%, with a confidence of 99%.</p>
 *
 * @see <a href="https://datasketches.apache.org/docs/KLL/KLLSketch.html">KLL Sketch</a>
 * @see QuantilesAPI
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
public abstract class KllSketch implements QuantilesAPI, MemorySegmentStatus {

  /**
   * The default K
   */
  public static final int DEFAULT_K = 200;

  /**
   * The maximum K
   */
  public static final int MAX_K = (1 << 16) - 1; // serialized as an unsigned short

  /**
   * The default M. The parameter <i>m</i> is the minimum level size in number of quantiles.
   * Currently, the public default is 8, but this can be overridden using Package Private methods to
   * 2, 4, 6 or 8, and the sketch works just fine.  The number 8 was chosen as a compromise between speed and size.
   * Choosing an <i>m</i> smaller than 8 will make the sketch slower.
   */
  static final int DEFAULT_M = 8;
  static final int MAX_M = 8; //The maximum M
  static final int MIN_M = 2; //The minimum M
  static final Random random = new Random();

  final SketchType sketchType;
  final SketchStructure sketchStructure;
  boolean readOnly;
  int[] levelsArr; //Always updatable form

  /**
   * Constructor for on-heap.
   * @param sketchType either DOUBLES_SKETCH, FLOATS_SKETCH or ITEMS_SKETCH
   * @param sketchStructure the current sketch structure
   */
  KllSketch(
      final SketchType sketchType,
      final SketchStructure sketchStructure) {
   this.sketchType = sketchType;
   this.sketchStructure = sketchStructure;
  }

  /**
   * Constructor for MemorySegment based sketch
   * @param segVal MemorySegment validator.
   */
  KllSketch(final KllMemorySegmentValidate segVal) {
    sketchType = segVal.sketchType;
    sketchStructure = segVal.sketchStructure;
    levelsArr = segVal.levelsArr; //always converted to writable form.
    readOnly = segVal.srcSeg.isReadOnly() || (segVal.sketchStructure != UPDATABLE);
  }

  /**
   * Gets the string value of the item at the given index.
   * @param index the index of the value
   * @return the string value of the item at the given index.
   */
  abstract String getItemAsString(int index);

  /**
   * Gets the approximate <em>k</em> to use given epsilon, the normalized rank error.
   * @param epsilon the normalized rank error between zero and one.
   * @param pmf if true, this function returns the <em>k</em> assuming the input epsilon
   * is the desired "double-sided" epsilon for the getPMF() function. Otherwise, this function
   * returns <em>k</em> assuming the input epsilon is the desired "single-sided"
   * epsilon for all the other queries.
   * @return <i>k</i> given epsilon.
   */
  public static int getKFromEpsilon(final double epsilon, final boolean pmf) {
    return KllHelper.getKFromEpsilon(epsilon, pmf);
  }

  /**
   * Returns upper bound on the serialized size of a KllSketch given the following parameters.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @param sketchType Only DOUBLES_SKETCH, LONGS_SKETCH and FLOATS_SKETCH are supported for this operation.
   * @param updatableFormat true if updatable MemorySegment format, otherwise the standard compact format.
   * @return upper bound on the serialized size of a KllSketch.
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n,
      final SketchType sketchType, final boolean updatableFormat) {
    if (sketchType == KLL_ITEMS_SKETCH) { throw new SketchesArgumentException(UNSUPPORTED_MSG); }
    final KllHelper.GrowthStats gStats =
        KllHelper.getGrowthSchemeForGivenN(k, DEFAULT_M, n, sketchType, false);
    return updatableFormat ? gStats.updatableBytes : gStats.compactBytes;
  }

  /**
   * Gets the string value of the max item
   * @return the string value of the max item
   */
  abstract String getMaxItemAsString();

  /**
   * Gets the string value of the min item
   * @return the string value of the min item
   */
  abstract String getMinItemAsString();

  /**
   * Gets the normalized rank error given k and pmf.
   * Static method version of the <i>getNormalizedRankError(boolean)</i>.
   * The epsilon returned is a best fit to 99 percent confidence empirically measured max error
   * in thousands of trials.
   * @param k the configuration parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   */
  public static double getNormalizedRankError(final int k, final boolean pmf) {
    return KllHelper.getNormalizedRankError(k, pmf);
  }

  @Override
  public final double getNormalizedRankError(final boolean pmf) {
    return getNormalizedRankError(getMinK(), pmf);
  }

  @Override
  public final int getNumRetained() {
    return levelsArr[getNumLevels()] - levelsArr[0];
  }

  /**
   * Returns the current number of bytes this Sketch would require if serialized in compact form.
   * @return the number of bytes this sketch would require if serialized.
   */
  public int getSerializedSizeBytes() {
    //current policy is that public method cannot return Updatable structure:
    return currentSerializedSizeBytes(false);
  }

  @Override
  public abstract boolean hasMemorySegment();

  /**
   * Returns true if this sketch is in a Compact MemorySegment Format.
   * @return true if this sketch is in a Compact MemorySegment Format.
   */
  public boolean isCompactMemorySegmentFormat() {
    return hasMemorySegment() && (sketchStructure != UPDATABLE);
  }

  @Override
  public abstract boolean isOffHeap();

  @Override
  public final boolean isEmpty() {
    return getN() == 0;
  }

  @Override
  public final boolean isEstimationMode() {
    return getNumLevels() > 1;
  }

  /**
   * Returns true if the backing MemorySegment is in updatable format.
   * @return true if the backing MemorySegment is in updatable format.
   */
  public final boolean isMemorySegmentUpdatableFormat() {
    return hasMemorySegment() && (sketchStructure == UPDATABLE);
  }

  @Override
  public final boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public abstract boolean isSameResource(MemorySegment that);

  /**
   * Merges another sketch into this one.
   * Attempting to merge a sketch of the wrong type will throw an exception.
   * @param other sketch to merge into this one
   */
  public final void merge(KllSketch other) {
    merge(other, random);
  }

  /**
   * Merges another sketch into this one.
   * Attempting to merge a sketch of the wrong type will throw an exception.
   *
   * <p><b>Warning:</b> providing a custom number generator might break the
   * error bounds of the KllSketch.</p>
   *
   * @param other sketch to merge into this one
   * @param random random number generator to be used
   */
  public abstract void merge(KllSketch other, Random random);

  @Override
  public final String toString() {
    return toString(false, false);
  }

  /**
   * Returns human readable summary information about this sketch.
   * Used for debugging.
   * @param withLevels if true includes sketch levels array summary information
   * @param withLevelsAndItems if true include detail of levels array and items array together
   * @return human readable summary information about this sketch.
   */
  public abstract String toString(final boolean withLevels, final boolean withLevelsAndItems);

  //restricted

  /**
   * Compute serialized size in bytes independent of the current sketch.
   * For KllItemsSketch the result is always in non-updatable, compact form.
   * @param updatable true if the desired result is for updatable structure.
   * @return serialized size in bytes given a SketchStructure.
   */
  final int currentSerializedSizeBytes(final boolean updatable) {
    final boolean myUpdatable = sketchType == KLL_ITEMS_SKETCH ? false : updatable;
    final long srcN = getN();
    final SketchStructure tgtStructure;
    if (myUpdatable) { tgtStructure = UPDATABLE; }
    else if (srcN == 0) { tgtStructure = COMPACT_EMPTY; }
    else if (srcN == 1) { tgtStructure = COMPACT_SINGLE; }
    else { tgtStructure = COMPACT_FULL; }
    final int totalBytes;
    if (tgtStructure == COMPACT_EMPTY) {
      totalBytes = N_LONG_ADR;
    }
    else if (tgtStructure == COMPACT_SINGLE) {
      totalBytes = DATA_START_ADR_SINGLE_ITEM
          + getSingleItemSizeBytes();
    }
    else if (tgtStructure == COMPACT_FULL) {
      totalBytes = DATA_START_ADR
          + getLevelsArrSizeBytes(tgtStructure)
          + getMinMaxSizeBytes()
          + getRetainedItemsSizeBytes();
    }
    else { //structure = UPDATABLE
      totalBytes = DATA_START_ADR
          + getLevelsArrSizeBytes(tgtStructure)
          + getMinMaxSizeBytes()
          + getTotalItemsNumBytes();
    }
    return totalBytes;
  }

  int[] getLevelsArray(final SketchStructure structure) {
    if (structure == UPDATABLE) { return levelsArr.clone(); }
    else if (structure == COMPACT_FULL) { return Arrays.copyOf(levelsArr, levelsArr.length - 1); }
    else { return new int[0]; }
  }

  final int getLevelsArrSizeBytes(final SketchStructure structure) {
    if (structure == UPDATABLE) { return levelsArr.length * Integer.BYTES; }
    else if (structure == COMPACT_FULL) { return (levelsArr.length - 1) * Integer.BYTES; }
    else { return 0; }
  }

  /**
   * Returns the configured parameter <i>m</i>, which is the minimum level size in number of items.
   * Currently, the public default is 8, but this can be overridden using Package Private methods to
   * 2, 4, 6 or 8, and the sketch works just fine.  The number 8 was chosen as a compromise between speed and size.
   * Choosing smaller <i>m</i> will make the sketch much slower.
   * @return the configured parameter m
   */
  abstract int getM();

  /**
   * Gets the MemorySegmentRequest object or null.
   * @return the MemorySegmentRequest or null.
   */
  abstract MemorySegmentRequest getMemorySegmentRequest();

  /**
   * MinK is the K that results from a merge with a sketch configured with a K lower than
   * the K of this sketch. This is then used in computing the estimated upper and lower bounds of error.
   * @return The minimum K as a result of merging sketches with lower k.
   */
  abstract int getMinK();

  /**
   * Gets the combined minItem and maxItem in a serialized byte array.
   * @return the combined minItem and maxItem in a serialized byte array.
   */
  abstract byte[] getMinMaxByteArr();

  /**
   * Gets the size in bytes of the combined minItem and maxItem serialized byte array.
   * @return the size in bytes of the combined minItem and maxItem serialized byte array.
   */
  abstract int getMinMaxSizeBytes();

  /**
   * Gets the current number of levels
   * @return the current number of levels
   */
  final int getNumLevels() {
    if ((sketchStructure == UPDATABLE) || (sketchStructure == COMPACT_FULL)) { return levelsArr.length - 1; }
    return 1;
  }

  /**
   * Gets the serialized byte array of the valid retained items as a byte array.
   * It does not include the preamble, the levels array, minimum or maximum items, or free space.
   * @return the serialized bytes of the retained data.
   */
  abstract byte[] getRetainedItemsByteArr();

  /**
   * Gets the size in bytes of the valid retained items.
   * It does not include the preamble, the levels array, minimum or maximum items, or free space.
   * @return the size of the retained data in bytes.
   */
  abstract int getRetainedItemsSizeBytes();

  /**
   * Gets the serializer / deserializer or null.
   * @return the serializer / deserializer or null.
   */
  abstract ArrayOfItemsSerDe<?> getSerDe();

  /**
   * Gets the serialized byte array of the Single Item that corresponds to the Single Item Flag being true.
   * @return the serialized byte array of the Single Item.
   */
  abstract byte[] getSingleItemByteArr();

  /**
   * Gets the size in bytes of the serialized Single Item that corresponds to the Single Item Flag being true.
   * @return the size in bytes of the serialized Single Item.
   */
  abstract int getSingleItemSizeBytes();

  /**
   * Gets the serialized byte array of the entire internal items hypothetical structure.
   * It does not include the preamble, the levels array, or minimum or maximum items.
   * It may include empty or free space.
   * @return the serialized bytes of the retained data.
   */
  abstract byte[] getTotalItemsByteArr();

  /**
   * Gets the size in bytes of the entire internal items hypothetical structure.
   * It does not include the preamble, the levels array, or minimum or maximum items.
   * It may include empty or free space.
   * @return the size of the retained data in bytes.
   */
  abstract int getTotalItemsNumBytes();

  /**
   * This returns the MemorySegment for Direct type sketches,
   * otherwise returns null.
   * @return the MemorySegment for Direct type sketches, otherwise null.
   */
  abstract MemorySegment getMemorySegment();

  abstract void incN(int increment);

  abstract void incNumLevels();

  final boolean isCompactSingleItem() {
    return hasMemorySegment() && (sketchStructure == COMPACT_SINGLE) && (getN() == 1);
  }

  boolean isKllDoublesSketch() { return sketchType == KLL_DOUBLES_SKETCH; }

  boolean isKllFloatsSketch() { return sketchType == KLL_FLOATS_SKETCH; }

  boolean isKllLongsSketch() { return sketchType == KLL_LONGS_SKETCH; }

  boolean isKllItemsSketch() { return sketchType == KLL_ITEMS_SKETCH; }

  abstract boolean isLevelZeroSorted();

  /**
   * @return true if N == 1.
   */
  boolean isSingleItem() { return getN() == 1; }

  final void setLevelsArray(final int[] levelsArr) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    this.levelsArr = levelsArr;
    final MemorySegment wseg = getMemorySegment();
    if (wseg != null) {
      MemorySegment.copy(this.levelsArr, 0, wseg, JAVA_INT_UNALIGNED, DATA_START_ADR, levelsArr.length);
    }
  }

  final void setLevelsArrayAt(final int index, final int idxVal) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    levelsArr[index] = idxVal;
    final MemorySegment wseg = getMemorySegment();
    if (wseg != null) {
      final int offset = DATA_START_ADR + (index * Integer.BYTES);
      wseg.set(JAVA_INT_UNALIGNED, offset, idxVal);
    }
  }

  abstract void setLevelZeroSorted(boolean sorted);

  abstract void setMinK(int minK);

  abstract void setN(long n);

  abstract void setNumLevels(int numLevels);

  abstract void setMemorySegment(final MemorySegment wseg);

  /**
   * Used to define the variable type of the current instance of this class.
   */
  public enum SketchType {
    /**
     * KllDoublesSketch
     */
    KLL_DOUBLES_SKETCH(Double.BYTES, "KllDoublesSketch"),
    /**
     * KllFloatsSketch
     */
    KLL_FLOATS_SKETCH(Float.BYTES, "KllFloatsSketch"),
    /**
     * KllItemsSketch
     */
    KLL_ITEMS_SKETCH(0, "KllItemsSketch"),
    /**
     * KllDoublesSketch
     */
    KLL_LONGS_SKETCH(Long.BYTES, "KllLongsSketch");

    private final int typeBytes;
    private final String name;

    SketchType(final int typeBytes, final String name) {
      this.typeBytes = typeBytes;
      this.name = name;
    }

    /**
     * Gets the item size in bytes. If the item is generic, this returns zero.
     * @return the item size in bytes
     */
    public int getBytes() { return typeBytes; }

    /**
     * Get the name of the associated sketch
     * @return the name of the associated sketch
     */
    public String getName() { return name; }
  }

  /**
   * Used primarily to define the structure of the serialized sketch. Also used by the Heap Sketch.
   */
  public enum SketchStructure {
    /** Compact Empty Structure */
    COMPACT_EMPTY(PREAMBLE_INTS_EMPTY_SINGLE, SERIAL_VERSION_EMPTY_FULL),
    /** Compact Single Item Structure */
    COMPACT_SINGLE(PREAMBLE_INTS_EMPTY_SINGLE, SERIAL_VERSION_SINGLE),
    /** Compact Full Preamble Structure */
    COMPACT_FULL(PREAMBLE_INTS_FULL, SERIAL_VERSION_EMPTY_FULL),
    /** Updatable Preamble Structure */
    UPDATABLE(PREAMBLE_INTS_FULL, SERIAL_VERSION_UPDATABLE); //also used by the heap sketch.

    private final int preInts;
    private final int serVer;

    SketchStructure(final int preInts, final int serVer) {
      this.preInts = preInts;
      this.serVer = serVer;
    }

    /**
     * gets the Preamble Integers for this Structure.
     * @return the Preamble Integers for this Structure
     */
    public int getPreInts() { return preInts; }

    /**
     * gets the Serialization Version for this Structure.
     * @return the Serialization Version for this Structure.
     */
    public int getSerVer() { return serVer; }

    /**
     * gets the SketchStructure given preInts and serVer.
     * @param preInts the given preamble size in integers
     * @param serVer the given Serialization Version
     * @return the SketchStructure given preInts and serVer.
     */
    public static SketchStructure getSketchStructure(final int preInts, final int serVer) {
      final SketchStructure[] ssArr = SketchStructure.values();
      for (int i = 0; i < ssArr.length; i++) {
        if ((ssArr[i].preInts == preInts) && (ssArr[i].serVer == serVer)) {
          return ssArr[i];
        }
      }
      throw new SketchesArgumentException("Error combination of PreInts and SerVer: "
          + "PreInts: " + preInts + ", SerVer: " + serVer);
    }
  }

}
