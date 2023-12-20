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
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.ITEMS_SKETCH;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.QuantilesAPI;

/*
 * Sampled stream data (floats, doubles, or items) is stored as a heap array called itemsArr or as part of a
 * WritableMemory/Memory object.
 * This array is partitioned into sections called levels and the indices into the array of items
 * are tracked by a small integer array called levelsArr.
 * The data for level i lies in positions levelsArr[i] through levelsArr[i + 1] - 1 inclusive.
 * Hence, the levelsArr must contain (numLevels + 1) elements.
 * The valid portion of the itemsArr is completely packed and sorted, except for level 0,
 * which is filled from the top down. Any items below the index levelsArr[0] is garbage and will be
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
 * of either sketch type (e.g., float, double or generic item) and independent of whether the sketch is targeted
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
public abstract class KllSketch implements QuantilesAPI {

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
  int[] levelsArr; //Always writable form

  /**
   * Constructor for on-heap and off-heap.
   * If both wmem and memReqSvr are null, this is a heap constructor.
   * If wmem != null and wmem is not readOnly, then memReqSvr must not be null.
   * If wmem was derived from an original Memory instance via a cast, it will be readOnly.
   * @param sketchType either DOUBLES_SKETCH, FLOATS_SKETCH or ITEMS_SKETCH
   * @param wmem  the current WritableMemory or null
   */
  KllSketch(
      final SketchType sketchType,
      final SketchStructure sketchStructure) {
   this.sketchType = sketchType;
   this.sketchStructure = sketchStructure;
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
   * @param sketchType Only DOUBLES_SKETCH and FLOATS_SKETCH is supported for this operation.
   * @param updatableMemFormat true if updatable Memory format, otherwise the standard compact format.
   * @return upper bound on the serialized size of a KllSketch.
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n,
      final SketchType sketchType, final boolean updatableMemFormat) {
    if (sketchType == ITEMS_SKETCH) { throw new SketchesArgumentException(UNSUPPORTED_MSG); }
    final KllHelper.GrowthStats gStats =
        KllHelper.getGrowthSchemeForGivenN(k, DEFAULT_M, n, sketchType, false);
    return updatableMemFormat ? gStats.updatableBytes : gStats.compactBytes;
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

  /**
   * Gets the approximate rank error of this sketch normalized as a fraction between zero and one.
   * The epsilon returned is a best fit to 99 percent confidence empirically measured max error
   * in thousands of trials.
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   */
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
  public boolean hasMemory() {
    final WritableMemory wmem = getWritableMemory();
    return (wmem != null);
  }

  public boolean isCompactMemoryFormat() {
    return hasMemory() && sketchStructure != UPDATABLE;
  }

  @Override
  public boolean isDirect() {
    final WritableMemory wmem = getWritableMemory();
    return (wmem != null) ? wmem.isDirect() : false;
  }

  @Override
  public final boolean isEmpty() {
    return getN() == 0;
  }

  @Override
  public final boolean isEstimationMode() {
    return getNumLevels() > 1;
  }

  /**
   * Returns true if the backing WritableMemory is in updatable format.
   * @return true if the backing WritableMemory is in updatable format.
   */
  public final boolean isMemoryUpdatableFormat() {
    return hasMemory() && sketchStructure == UPDATABLE;
  }

  @Override
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
    final WritableMemory wmem = getWritableMemory();
    return (wmem != null) && wmem.isSameResource(that);
  }

  /**
   * Merges another sketch into this one.
   * Attempting to merge a sketch of the wrong type will throw an exception.
   * @param other sketch to merge into this one
   */
  public abstract void merge(KllSketch other);

  @Override
  public final String toString() {
    return toString(true, false);
  }

  /**
   * Returns a summary of the sketch as a string.
   * @param withSummary if true includes sketch summary information
   * @param withData if true include sketch data
   * @return string representation of sketch summary
   */
  public String toString(final boolean withSummary, final boolean withData) {
    return KllHelper.toStringImpl(this, withSummary, withData, getSerDe());
  }

  //restricted

  /**
   * Compute serialized size in bytes independent of the current sketch.
   * For KllItemsSketch the result is always in non-updatable, compact form.
   * @param updatable true if the desired result is for updatable structure.
   * @return serialized size in bytes given a SketchStructure.
   */
  final int currentSerializedSizeBytes(final boolean updatable) {
    final boolean myUpdatable = sketchType == ITEMS_SKETCH ? false : updatable;
    final long srcN = this.getN();
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
   * Gets the MemoryRequestServer or null.
   * @return the MemoryRequestServer or null.
   */
  abstract MemoryRequestServer getMemoryRequestServer();

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
    if (sketchStructure == UPDATABLE || sketchStructure == COMPACT_FULL) { return levelsArr.length - 1; }
    return 1;
  }

  /**
   * Gets the serialized byte array of the valid retained items as a byte array.
   * It does not include the preamble, the levels array, minimum or maximum items, or garbage data.
   * @return the serialized bytes of the retained data.
   */
  abstract byte[] getRetainedItemsByteArr();

  /**
   * Gets the size in bytes of the valid retained items.
   * It does not include the preamble, the levels array, minimum or maximum items, or garbage data.
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
   * It may include empty or garbage items.
   * @return the serialized bytes of the retained data.
   */
  abstract byte[] getTotalItemsByteArr();

  /**
   * Gets the size in bytes of the entire internal items hypothetical structure.
   * It does not include the preamble, the levels array, or minimum or maximum items.
   * It may include empty or garbage items.
   * @return the size of the retained data in bytes.
   */
  abstract int getTotalItemsNumBytes();

  /**
   * This returns the WritableMemory for Direct type sketches,
   * otherwise returns null.
   * @return the WritableMemory for Direct type sketches, otherwise null.
   */
  abstract WritableMemory getWritableMemory();

  abstract void incN();

  abstract void incNumLevels();

  final boolean isCompactSingleItem() {
    return hasMemory() && sketchStructure == COMPACT_SINGLE && (getN() == 1);
  }

  boolean isDoublesSketch() { return sketchType == DOUBLES_SKETCH; }

  boolean isFloatsSketch() { return sketchType == FLOATS_SKETCH; }

  boolean isItemsSketch() { return sketchType == ITEMS_SKETCH; }

  abstract boolean isLevelZeroSorted();

  /**
   * @return true if N == 1.
   */
  boolean isSingleItem() { return getN() == 1; }

  final void setLevelsArray(final int[] levelsArr) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    this.levelsArr = levelsArr;
    final WritableMemory wmem = getWritableMemory();
    if (wmem != null) {
      wmem.putIntArray(DATA_START_ADR, this.levelsArr, 0, levelsArr.length);
    }
  }

  final void setLevelsArrayAt(final int index, final int idxVal) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    this.levelsArr[index] = idxVal;
    final WritableMemory wmem = getWritableMemory();
    if (wmem != null) {
      final int offset = DATA_START_ADR + index * Integer.BYTES;
      wmem.putInt(offset, idxVal);
    }
  }

  abstract void setLevelZeroSorted(boolean sorted);

  abstract void setMinK(int minK);

  abstract void setN(long n);

  abstract void setNumLevels(int numLevels);

  abstract void setWritableMemory(final WritableMemory wmem);

  /**
   * Used to define the variable type of the current instance of this class.
   */
  public enum SketchType {
    DOUBLES_SKETCH(Double.BYTES, "DoublesSketch"),
    FLOATS_SKETCH(Float.BYTES, "FloatsSketch"),
    ITEMS_SKETCH(0, "ItemsSketch");

    private int typeBytes;
    private String name;

    private SketchType(final int typeBytes, final String name) {
      this.typeBytes = typeBytes;
      this.name = name;
    }

    public int getBytes() { return typeBytes; }

    public String getName() { return name; }
  }

  /**
   * Used primarily to define the structure of the serialized sketch. Also used by the Heap Sketch.
   */
  public enum SketchStructure {
    COMPACT_EMPTY(PREAMBLE_INTS_EMPTY_SINGLE, SERIAL_VERSION_EMPTY_FULL),
    COMPACT_SINGLE(PREAMBLE_INTS_EMPTY_SINGLE, SERIAL_VERSION_SINGLE),
    COMPACT_FULL(PREAMBLE_INTS_FULL, SERIAL_VERSION_EMPTY_FULL),
    UPDATABLE(PREAMBLE_INTS_FULL, SERIAL_VERSION_UPDATABLE); //also used by the heap sketch.

    private int preInts;
    private int serVer;

    private SketchStructure(final int preInts, final int serVer) {
      this.preInts = preInts;
      this.serVer = serVer;
    }

    public int getPreInts() { return preInts; }

    public int getSerVer() { return serVer; }

    public static SketchStructure getSketchStructure(final int preInts, final int serVer) {
      final SketchStructure[] ssArr = SketchStructure.values();
      for (int i = 0; i < ssArr.length; i++) {
        if (ssArr[i].preInts == preInts && ssArr[i].serVer == serVer) {
          return ssArr[i];
        }
      }
      throw new SketchesArgumentException("Error combination of PreInts and SerVer: "
          + "PreInts: " + preInts + ", SerVer: " + serVer);
    }
  }

}
