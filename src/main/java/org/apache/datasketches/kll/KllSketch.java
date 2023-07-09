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
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_READ_ONLY;
import static org.apache.datasketches.kll.KllSketch.Error.UNSUPPORTED_TYPE;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.ITEMS_SKETCH;

import java.util.Random;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.QuantilesAPI;

/*
 * Sampled stream data (floats or doubles) is stored as an array or as part of a Memory object.
 * This array is partitioned into sections called levels and the indices into the array of quantiles
 * are tracked by a small integer array called levels or levels array.
 * The data for level i lies in positions levelsArray[i] through levelsArray[i + 1] - 1 inclusive.
 * Hence, the levelsArray must contain (numLevels + 1) indices.
 * The valid portion of the quantiles array is completely packed and sorted, except for level 0,
 * which is filled from the top down. Any quantiles below the index levelsArray[0] is garbage and will be
 * overwritten by subsequent updates.
 *
 * Invariants:
 * 1) After a compaction, or an update, or a merge, every level is sorted except for level zero.
 * 2) After a compaction, (sum of capacities) - (sum of quantiles) >= 1,
 *  so there is room for least 1 more quantile in level zero.
 * 3) There are no gaps except at the bottom, so if levels_[0] = 0,
 *  the sketch is exactly filled to capacity and must be compacted or the quantiles array and levels array
 *  must be expanded to include more levels.
 * 4) Sum of weights of all retained quantiles == N.
 * 5) Current total quantile capacity = itemsArray.length = levelsArray[numLevels].
 */

/**
 * This class is the root of the KLL sketch class hierarchy. It includes the public API that is independent
 * of either sketch type (float or double) and independent of whether the sketch is targeted for use on the
 * heap or Direct (off-heap).
 *
 * <p>KLL is an implementation of a very compact quantiles sketch with lazy compaction scheme
 * and nearly optimal accuracy per retained quantile.</p>
 *
 * <p>Reference <a href="https://arxiv.org/abs/1603.05346v2">Optimal Quantile Approximation in Streams</a>.</p>
 *
 * <p>The default <i>k</i> of 200 yields a "single-sided" epsilon of about 1.33% and a
 * "double-sided" (PMF) epsilon of about 1.65%, with a confidence of 99%.</p>
 * @see <a href="https://datasketches.apache.org/docs/KLL/KLLSketch.html">KLL Sketch</a>
 * @see QuantilesAPI
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 * @author Alexander Saydakov
 * @param T The generic type used with the KllItemsSketch
 */
public abstract class KllSketch implements QuantilesAPI {

  /**
   * Used to define the variable type of the current instance of this class.
   */
  public enum SketchType {
    DOUBLES_SKETCH(Double.BYTES),
    FLOATS_SKETCH(Float.BYTES),
    ITEMS_SKETCH(0);

    private int typeBytes;

    private SketchType(final int typeBytes) {
      this.typeBytes = typeBytes;
    }

    public int getBytes() { return typeBytes; }
  }

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
   * Choosing a smaller <i>m</i> less than 8 will make the sketch slower.
   */
  static final int DEFAULT_M = 8;
  static final int MAX_M = 8; //The maximum M
  static final int MIN_M = 2; //The minimum M
  static final Random random = new Random();

  final SketchType sketchType;
  final boolean serialVersionUpdatable;
  final MemoryRequestServer memReqSvr; //TODO ?
  final boolean readOnly; //TODO ?
  int[] levelsArr; //Always writable form
  WritableMemory wmem; //TODO ?
  ArrayOfItemsSerDe<?> serDe = null; //TODO ?

  /**
   * Constructor for on-heap and off-heap.
   * If both wmem and memReqSvr are null, this is a heap constructor.
   * If wmem != null and wmem is not readOnly, then memReqSvr must not be null.
   * If wmem was derived from an original Memory instance via a cast, it will be readOnly.
   * @param sketchType either DOUBLES_SKETCH, FLOATS_SKETCH or ITEMS_SKETCH
   * @param wmem  the current WritableMemory or null
   * @param memReqSvr the given MemoryRequestServer or null
   *
   */
  KllSketch(final SketchType sketchType, final WritableMemory wmem, final MemoryRequestServer memReqSvr) {
   this.sketchType = sketchType;

   //this.serDe = serDe;
   this.wmem = wmem;
   if (wmem != null) {
     this.serialVersionUpdatable = KllPreambleUtil.getMemorySerVer(wmem) == SERIAL_VERSION_UPDATABLE;
     this.readOnly = wmem.isReadOnly() || !serialVersionUpdatable;
     if (readOnly) {
       this.memReqSvr = null;
     } else {
       if (memReqSvr == null) { kllSketchThrow(Error.MRS_MUST_NOT_BE_NULL); }
       this.memReqSvr = memReqSvr;
     }
   } else { //wmem is null, heap case
     this.serialVersionUpdatable = false; //TODO Should be true?
     this.memReqSvr = null;
     this.readOnly = false;
   }
  }

  /**
   * Returns the current number of bytes this sketch would require to store in the compact Memory Format.
   * @return the current number of bytes this sketch would require to store in the compact Memory Format.
   * @deprecated version 4.0.0 use {@link #getSerializedSizeBytes}.
   */
  @Deprecated //ready to delete
  public int getCurrentCompactSerializedSizeBytes() {
    return currentSerializedSizeBytes(false);
  }

  /**
   * Returns the current number of bytes this sketch would require to store in the updatable Memory Format.
   * @return the current number of bytes this sketch would require to store in the updatable Memory Format.
   * @deprecated version 4.0.0 use {@link #getSerializedSizeBytes}.
   */
  @Deprecated //ready to delete
  public int getCurrentUpdatableSerializedSizeBytes() {
    return currentSerializedSizeBytes(true);
  }

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
    if (sketchType == ITEMS_SKETCH) { kllSketchThrow(UNSUPPORTED_TYPE); }
    final KllHelper.GrowthStats gStats =
        KllHelper.getGrowthSchemeForGivenN(k, DEFAULT_M, n, sketchType, false);
    return updatableMemFormat ? gStats.updatableBytes : gStats.compactBytes;
  }

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
   * Returns the current number of bytes this Sketch would require if serialized.
   * @return the number of bytes this sketch would require if serialized.
   */
  public int getSerializedSizeBytes() {
    return currentSerializedSizeBytes(serialVersionUpdatable);
  }

  /**
   * This returns the WritableMemory for Direct type sketches,
   * otherwise returns null.
   * @return the WritableMemory for Direct type sketches, otherwise null.
   */
  WritableMemory getWritableMemory() {
    return wmem;
  }

  @Override
  public boolean hasMemory() {
    return (wmem != null);
  }

  public boolean isCompactMemoryFormat() {
    return hasMemory() && !serialVersionUpdatable;
  }

  @Override
  public boolean isDirect() {
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
    return hasMemory() && serialVersionUpdatable;
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

  //restricted

  /**
   * Compute serialized size in bytes independent of the current
   * state of Serialization Version. For KllItemsSketch the result is
   * always in non-updatable, compact form.
   * @param serVerUpdatable reflects the hypothetical state of SerVer.
   * @return serialized size in bytes assuming a SerVer state.
   */
  final int currentSerializedSizeBytes(final boolean serVerUpdatable) {
    final int numLevels = getNumLevels();
    final int numItems;
    final int levelsArrBytes;
    if (!serVerUpdatable || sketchType == ITEMS_SKETCH) {
      numItems = getNumRetained();
      if (numItems == 0) { return N_LONG_ADR; }
      if (numItems == 1) { return DATA_START_ADR_SINGLE_ITEM + getSingleItemSizeBytes(); }
      levelsArrBytes = numLevels * Integer.BYTES;
      return DATA_START_ADR + levelsArrBytes + getMinMaxSizeBytes() + getRetainedDataSizeBytes();
    } else { //Double and Float sketches only
      numItems = KllHelper.computeTotalItemCapacity(getK(), getM(), numLevels);
      levelsArrBytes = (numLevels + 1) * Integer.BYTES;
      return DATA_START_ADR + levelsArrBytes + getMinMaxSizeBytes() + numItems * sketchType.typeBytes;
    }
  }

  enum Error {
    TGT_IS_READ_ONLY("Target sketch is Read Only, cannot write."),
    MRS_MUST_NOT_BE_NULL("MemoryRequestServer cannot be null."),
    NOT_SINGLE_ITEM("Sketch is empty or does not have just one item."),
    UNSUPPORTED_TYPE("Unsupported operation for this Sketch Type."),
    EMPTY("Sketch must not be empty for this operation.");

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

  final int[] getLevelsArray() {
    return levelsArr;
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
   * MinK is the K that results from a merge with a sketch configured with a K lower than
   * the K of this sketch. This is then used in computing the estimated upper and lower bounds of error.
   * @return The minimum K as a result of merging sketches with lower k.
   */
  abstract int getMinK();

  abstract byte[] getMinMaxByteArr();

  abstract int getMinMaxSizeBytes();

  final int getNumLevels() {
    return levelsArr.length - 1;
  }

  /**
   * Gets the serialized bytes of the data block as a byte array.
   * This only includes the valid retained items.
   * It does not include the preamble, the levels array, minimum or maximum items, or garbage data.
   * @return the serialized bytes of the retained data.
   */
  abstract byte[] getRetainedDataByteArr();

  /**
   * Gets the size of the data block in bytes.
   * This only includes the valid retained items.
   * It does not include the preamble, the levels array, minimum or maximum items, or garbage data.
   * @return the size of the retained data in bytes.
   */
  abstract int getRetainedDataSizeBytes();

  abstract byte[] getSingleItemByteArr();

  abstract int getSingleItemSizeBytes();

  abstract void incN();

  abstract void incNumLevels();

  final boolean isCompactSingleItem() {
    return hasMemory() && !serialVersionUpdatable && (getN() == 1);
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
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    this.levelsArr = levelsArr;
    if (wmem != null) {
      wmem.putIntArray(DATA_START_ADR, this.levelsArr, 0, levelsArr.length);
    }
  }

  final void setLevelsArrayAt(final int index, final int idxVal) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    this.levelsArr[index] = idxVal;
    if (wmem != null) {
      final int offset = DATA_START_ADR + index * Integer.BYTES;
      wmem.putInt(offset, idxVal);
    }
  }

  abstract void setLevelZeroSorted(boolean sorted);

  abstract void setMinK(int minK);

  abstract void setN(long n);

  abstract void setNumLevels(int numLevels);

}
