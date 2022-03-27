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

import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static org.apache.datasketches.Util.isOdd;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_DOUBLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.DEFAULT_M;
import static org.apache.datasketches.kll.KllPreambleUtil.MAX_K;
import static org.apache.datasketches.kll.KllPreambleUtil.MIN_K;
import static org.apache.datasketches.kll.KllPreambleUtil.N_LONG_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_DOUBLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.insertDoubleSketchFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.insertDyMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.insertEmptyFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.insertFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.insertK;
import static org.apache.datasketches.kll.KllPreambleUtil.insertLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.insertM;
import static org.apache.datasketches.kll.KllPreambleUtil.insertN;
import static org.apache.datasketches.kll.KllPreambleUtil.insertNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.insertPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.insertSerVer;
import static org.apache.datasketches.kll.KllPreambleUtil.insertSingleItemFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.insertUpdatableFlag;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
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
 * 1) After a compaction, or an update, or a merge, all levels are sorted except for level zero.
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
 * heap or Direct (off-heap.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
public abstract class KllSketch {
  static final Random random = new Random();
  static final int M = DEFAULT_M; // configured minimum buffer "width", Must always be 8 for now.
  static final boolean compatible = true; //rank 0.0 and 1.0. compatible with classic Quantiles Sketch
  SketchType sketchType;
  WritableMemory wmem;
  MemoryRequestServer memReqSvr;
  boolean direct;

  /**
   *
   * @param sketchType either DOUBLE_SKETCH or FLOAT_SKETCH
   * @param wmem  the current WritableMemory or null
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   */
  KllSketch(final SketchType sketchType, final WritableMemory wmem, final MemoryRequestServer memReqSvr) {
   this.sketchType = sketchType;
   this.wmem = wmem;
   if (wmem != null) {
     this.direct = true;
     this.memReqSvr = memReqSvr;
   } else {
     this.direct = false;
     this.memReqSvr = null;
   }
  }

public enum SketchType { FLOATS_SKETCH, DOUBLES_SKETCH }

  //Static methods

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
  // constants were derived as the best fit to 99 percentile empirically measured max error in
  // thousands of trials
  public static int getKFromEpsilon(final double epsilon, final boolean pmf) {
    //Ensure that eps is >= than the lowest possible eps given MAX_K and pmf=false.
    final double eps = max(epsilon, 4.7634E-5);
    final double kdbl = pmf
        ? exp(log(2.446 / eps) / 0.9433)
        : exp(log(2.296 / eps) / 0.9723);
    final double krnd = round(kdbl);
    final double del = abs(krnd - kdbl);
    final int k = (int) (del < 1E-6 ? krnd : ceil(kdbl));
    return max(MIN_K, min(MAX_K, k));
  }

  /**
   * Returns upper bound on the compact serialized size of a FloatsSketch given a parameter
   * <em>k</em> and stream length. This method can be used if allocation of storage
   * is necessary beforehand.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @return upper bound on the compact serialized size
   * @deprecated use {@link #getMaxSerializedSizeBytes(int, long, SketchType, boolean)} instead.
   */
  @Deprecated
  public static int getMaxSerializedSizeBytes(final int k, final long n) {
    final KllHelper.GrowthStats gStats =  KllHelper.getGrowthSchemeForGivenN(k, n, FLOATS_SKETCH, false);
    return gStats.compactBytes;
  }

  /**
   * Returns upper bound on the serialized size of a KllSketch given the following parameters.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @param sketchType either DOUBLES_SKETCH or FLOATS_SKETCH
   * @param updatable true if updatable form, otherwise the standard compact form.
   * @return upper bound on the serialized size of a KllSketch.
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n,
      final SketchType sketchType, final boolean updatable) {
    final KllHelper.GrowthStats gStats = KllHelper.getGrowthSchemeForGivenN(k, n, sketchType, false);
    return updatable ? gStats.updatableBytes : gStats.compactBytes;
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

  /**
   * Returns the current number of bytes this Sketch would require if serialized.
   * @return the number of bytes this sketch would require if serialized.
   */
  public int getSerializedSizeBytes() {
    return (direct)
        ? getCurrentUpdatableSerializedSizeBytes()
        : getCurrentCompactSerializedSizeBytes();
  }

  static int getSerializedSizeBytes(final int numLevels, final int numItems,
      final SketchType sketchType, final boolean updatable) {
    int levelsBytes = 0;
    if (!updatable) {
      if (numItems == 0) { return N_LONG_ADR; }
      if (numItems == 1) {
        return DATA_START_ADR_SINGLE_ITEM + (sketchType == DOUBLES_SKETCH ? Double.BYTES : Float.BYTES);
      }
      levelsBytes = numLevels * Integer.BYTES;
    } else {
      levelsBytes = (numLevels + 1) * Integer.BYTES;
    }
    if (sketchType == DOUBLES_SKETCH) {
      return DATA_START_ADR_DOUBLE + levelsBytes + (numItems + 2) * Double.BYTES; //+2 is for min & max
    } else {
      return DATA_START_ADR_FLOAT + levelsBytes + (numItems + 2) * Float.BYTES;
    }
  }

  final static boolean isCompatible() {
    return compatible;
  }

  final static void kllSketchThrow(final int errNo) {
    String msg = "";
    switch (errNo) {
      case 30: msg = "Given sketch Memory is immutable, cannot write."; break;
      case 31: msg = "Given sketch Memory is immutable and incompatible."; break;
      case 32: msg = "Given sketch must be of type Direct."; break;
      case 33: msg = "Given sketch must be of type Double."; break;
      case 34: msg = "Given sketch must be of type Float."; break;
      case 35: msg = "Given sketch must not be of type Direct."; break;
      default: msg = "Unknown error: errNo: " + errNo; break;
    }
    throw new SketchesArgumentException(msg);
  }

  //Public Non-static methods

  /**
   * Returns the current compact number of bytes this sketch would require to store.
   * @return the current compact number of bytes this sketch would require to store.
   */
  public final int getCurrentCompactSerializedSizeBytes() {
    return KllSketch.getSerializedSizeBytes(getNumLevels(), getNumRetained(), sketchType, false);
  }

  /**
   * Returns the current updatable number of bytes this sketch would require to store.
   * @return the current updatable number of bytes this sketch would require to store.
   */
  public final int getCurrentUpdatableSerializedSizeBytes() {
    final int itemCap = KllHelper.computeTotalItemCapacity(getK(), M, getNumLevels());
    return KllSketch.getSerializedSizeBytes(getNumLevels(), itemCap, sketchType, true);
  }

  /**
   * Returns the parameter k
   * @return parameter k
   */
  public abstract int getK();

  /**
   * Returns the length of the input stream.
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
    return getNormalizedRankError(getDyMinK(), pmf);
  }

  /**
   * Returns the number of retained items (samples) in the sketch.
   * @return the number of retained items (samples) in the sketch
   */
  public final int getNumRetained() {
    return getLevelsArray()[getNumLevels()] - getLevelsArray()[0];
  }

  /**
   * This returns the WritableMemory for Direct type sketches,
   * otherwise returns null.
   * @return the WritableMemory for Direct type sketches, otherwise null.
   */
  public WritableMemory getWritableMemory() {
    return wmem;
  }

  public final boolean isDirect() {
    return direct;
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
   * Returns serialized sketch in a compact byte array form.
   * @return serialized sketch in a compact byte array form.
   */
  public byte[] toByteArray() {
    return toCompactByteArrayImpl();
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
    return toStringImpl(withLevels, withData);
  }

  /**
   * Returns serialized sketch in an updatable byte array form.
   * @return serialized sketch in an updatable byte array form.
   */
  public byte[] toUpdatableByteArray() {
    return toUpdatableByteArrayImpl();
  }

  //package-private non-static methods

  final void buildHeapKllSketchFromMemory(final MemoryValidate memVal) {
    final boolean doubleType = (sketchType == DOUBLES_SKETCH);
    final boolean updatable = memVal.updatable;
    setLevelZeroSorted(memVal.level0Sorted);
    setN(memVal.n);
    setDyMinK(memVal.dyMinK);
    setNumLevels(memVal.numLevels);
    final int[] myLevelsArr = new int[getNumLevels() + 1];

    if (updatable) {
      memVal.levelsArrUpdatable.getIntArray(0, myLevelsArr, 0, getNumLevels() + 1);
      setLevelsArray(myLevelsArr);
      if (doubleType) {
        setMinDoubleValue(memVal.minMaxArrUpdatable.getDouble(0));
        setMaxDoubleValue(memVal.minMaxArrUpdatable.getDouble(Double.BYTES));
        final int itemsCap = (int)memVal.itemsArrUpdatable.getCapacity() / Double.BYTES;
        final double[] myItemsArr = new double[itemsCap];
        memVal.itemsArrUpdatable.getDoubleArray(0, myItemsArr, 0, itemsCap);
        setDoubleItemsArray(myItemsArr);
      } else { //float
        setMinFloatValue(memVal.minMaxArrUpdatable.getFloat(0));
        setMaxFloatValue(memVal.minMaxArrUpdatable.getFloat(Float.BYTES));
        final int itemsCap = (int)memVal.itemsArrUpdatable.getCapacity() / Float.BYTES;
        final float[] myItemsArr = new float[itemsCap];
        memVal.itemsArrUpdatable.getFloatArray(0, myItemsArr, 0, itemsCap);
        setFloatItemsArray(myItemsArr);
      }
    } else { //compact
      memVal.levelsArrCompact.getIntArray(0, myLevelsArr, 0, getNumLevels() + 1);
      setLevelsArray(myLevelsArr);
      if (doubleType) {
        setMinDoubleValue(memVal.minMaxArrCompact.getDouble(0));
        setMaxDoubleValue(memVal.minMaxArrCompact.getDouble(Double.BYTES));
        final int itemsCap = (int)memVal.itemsArrCompact.getCapacity() / Double.BYTES;
        final double[] myItemsArr = new double[itemsCap];
        memVal.itemsArrCompact.getDoubleArray(0, myItemsArr, 0, itemsCap);
        setDoubleItemsArray(myItemsArr);
      } else { //float
        setMinFloatValue(memVal.minMaxArrCompact.getFloat(0));
        setMaxFloatValue(memVal.minMaxArrCompact.getFloat(Float.BYTES));
        final int itemsCap = (int)memVal.itemsArrCompact.getCapacity() / Float.BYTES;
        final float[] myItemsArr = new float[itemsCap];
        memVal.itemsArrCompact.getFloatArray(0, myItemsArr, 0, itemsCap);
        setFloatItemsArray(myItemsArr);
      }
    }
  }

  /**
   * @return full size of internal items array including garbage; for a floats sketch this will be null.
   */
  abstract double[] getDoubleItemsArray();

  final double getDoubleRank(final double value) {
    if (isEmpty()) { return Double.NaN; }
    int level = 0;
    int weight = 1;
    long total = 0;
    final double[] myDoubleItemsArr = getDoubleItemsArray();
    final int[] myLevelsArr = getLevelsArray();
    while (level < getNumLevels()) {
      final int fromIndex = myLevelsArr[level];
      final int toIndex = myLevelsArr[level + 1]; // exclusive
      for (int i = fromIndex; i < toIndex; i++) {
        if (myDoubleItemsArr[i] < value) {
          total += weight;
        } else if (level > 0 || isLevelZeroSorted()) {
          break; // levels above 0 are sorted, no point comparing further
        }
      }
      level++;
      weight *= 2;
    }
    return (double) total / getN();
  }

  final double[] getDoublesPmfOrCdf(final double[] splitPoints, final boolean isCdf) {
    if (isEmpty()) { return null; }
    KllDoublesHelper.validateDoubleValues(splitPoints);
    final double[] buckets = new double[splitPoints.length + 1];
    final int myNumLevels = getNumLevels();
    final int[] myLevelsArr = getLevelsArray();
    int level = 0;
    int weight = 1;
    while (level < myNumLevels) {
      final int fromIndex = myLevelsArr[level];
      final int toIndex = myLevelsArr[level + 1]; // exclusive
      if (level == 0 && !isLevelZeroSorted()) {
        incrementDoublesBucketsUnsortedLevel(fromIndex, toIndex, weight, splitPoints, buckets);
      } else {
        incrementDoublesBucketsSortedLevel(fromIndex, toIndex, weight, splitPoints, buckets);
      }
      level++;
      weight *= 2;
    }
    // normalize and, if CDF, convert to cumulative
    if (isCdf) {
      double subtotal = 0;
      for (int i = 0; i < buckets.length; i++) {
        subtotal += buckets[i];
        buckets[i] = subtotal / getN();
      }
    } else {
      for (int i = 0; i < buckets.length; i++) {
        buckets[i] /= getN();
      }
    }
    return buckets;
  }

  final double getDoublesQuantile(final double fraction) {
    if (isEmpty()) { return Double.NaN; }
    if (fraction < 0.0 || fraction > 1.0) {
      throw new SketchesArgumentException("Fraction cannot be less than zero nor greater than 1.0");
    }
    if (isCompatible()) {
      if (fraction == 0.0) { return getMinDoubleValue(); }
      if (fraction == 1.0) { return getMaxDoubleValue(); }
    }
    final KllDoublesQuantileCalculator quant = getDoublesQuantileCalculator();
    return quant.getQuantile(fraction);
  }

  final double[] getDoublesQuantiles(final double[] fractions) {
    if (isEmpty()) { return null; }
    KllDoublesQuantileCalculator quant = null;
    final double[] quantiles = new double[fractions.length];
    for (int i = 0; i < fractions.length; i++) {
      final double fraction = fractions[i];
      if (fraction < 0.0 || fraction > 1.0) {
        throw new SketchesArgumentException("Fraction cannot be less than zero nor greater than 1.0");
      }
      if      (fraction == 0.0 && isCompatible()) { quantiles[i] = getMinDoubleValue(); }
      else if (fraction == 1.0 && isCompatible()) { quantiles[i] = getMaxDoubleValue(); }
      else {
        if (quant == null) {
          quant = getDoublesQuantileCalculator();
        }
        quantiles[i] = quant.getQuantile(fraction);
      }
    }
    return quantiles;
  }

  abstract int getDyMinK();

  /**
   * @return full size of internal items array including garbage; for a doubles sketch this will be null.
   */
  abstract float[] getFloatItemsArray();

  final double getFloatRank(final float value) {
    if (isEmpty()) { return Double.NaN; }
    int level = 0;
    int weight = 1;
    long total = 0;
    final float[] myFloatItemsArr = getFloatItemsArray();
    final int[] myLevelsArr = getLevelsArray();
    while (level < getNumLevels()) {
      final int fromIndex = myLevelsArr[level];
      final int toIndex = myLevelsArr[level + 1]; // exclusive
      for (int i = fromIndex; i < toIndex; i++) {
        if (myFloatItemsArr[i] < value) {
          total += weight;
        } else if (level > 0 || isLevelZeroSorted()) {
          break; // levels above 0 are sorted, no point comparing further
        }
      }
      level++;
      weight *= 2;
    }
    return (double) total / getN();
  }

  final double[] getFloatsPmfOrCdf(final float[] splitPoints, final boolean isCdf) {
    if (isEmpty()) { return null; }
    KllFloatsHelper.validateFloatValues(splitPoints);
    final double[] buckets = new double[splitPoints.length + 1];
    final int myNumLevels = getNumLevels();
    final int[] myLevelsArr = getLevelsArray();
    int level = 0;
    int weight = 1;
    while (level < myNumLevels) {
      final int fromIndex = myLevelsArr[level];
      final int toIndex = myLevelsArr[level + 1]; // exclusive
      if (level == 0 && !isLevelZeroSorted()) {
        incrementFloatBucketsUnsortedLevel(fromIndex, toIndex, weight, splitPoints, buckets);
      } else {
        incrementFloatBucketsSortedLevel(fromIndex, toIndex, weight, splitPoints, buckets);
      }
      level++;
      weight *= 2;
    }
    // normalize and, if CDF, convert to cumulative
    if (isCdf) {
      double subtotal = 0;
      for (int i = 0; i < buckets.length; i++) {
        subtotal += buckets[i];
        buckets[i] = subtotal / getN();
      }
    } else {
      for (int i = 0; i < buckets.length; i++) {
        buckets[i] /= getN();
      }
    }
    return buckets;
  }

  final float getFloatsQuantile(final double fraction) {
    if (isEmpty()) { return Float.NaN; }
    if (fraction < 0.0 || fraction > 1.0) {
      throw new SketchesArgumentException("Fraction cannot be less than zero nor greater than 1.0");
    }
    if (isCompatible()) {
      if (fraction == 0.0) { return getMinFloatValue(); }
      if (fraction == 1.0) { return getMaxFloatValue(); }
    }
    final KllFloatsQuantileCalculator quant = getFloatsQuantileCalculator();
    return quant.getQuantile(fraction);
  }

  final float[] getFloatsQuantiles(final double[] fractions) {
    if (isEmpty()) { return null; }
    KllFloatsQuantileCalculator quant = null;
    final float[] quantiles = new float[fractions.length];
    for (int i = 0; i < fractions.length; i++) {
      final double fraction = fractions[i];
      if (fraction < 0.0 || fraction > 1.0) {
        throw new SketchesArgumentException("Fraction cannot be less than zero nor greater than 1.0");
      }
      if      (fraction == 0.0 && isCompatible()) { quantiles[i] = getMinFloatValue(); }
      else if (fraction == 1.0 && isCompatible()) { quantiles[i] = getMaxFloatValue(); }
      else {
        if (quant == null) {
          quant = getFloatsQuantileCalculator();
        }
        quantiles[i] = quant.getQuantile(fraction);
      }
    }
    return quantiles;
  }

  abstract double getDoubleItemsArrayAt(int index);

  abstract float getFloatItemsArrayAt(int index);

  abstract String getLayout();

  abstract int[] getLevelsArray();

  abstract int getLevelsArrayAt(int index);

  abstract double getMaxDoubleValue();

  abstract float getMaxFloatValue();

  abstract double getMinDoubleValue();

  abstract float getMinFloatValue();

  abstract int getNumLevels();

  abstract void incN();

  abstract void incNumLevels();

  boolean isDoublesSketch() { return sketchType == DOUBLES_SKETCH; }

  boolean isFloatsSketch() { return sketchType != DOUBLES_SKETCH; }

  abstract boolean isLevelZeroSorted();

  /**
   * This method is for direct Double and Float sketches only and does the following:
   * <ul><li>Allocates a new WritableMemory of the required size</li>
   * <li>Copies over the preamble as is (20 or 24 bytes)</li>
   * <li>Creates new memory regions for Levels Array, Min/Max Array, Items Array, but
   * does not fill them. They may contain garbage.</li>
   * </ul>
   * The caller is responsible for filling these regions and updating the preamble.
   * @param sketch The current sketch that needs to be expanded.
   * @param newLevelsArrLen the element length of the new Levels array.
   * @param newItemsArrLen the element length of the new Items array.
   * @return the new expanded memory with preamble.
   */
  static WritableMemory memorySpaceMgmt(
      final KllSketch sketch,
      final int newLevelsArrLen,
      final int newItemsArrLen) {
    final SketchType sketchType = sketch.sketchType;
    final WritableMemory oldWmem = sketch.wmem;
    final int typeBytes;
    final int startAdr;

    if (sketchType == DOUBLES_SKETCH) {
      typeBytes = Double.BYTES;
      startAdr = DATA_START_ADR_DOUBLE;
    } else {
      typeBytes = Float.BYTES;
      startAdr = DATA_START_ADR_FLOAT;
    }
    int totalSketchBytes = startAdr;
    totalSketchBytes += newLevelsArrLen * Integer.BYTES;
    totalSketchBytes += 2 * typeBytes;
    totalSketchBytes += newItemsArrLen * typeBytes;
    final WritableMemory newWmem;

    if (totalSketchBytes > oldWmem.getCapacity()) { //Acquire new WritableMemory
      newWmem = sketch.memReqSvr.request(oldWmem, totalSketchBytes);
      oldWmem.copyTo(0, newWmem, 0, startAdr); //copy preamble
    }
    else { //Expand in current memory
      newWmem = oldWmem;
    }

    int offset = startAdr;
    //LEVELS ARR
    int lengthBytes = newLevelsArrLen * Integer.BYTES;
    sketch.setLevelsArrayUpdatable(newWmem.writableRegion(offset, lengthBytes)); //
    offset += lengthBytes;
    //MIN MAX ARR
    lengthBytes = 2 * typeBytes;
    sketch.setMinMaxArrayUpdatable(newWmem.writableRegion(offset, lengthBytes));
    offset += lengthBytes;
    //ITEMS ARR
    lengthBytes = newItemsArrLen * typeBytes;
    sketch.setItemsArrayUpdatable(newWmem.writableRegion(offset, lengthBytes));
    assert totalSketchBytes <= newWmem.getCapacity();
    return newWmem;
  }

  final void mergeDoubleImpl(final KllSketch other) {
    if (other.isEmpty()) { return; }
    final long finalN = getN() + other.getN();
    //update this sketch with level0 items from the other sketch
    final double[] otherDoubleItemsArr = other.getDoubleItemsArray();
    final int[] otherLevelsArr = other.getLevelsArray();
    for (int i = otherLevelsArr[0]; i < otherLevelsArr[1]; i++) {
      updateDouble(otherDoubleItemsArr[i]);
    }
    // after the level 0 update, we capture the key mutable variables
    final double myMin = getMinDoubleValue();
    final double myMax = getMaxDoubleValue();
    final int myDyMinK = getDyMinK();

    final int myCurNumLevels = getNumLevels();
    final int[] myCurLevelsArr = getLevelsArray();
    final double[] myCurDoubleItemsArr = getDoubleItemsArray();

    final int myNewNumLevels;
    final int[] myNewLevelsArr;
    final double[] myNewDoubleItemsArr;

    if (other.getNumLevels() > 1) { //now merge other levels if they exist
      final int tmpSpaceNeeded = getNumRetained()
          + KllHelper.getNumRetainedAboveLevelZero(other.getNumLevels(), otherLevelsArr);
      final double[] workbuf = new double[tmpSpaceNeeded];
      final int ub = KllHelper.ubOnNumLevels(finalN);
      final int[] worklevels = new int[ub + 2]; // ub+1 does not work
      final int[] outlevels  = new int[ub + 2];

      final int provisionalNumLevels = max(myCurNumLevels, other.getNumLevels());

      populateDoubleWorkArrays(other, workbuf, worklevels, provisionalNumLevels);

      // notice that workbuf is being used as both the input and output
      final int[] result = KllDoublesHelper.generalDoublesCompress(getK(), M, provisionalNumLevels, workbuf,
          worklevels, workbuf, outlevels, isLevelZeroSorted(), random);
      final int targetItemCount = result[1]; //was finalCapacity. Max size given k, m, numLevels
      final int curItemCount = result[2]; //was finalPop

      // now we need to finalize the results for the "self" sketch

      //THE NEW NUM LEVELS
      myNewNumLevels = result[0]; //was finalNumLevels
      assert myNewNumLevels <= ub; // ub may be much bigger

      // THE NEW ITEMS ARRAY (was newbuf)
      myNewDoubleItemsArr = (targetItemCount == myCurDoubleItemsArr.length)
          ? myCurDoubleItemsArr
          : new double[targetItemCount];
      final int freeSpaceAtBottom = targetItemCount - curItemCount;
      //shift the new items array
      System.arraycopy(workbuf, outlevels[0], myNewDoubleItemsArr, freeSpaceAtBottom, curItemCount);
      final int theShift = freeSpaceAtBottom - outlevels[0];

      //calculate the new levels array length
      final int finalLevelsArrLen;
      if (myCurLevelsArr.length < myNewNumLevels + 1) { finalLevelsArrLen = myNewNumLevels + 1; }
      else { finalLevelsArrLen = myCurLevelsArr.length; }

      //THE NEW LEVELS ARRAY
      myNewLevelsArr = new int[finalLevelsArrLen];
      for (int lvl = 0; lvl < myNewNumLevels + 1; lvl++) { // includes the "extra" index
        myNewLevelsArr[lvl] = outlevels[lvl] + theShift;
      }

      //MEMORY SPACE MANAGEMENT
      if (direct) {
        wmem = memorySpaceMgmt(this, myNewLevelsArr.length, myNewDoubleItemsArr.length);
      } //End direct

    } else {
      myNewNumLevels = myCurNumLevels;
      myNewLevelsArr = myCurLevelsArr;
      myNewDoubleItemsArr = myCurDoubleItemsArr;
    }

    //Update Preamble:
    setN(finalN);
    if (other.isEstimationMode()) { //otherwise the merge brings over exact items.
      setDyMinK(min(myDyMinK, other.getDyMinK()));
    }

    //Update min, max values
    final double otherMin = other.getMinDoubleValue();
    final double otherMax = other.getMaxDoubleValue();
    setMinDoubleValue(resolveDoubleMinValue(myMin, otherMin));
    setMaxDoubleValue(resolveDoubleMaxValue(myMax, otherMax));

    //Update numLevels, levelsArray, items
    setNumLevels(myNewNumLevels);
    setLevelsArray(myNewLevelsArr);
    setDoubleItemsArray(myNewDoubleItemsArr);
    assert KllHelper.sumTheSampleWeights(getNumLevels(), getLevelsArray()) == getN();
  }

  private static double resolveDoubleMinValue(final double myMin, final double otherMin) {
    if (Double.isNaN(myMin) && Double.isNaN(otherMin)) { return Double.NaN; }
    if (Double.isNaN(myMin)) { return otherMin; }
    if (Double.isNaN(otherMin)) { return myMin; }
    return min(myMin, otherMin);
  }

  private static double resolveDoubleMaxValue(final double myMax, final double otherMax) {
    if (Double.isNaN(myMax) && Double.isNaN(otherMax)) { return Double.NaN; }
    if (Double.isNaN(myMax)) { return otherMax; }
    if (Double.isNaN(otherMax)) { return myMax; }
    return max(myMax, otherMax);
  }

  final void mergeFloatImpl(final KllSketch other) {
    if (other.isEmpty()) { return; }
    final long finalN = getN() + other.getN();
    //update this sketch with level0 items from the other sketch
    final float[] otherFloatItemsArr = other.getFloatItemsArray();
    final int[] otherLevelsArr = other.getLevelsArray();
    for (int i = otherLevelsArr[0]; i < otherLevelsArr[1]; i++) {
      updateFloat(otherFloatItemsArr[i]);
    }
    // after the level 0 update, we capture the key mutable variables
    final float myMin = getMinFloatValue();
    final float myMax = getMaxFloatValue();
    final int myDyMinK = getDyMinK();

    final int myCurNumLevels = getNumLevels();
    final int[] myCurLevelsArr = getLevelsArray();
    final float[] myCurFloatItemsArr = getFloatItemsArray();

    final int myNewNumLevels;
    final int[] myNewLevelsArr;
    final float[] myNewFloatItemsArr;

    if (other.getNumLevels() > 1) { //now merge other levels if they exist
      final int tmpSpaceNeeded = getNumRetained()
          + KllHelper.getNumRetainedAboveLevelZero(other.getNumLevels(), otherLevelsArr);
      final float[] workbuf = new float[tmpSpaceNeeded];
      final int ub = KllHelper.ubOnNumLevels(finalN);
      final int[] worklevels = new int[ub + 2]; // ub+1 does not work
      final int[] outlevels  = new int[ub + 2];

      final int provisionalNumLevels = max(myCurNumLevels, other.getNumLevels());

      populateFloatWorkArrays(other, workbuf, worklevels, provisionalNumLevels);

      // notice that workbuf is being used as both the input and output
      final int[] result = KllFloatsHelper.generalFloatsCompress(getK(), M, provisionalNumLevels, workbuf,
          worklevels, workbuf, outlevels, isLevelZeroSorted(), random);
      final int targetItemCount = result[1]; //was finalCapacity. Max size given k, m, numLevels
      final int curItemCount = result[2]; //was finalPop

      // now we need to finalize the results for the "self" sketch

      //THE NEW NUM LEVELS
      myNewNumLevels = result[0]; //was finalNumLevels
      assert myNewNumLevels <= ub; // ub may be much bigger

      // THE NEW ITEMS ARRAY (was newbuf)
      myNewFloatItemsArr = (targetItemCount == myCurFloatItemsArr.length)
          ? myCurFloatItemsArr
          : new float[targetItemCount];
      final int freeSpaceAtBottom = targetItemCount - curItemCount;
      //shift the new items array
      System.arraycopy(workbuf, outlevels[0], myNewFloatItemsArr, freeSpaceAtBottom, curItemCount);
      final int theShift = freeSpaceAtBottom - outlevels[0];

      //calculate the new levels array length
      final int finalLevelsArrLen;
      if (myCurLevelsArr.length < myNewNumLevels + 1) { finalLevelsArrLen = myNewNumLevels + 1; }
      else { finalLevelsArrLen = myCurLevelsArr.length; }

      //THE NEW LEVELS ARRAY
      myNewLevelsArr = new int[finalLevelsArrLen];
      for (int lvl = 0; lvl < myNewNumLevels + 1; lvl++) { // includes the "extra" index
        myNewLevelsArr[lvl] = outlevels[lvl] + theShift;
      }

      //MEMORY SPACE MANAGEMENT
      if (direct) {
        wmem = memorySpaceMgmt(this, myNewLevelsArr.length, myNewFloatItemsArr.length);
      } //End direct

    } else {
      myNewNumLevels = myCurNumLevels;
      myNewLevelsArr = myCurLevelsArr;
      myNewFloatItemsArr = myCurFloatItemsArr;
    }

    //Update Preamble:
    setN(finalN);
    if (other.isEstimationMode()) { //otherwise the merge brings over exact items.
      setDyMinK(min(myDyMinK, other.getDyMinK()));
    }

    //Update min, max values
    final float otherMin = other.getMinFloatValue();
    final float otherMax = other.getMaxFloatValue();
    setMinFloatValue(resolveFloatMinValue(myMin, otherMin));
    setMaxFloatValue(resolveFloatMaxValue(myMax, otherMax));

    //Update numLevels, levelsArray, items
    setNumLevels(myNewNumLevels);
    setLevelsArray(myNewLevelsArr);
    setFloatItemsArray(myNewFloatItemsArr);
    assert KllHelper.sumTheSampleWeights(getNumLevels(), getLevelsArray()) == getN();
  }

  private static float resolveFloatMinValue(final float myMin, final float otherMin) {
    if (Float.isNaN(myMin) && Float.isNaN(otherMin)) { return Float.NaN; }
    if (Float.isNaN(myMin)) { return otherMin; }
    if (Float.isNaN(otherMin)) { return myMin; }
    return min(myMin, otherMin);
  }

  private static float resolveFloatMaxValue(final float myMax, final float otherMax) {
    if (Float.isNaN(myMax) && Float.isNaN(otherMax)) { return Float.NaN; }
    if (Float.isNaN(myMax)) { return otherMax; }
    if (Float.isNaN(otherMax)) { return myMax; }
    return max(myMax, otherMax);
  }

  abstract void setDoubleItemsArray(double[] floatItems);

  abstract void setDoubleItemsArrayAt(int index, double value);

  abstract void setDyMinK(int dyMinK);

  abstract void setFloatItemsArray(float[] floatItems);

  abstract void setFloatItemsArrayAt(int index, float value);

  abstract void setItemsArrayUpdatable(WritableMemory itemsMem);

  abstract void setLevelsArray(int[] levelsArr);

  abstract void setLevelsArrayAt(int index, int value);

  abstract void setLevelsArrayAtPlusEq(int index, int plusEq);

  abstract void setLevelsArrayAtMinusEq(int index, int minusEq);

  abstract void setLevelsArrayUpdatable(WritableMemory levelsMem);

  abstract void setLevelZeroSorted(boolean sorted);

  abstract void setMaxDoubleValue(double value);

  abstract void setMaxFloatValue(float value);

  abstract void setMinDoubleValue(double value);

  abstract void setMinFloatValue(float value);

  abstract void setMinMaxArrayUpdatable(WritableMemory minMaxMem);

  abstract void setN(long n);

  abstract void setNumLevels(int numLevels);

  final byte[] toCompactByteArrayImpl() {
    final byte[] byteArr = new byte[getCurrentCompactSerializedSizeBytes()];
    final WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    loadFirst8Bytes(this, wmem, false);
    if (getN() == 0) { return byteArr; } //empty
    final boolean doubleType = (sketchType == DOUBLES_SKETCH);

    //load data
    int offset = DATA_START_ADR_SINGLE_ITEM;
    final int[] myLevelsArr = getLevelsArray();
    if (getN() == 1) { //single item
      if (doubleType) {
        wmem.putDouble(offset,  getDoubleItemsArray()[myLevelsArr[0]]);
      } else {
        wmem.putFloat(offset, getFloatItemsArray()[myLevelsArr[0]]);
      }
    } else { // n > 1
      //remainder of preamble after first 8 bytes
      insertN(wmem, getN());
      insertDyMinK(wmem, getDyMinK());
      insertNumLevels(wmem, getNumLevels());
      offset = (doubleType) ? DATA_START_ADR_DOUBLE : DATA_START_ADR_FLOAT;

      //LOAD LEVELS ARR the last integer in levels_ is NOT serialized
      final int len = myLevelsArr.length - 1;
      wmem.putIntArray(offset, myLevelsArr, 0, len);
      offset += len * Integer.BYTES;

      //LOAD MIN, MAX VALUES FOLLOWED BY ITEMS ARRAY
      if (doubleType) {
        wmem.putDouble(offset, getMinDoubleValue());
        offset += Double.BYTES;
        wmem.putDouble(offset, getMaxDoubleValue());
        offset += Double.BYTES;
        wmem.putDoubleArray(offset, getDoubleItemsArray(), myLevelsArr[0], getNumRetained());
      } else {
        wmem.putFloat(offset, getMinFloatValue());
        offset += Float.BYTES;
        wmem.putFloat(offset, getMaxFloatValue());
        offset += Float.BYTES;
        wmem.putFloatArray(offset, getFloatItemsArray(), myLevelsArr[0], getNumRetained());
      }
    }
    return byteArr;
  }

  private static void loadFirst8Bytes(final KllSketch sk, final WritableMemory wmem,
      final boolean updatable) {
    final boolean empty = sk.getN() == 0;
    final boolean lvlZeroSorted = sk.isLevelZeroSorted();
    final boolean singleItem = sk.getN() == 1;
    final boolean doubleType = (sk.sketchType == DOUBLES_SKETCH);
    final int preInts =
        updatable
        ? (doubleType ? PREAMBLE_INTS_DOUBLE : PREAMBLE_INTS_FLOAT)
        : ((empty || singleItem)
          ? PREAMBLE_INTS_EMPTY_SINGLE
          : (doubleType) ? PREAMBLE_INTS_DOUBLE : PREAMBLE_INTS_FLOAT);
    //load the preamble
    insertPreInts(wmem, preInts);
    final int server = updatable ? SERIAL_VERSION_UPDATABLE
        : (singleItem ? SERIAL_VERSION_SINGLE : SERIAL_VERSION_EMPTY_FULL);
    insertSerVer(wmem, server);
    insertFamilyID(wmem, Family.KLL.getID());
    insertEmptyFlag(wmem, empty);
    insertLevelZeroSortedFlag(wmem, lvlZeroSorted);
    insertSingleItemFlag(wmem, singleItem);
    insertDoubleSketchFlag(wmem, doubleType);
    insertUpdatableFlag(wmem, updatable);
    insertK(wmem, sk.getK());
    insertM(wmem, M);
  }

  @SuppressWarnings("null")
  final String toStringImpl(final boolean withLevels, final boolean withData) {
    final boolean doubleType = (sketchType == DOUBLES_SKETCH);
    final int k = getK();
    final String epsPct = String.format("%.3f%%", getNormalizedRankError(false) * 100);
    final String epsPMFPct = String.format("%.3f%%", getNormalizedRankError(true) * 100);
    final StringBuilder sb = new StringBuilder();
    final String skType = (doubleType) ? "Doubles" : "Floats";
    sb.append(Util.LS).append("### KLL ").append(skType).append("Sketch summary:").append(Util.LS);
    sb.append("   K                    : ").append(k).append(Util.LS);
    sb.append("   Dynamic min K        : ").append(getDyMinK()).append(Util.LS);
    sb.append("   M                    : ").append(M).append(Util.LS);
    sb.append("   N                    : ").append(getN()).append(Util.LS);
    sb.append("   Epsilon              : ").append(epsPct).append(Util.LS);
    sb.append("   Epsison PMF          : ").append(epsPMFPct).append(Util.LS);
    sb.append("   Empty                : ").append(isEmpty()).append(Util.LS);
    sb.append("   Estimation Mode      : ").append(isEstimationMode()).append(Util.LS);
    sb.append("   Levels               : ").append(getNumLevels()).append(Util.LS);
    sb.append("   Level 0 Sorted       : ").append(isLevelZeroSorted()).append(Util.LS);
    final int cap = (doubleType) ? getDoubleItemsArray().length : getFloatItemsArray().length;
    sb.append("   Capacity Items       : ").append(cap).append(Util.LS);
    sb.append("   Retained Items       : ").append(getNumRetained()).append(Util.LS);
    sb.append("   Compact Storage Bytes: ").append(getCurrentCompactSerializedSizeBytes()).append(Util.LS);
    if (doubleType) {
      sb.append("   Min Value            : ").append(getMinDoubleValue()).append(Util.LS);
      sb.append("   Max Value            : ").append(getMaxDoubleValue()).append(Util.LS);
    } else {
      sb.append("   Min Value            : ").append(getMinFloatValue()).append(Util.LS);
      sb.append("   Max Value            : ").append(getMaxFloatValue()).append(Util.LS);
    }
    sb.append("### End sketch summary").append(Util.LS);

    final int myNumLevels = getNumLevels();
    final int[] myLevelsArr = getLevelsArray();
    double[] myDoubleItemsArr = null;
    float[] myFloatItemsArr = null;
    if (doubleType) {
      myDoubleItemsArr = getDoubleItemsArray();
    } else {
      myFloatItemsArr = getFloatItemsArray();
    }
    if (withLevels) {
      sb.append(outputLevels(k, myNumLevels, myLevelsArr));
    }
    if (withData) {
      sb.append(outputData(doubleType, myNumLevels, myLevelsArr, myFloatItemsArr, myDoubleItemsArr));
    }
    return sb.toString();
  }

  static String outputLevels(final int k, final int numLevels, final int[] levelsArr) {
    final StringBuilder sb =  new StringBuilder();
    sb.append("### KLL levels array:").append(Util.LS)
    .append(" level, offset: nominal capacity, actual size").append(Util.LS);
    int level = 0;
    for ( ; level < numLevels; level++) {
      sb.append("   ").append(level).append(", ").append(levelsArr[level]).append(": ")
      .append(KllHelper.levelCapacity(k, numLevels, level, M))
      .append(", ").append(KllHelper.currentLevelSize(level, numLevels, levelsArr)).append(Util.LS);
    }
    sb.append("   ").append(level).append(", ").append(levelsArr[level]).append(": (Exclusive)")
    .append(Util.LS);
    sb.append("### End levels array").append(Util.LS);
    return sb.toString();
  }

  static String outputData(final boolean doubleType, final int numLevels, final int[] levelsArr,
      final float[] floatItemsArr, final double[] doubleItemsArr) {
    final StringBuilder sb =  new StringBuilder();
    sb.append("### KLL items data {index, item}:").append(Util.LS);
    if (levelsArr[0] > 0) {
      sb.append(" Garbage:" + Util.LS);
      if (doubleType) {
        for (int i = 0; i < levelsArr[0]; i++) {
          sb.append("   ").append(i + ", ").append(doubleItemsArr[i]).append(Util.LS);
        }
      } else {
        for (int i = 0; i < levelsArr[0]; i++) {
          sb.append("   ").append(i + ", ").append(floatItemsArr[i]).append(Util.LS);
        }
      }
    }
    int level = 0;
    if (doubleType) {
      while (level < numLevels) {
        final int fromIndex = levelsArr[level];
        final int toIndex = levelsArr[level + 1]; // exclusive
        if (fromIndex < toIndex) {
          sb.append(" level[").append(level).append("]: offset: " + levelsArr[level] + " wt: " + (1 << level));
          sb.append(Util.LS);
        }

        for (int i = fromIndex; i < toIndex; i++) {
          sb.append("   ").append(i + ", ").append(doubleItemsArr[i]).append(Util.LS);
        }
        level++;
      }
    }
    else {
      while (level < numLevels) {
        final int fromIndex = levelsArr[level];
        final int toIndex = levelsArr[level + 1]; // exclusive
        if (fromIndex <= toIndex) {
          sb.append(" level[").append(level).append("]: offset: " + levelsArr[level] + " wt: " + (1 << level));
          sb.append(Util.LS);
        }

        for (int i = fromIndex; i < toIndex; i++) {
          sb.append("   ").append(i + ", ").append(floatItemsArr[i]).append(Util.LS);
        }
        level++;
      }
    }
    sb.append(" level[" + level + "]: offset: " + levelsArr[level] + " (Exclusive)");
    sb.append(Util.LS);
    sb.append("### End items data").append(Util.LS);

    return sb.toString();
  }

  final byte[] toUpdatableByteArrayImpl() {
    final byte[] byteArr = new byte[getCurrentUpdatableSerializedSizeBytes()];
    final WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    loadFirst8Bytes(this, wmem, true);
    //remainder of preamble after first 8 bytes
    insertN(wmem, getN());
    insertDyMinK(wmem, getDyMinK());
    insertNumLevels(wmem, getNumLevels());

    //load data
    final boolean doubleType = (sketchType == DOUBLES_SKETCH);
    int offset = (doubleType) ? DATA_START_ADR_DOUBLE : DATA_START_ADR_FLOAT;

    //LOAD LEVELS ARRAY the last integer in levels_ IS serialized
    final int[] myLevelsArr = getLevelsArray();
    final int len = myLevelsArr.length;
    wmem.putIntArray(offset, myLevelsArr, 0, len);
    offset += len * Integer.BYTES;

    //LOAD MIN, MAX VALUES FOLLOWED BY ITEMS ARRAY
    if (doubleType) {
      wmem.putDouble(offset, getMinDoubleValue());
      offset += Double.BYTES;
      wmem.putDouble(offset, getMaxDoubleValue());
      offset += Double.BYTES;
      final double[] doubleItemsArr = getDoubleItemsArray();
      wmem.putDoubleArray(offset, doubleItemsArr, 0, doubleItemsArr.length);
    } else {
      wmem.putFloat(offset, getMinFloatValue());
      offset += Float.BYTES;
      wmem.putFloat(offset, getMaxFloatValue());
      offset += Float.BYTES;
      final float[] floatItemsArr = getFloatItemsArray();
      wmem.putFloatArray(offset, floatItemsArr, 0, floatItemsArr.length);
    }
    return byteArr;
  }

  final void updateDouble(final double value) {
    if (Double.isNaN(value)) { return; }
    if (isEmpty()) {
      setMinDoubleValue(value);
      setMaxDoubleValue(value);
    } else {
      if (value < getMinDoubleValue()) { setMinDoubleValue(value); }
      if (value > getMaxDoubleValue()) { setMaxDoubleValue(value); }
    }
    if (getLevelsArrayAt(0) == 0) { compressWhileUpdatingSketch(); }
    incN();
    setLevelZeroSorted(false);
    final int nextPos = getLevelsArrayAt(0) - 1;
    assert getLevelsArrayAt(0) >= 0;
    setLevelsArrayAt(0, nextPos);
    setDoubleItemsArrayAt(nextPos, value);
  }

  final void updateFloat(final float value) {
    if (Float.isNaN(value)) { return; }
    if (isEmpty()) {
      setMinFloatValue(value);
      setMaxFloatValue(value);
    } else {
      if (value < getMinFloatValue()) { setMinFloatValue(value); }
      if (value > getMaxFloatValue()) { setMaxFloatValue(value); }
    }

    if (getLevelsArrayAt(0) == 0) { compressWhileUpdatingSketch(); }
    incN();
    setLevelZeroSorted(false);
    final int nextPos = getLevelsArrayAt(0) - 1;
    assert getLevelsArrayAt(0) >= 0;
    setLevelsArrayAt(0, nextPos);
    setFloatItemsArrayAt(nextPos, value);
  }

  //Private non-static methods

  /**
   * This grows the levels arr by 1 (if needed) and increases the capacity of the items array
   * at the bottom.  Only numLevels, the levels array and the items array are affected.
   */
  @SuppressWarnings("null")
  private void addEmptyTopLevelToCompletelyFullSketch() {
    final int[] myCurLevelsArr = getLevelsArray();
    final int myCurNumLevels = getNumLevels();
    final int myCurTotalItemsCapacity = myCurLevelsArr[myCurNumLevels];
    double minDouble = Double.NaN;
    double maxDouble = Double.NaN;
    float minFloat = Float.NaN;
    float maxFloat = Float.NaN;

    double[] myCurDoubleItemsArr = null;
    float[] myCurFloatItemsArr = null;

    final int myNewNumLevels;
    final int[] myNewLevelsArr;
    final int myNewTotalItemsCapacity;

    float[] myNewFloatItemsArr = null;
    double[] myNewDoubleItemsArr = null;

    if (sketchType == DOUBLES_SKETCH) {
      minDouble = getMinDoubleValue();
      maxDouble = getMaxDoubleValue();
      myCurDoubleItemsArr = getDoubleItemsArray();
      //assert we are following a certain growth scheme
      assert myCurDoubleItemsArr.length == myCurTotalItemsCapacity;
    } else { //FLOATS_SKETCH
      minFloat = getMinFloatValue();
      maxFloat = getMaxFloatValue();
      myCurFloatItemsArr = getFloatItemsArray();
      assert myCurFloatItemsArr.length == myCurTotalItemsCapacity;
    }
    assert myCurLevelsArr[0] == 0; //definition of full is part of the growth scheme

    final int deltaItemsCap = KllHelper.levelCapacity(getK(), myCurNumLevels + 1, 0, M);
    myNewTotalItemsCapacity = myCurTotalItemsCapacity + deltaItemsCap;

    // Check if growing the levels arr if required.
    // Note that merging MIGHT over-grow levels_, in which case we might not have to grow it
    final boolean growLevelsArr = myCurLevelsArr.length < myCurNumLevels + 2;

    // GROW LEVELS ARRAY
    if (growLevelsArr) {
      //grow levels arr by one and copy the old data to the new array, extra space at the top.
      myNewLevelsArr = Arrays.copyOf(myCurLevelsArr, myCurNumLevels + 2);
      assert myNewLevelsArr.length == myCurLevelsArr.length + 1;
      myNewNumLevels = myCurNumLevels + 1;
      incNumLevels(); //increment the class member
    } else {
      myNewLevelsArr = myCurLevelsArr;
      myNewNumLevels = myCurNumLevels;
    }
    // This loop updates all level indices EXCLUDING the "extra" index at the top
    for (int level = 0; level <= myNewNumLevels - 1; level++) {
      myNewLevelsArr[level] += deltaItemsCap;
    }
    myNewLevelsArr[myNewNumLevels] = myNewTotalItemsCapacity; // initialize the new "extra" index at the top

    // GROW ITEMS ARRAY
    if (sketchType == DOUBLES_SKETCH) {
      myNewDoubleItemsArr = new double[myNewTotalItemsCapacity];
      // copy and shift the current data into the new array
      System.arraycopy(myCurDoubleItemsArr, 0, myNewDoubleItemsArr, deltaItemsCap, myCurTotalItemsCapacity);
    } else {
      myNewFloatItemsArr = new float[myNewTotalItemsCapacity];
      // copy and shift the current items data into the new array
      System.arraycopy(myCurFloatItemsArr, 0, myNewFloatItemsArr, deltaItemsCap, myCurTotalItemsCapacity);
    }

    //MEMORY SPACE MANAGEMENT
    if (direct) {
      wmem = memorySpaceMgmt(this, myNewLevelsArr.length, myNewTotalItemsCapacity);
    }
    //update our sketch with new expanded spaces
    setNumLevels(myNewNumLevels);
    setLevelsArray(myNewLevelsArr);
    if (sketchType == DOUBLES_SKETCH) {
      setMinDoubleValue(minDouble);
      setMaxDoubleValue(maxDouble);
      setDoubleItemsArray(myNewDoubleItemsArr);
    } else { //Float sketch
      setMinFloatValue(minFloat);
      setMaxFloatValue(maxFloat);
      setFloatItemsArray(myNewFloatItemsArr);
    }
  }

  // The following code is only valid in the special case of exactly reaching capacity while updating.
  // It cannot be used while merging, while reducing k, or anything else.
  @SuppressWarnings("null")
  private void compressWhileUpdatingSketch() {
    final int level = KllHelper.findLevelToCompact(getK(), M, getNumLevels(), getLevelsArray());
    if (level == getNumLevels() - 1) {
      //The level to compact is the top level, thus we need to add a level.
      //Be aware that this operation grows the items array,
      //shifts the items data and the level boundaries of the data,
      //and grows the levels array and increments numLevels_.
      addEmptyTopLevelToCompletelyFullSketch();
    }

    final int[] myLevelsArr = getLevelsArray();
    final int rawBeg = myLevelsArr[level];
    final int rawEnd = myLevelsArr[level + 1];
    // +2 is OK because we already added a new top level if necessary
    final int popAbove = myLevelsArr[level + 2] - rawEnd;
    final int rawPop = rawEnd - rawBeg;
    final boolean oddPop = isOdd(rawPop);
    final int adjBeg = oddPop ? rawBeg + 1 : rawBeg;
    final int adjPop = oddPop ? rawPop - 1 : rawPop;
    final int halfAdjPop = adjPop / 2;

    // level zero might not be sorted, so we must sort it if we wish to compact it
    float[] myFloatItemsArr;
    double[] myDoubleItemsArr;

    if (sketchType == DOUBLES_SKETCH) {
      myFloatItemsArr = null;
      myDoubleItemsArr = getDoubleItemsArray();
      if (level == 0) {
        if (direct) {
          myDoubleItemsArr = getDoubleItemsArray();
          Arrays.sort(myDoubleItemsArr, adjBeg, adjBeg + adjPop);
          setDoubleItemsArray(myDoubleItemsArr);
        } else {
          Arrays.sort(getDoubleItemsArray(), adjBeg, adjBeg + adjPop);
        }
      }
      if (popAbove == 0) {
        if (direct) {
          myDoubleItemsArr = getDoubleItemsArray();
          KllDoublesHelper.randomlyHalveUpDoubles(myDoubleItemsArr, adjBeg, adjPop, random);
          setDoubleItemsArray(myDoubleItemsArr);
        } else {
          KllDoublesHelper.randomlyHalveUpDoubles(getDoubleItemsArray(), adjBeg, adjPop, random);
        }
      } else {
        if (direct) {
          myDoubleItemsArr = getDoubleItemsArray();
          KllDoublesHelper.randomlyHalveDownDoubles(myDoubleItemsArr, adjBeg, adjPop, random);
          setDoubleItemsArray(myDoubleItemsArr);
        } else {
          KllDoublesHelper.randomlyHalveDownDoubles(getDoubleItemsArray(), adjBeg, adjPop, random);
        }
        if (direct ) {
          myDoubleItemsArr = getDoubleItemsArray();
          KllDoublesHelper.mergeSortedDoubleArrays(
              myDoubleItemsArr, adjBeg, halfAdjPop,
              myDoubleItemsArr, rawEnd, popAbove,
              myDoubleItemsArr, adjBeg + halfAdjPop);
          setDoubleItemsArray(myDoubleItemsArr);
        } else {
          myDoubleItemsArr = getDoubleItemsArray();
          KllDoublesHelper.mergeSortedDoubleArrays(
              myDoubleItemsArr, adjBeg, halfAdjPop,
              myDoubleItemsArr, rawEnd, popAbove,
              myDoubleItemsArr, adjBeg + halfAdjPop);
        }
      }
    } else { //Float sketch
      myFloatItemsArr = getFloatItemsArray();
      myDoubleItemsArr = null;
      if (level == 0) {
        if (direct) {
          myFloatItemsArr = getFloatItemsArray();
          Arrays.sort(myFloatItemsArr, adjBeg, adjBeg + adjPop);
          setFloatItemsArray(myFloatItemsArr);
        } else {
          Arrays.sort(getFloatItemsArray(), adjBeg, adjBeg + adjPop);
        }
      }
      if (popAbove == 0) {
        if (direct) {
          myFloatItemsArr = getFloatItemsArray();
          KllFloatsHelper.randomlyHalveUpFloats(myFloatItemsArr, adjBeg, adjPop, random);
          setFloatItemsArray(myFloatItemsArr);
        } else {
          KllFloatsHelper.randomlyHalveUpFloats(getFloatItemsArray(), adjBeg, adjPop, random);
        }
      } else {
        if (direct) {
          myFloatItemsArr = getFloatItemsArray();
          KllFloatsHelper.randomlyHalveDownFloats(myFloatItemsArr, adjBeg, adjPop, random);
          setFloatItemsArray(myFloatItemsArr);
        } else {
          KllFloatsHelper.randomlyHalveDownFloats(getFloatItemsArray(), adjBeg, adjPop, random);
        }
        if (direct ) {
          myFloatItemsArr = getFloatItemsArray();
          KllFloatsHelper.mergeSortedFloatArrays(
              myFloatItemsArr, adjBeg, halfAdjPop,
              myFloatItemsArr, rawEnd, popAbove,
              myFloatItemsArr, adjBeg + halfAdjPop);
          setFloatItemsArray(myFloatItemsArr);
        } else {
          myFloatItemsArr = getFloatItemsArray();
          KllFloatsHelper.mergeSortedFloatArrays(
              myFloatItemsArr, adjBeg, halfAdjPop,
              myFloatItemsArr, rawEnd, popAbove,
              myFloatItemsArr, adjBeg + halfAdjPop);
        }
      }
    }
    setLevelsArrayAtMinusEq(level + 1, halfAdjPop); // adjust boundaries of the level above

    if (oddPop) {
      setLevelsArrayAt(level, getLevelsArrayAt(level + 1) - 1); // the current level now contains one item
      if (sketchType == DOUBLES_SKETCH) {
        setDoubleItemsArrayAt(getLevelsArrayAt(level), getDoubleItemsArrayAt(rawBeg)); // namely this leftover guy
      } else {
        setFloatItemsArrayAt(getLevelsArrayAt(level), getFloatItemsArrayAt(rawBeg)); // namely this leftover guy
      }

    } else {
      setLevelsArrayAt(level, getLevelsArrayAt(level + 1)); // the current level is now empty
    }

    // verify that we freed up halfAdjPop array slots just below the current level
    assert getLevelsArrayAt(level) == rawBeg + halfAdjPop;

    // finally, we need to shift up the data in the levels below
    // so that the freed-up space can be used by level zero
    if (level > 0) {
      final int amount = rawBeg - getLevelsArrayAt(0);
      if (sketchType == DOUBLES_SKETCH) {
        if (direct) {
          myDoubleItemsArr = getDoubleItemsArray();
          System.arraycopy(myDoubleItemsArr, myLevelsArr[0], myDoubleItemsArr, myLevelsArr[0] + halfAdjPop, amount);
          setDoubleItemsArray(myDoubleItemsArr);
        } else {
          System.arraycopy(myDoubleItemsArr, myLevelsArr[0], myDoubleItemsArr, myLevelsArr[0] + halfAdjPop, amount);
        }
      } else {
        if (direct) {
          myFloatItemsArr = getFloatItemsArray();
          System.arraycopy(myFloatItemsArr, myLevelsArr[0], myFloatItemsArr, myLevelsArr[0] + halfAdjPop, amount);
          setFloatItemsArray(myFloatItemsArr);
        } else {
          System.arraycopy(myFloatItemsArr, myLevelsArr[0], myFloatItemsArr, myLevelsArr[0] + halfAdjPop, amount);
        }
      }
      for (int lvl = 0; lvl < level; lvl++) {
        setLevelsArrayAtPlusEq(lvl, halfAdjPop);
      }
    }
  }

  private KllDoublesQuantileCalculator getDoublesQuantileCalculator() {
    final int[] myLevelsArr = getLevelsArray();
    final double[] myDoubleItemsArr = getDoubleItemsArray();
    if (!isLevelZeroSorted()) {
      Arrays.sort(getDoubleItemsArray(), myLevelsArr[0], myLevelsArr[1]);
      setLevelZeroSorted(true);
    }
    return new KllDoublesQuantileCalculator(myDoubleItemsArr, myLevelsArr, getNumLevels(), getN());
  }

  private KllFloatsQuantileCalculator getFloatsQuantileCalculator() {
    final int[] myLevelsArr = getLevelsArray();
    final float[] myFloatItemsArr = getFloatItemsArray();
    if (!isLevelZeroSorted()) {
      Arrays.sort(myFloatItemsArr, myLevelsArr[0], myLevelsArr[1]);
      setLevelZeroSorted(true);
    }
    return new KllFloatsQuantileCalculator(myFloatItemsArr, myLevelsArr, getNumLevels(), getN());
  }

  private void incrementDoublesBucketsSortedLevel(final int fromIndex, final int toIndex,
      final int weight, final double[] splitPoints, final double[] buckets) {
    final double[] myDoubleItemsArr = getDoubleItemsArray();
    int i = fromIndex;
    int j = 0;
    while (i <  toIndex && j < splitPoints.length) {
      if (myDoubleItemsArr[i] < splitPoints[j]) {
        buckets[j] += weight; // this sample goes into this bucket
        i++; // move on to next sample and see whether it also goes into this bucket
      } else {
        j++; // no more samples for this bucket
      }
    }
    // now either i == toIndex (we are out of samples), or
    // j == numSplitPoints (we are out of buckets, but there are more samples remaining)
    // we only need to do something in the latter case
    if (j == splitPoints.length) {
      buckets[j] += weight * (toIndex - i);
    }
  }

  private void incrementDoublesBucketsUnsortedLevel(final int fromIndex, final int toIndex,
      final int weight, final double[] splitPoints, final double[] buckets) {
    final double[] myDoubleItemsArr = getDoubleItemsArray();
    for (int i = fromIndex; i < toIndex; i++) {
      int j;
      for (j = 0; j < splitPoints.length; j++) {
        if (myDoubleItemsArr[i] < splitPoints[j]) {
          break;
        }
      }
      buckets[j] += weight;
    }
  }

  private void incrementFloatBucketsSortedLevel(final int fromIndex, final int toIndex,
      final int weight, final float[] splitPoints, final double[] buckets) {
    final float[] myFloatItemsArr = getFloatItemsArray();
    int i = fromIndex;
    int j = 0;
    while (i <  toIndex && j < splitPoints.length) {
      if (myFloatItemsArr[i] < splitPoints[j]) {
        buckets[j] += weight; // this sample goes into this bucket
        i++; // move on to next sample and see whether it also goes into this bucket
      } else {
        j++; // no more samples for this bucket
      }
    }
    // now either i == toIndex (we are out of samples), or
    // j == numSplitPoints (we are out of buckets, but there are more samples remaining)
    // we only need to do something in the latter case
    if (j == splitPoints.length) {
      buckets[j] += weight * (toIndex - i);
    }
  }

  private void incrementFloatBucketsUnsortedLevel(final int fromIndex, final int toIndex,
      final int weight, final float[] splitPoints, final double[] buckets) {
    final float[] myFloatItemsArr = getFloatItemsArray();
    for (int i = fromIndex; i < toIndex; i++) {
      int j;
      for (j = 0; j < splitPoints.length; j++) {
        if (myFloatItemsArr[i] < splitPoints[j]) {
          break;
        }
      }
      buckets[j] += weight;
    }
  }

  private void populateDoubleWorkArrays(final KllSketch other, final double[] workbuf,
      final int[] worklevels, final int provisionalNumLevels) {
    worklevels[0] = 0;
    final int[] myLevelsArr = getLevelsArray();
    final int[] otherLevelsArr = other.getLevelsArray();
    final double[] myDoubleItemsArr = getDoubleItemsArray();
    final double[] otherDoubleItemsArr = other.getDoubleItemsArray();

    // Note: the level zero data from "other" was already inserted into "self"
    final int selfPopZero = KllHelper.currentLevelSize(0, getNumLevels(),myLevelsArr);
    System.arraycopy(myDoubleItemsArr, myLevelsArr[0], workbuf, worklevels[0], selfPopZero);
    worklevels[1] = worklevels[0] + selfPopZero;

    for (int lvl = 1; lvl < provisionalNumLevels; lvl++) {
      final int selfPop = KllHelper.currentLevelSize(lvl, getNumLevels(), myLevelsArr);
      final int otherPop = KllHelper.currentLevelSize(lvl, other.getNumLevels(), otherLevelsArr);
      worklevels[lvl + 1] = worklevels[lvl] + selfPop + otherPop;

      if (selfPop > 0 && otherPop == 0) {
        System.arraycopy(myDoubleItemsArr, myLevelsArr[lvl], workbuf, worklevels[lvl], selfPop);
      } else if (selfPop == 0 && otherPop > 0) {
        System.arraycopy(otherDoubleItemsArr, otherLevelsArr[lvl], workbuf, worklevels[lvl], otherPop);
      } else if (selfPop > 0 && otherPop > 0) {
        KllDoublesHelper.mergeSortedDoubleArrays(myDoubleItemsArr, myLevelsArr[lvl], selfPop, otherDoubleItemsArr,
            otherLevelsArr[lvl], otherPop, workbuf, worklevels[lvl]);
      }
    }
  }

  private void populateFloatWorkArrays(final KllSketch other, final float[] workbuf,
      final int[] worklevels, final int provisionalNumLevels) {
    worklevels[0] = 0;
    final int[] myLevelsArr = getLevelsArray();
    final int[] otherLevelsArr = other.getLevelsArray();
    final float[] myFloatItemsArr = getFloatItemsArray();
    final float[] otherFloatItemsArr = other.getFloatItemsArray();

    // Note: the level zero data from "other" was already inserted into "self"
    final int selfPopZero = KllHelper.currentLevelSize(0, getNumLevels(), myLevelsArr);
    System.arraycopy( myFloatItemsArr, myLevelsArr[0], workbuf, worklevels[0], selfPopZero);
    worklevels[1] = worklevels[0] + selfPopZero;

    for (int lvl = 1; lvl < provisionalNumLevels; lvl++) {
      final int selfPop = KllHelper.currentLevelSize(lvl, getNumLevels(), myLevelsArr);
      final int otherPop = KllHelper.currentLevelSize(lvl, other.getNumLevels(), otherLevelsArr);
      worklevels[lvl + 1] = worklevels[lvl] + selfPop + otherPop;

      if (selfPop > 0 && otherPop == 0) {
        System.arraycopy( myFloatItemsArr, myLevelsArr[lvl], workbuf, worklevels[lvl], selfPop);
      } else if (selfPop == 0 && otherPop > 0) {
        System.arraycopy(otherFloatItemsArr, otherLevelsArr[lvl], workbuf, worklevels[lvl], otherPop);
      } else if (selfPop > 0 && otherPop > 0) {
        KllFloatsHelper.mergeSortedFloatArrays( myFloatItemsArr, myLevelsArr[lvl], selfPop, otherFloatItemsArr,
            otherLevelsArr[lvl], otherPop, workbuf, worklevels[lvl]);
      }
    }
  }

}
