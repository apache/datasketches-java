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
import static org.apache.datasketches.kll.KllHelper.getAllLevelStatsGivenN;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_DOUBLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.DEFAULT_M;
import static org.apache.datasketches.kll.KllPreambleUtil.DOUBLES_SKETCH_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.DY_MIN_K_SHORT_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.EMPTY_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.FAMILY_BYTE_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.FLAGS_BYTE_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.K_SHORT_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.LEVEL_ZERO_SORTED_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.MAX_K;
import static org.apache.datasketches.kll.KllPreambleUtil.MIN_K;
import static org.apache.datasketches.kll.KllPreambleUtil.M_BYTE_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.NUM_LEVELS_BYTE_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.N_LONG_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_BYTE_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_DOUBLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SER_VER_BYTE_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.SINGLE_ITEM_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.UPDATABLE_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.SketchType.DOUBLE_SKETCH;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.kll.KllHelper.LevelStats;
import org.apache.datasketches.kll.KllPreambleUtil.SketchType;
import org.apache.datasketches.memory.WritableMemory;


/**
 * These methods are used by both direct and on-heap as well as Double and Float type sketches.
 *
 * @author lrhodes
 */
abstract class KllSketch {
  static final Random random = new Random();
  static final int M = DEFAULT_M; // configured minimum buffer "width", Must always be 8 for now.
  static final boolean compatible = true; //rank 0.0 and 1.0. compatible with classic Quantiles Sketch
  static SketchType sketchType;

  KllSketch(final SketchType sketchType) {
    KllSketch.sketchType = sketchType;
  }

  //Static methods

  /**
   * Gets the approximate value of <em>k</em> to use given epsilon, the normalized rank error.
   * @param epsilon the normalized rank error between zero and one.
   * @param pmf if true, this function returns the value of <em>k</em> assuming the input epsilon
   * is the desired "double-sided" epsilon for the getPMF() function. Otherwise, this function
   * returns the value of <em>k</em> assuming the input epsilon is the desired "single-sided"
   * epsilon for all the other queries.
   * @return the value of <i>k</i> given a value of epsilon.
   * @see KllDoublesSketch
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
   * Returns upper bound on the compact serialized size of a sketch given a parameter <em>k</em> and stream
   * length. This method can be used if allocation of storage is necessary beforehand.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @return upper bound on the compact serialized size
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n) {
    final LevelStats lvlStats = getAllLevelStatsGivenN(k, M, n, false, false, sketchType);
    return lvlStats.getCompactBytes();
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

  static int getSerializedSizeBytes(final int numLevels, final int numRetained, final SketchType sketchType,
      final boolean updatable) {
    int levelsBytes = 0;
    if (!updatable) {
      if (numRetained == 0) { return N_LONG_ADR; }
      if (numRetained == 1) {
        return DATA_START_ADR_SINGLE_ITEM + (sketchType == DOUBLE_SKETCH ? Double.BYTES : Float.BYTES);
      }
      levelsBytes = numLevels * Integer.BYTES;
    } else {
      levelsBytes = (numLevels + 1) * Integer.BYTES;
    }
    if (sketchType == DOUBLE_SKETCH) {
      return DATA_START_ADR_DOUBLE + levelsBytes + (numRetained + 2) * Double.BYTES; //+2 is for min & max
    } else {
      return DATA_START_ADR_FLOAT + levelsBytes + (numRetained + 2) * Float.BYTES;
    }
  }

  final static boolean isCompatible() {
    return compatible;
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
   * @see KllDoublesSketch
   */
  public final double getNormalizedRankError(final boolean pmf) {
    return KllHelper.getNormalizedRankError(getDyMinK(), pmf);
  }

  /**
   * Returns the number of retained items (samples) in the sketch.
   * @return the number of retained items (samples) in the sketch
   */
  public final int getNumRetained() {
    return getLevelsArray()[getNumLevels()] - getLevelsArray()[0];
  }

  /**
   * Returns the number of bytes this sketch would require to store.
   * @return the number of bytes this sketch would require to store.
   * @deprecated use <i>getCurrentCompactSerializedSizeBytes()</i>
   */
  @Deprecated
  public int getSerializedSizeBytes() {
    return getCurrentCompactSerializedSizeBytes();
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

  public abstract boolean isUpdatable();

  /**
   * Returns serialized sketch in a compact byte array form.
   * @return serialized sketch in a compact byte array form.
   */
  public abstract byte[] toByteArray();

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
  public abstract String toString(final boolean withLevels, final boolean withData);

  /**
   * Returns serialized sketch in an updatable byte array form.
   * @return serialized sketch in an updatable byte array form.
   */
  public abstract byte[] toUpdatableByteArray();

  //package-private non-static methods

  final void buildHeapKllSketchFromMemory(final MemoryValidate memVal) {
    final boolean doubleType = (sketchType == DOUBLE_SKETCH);
    final boolean updatable = memVal.updatable;
    setLevelZeroSorted(memVal.level0Sorted);
    setN(memVal.n);
    setDyMinK(memVal.dyMinK);
    setNumLevels(memVal.numLevels);
    final int[] myLevelsArr = new int[getNumLevels() + 1];

    if (updatable) {
      memVal.levelsWmem.getIntArray(0, myLevelsArr, 0, getNumLevels() + 1);
      setLevelsArray(myLevelsArr);
      if (doubleType) {
        setMinDoubleValue(memVal.minMaxWmem.getDouble(0));
        setMaxDoubleValue(memVal.minMaxWmem.getDouble(Double.BYTES));
        final int itemsCap = (int)memVal.itemsWmem.getCapacity() / Double.BYTES;
        final double[] myItemsArr = new double[itemsCap];
        memVal.itemsWmem.getDoubleArray(0, myItemsArr, 0, itemsCap);
        setDoubleItemsArray(myItemsArr);
      } else { //float
        setMinFloatValue(memVal.minMaxWmem.getFloat(0));
        setMaxFloatValue(memVal.minMaxWmem.getFloat(Float.BYTES));
        final int itemsCap = (int)memVal.itemsWmem.getCapacity() / Float.BYTES;
        final float[] myItemsArr = new float[itemsCap];
        memVal.itemsWmem.getFloatArray(0, myItemsArr, 0, itemsCap);
        setFloatItemsArray(myItemsArr);
      }
    } else { //compact
      memVal.levelsMem.getIntArray(0, myLevelsArr, 0, getNumLevels() + 1);
      setLevelsArray(myLevelsArr);
      if (doubleType) {
        setMinDoubleValue(memVal.minMaxMem.getDouble(0));
        setMaxDoubleValue(memVal.minMaxMem.getDouble(Double.BYTES));
        final int itemsCap = (int)memVal.itemsMem.getCapacity() / Double.BYTES;
        final double[] myItemsArr = new double[itemsCap];
        memVal.itemsMem.getDoubleArray(0, myItemsArr, 0, itemsCap);
        setDoubleItemsArray(myItemsArr);
      } else { //float
        setMinFloatValue(memVal.minMaxMem.getFloat(0));
        setMaxFloatValue(memVal.minMaxMem.getFloat(Float.BYTES));
        final int itemsCap = (int)memVal.itemsMem.getCapacity() / Float.BYTES;
        final float[] myItemsArr = new float[itemsCap];
        memVal.itemsMem.getFloatArray(0, myItemsArr, 0, itemsCap);
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

  abstract String getLayout();

  abstract int[] getLevelsArray();

  abstract double getMaxDoubleValue();

  abstract float getMaxFloatValue();

  abstract double getMinDoubleValue();

  abstract float getMinFloatValue();

  abstract int getNumLevels();

  abstract void incN();

  abstract void incNumLevels();

  abstract boolean isLevelZeroSorted();

  final void mergeDouble(final KllDoublesSketch other) {
    if (other == null || other.isEmpty()) { return; }
    final long finalN = getN() + other.getN();
    //update this sketch with level0 items from the other sketch
    final double[] otherDoubleItemsArr = other.getDoubleItemsArray();
    final int[] otherLevelsArr = other.getLevelsArray();
    for (int i = otherLevelsArr[0]; i < otherLevelsArr[1]; i++) {
      updateDouble(otherDoubleItemsArr[i]);
    }
    if (other.getNumLevels() >= 2) { //now merge other levels if they exist
      mergeDoubleHigherLevels(other, finalN);
    }
    //update min, max values, n
    final double myMin = getMinDoubleValue();
    final double otherMin = other.getMinDoubleValue();
    final double myMax = getMaxDoubleValue();
    final double otherMax = other.getMaxDoubleValue();
    if (Double.isNaN(myMin) || otherMin < myMin) { setMinDoubleValue(otherMin); }
    if (Double.isNaN(myMax) || otherMax > myMax) { setMaxDoubleValue(otherMax); }
    setN(finalN);

    assert KllHelper.sumTheSampleWeights(getNumLevels(), getLevelsArray()) == getN();
    if (other.isEstimationMode()) {
      setDyMinK(min(getDyMinK(), other.getDyMinK()));
    }
  }

  final void mergeFloat(final KllFloatsSketch other) {
    if (other == null || other.isEmpty()) { return; }
    final long finalN = getN() + other.getN();
    //update this sketch with level0 items from the other sketch
    final float[] otherFloatItemsArr = other.getFloatItemsArray();
    final int[] otherLevelsArr = other.getLevelsArray();
    for (int i = otherLevelsArr[0]; i < otherLevelsArr[1]; i++) {
      updateFloat(otherFloatItemsArr[i]);
    }
    if (other.getNumLevels() >= 2) { //now merge other levels if they exist
      mergeFloatHigherLevels(other, finalN);
    }
    //update min, max values, n
    final float myMin = getMinFloatValue();
    final float otherMin = other.getMinFloatValue();
    final float myMax = getMaxFloatValue();
    final float otherMax = other.getMaxFloatValue();
    if (Float.isNaN(myMin) || otherMin < myMin) { setMinFloatValue(otherMin); }
    if (Float.isNaN(myMax) || otherMax > myMax) { setMaxFloatValue(otherMax); }
    setN(finalN);

    assert KllHelper.sumTheSampleWeights(getNumLevels(), getLevelsArray()) == getN();
    if (other.isEstimationMode()) {
      setDyMinK(min(getDyMinK(), other.getDyMinK()));
    }
  }

  abstract void setDoubleItemsArray(double[] floatItems);

  abstract void setDyMinK(int dyMinK);

  abstract void setFloatItemsArray(float[] floatItems);

  //Only for internal changes to the array, NOT for changing its size
  abstract void updateLevelsArray(int[] levels);

  abstract void setLevelsArray(int[] levelsArr);

  abstract void setLevelZeroSorted(boolean sorted);

  abstract void setMaxDoubleValue(double value);

  abstract void setMaxFloatValue(float value);

  abstract void setMinDoubleValue(double value);

  abstract void setMinFloatValue(float value);

  abstract void setN(long n);

  abstract void setNumLevels(int numLevels);

  final byte[] toGenericCompactByteArray() { //From Heap Only
    final boolean doubleType = (sketchType == DOUBLE_SKETCH);
    final byte[] byteArr = new byte[getCurrentCompactSerializedSizeBytes()];
    final WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    final boolean singleItem = getN() == 1;
    final boolean empty = isEmpty();
    //load the preamble
    if (doubleType) {
      wmem.putByte(PREAMBLE_INTS_BYTE_ADR, (byte)
          (empty || singleItem ? PREAMBLE_INTS_EMPTY_SINGLE : PREAMBLE_INTS_DOUBLE));
    } else {
      wmem.putByte(PREAMBLE_INTS_BYTE_ADR, (byte)
          (empty || singleItem ? PREAMBLE_INTS_EMPTY_SINGLE : PREAMBLE_INTS_FLOAT));
    }
    wmem.putByte(SER_VER_BYTE_ADR, singleItem ? SERIAL_VERSION_SINGLE : SERIAL_VERSION_EMPTY_FULL);
    wmem.putByte(FAMILY_BYTE_ADR, (byte) Family.KLL.getID());
    byte flags = (byte) (
        (empty ? EMPTY_BIT_MASK : 0)
      | (singleItem ? SINGLE_ITEM_BIT_MASK : 0)
      | (isLevelZeroSorted() ? LEVEL_ZERO_SORTED_BIT_MASK : 0));

    flags |= (byte) (doubleType ? DOUBLES_SKETCH_BIT_MASK : 0);
    wmem.putByte(FLAGS_BYTE_ADR, flags);
    wmem.putShort(K_SHORT_ADR, (short) getK());
    wmem.putByte(M_BYTE_ADR, (byte) M);
    if (empty) { return byteArr; }

    //load data
    int offset = DATA_START_ADR_SINGLE_ITEM;
    final int[] myLevelsArr = getLevelsArray();
    if (!singleItem) {
      wmem.putLong(N_LONG_ADR, getN());
      wmem.putShort(DY_MIN_K_SHORT_ADR, (short) getDyMinK());
      wmem.putByte(NUM_LEVELS_BYTE_ADR, (byte) getNumLevels());
      offset = (doubleType) ? DATA_START_ADR_DOUBLE : DATA_START_ADR_FLOAT;
      // the last integer in levels_ is not serialized because it can be derived
      final int len = myLevelsArr.length - 1;
      wmem.putIntArray(offset, myLevelsArr, 0, len);
      offset += len * Integer.BYTES;
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
    } else { //single item
      if (doubleType) {
        final double value = getDoubleItemsArray()[myLevelsArr[0]];
        wmem.putDouble(offset, value);
      } else {
        final float value = getFloatItemsArray()[myLevelsArr[0]];
        wmem.putFloat(offset, value);
      }
    }
    return byteArr;
  }

  @SuppressWarnings("null")
  final String toGenericString(final boolean withLevels, final boolean withData) {
    final boolean doubleType = (sketchType == DOUBLE_SKETCH);
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
      sb.append("### KLL levels array:").append(Util.LS)
      .append(" level, offset: nominal capacity, actual size").append(Util.LS);
      int level = 0;
      for ( ; level < myNumLevels; level++) {
        sb.append("   ").append(level).append(", ").append(myLevelsArr[level]).append(": ")
        .append(KllHelper.levelCapacity(k, myNumLevels, level, M))
        .append(", ").append(KllHelper.currentLevelSize(level, myNumLevels, myLevelsArr)).append(Util.LS);
      }
      sb.append("   ").append(level).append(", ").append(myLevelsArr[level]).append(": (Exclusive)")
      .append(Util.LS);
      sb.append("### End levels array").append(Util.LS);
    }

    if (withData) {
      sb.append("### KLL items data {index, item}:").append(Util.LS);
      if (myLevelsArr[0] > 0) {
        sb.append(" Garbage:" + Util.LS);
        if (doubleType) {
          for (int i = 0; i < myLevelsArr[0]; i++) {
            sb.append("   ").append(i + ", ").append(myDoubleItemsArr[i]).append(Util.LS);
          }
        } else {
          for (int i = 0; i < myLevelsArr[0]; i++) {
            sb.append("   ").append(i + ", ").append(myFloatItemsArr[i]).append(Util.LS);
          }
        }
      }
      int level = 0;
      if (doubleType) {
        while (level < myNumLevels) {
          final int fromIndex = myLevelsArr[level];
          final int toIndex = myLevelsArr[level + 1]; // exclusive
          if (fromIndex < toIndex) {
            sb.append(" level[").append(level).append("]: offset: " + myLevelsArr[level] + " wt: " + (1 << level));
            sb.append(Util.LS);
          }

          for (int i = fromIndex; i < toIndex; i++) {
            sb.append("   ").append(i + ", ").append(myDoubleItemsArr[i]).append(Util.LS);
          }
          level++;
        }
      }
      else {
        while (level < myNumLevels) {
          final int fromIndex = myLevelsArr[level];
          final int toIndex = myLevelsArr[level + 1]; // exclusive
          if (fromIndex <= toIndex) {
            sb.append(" level[").append(level).append("]: offset: " + myLevelsArr[level] + " wt: " + (1 << level));
            sb.append(Util.LS);
          }

          for (int i = fromIndex; i < toIndex; i++) {
            sb.append("   ").append(i + ", ").append(myFloatItemsArr[i]).append(Util.LS);
          }
          level++;
        }
      }
      sb.append(" level[" + level + "]: offset: " + myLevelsArr[level] + " (Exclusive)");
      sb.append(Util.LS);
      sb.append("### End items data").append(Util.LS);
    }
    return sb.toString();
  }

  final byte[] toGenericUpdatableByteArray() {
    final boolean doubleType = (sketchType == DOUBLE_SKETCH);
    final byte[] byteArr = new byte[getCurrentUpdatableSerializedSizeBytes()];
    final WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    final boolean singleItem = getN() == 1;
    final boolean empty = isEmpty();
    //load the preamble
    if (doubleType) {
      wmem.putByte(PREAMBLE_INTS_BYTE_ADR, (byte) PREAMBLE_INTS_DOUBLE); //ignore empty, singleItem
    } else {
      wmem.putByte(PREAMBLE_INTS_BYTE_ADR, (byte) PREAMBLE_INTS_FLOAT); //ignore empty, singleItem
    }
    wmem.putByte(SER_VER_BYTE_ADR, SERIAL_VERSION_UPDATABLE);
    wmem.putByte(FAMILY_BYTE_ADR, (byte) Family.KLL.getID());
    byte flags = (byte) (
          (empty ? EMPTY_BIT_MASK : 0) //set but not used
        | (singleItem ? SINGLE_ITEM_BIT_MASK : 0) //set but not used
        | (isLevelZeroSorted() ? LEVEL_ZERO_SORTED_BIT_MASK : 0)
        | UPDATABLE_BIT_MASK);
    flags |= (byte) (doubleType ? DOUBLES_SKETCH_BIT_MASK : 0);
    wmem.putByte(FLAGS_BYTE_ADR, flags);
    wmem.putShort(K_SHORT_ADR, (short) getK());
    wmem.putByte(M_BYTE_ADR, (byte) M);
    //load data
    wmem.putLong(N_LONG_ADR, getN());
    wmem.putShort(DY_MIN_K_SHORT_ADR, (short) getDyMinK());
    wmem.putByte(NUM_LEVELS_BYTE_ADR, (byte) getNumLevels());
    int offset = (doubleType) ? DATA_START_ADR_DOUBLE : DATA_START_ADR_FLOAT;
    // the last integer in levels_ IS serialized
    final int[] myLevelsArr = getLevelsArray();
    final int len = myLevelsArr.length;
    wmem.putIntArray(offset, myLevelsArr, 0, len);
    offset += len * Integer.BYTES;
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
    int[] myLevelsArr = getLevelsArray();
    double[] myDoubleItemsArr = getDoubleItemsArray();
    if (myLevelsArr[0] == 0) {
      compressWhileUpdatingDoublesSketch();
    }
    myLevelsArr = getLevelsArray(); //refresh
    myDoubleItemsArr = getDoubleItemsArray();
    incN();
    setLevelZeroSorted(false);
    final int nextPos = myLevelsArr[0] - 1;
    assert myLevelsArr[0] >= 0;
    myLevelsArr[0] = nextPos;
    myDoubleItemsArr[nextPos] = value;
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
    int[] myLevelsArr = getLevelsArray();
    float[] myFloatItemsArr = getFloatItemsArray();
    if (myLevelsArr[0] == 0) {
      compressWhileUpdatingFloatsSketch();
    }
    myLevelsArr = getLevelsArray(); //refresh
    myFloatItemsArr = getFloatItemsArray();
    incN();
    setLevelZeroSorted(false);
    final int nextPos = myLevelsArr[0] - 1;
    assert myLevelsArr[0] >= 0;
    myLevelsArr[0] = nextPos;
    myFloatItemsArr[nextPos] = value;
  }

  //Private non-static methods

  /**
   * This grows the levels arr by 1 (if needed) and increases the capacity of the items array at the bottom
   */
  private void addEmptyTopLevelToCompletelyFullDoublesSketch() {
    final int[] myCurLevelsArr = getLevelsArray();
    final double[] myCurDoubleItemsArr = getDoubleItemsArray();
    final int myCurNumLevels = getNumLevels();
    final int myCurTotalItemsCap = myCurLevelsArr[myCurNumLevels];
    final int[] myNewLevelsArr;
    final double[] myNewDoubleItemsArr;
    final int myNewNumLevels;
    final int myNewTotalItemsCap;

    // make sure that we are following a certain growth scheme
    assert myCurLevelsArr[0] == 0; //definition of full
    assert myCurDoubleItemsArr.length == myCurTotalItemsCap;

    //this is a little out of sequence so that we can pre-compute the total required increase in space
    final int deltaItemsCap = KllHelper.levelCapacity(getK(), myCurNumLevels + 1, 0, M);
    myNewTotalItemsCap = myCurTotalItemsCap + deltaItemsCap;

    // Check if growing the levels arr if required.
    // Note that merging MIGHT over-grow levels_, in which case we might not have to grow it
    final boolean growLevelsArr = myCurLevelsArr.length < myCurNumLevels + 2;

    //INSERT SPACE MANAGEMENT HERE
    //int totalDeltaSpaceRequired = deltaItemsCap * Double.BYTES;
    //if (growLevelsArr) { totalDeltaSpaceRequired += Integer.BYTES; }
    // ...

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
    myNewLevelsArr[myNewNumLevels] = myNewTotalItemsCap; // initialize the new "extra" index at the top
    setLevelsArray(myNewLevelsArr);

    // GROW ITEMS ARRAY
    myNewDoubleItemsArr = new double[myNewTotalItemsCap];
    // copy and shift the current data into the new array
    System.arraycopy(myCurDoubleItemsArr, 0, myNewDoubleItemsArr, deltaItemsCap, myCurTotalItemsCap);
    //Update the items array
    setDoubleItemsArray(myNewDoubleItemsArr);
  }

  /**
   * This grows the levels arr by 1 (if needed) and increases the capacity of the items array at the bottom
   */
  private void addEmptyTopLevelToCompletelyFullFloatsSketch() {
    final int[] myCurLevelsArr = getLevelsArray();
    final float[] myCurFloatItemsArr = getFloatItemsArray();
    final int myCurNumLevels = getNumLevels();
    final int myCurTotalItemsCap = myCurLevelsArr[myCurNumLevels];
    final int[] myNewLevelsArr;
    final float[] myNewFloatItemsArr;
    final int myNewNumLevels;
    final int myNewTotalItemsCap;

    // make sure that we are following a certain growth scheme
    assert myCurLevelsArr[0] == 0; //definition of full
    assert myCurFloatItemsArr.length == myCurTotalItemsCap;

    //this is a little out of sequence so that we can pre-compute the total required increase in space
    final int deltaItemsCap = KllHelper.levelCapacity(getK(), myCurNumLevels + 1, 0, M);
    myNewTotalItemsCap = myCurTotalItemsCap + deltaItemsCap;

    // Check if growing the levels arr if required.
    // Note that merging MIGHT over-grow levels_, in which case we might not have to grow it
    final boolean growLevelsArr = myCurLevelsArr.length < myCurNumLevels + 2;

    //INSERT SPACE MANAGEMENT HERE
    //int totalDeltaSpaceRequired = deltaItemsCap * Float.BYTES;
    //if (growLevelsArr) { totalDeltaSpaceRequired += Integer.BYTES; }
    // ...

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
    myNewLevelsArr[myNewNumLevels] = myNewTotalItemsCap; // initialize the new "extra" index at the top
    setLevelsArray(myNewLevelsArr);

    // GROW ITEMS ARRAY
    myNewFloatItemsArr = new float[myNewTotalItemsCap];
    // copy and shift the current items data into the new array
    System.arraycopy(myCurFloatItemsArr, 0, myNewFloatItemsArr, deltaItemsCap, myCurTotalItemsCap);
    //Update the items array
    setFloatItemsArray(myNewFloatItemsArr);
  }

  // The following code is only valid in the special case of exactly reaching capacity while updating.
  // It cannot be used while merging, while reducing k, or anything else.
  private void compressWhileUpdatingDoublesSketch() {
    final int level = KllHelper.findLevelToCompact(getK(), M, getNumLevels(), getLevelsArray());

    // It is important to add the new top level right here. Be aware that this next operation
    // grows the items array, shifts the items data and the level boundaries of the data.
    // It also grows the levels array and increments numLevels_.
    if (level == getNumLevels() - 1) {
      addEmptyTopLevelToCompletelyFullDoublesSketch();
    }
    final int[] myLevelsArr = getLevelsArray(); //new levels arr
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

    final double[] myDoubleItemsArr = getDoubleItemsArray();
    if (level == 0) {
      Arrays.sort(myDoubleItemsArr, adjBeg, adjBeg + adjPop);
    }
    if (popAbove == 0) {
      KllDoublesHelper.randomlyHalveUpDoubles(myDoubleItemsArr, adjBeg, adjPop, random);
    } else {
      KllDoublesHelper.randomlyHalveDownDoubles(myDoubleItemsArr, adjBeg, adjPop, random);
      KllDoublesHelper.mergeSortedDoubleArrays(
          myDoubleItemsArr, adjBeg, halfAdjPop,
          myDoubleItemsArr, rawEnd, popAbove,
          myDoubleItemsArr, adjBeg + halfAdjPop);
    }
    myLevelsArr[level + 1] -= halfAdjPop; // adjust boundaries of the level above

    if (oddPop) {
      myLevelsArr[level] = myLevelsArr[level + 1] - 1; // the current level now contains one item
      myDoubleItemsArr[myLevelsArr[level]] = myDoubleItemsArr[rawBeg]; // namely this leftover guy
    } else {
      myLevelsArr[level] = myLevelsArr[level + 1]; // the current level is now empty
    }

    // verify that we freed up halfAdjPop array slots just below the current level
    assert myLevelsArr[level] == rawBeg + halfAdjPop;

    // finally, we need to shift up the data in the levels below
    // so that the freed-up space can be used by level zero
    if (level > 0) {
      final int amount = rawBeg - myLevelsArr[0];
      System.arraycopy(myDoubleItemsArr, myLevelsArr[0],
          myDoubleItemsArr, myLevelsArr[0] + halfAdjPop, amount);
      for (int lvl = 0; lvl < level; lvl++) {
        myLevelsArr[lvl] += halfAdjPop;
      }
    }
  }

  // The following code is only valid in the special case of exactly reaching capacity while updating.
  // It cannot be used while merging, while reducing k, or anything else.
  private void compressWhileUpdatingFloatsSketch() {
    final int level = KllHelper.findLevelToCompact(getK(), M, getNumLevels(), getLevelsArray());

    // It is important to add the new top level right here. Be aware that this next operation
    // grows the items array, shifts the items data and the level boundaries of the data.
    // It also grows the levels array and increments numLevels_.
    if (level == getNumLevels() - 1) {
      addEmptyTopLevelToCompletelyFullFloatsSketch();
    }
    final int[] myLevelsArr = getLevelsArray(); //new levels arr
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

    final float[] myFloatItemsArr = getFloatItemsArray();
    if (level == 0) {
      Arrays.sort(myFloatItemsArr, adjBeg, adjBeg + adjPop);
    }
    if (popAbove == 0) {
      KllFloatsHelper.randomlyHalveUpFloats(myFloatItemsArr, adjBeg, adjPop, random);
    } else {
      KllFloatsHelper.randomlyHalveDownFloats(myFloatItemsArr, adjBeg, adjPop, random);
      KllFloatsHelper.mergeSortedFloatArrays(
          myFloatItemsArr, adjBeg, halfAdjPop,
          myFloatItemsArr, rawEnd, popAbove,
          myFloatItemsArr, adjBeg + halfAdjPop);
    }
    myLevelsArr[level + 1] -= halfAdjPop; // adjust boundaries of the level above

    if (oddPop) {
      myLevelsArr[level] = myLevelsArr[level + 1] - 1; // the current level now contains one item
      myFloatItemsArr[myLevelsArr[level]] = myFloatItemsArr[rawBeg]; // namely this leftover guy
    } else {
      myLevelsArr[level] = myLevelsArr[level + 1]; // the current level is now empty
    }

    // verify that we freed up halfAdjPop array slots just below the current level
    assert myLevelsArr[level] == rawBeg + halfAdjPop;

    // finally, we need to shift up the data in the levels below
    // so that the freed-up space can be used by level zero
    if (level > 0) {
      final int amount = rawBeg - myLevelsArr[0];
      System.arraycopy(myFloatItemsArr, myLevelsArr[0],
          myFloatItemsArr, myLevelsArr[0] + halfAdjPop, amount);
      for (int lvl = 0; lvl < level; lvl++) {
        myLevelsArr[lvl] += halfAdjPop;
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

  private void mergeDoubleHigherLevels(final KllDoublesSketch other, final long finalN) {
    final int myCurNumLevels = getNumLevels();
    final int myCurLevelsArrLength = getLevelsArray().length;
    final int myCurItemsArrLength = getDoubleItemsArray().length;

    final int tmpSpaceNeeded = getNumRetained()
        + KllHelper.getNumRetainedAboveLevelZero(other.getNumLevels(), other.getLevelsArray());
    final double[] workbuf = new double[tmpSpaceNeeded];
    final int ub = KllHelper.ubOnNumLevels(finalN);
    final int[] worklevels = new int[ub + 2]; // ub+1 does not work
    final int[] outlevels  = new int[ub + 2];

    final int provisionalNumLevels = max(myCurNumLevels, other.getNumLevels());

    populateDoubleWorkArrays(other, workbuf, worklevels, provisionalNumLevels);

    // notice that workbuf is being used as both the input and output here
    final int[] result = KllDoublesHelper.generalDoublesCompress(getK(), M, provisionalNumLevels, workbuf,
        worklevels, workbuf, outlevels, isLevelZeroSorted(), random);
    final int finalNumLevels = result[0];
    final int finalCapacity = result[1];
    final int finalPop = result[2];

    assert finalNumLevels <= ub; // ub may be much bigger

    // now we need to transfer the results back into the "self" sketch
    final double[] newbuf = finalCapacity == myCurItemsArrLength
        ? getDoubleItemsArray() : new double[finalCapacity];
    final int freeSpaceAtBottom = finalCapacity - finalPop;
    System.arraycopy(workbuf, outlevels[0], newbuf, freeSpaceAtBottom, finalPop);
    final int theShift = freeSpaceAtBottom - outlevels[0];

    final int finalLevelsArrLen;
    if (myCurLevelsArrLength < finalNumLevels + 1) {
      finalLevelsArrLen = finalNumLevels + 1;
    } else { finalLevelsArrLen = myCurLevelsArrLength; }

    final int[] myFinalLevelsArr = new int[finalLevelsArrLen];

    for (int lvl = 0; lvl < finalNumLevels + 1; lvl++) { // includes the "extra" index
      myFinalLevelsArr[lvl] = outlevels[lvl] + theShift;
    }

    //MEMORY MANAGEMENT
//  final int itemsDeltaBytes = (newbuf.length - myCurItemsArrLength) * Double.BYTES;
//  final int levelsDeltaBytes = finalLevelsArrLen * Integer.BYTES;
//  final int totalDeltaBytes = itemsDeltaBytes + levelsDeltaBytes;

    setLevelsArray(myFinalLevelsArr);
    setDoubleItemsArray(newbuf);
    setNumLevels(finalNumLevels);
  }

  private void mergeFloatHigherLevels(final KllFloatsSketch other, final long finalN) {
    final int myCurNumLevels = getNumLevels();
    final int myCurLevelsArrLength = getLevelsArray().length;
    final int myCurItemsArrLength = getFloatItemsArray().length;

    final int tmpSpaceNeeded = getNumRetained()
        + KllHelper.getNumRetainedAboveLevelZero(other.getNumLevels(), other.getLevelsArray());
    final float[] workbuf = new float[tmpSpaceNeeded];
    final int ub = KllHelper.ubOnNumLevels(finalN);
    final int[] worklevels = new int[ub + 2]; // ub+1 does not work
    final int[] outlevels  = new int[ub + 2];

    final int provisionalNumLevels = max(myCurNumLevels, other.getNumLevels());

    populateFloatWorkArrays(other, workbuf, worklevels, provisionalNumLevels);

    // notice that workbuf is being used as both the input and output here
    final int[] result = KllFloatsHelper.generalFloatsCompress(getK(), M, provisionalNumLevels, workbuf,
        worklevels, workbuf, outlevels, isLevelZeroSorted(), random);
    final int finalNumLevels = result[0];
    final int finalCapacity = result[1];
    final int finalPop = result[2];

    assert finalNumLevels <= ub; // ub may be much bigger

    // now we need to transfer the results back into the "self" sketch
    final float[] newbuf = finalCapacity == myCurItemsArrLength
        ? getFloatItemsArray() : new float[finalCapacity];
    final int freeSpaceAtBottom = finalCapacity - finalPop;
    System.arraycopy(workbuf, outlevels[0], newbuf, freeSpaceAtBottom, finalPop);
    final int theShift = freeSpaceAtBottom - outlevels[0];

    final int finalLevelsArrLen;
    if (myCurLevelsArrLength < finalNumLevels + 1) {
      finalLevelsArrLen = finalNumLevels + 1;
    } else { finalLevelsArrLen = myCurLevelsArrLength; }

    final int[] myFinalLevelsArr = new int[finalLevelsArrLen];

    for (int lvl = 0; lvl < finalNumLevels + 1; lvl++) { // includes the "extra" index
      myFinalLevelsArr[lvl] = outlevels[lvl] + theShift;
    }

    //MEMORY MANAGEMENT
//  final int itemsDeltaBytes = (newbuf.length - myCurItemsArrLength) * Float.BYTES;
//  final int levelsDeltaBytes = finalLevelsArrLen * Integer.BYTES;
//  final int totalDeltaBytes = itemsDeltaBytes + levelsDeltaBytes;

    setLevelsArray(myFinalLevelsArr);
    setFloatItemsArray(newbuf);
    setNumLevels(finalNumLevels);
  }

  private void populateDoubleWorkArrays(final KllDoublesSketch other, final double[] workbuf,
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

  private void populateFloatWorkArrays(final KllFloatsSketch other, final float[] workbuf,
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
