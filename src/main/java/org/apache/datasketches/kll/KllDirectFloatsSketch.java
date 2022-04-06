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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.UPDATABLE_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryN;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryFlags;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryK;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryM;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryN;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySerVer;
import static org.apache.datasketches.kll.KllSketch.Error.MUST_NOT_CALL;
import static org.apache.datasketches.kll.KllSketch.Error.SRC_MUST_BE_FLOAT;
import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_IMMUTABLE;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;

import org.apache.datasketches.Family;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

//Intentional extra blank line so the code lines up with KllDirectDoublesSketch
/**
 * This class implements an off-heap floats KllSketch via a WritableMemory instance of the sketch.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
public final class KllDirectFloatsSketch extends KllSketch {

  /**
   * The actual constructor
   * @param wmem the current WritableMemory
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @param memVal the MemoryValadate object
   */
  private KllDirectFloatsSketch(final WritableMemory wmem, final MemoryRequestServer memReqSvr,
   final KllMemoryValidate memVal) {
   super(SketchType.FLOATS_SKETCH, wmem, memReqSvr);
   updatableMemory = memVal.updatableMemory && memReqSvr != null;
   levelsArrUpdatable = memVal.levelsArrUpdatable;
   minMaxArrUpdatable = memVal.minMaxArrUpdatable;
   itemsArrUpdatable = memVal.itemsArrUpdatable;
  }

  /**
   * Heapifies the given Memory object and returns a KllFloatsSketch
   * @param mem the given Memory object.
   * @return a KllFloatsSketch
   */
  public static KllHeapFloatsSketch heapify(final Memory mem) {
    return KllHeapFloatsSketch.heapify(mem);
  }

  /**
   * Create a new instance of this sketch using the default <i>m</i> of 8.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new instance of this sketch
   */
  public static KllDirectFloatsSketch newInstance(final int k, final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    return newInstance(k, KllSketch.DEFAULT_M, dstMem, memReqSvr);
  }

  /**
   * Create a new instance of this sketch.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param m parameter that controls the minimum level width in items.
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new instance of this sketch
   */
  static KllDirectFloatsSketch newInstance(final int k, final int m, final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    setMemoryPreInts(dstMem, PREAMBLE_INTS_FULL);
    setMemorySerVer(dstMem, SERIAL_VERSION_UPDATABLE);
    setMemoryFamilyID(dstMem, Family.KLL.getID());
    setMemoryFlags(dstMem, UPDATABLE_BIT_MASK);
    setMemoryK(dstMem, k);
    setMemoryM(dstMem, m);
    setMemoryN(dstMem, 0);
    setMemoryMinK(dstMem, k);
    setMemoryNumLevels(dstMem, 1);
    int offset = DATA_START_ADR;
    dstMem.putIntArray(offset, new int[] {k, k}, 0, 2);
    offset += 2 * Integer.BYTES;
    dstMem.putFloatArray(offset, new float[] {Float.NaN, Float.NaN}, 0, 2);
    offset += 2 * Float.BYTES;
    dstMem.putFloatArray(offset, new float[k], 0, k);
    final KllMemoryValidate memVal = new KllMemoryValidate(dstMem);
    return new KllDirectFloatsSketch(dstMem, memReqSvr, memVal);
  }

  /**
   * Wrap a sketch around the given source Memory containing sketch data that originated from
   * this sketch.
   * @param srcMem a WritableMemory that contains data.
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return instance of this sketch
   */
  public static KllDirectFloatsSketch writableWrap(final WritableMemory srcMem, final MemoryRequestServer memReqSvr) {
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem);
    return new KllDirectFloatsSketch(srcMem, memReqSvr, memVal);
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of splitPoint (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(false) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing float values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @return an array of m+1 double values on the interval [0.0, 1.0),
   * which are a consecutive approximation to the CDF of the input stream given the splitPoints.
   * The value at array position j of the returned CDF array is the sum of the returned values
   * in positions 0 through j of the returned PMF array.
   */
  public double[] getCDF(final float[] splitPoints) {
    return KllFloatsHelper.getFloatsPmfOrCdf(this, splitPoints, true);
  }

  /**
   * Returns the max value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the max value of the stream
   */
  public float getMaxValue() { return getMaxFloatValue(); }

  /**
   * Returns the min value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the min value of the stream
   */
  public float getMinValue() { return getMinFloatValue(); }

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of splitPoints (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(true) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing float values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @return an array of m+1 doubles on the interval [0.0, 1.0),
   * each of which is an approximation to the fraction of the total input stream values
   * (the mass) that fall into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint, with the exception that the last interval will include maximum value.
   */
  public double[] getPMF(final float[] splitPoints) {
    return KllFloatsHelper.getFloatsPmfOrCdf(this, splitPoints, false);
  }

  /**
   * Returns an approximation to the value of the data item
   * that would be preceded by the given fraction of a hypothetical sorted
   * version of the input stream so far.
   *
   * <p>We note that this method has a fairly large overhead (microseconds instead of nanoseconds)
   * so it should not be called multiple times to get different quantiles from the same
   * sketch. Instead use getQuantiles(), which pays the overhead only once.
   *
   * <p>If the sketch is empty this returns NaN.
   *
   * @param fraction the specified fractional position in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * If fraction = 0.0, the true minimum value of the stream is returned.
   * If fraction = 1.0, the true maximum value of the stream is returned.
   *
   * @return the approximation to the value at the given fraction
   */
  public float getQuantile(final double fraction) {
    return KllFloatsHelper.getFloatsQuantile(this, fraction);
  }

  /**
   * Gets the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public float getQuantileLowerBound(final double fraction) {
    return getQuantile(max(0, fraction - KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  /**
   * This is a more efficient multiple-query version of getQuantile().
   *
   * <p>This returns an array that could have been generated by using getQuantile() with many
   * different fractional ranks, but would be very inefficient.
   * This method incurs the internal set-up overhead once and obtains multiple quantile values in
   * a single query. It is strongly recommend that this method be used instead of multiple calls
   * to getQuantile().
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param fractions given array of fractional positions in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * These fractions must be in the interval [0.0, 1.0], inclusive.
   *
   * @return array of approximations to the given fractions in the same order as given fractions
   * array.
   */
  public float[] getQuantiles(final double[] fractions) {
    return KllFloatsHelper.getFloatsQuantiles(this, fractions);
  }

  /**
   * This is also a more efficient multiple-query version of getQuantile() and allows the caller to
   * specify the number of evenly spaced fractional ranks.
   *
   * <p>If the sketch is empty this returns null.
   *
   * @param numEvenlySpaced an integer that specifies the number of evenly spaced fractional ranks.
   * This must be a positive integer greater than 0. A value of 1 will return the min value.
   * A value of 2 will return the min and the max value. A value of 3 will return the min,
   * the median and the max value, etc.
   *
   * @return array of approximations to the given fractions in the same order as given fractions
   * array.
   */
  public float[] getQuantiles(final int numEvenlySpaced) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.Util.evenlySpaced(0.0, 1.0, numEvenlySpaced));
  }

  /**
   * Gets the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the upper bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public float getQuantileUpperBound(final double fraction) {
    return getQuantile(min(1.0, fraction + KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  /**
   * Returns an approximation to the normalized (fractional) rank of the given value from 0 to 1,
   * inclusive.
   *
   * <p>The resulting approximation has a probabilistic guarantee that can be obtained from the
   * getNormalizedRankError(false) function.
   *
   * <p>If the sketch is empty this returns NaN.</p>
   *
   * @param value to be ranked
   * @return an approximate rank of the given value
   */
  public double getRank(final float value) {
    return KllFloatsHelper.getFloatRank(this, value);
  }

  /**
   * @return the iterator for this class
   */
  public KllFloatsSketchIterator iterator() {
    return new KllFloatsSketchIterator(getFloatItemsArray(), getLevelsArray(), getNumLevels());
  }

  /**
   * Merges another sketch into this one.
   * @param other sketch to merge into this one
   */
  public void merge(final KllSketch other) {
    if (!other.isFloatsSketch()) { kllSketchThrow(SRC_MUST_BE_FLOAT); }
    KllFloatsHelper.mergeFloatImpl(this, other);
  }

  /**
   * Updates this sketch with the given data item.
   *
   * @param value an item from a stream of items. NaNs are ignored.
   */
  public void update(final float value) {
    KllFloatsHelper.updateFloat(this, value);
  }

  @Override
  double[] getDoubleItemsArray() { kllSketchThrow(MUST_NOT_CALL); return null; }

  @Override
  double getDoubleItemsArrayAt(final int index) { kllSketchThrow(MUST_NOT_CALL); return Double.NaN; }

  @Override
  float[] getFloatItemsArray() {
    final int items = getItemsArrLengthItems();
    final float[] itemsArr = new float[items];
    itemsArrUpdatable.getFloatArray(0, itemsArr, 0, items);
    return itemsArr;
  }

  @Override
  float getFloatItemsArrayAt(final int index) {
    return itemsArrUpdatable.getFloat((long)index * Float.BYTES);
  }

  @Override
  double getMaxDoubleValue() { kllSketchThrow(MUST_NOT_CALL); return Double.NaN; }

  @Override
  float getMaxFloatValue() {
    return minMaxArrUpdatable.getFloat(Float.BYTES);
  }

  @Override
  double getMinDoubleValue() { kllSketchThrow(MUST_NOT_CALL); return Double.NaN; }

  @Override
  float getMinFloatValue() {
    return minMaxArrUpdatable.getFloat(0);
  }

  @Override
  void setDoubleItemsArray(final double[] doubleItems) { kllSketchThrow(MUST_NOT_CALL); }

  @Override
  void setDoubleItemsArrayAt(final int index, final double value) { kllSketchThrow(MUST_NOT_CALL); }

  @Override
  void setFloatItemsArray(final float[] floatItems) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    itemsArrUpdatable.putFloatArray(0, floatItems, 0, floatItems.length);
  }

  @Override
  void setFloatItemsArrayAt(final int index, final float value) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    itemsArrUpdatable.putFloat((long)index * Float.BYTES, value);
  }

  @Override
  void setMaxDoubleValue(final double value) { kllSketchThrow(MUST_NOT_CALL); }

  @Override
  void setMaxFloatValue(final float value) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    minMaxArrUpdatable.putFloat(Float.BYTES, value);
  }

  @Override
  void setMinDoubleValue(final double value) { kllSketchThrow(MUST_NOT_CALL); }

  @Override
  void setMinFloatValue(final float value) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    minMaxArrUpdatable.putFloat(0, value);
  }

  //**********************************************************************************
  final boolean updatableMemory;
  WritableMemory levelsArrUpdatable;
  WritableMemory minMaxArrUpdatable;
  WritableMemory itemsArrUpdatable;

  @Override
  public int getK() {
    return getMemoryK(wmem);
  }

  @Override
  public long getN() {
    return getMemoryN(wmem);
  }

  @Override
  public void reset() {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelsArray(new int[] {k, k});
    setLevelZeroSorted(false);
    final int newLevelsArrLen = 2 * Integer.BYTES;
    final int newItemsArrLen = k;
    KllHelper.memorySpaceMgmt(this, newLevelsArrLen, newItemsArrLen);
    levelsArrUpdatable.putIntArray(0L, new int[] {k, k}, 0, 2);
    if (sketchType == SketchType.DOUBLES_SKETCH) {
      minMaxArrUpdatable.putDoubleArray(0L, new double[] {Double.NaN, Double.NaN}, 0, 2);
      itemsArrUpdatable.putDoubleArray(0L, new double[k], 0, k);
    } else {
      minMaxArrUpdatable.putFloatArray(0L, new float[] {Float.NaN, Float.NaN}, 0, 2);
      itemsArrUpdatable.putFloatArray(0L, new float[k], 0, k);
    }
  }

  @Override
  public byte[] toUpdatableByteArray() {
    final int bytes = (int) wmem.getCapacity();
    final byte[] byteArr = new byte[bytes];
    wmem.getByteArray(0, byteArr, 0, bytes);
    return byteArr;
  }

  int getItemsArrLengthItems() {
    return getLevelsArray()[getNumLevels()];
  }

  @Override
  int[] getLevelsArray() {
    final int numInts = getNumLevels() + 1;
    final int[] myLevelsArr = new int[numInts];
    levelsArrUpdatable.getIntArray(0, myLevelsArr, 0, numInts);
    return myLevelsArr;
  }

  @Override
  int getLevelsArrayAt(final int index) {
    return levelsArrUpdatable.getInt((long)index * Integer.BYTES);
  }

  @Override
  int getM() {
    return getMemoryM(wmem);
  }

  @Override
  int getMinK() {
    return getMemoryMinK(wmem);
  }

  @Override
  int getNumLevels() {
    return getMemoryNumLevels(wmem);
  }

  @Override
  void incN() {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    long n = getMemoryN(wmem);
    setMemoryN(wmem, ++n);
  }

  @Override
  void incNumLevels() {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    int numLevels = getMemoryNumLevels(wmem);
    setMemoryNumLevels(wmem, ++numLevels);
  }

  @Override
  boolean isLevelZeroSorted() {
    return getMemoryLevelZeroSortedFlag(wmem);
  }

  @Override
  void setItemsArrayUpdatable(final WritableMemory itemsMem) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    itemsArrUpdatable = itemsMem;
  }

  @Override
  void setLevelsArray(final int[] levelsArr) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    levelsArrUpdatable.putIntArray(0, levelsArr, 0, levelsArr.length);
  }

  @Override
  void setLevelsArrayAt(final int index, final int value) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    levelsArrUpdatable.putInt((long)index * Integer.BYTES, value);
  }

  @Override
  void setLevelsArrayAtMinusEq(final int index, final int minusEq) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    final int offset = index * Integer.BYTES;
    final int curV = levelsArrUpdatable.getInt(offset);
    levelsArrUpdatable.putInt(offset, curV - minusEq);
  }

  @Override
  void setLevelsArrayAtPlusEq(final int index, final int plusEq) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    final int offset = index * Integer.BYTES;
    final int curV = levelsArrUpdatable.getInt(offset);
    levelsArrUpdatable.putInt(offset, curV + plusEq);
  }

  @Override
  void setLevelsArrayUpdatable(final WritableMemory levelsMem) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    levelsArrUpdatable = levelsMem;
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    setMemoryLevelZeroSortedFlag(wmem, sorted);
  }

  @Override
  void setMinK(final int minK) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    setMemoryMinK(wmem, minK);
  }

  @Override
  void setMinMaxArrayUpdatable(final WritableMemory minMaxMem) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    minMaxArrUpdatable = minMaxMem;
  }

  @Override
  void setN(final long n) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    setMemoryN(wmem, n);
  }

  @Override
  void setNumLevels(final int numLevels) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    setMemoryNumLevels(wmem, numLevels);
  }

}
