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
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.DEFAULT_M;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.UPDATABLE_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.insertDyMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.insertFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.insertFlags;
import static org.apache.datasketches.kll.KllPreambleUtil.insertK;
import static org.apache.datasketches.kll.KllPreambleUtil.insertM;
import static org.apache.datasketches.kll.KllPreambleUtil.insertN;
import static org.apache.datasketches.kll.KllPreambleUtil.insertNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.insertPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.insertSerVer;

import org.apache.datasketches.Family;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

//Intentional
/**
 * This class implements an off-heap floats KllSketch via a WritableMemory instance of the sketch.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
public final class KllDirectFloatsSketch extends KllDirectSketch {

  /**
   * The actual constructor
   * @param wmem the current WritableMemory
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @param memVal the MemoryValadate object
   */
  private KllDirectFloatsSketch(final WritableMemory wmem, final MemoryRequestServer memReqSvr,
      final MemoryValidate memVal) {
   super(SketchType.FLOATS_SKETCH, wmem, memReqSvr, memVal);
  }

  /**
   * Wrap a sketch around the given source Memory containing sketch data that originated from
   * this sketch.
   * @param srcMem a WritableMemory that contains data.
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return instance of this sketch
   */
  public static KllDirectFloatsSketch writableWrap(final WritableMemory srcMem, final MemoryRequestServer memReqSvr) {
    final MemoryValidate memVal = new MemoryValidate(srcMem);
    return new KllDirectFloatsSketch(srcMem, memReqSvr, memVal);
  }

  /**
   * Create a new instance of this sketch using default M.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new instance of this sketch
   */
  public static KllDirectFloatsSketch newInstance(final int k, final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    return newInstance(k, DEFAULT_M, dstMem, memReqSvr);
  }

  /**
   * Create a new instance of this sketch.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param m parameter that controls the minimum level width.
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new instance of this sketch
   */
  public static KllDirectFloatsSketch newInstance(final int k, final int m, final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    insertPreInts(dstMem, PREAMBLE_INTS_FLOAT);
    insertSerVer(dstMem, SERIAL_VERSION_UPDATABLE);
    insertFamilyID(dstMem, Family.KLL.getID());
    insertFlags(dstMem, UPDATABLE_BIT_MASK);
    insertK(dstMem, k);
    insertM(dstMem, m);
    insertN(dstMem, 0);
    insertDyMinK(dstMem, k);
    insertNumLevels(dstMem, 1);
    int offset = DATA_START_ADR_FLOAT;
    dstMem.putIntArray(offset, new int[] {k, k}, 0, 2);
    offset += 2 * Integer.BYTES;
    dstMem.putFloatArray(offset, new float[] {Float.NaN, Float.NaN}, 0, 2);
    offset += 2 * Float.BYTES;
    dstMem.putFloatArray(offset, new float[k], 0, k);
    final MemoryValidate memVal = new MemoryValidate(dstMem);
    return new KllDirectFloatsSketch(dstMem, memReqSvr, memVal);
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
   * @return an array of m+1 double values on the interval [0.0, 1.0) exclusive,
   * which are a consecutive approximation to the CDF of the input stream given the splitPoints.
   * The value at array position j of the returned CDF array is the sum of the returned values
   * in positions 0 through j of the returned PMF array.
   */
  public double[] getCDF(final float[] splitPoints) {
    return getFloatsPmfOrCdf(splitPoints, true);
  }

  /**
   * Returns the max value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the max value of the stream
   */
  public float getMaxValue() {
    return getMaxFloatValue();
  }

  /**
   * Returns the min value of the stream.
   * If the sketch is empty this returns NaN.
   *
   * @return the min value of the stream
   */
  public float getMinValue() {
    return getMinFloatValue();
  }

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
   * @return an array of m+1 doubles on the interval [0.0, 1.0) exclusive,
   * each of which is an approximation to the fraction of the total input stream values
   * (the mass) that fall into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint, with the exception that the last interval will include maximum value.
   */
  public double[] getPMF(final float[] splitPoints) {
    return getFloatsPmfOrCdf(splitPoints, false);
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
    return getFloatsQuantile(fraction);
  }

  /**
   * Gets the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%.
   * @param fraction the given normalized rank as a fraction
   * @return the lower bound of the value interval in which the true quantile of the given rank
   * exists with a confidence of at least 99%. Returns NaN if the sketch is empty.
   */
  public float getQuantileLowerBound(final double fraction) {
    return getQuantile(max(0, fraction - KllHelper.getNormalizedRankError(getDyMinK(), false)));
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
    return getFloatsQuantiles(fractions);
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
    return getQuantile(min(1.0, fraction + KllHelper.getNormalizedRankError(getDyMinK(), false)));
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
    return getFloatRank(value);
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
    if (!other.isDirect()) { kllSketchThrow(32); }
    if (!other.isFloatsSketch()) { kllSketchThrow(34); }
    mergeFloatImpl(other);
  }

  @Override
  public byte[] toByteArray() {
    return toCompactByteArrayImpl();
  }

  @Override
  public String toString(final boolean withLevels, final boolean withData) {
    return toStringImpl(withLevels, withData);
  }

  /**
   * Updates this sketch with the given data item.
   *
   * @param value an item from a stream of items. NaNs are ignored.
   */
  public void update(final float value) {
    updateFloat(value);
  }

}
