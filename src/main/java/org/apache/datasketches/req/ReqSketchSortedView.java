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

package org.apache.datasketches.req;

import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.QuantileSearchCriteria.EXCLUSIVE;

import java.util.List;

import org.apache.datasketches.FloatsSortedView;
import org.apache.datasketches.InequalitySearch;
import org.apache.datasketches.QuantileSearchCriteria;
import org.apache.datasketches.SketchesArgumentException;

/**
 * The SortedView of the ReqSketch.
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public class ReqSketchSortedView implements FloatsSortedView {
  private float[] values;
  private long[] cumWeights;
  private final long totalN;

  /**
   * Construct from elements for testing.
   * @param values sorted array of values
   * @param cumWeights sorted, monotonically increasing cumulative weights.
   * @param totalN the total number of values presented to the sketch.
   */
  ReqSketchSortedView(final float[] values, final long[] cumWeights, final long totalN) {
    this.values = values;
    this.cumWeights  = cumWeights;
    this.totalN = totalN;
  }

  /**
   * Constructs this Sorted View given the sketch
   * @param sk the given ReqSketch
   */
  public ReqSketchSortedView(final ReqSketch sk) {
    totalN = sk.getN();
    buildSortedViewArrays(sk);
  }

  @Override
  public float getQuantile(final double normRank, final QuantileSearchCriteria searchCrit) {
    final int len = cumWeights.length;
    final long rank = (int)(normRank * totalN);
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, rank, crit);
    if (index == -1) {
      if (searchCrit == EXCLUSIVE) { return Float.NaN; } //GT: normRank == 1.0;
    }
    return values[index];
  }

  @Override
  public double getRank(final float value, final QuantileSearchCriteria searchCrit) {
    final int len = values.length;
    final InequalitySearch crit = (searchCrit == INCLUSIVE) ? InequalitySearch.LE : InequalitySearch.LT;
    final int index = InequalitySearch.find(values,  0, len - 1, value, crit);
    if (index == -1) {
      return 0; //LT: value <= minValue; LE: value < minValue
    }
    return (double)cumWeights[index] / totalN;
  }

  @Override
  public double[] getCDF(final float[] splitPoints, final QuantileSearchCriteria searchCrit) {
    validateFloatValues(splitPoints);
    final int len = splitPoints.length + 1;
    final double[] buckets = new double[len];
    for (int i = 0; i < len - 1; i++) {
      buckets[i] = getRank(splitPoints[i], searchCrit);
    }
    buckets[len - 1] = 1.0;
    return buckets;
  }

  @Override
  public double[] getPMF(final float[] splitPoints, final QuantileSearchCriteria searchCrit) {
    final double[] buckets = getCDF(splitPoints, searchCrit);
    final int len = buckets.length;
    for (int i = len; i-- > 1; ) {
      buckets[i] -= buckets[i - 1];
    }
    return buckets;
  }

  @Override
  public long[] getCumulativeWeights() {
    return cumWeights;
  }

  @Override
  public float[] getValues() {
    return values;
  }

  @Override
  public ReqSketchSortedViewIterator iterator() {
    return new ReqSketchSortedViewIterator(values, cumWeights);
  }

  //restricted methods

  private void buildSortedViewArrays(final ReqSketch sk) {
    final List<ReqCompactor> compactors = sk.getCompactors();
    final int numComp = compactors.size();
    final int totalValues = sk.getNumRetained();
    values = new float[totalValues];
    cumWeights = new long[totalValues];
    int count = 0;
    for (int i = 0; i < numComp; i++) {
      final ReqCompactor c = compactors.get(i);
      final FloatBuffer bufIn = c.getBuffer();
      final long bufWeight = 1 << c.getLgWeight();
      final int bufInLen = bufIn.getCount();
      mergeSortIn(bufIn, bufWeight, count, sk.getHighRankAccuracyMode());
      count += bufInLen;
    }
    createCumulativeNativeRanks();
  }

  /**
   * Specially modified version of FloatBuffer.mergeSortIn(). Here spaceAtBottom is always false and
   * the ultimate array size has already been set.  However, this must simultaneously deal with
   * sorting the base FloatBuffer as well.
   *
   * @param bufIn given FloatBuffer. If not sorted it will be sorted here.
   * @param bufWeight associated weight of input FloatBuffer
   * @param count tracks number of values inserted into the class arrays
   */
  private void mergeSortIn(final FloatBuffer bufIn, final long bufWeight, final int count, final boolean hra) {
    if (!bufIn.isSorted()) { bufIn.sort(); }
    final float[] arrIn = bufIn.getArray(); //may be larger than its value count.
    final int bufInLen = bufIn.getCount();
    final int totLen = count + bufInLen;
    int i = count - 1;
    int j = bufInLen - 1;
    int h = hra ? bufIn.getCapacity() - 1 : bufInLen - 1;
    for (int k = totLen; k-- > 0; ) {
      if (i >= 0 && j >= 0) { //both valid
        if (values[i] >= arrIn[h]) {
          values[k] = values[i];
          cumWeights[k] = cumWeights[i--]; //not yet natRanks, just individual wts
        } else {
          values[k] = arrIn[h--]; j--;
          cumWeights[k] = bufWeight;
        }
      } else if (i >= 0) { //i is valid
        values[k] = values[i];
        cumWeights[k] = cumWeights[i--];
      } else if (j >= 0) { //j is valid
        values[k] = arrIn[h--]; j--;
        cumWeights[k] = bufWeight;
      } else {
        break;
      }
    }
  }

  private void createCumulativeNativeRanks() {
    final int len = values.length;
    for (int i = 1; i < len; i++) {
      cumWeights[i] +=  cumWeights[i - 1];
    }
    if (totalN > 0) {
      assert cumWeights[len - 1] == totalN;
    }
  }

  /**
   * Checks the sequential validity of the given array of splitpoints as floats.
   * They must be unique, monotonically increasing and not NaN.
   * Only used for getPmfOrCdf().
   * @param values the given array of values
   */
  private static void validateFloatValues(final float[] values) {
    for (int i = 0; i < values.length; i++) {
      if (!Float.isFinite(values[i])) {
        throw new SketchesArgumentException("Values must be finite");
      }
      if (i < values.length - 1 && values[i] >= values[i + 1]) {
        throw new SketchesArgumentException(
          "Values must be unique and monotonically increasing");
      }
    }
  }

}
