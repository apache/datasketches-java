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

import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.InequalitySearch;

/**
 * Supports searches for quantiles, Ranks, Iterator and Sorted View
 * @author Lee Rhodes
 */
public class ReqSketchSortedView {
  private static final String LS = System.getProperty("line.separator");
  private float[] values;
  private long[] cumWeights;
  private final boolean hra; //used in merge
  private final long N;

  public ReqSketchSortedView(final ReqSketch sk) {
    hra = sk.getHighRankAccuracy();
    N = sk.getN();
    buildAuxTable(sk);
  }

  /**
   * Testing only! Allows testing of mergeSortIn without a sketch.
   * Arrays must be appropriately sized.
   * @param values given values
   * @param natRanks currently not used for the test.
   * @param hra hra vs lra
   * @param N total stream size in number of values presented to the sketch.
   */
  ReqSketchSortedView(final float[] values, final long[] natRanks, final boolean hra, final long N) {
    this.hra = hra;
    this.N = N;
    this.values = values;
    this.cumWeights = natRanks;
  }

  /**
   * Gets the quantile based on the given normalized rank,
   * which must be in the range [0.0, 1.0], inclusive.
   * @param normRank the given normalized rank
   * @param inclusive determines the search criterion used.
   * @return the quantile
   */
  public float getQuantile(final double normRank, final boolean inclusive) {
    final int len = cumWeights.length;
    final long rank = (int)(normRank * N);
    final InequalitySearch crit = inclusive ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(cumWeights, 0, len - 1, rank, crit);
    if (index == -1) {
      return values[len - 1]; //GT: normRank >= 1.0; GE: normRank > 1.0
    }
    return values[index];
  }

  /**
   * Gets the normalized rank based on the given value.
   * @param value the given value
   * @param ltEq determines the search criterion used.
   * @return the normalized rank
   */
  public double getRank(final float value, final boolean ltEq) {
    final int len = values.length;
    final InequalitySearch crit = ltEq ? InequalitySearch.LE : InequalitySearch.LT;
    final int index = InequalitySearch.find(values,  0, len - 1, value, crit);
    if (index == -1) {
      return 0; //LT: value <= minValue; LE: value < minValue
    }
    return (double)cumWeights[index] / N;
  }

  public ReqSketchSortedViewIterator iterator() {
    return new ReqSketchSortedViewIterator(values, cumWeights);
  }

  public String toString(final int precision, final int fieldSize) {
    final StringBuilder sb = new StringBuilder();
    final int p = precision;
    final int z = Math.max(fieldSize, 6);
    final String ff = "%" + z + "." + p + "f";
    final String sf = "%" + z + "s";
    final String df = "%"  + z + "d";
    final String dfmt = ff + df + LS;
    final String sfmt = sf + sf + LS;
    sb.append("REQ Sorted View Data:").append(LS + LS);
    sb.append(String.format(sfmt, "Value", "CumWeight"));
    final int totalCount = values.length;
    for (int i = 0; i < totalCount; i++) {
      final Row row = getRow(i);
      sb.append(String.format(dfmt, row.value, row.cumWeight));
    }
    return sb.toString();
  }

  private void buildAuxTable(final ReqSketch sk) {
    final List<ReqCompactor> compactors = sk.getCompactors();
    final int numComp = compactors.size();
    final int totalValues = sk.getRetainedItems();
    values = new float[totalValues];
    cumWeights = new long[totalValues];
    int count = 0;
    for (int i = 0; i < numComp; i++) {
      final ReqCompactor c = compactors.get(i);
      final FloatBuffer bufIn = c.getBuffer();
      final long bufWeight = 1 << c.getLgWeight();
      final int bufInLen = bufIn.getCount();
      mergeSortIn(bufIn, bufWeight, count);
      count += bufInLen;
    }
    createCumulativeNativeRanks();
    dedup();
  }

  private void createCumulativeNativeRanks() {
    final int len = values.length;
    for (int i = 1; i < len; i++) {
      cumWeights[i] +=  cumWeights[i - 1];
    }
    assert cumWeights[len - 1] == N;
  }

  private void dedup() {
    final int valuesLen = values.length;
    final float[] valuesB = new float[valuesLen];
    final long[] natRanksB = new long[valuesLen];
    int bidx = 0;
    int i = 0;
    while (i < valuesLen) {
      int j = i + 1;
      int hidup = j;
      while (j < valuesLen && values[i] == values[j]) {
        hidup = j++;
      }
      if (j - i == 1) { //no dups
        valuesB[bidx] = values[i];
        natRanksB[bidx++] = cumWeights[i];
        i++;
        continue;
      } else {
        valuesB[bidx] = values[hidup];
        natRanksB[bidx++] = cumWeights[hidup];
        i = j;
        continue;
      }
    }
    values = Arrays.copyOf(valuesB, bidx);
    cumWeights = Arrays.copyOf(natRanksB, bidx);
  }


  /**
   * Specially modified version of FloatBuffer.mergeSortIn(). Here spaceAtBottom is always false and
   * the ultimate array size has already been set.  However, this must simultaneously deal with
   * sorting the base FloatBuffer as well.  Also used in test.
   *
   * @param bufIn given FloatBuffer. If not sorted it will be sorted here.
   * @param bufWeight associated weight of input FloatBuffer
   * @param count tracks number of values inserted into the class arrays
   */
  void mergeSortIn(final FloatBuffer bufIn, final long bufWeight, final int count) {
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

  //used for testing

  Row getRow(final int index) {
    return new Row(values[index], cumWeights[index]);
  }

  static class Row {
    float value;
    long cumWeight;

    Row(final float value, final long cumWeight) {
      this.value = value;
      this.cumWeight = cumWeight;
    }
  }

}
