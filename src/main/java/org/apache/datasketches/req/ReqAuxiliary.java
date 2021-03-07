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
 * Supports searches for quantiles
 * @author Lee Rhodes
 */
class ReqAuxiliary {
  private static final String LS = System.getProperty("line.separator");
  private float[] items;
  private long[] weights;
  private final boolean hra; //used in merge
  private final long N;

  ReqAuxiliary(final ReqSketch sk) {
    hra = sk.getHighRankAccuracy();
    N = sk.getN();
    buildAuxTable(sk);
  }

  //Testing only! Allows testing of support methods without a sketch.
  ReqAuxiliary(final float[] items, final long[] weights, final boolean hra, final long N) {
    this.hra = hra;
    this.N = N;
    this.items = items;
    this.weights = weights;
  }

  private void buildAuxTable(final ReqSketch sk) {
    final List<ReqCompactor> compactors = sk.getCompactors();
    final int numComp = compactors.size();
    final int totalItems = sk.getRetainedItems();
    items = new float[totalItems];
    weights = new long[totalItems];
    int auxCount = 0;
    for (int i = 0; i < numComp; i++) {
      final ReqCompactor c = compactors.get(i);
      final FloatBuffer bufIn = c.getBuffer();
      final long weight = 1 << c.getLgWeight();
      final int bufInLen = bufIn.getCount();
      mergeSortIn(bufIn, weight, auxCount);
      auxCount += bufInLen;
    }
    createCumulativeWeights();
    dedup();
  }

  private void createCumulativeWeights() {
    final int len = items.length;
    for (int i = 1; i < len; i++) {
      weights[i] +=  weights[i - 1];
    }
    assert weights[len - 1] == N;
  }

  void dedup() {
    final int itemsLen = items.length;
    final float[] itemsB = new float[itemsLen];
    final long[] wtsB = new long[itemsLen];
    int bidx = 0;
    int i = 0;
    while (i < itemsLen) {
      int j = i + 1;
      int hidup = j;
      while (j < itemsLen && items[i] == items[j]) {
        hidup = j++;
      }
      if (j - i == 1) { //no dups
        itemsB[bidx] = items[i];
        wtsB[bidx++] = weights[i];
        i++;
        continue;
      } else {
        itemsB[bidx] = items[hidup]; //lgtm [java/index-out-of-bounds]
        wtsB[bidx++] = weights[hidup];
        i = j;
        continue;
      }
    }
    items = Arrays.copyOf(itemsB, bidx);
    weights = Arrays.copyOf(wtsB, bidx);
  }

  //Specially modified version of FloatBuffer.mergeSortIn(). Here spaceAtBottom is always false and
  // the ultimate array size has already been set.  However, this must simultaneously deal with
  // sorting the weights as well.  Also used in test.
  void mergeSortIn(final FloatBuffer bufIn, final long weight, final int auxCount) {
    if (!bufIn.isSorted()) { bufIn.sort(); }
    final float[] arrIn = bufIn.getArray(); //may be larger than its item count.
    final int bufInLen = bufIn.getCount();
    final int totLen = auxCount + bufInLen;
    int i = auxCount - 1;
    int j = bufInLen - 1;
    int h = hra ? bufIn.getCapacity() - 1 : bufInLen - 1;
    for (int k = totLen; k-- > 0; ) {
      if (i >= 0 && j >= 0) { //both valid
        if (items[i] >= arrIn[h]) {
          items[k] = items[i];
          weights[k] = weights[i--];
        } else {
          items[k] = arrIn[h--]; j--;
          weights[k] = weight;
        }
      } else if (i >= 0) { //i is valid
        items[k] = items[i];
        weights[k] = weights[i--];
      } else if (j >= 0) { //j is valid
        items[k] = arrIn[h--]; j--;
        weights[k] = weight;
      } else {
        break;
      }
    }
  }

  /**
   * Gets the quantile based on the given normalized rank,
   * which must be in the range [0.0, 1.0], inclusive.
   * @param normRank the given normalized rank
   * @param ltEq determines the search method used.
   * @return the quantile based on given normalized rank and ltEq.
   */
  float getQuantile(final double normRank, final boolean ltEq) {
    final int len = weights.length;
    final long rank = (int)(normRank * N);
    //Note that when ltEq=false, GT matches KLL & Quantiles behavior.
    final InequalitySearch crit = ltEq ? InequalitySearch.GE : InequalitySearch.GT;
    final int index = InequalitySearch.find(weights, 0, len - 1, rank, crit);
    if (index == -1) {
      return items[len - 1]; //resolves high end (GE & GT) -1 only!
    }
    return items[index];
  }

  //used for testing

  Row getRow(final int index) {
    return new Row(items[index], weights[index]);
  }

  static class Row {
    float item;
    long weight;

    Row(final float item, final long weight) {
      this.item = item;
      this.weight = weight;
    }
  }

  String toString(final int precision, final int fieldSize) {
    final StringBuilder sb = new StringBuilder();
    final int p = precision;
    final int z = fieldSize;
    final String ff = "%" + z + "." + p + "f";
    final String sf = "%" + z + "s";
    final String df = "%"  + z + "d";
    final String dfmt = ff + df + LS;
    final String sfmt = sf + sf + LS;
    sb.append("Aux Detail").append(LS);
    sb.append(String.format(sfmt, "Item", "Weight"));
    final int totalCount = items.length;
    for (int i = 0; i < totalCount; i++) {
      final Row row = getRow(i);
      sb.append(String.format(dfmt, row.item, row.weight));
    }
    return sb.toString();
  }

}
