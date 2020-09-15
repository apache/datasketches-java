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

import static org.apache.datasketches.req.ReqHelper.LS;
import static org.apache.datasketches.req.ReqHelper.binarySearchDouble;

import java.util.List;

/**
 * Supports searches for quantiles
 * @author Lee Rhodes
 */
class ReqAuxiliary {
  private float[] items;
  private long[] weights;
  private double[] normRanks;
  private final boolean hra;
  private final Criteria criterion;

  ReqAuxiliary(final ReqSketch sk) {
    hra = sk.getHighRanksAccuracy();
    criterion = sk.getCriterion();
    buildAuxTable(sk);
  }

  //For testing only
  ReqAuxiliary(final int arrLen, final boolean hra, final Criteria criterion) {
    this.hra = hra;
    this.criterion = criterion;
    items = new float[arrLen];
    weights = new long[arrLen];
    normRanks = new double[arrLen];
  }

  private void buildAuxTable(final ReqSketch sk) {
    final List<ReqCompactor> compactors = sk.getCompactors();
    final int numComp = compactors.size();
    final int totalItems = sk.getRetainedItems();
    final long N = sk.getN();
    items = new float[totalItems];
    weights = new long[totalItems];
    normRanks = new double[totalItems];
    int auxCount = 0;
    for (int i = 0; i < numComp; i++) {
      final ReqCompactor c = compactors.get(i);
      final FloatBuffer bufIn = c.getBuffer();
      final long wt = 1L << c.getLgWeight();
      final int bufInLen = bufIn.getLength();
      mergeSortIn(bufIn, wt, auxCount);
      auxCount += bufInLen;
    }
    float sum = 0;
    for (int i = 0; i < totalItems; i++) {
      sum += weights[i];
      normRanks[i] = sum / N;
    }
  }

  void mergeSortIn(final FloatBuffer bufIn, final long wt, final int auxCount) {
    if (!bufIn.isSorted()) { bufIn.sort(); }
    final float[] arrIn = bufIn.getArray(); //may be larger than its item count.
    final int bufInLen = bufIn.getLength();
    final int totLen = auxCount + bufInLen;
    int i = auxCount - 1;
    int j = bufInLen - 1;
    int h = (hra) ? bufIn.getCapacity() - 1 : bufInLen - 1;
    for (int k = totLen; k-- > 0; ) {
      if ((i >= 0) && (j >= 0)) { //both valid
        if (items[i] >= arrIn[h]) {
          items[k] = items[i];
          weights[k] = weights[i--];
        } else {
          items[k] = arrIn[h--]; j--;
          weights[k] = wt;
        }
      } else if (i >= 0) { //i is valid
        items[k] = items[i];
        weights[k] = weights[i--];
      } else if (j >= 0) { //j is valid
        items[k] = arrIn[h--]; j--;
        weights[k] = wt;
      } else {
        break;
      }
    }
  }

  /**
   * Gets the quantile of the largest normalized rank that is less than the given normalized rank,
   * which must be in the range [0.0, 1.0], inclusive, inclusive
   * @param normRank the given normalized rank
   * @return the largest quantile less than the given normalized rank.
   */
  float getQuantile(final double normRank) {
    final int len = normRanks.length;
    final int index = binarySearchDouble(normRanks, 0, len - 1, normRank, criterion);
    if (index == -1) { return Float.NaN; }
    return items[index];
  }

  //used for testing

  Row getRow(final int index) {
    return new Row(items[index], weights[index], normRanks[index]);
  }

  class Row {
    float item;
    long weight;
    double normRank;

    Row(final float item, final long weight, final double normRank) {
      this.item = item;
      this.weight = weight;
      this.normRank = normRank;
    }
  }

  String toString(final int precision, final int fieldSize) {
    final StringBuilder sb = new StringBuilder();
    final int p = precision;
    final int z = fieldSize;
    final String ff = "%" + z + "." + p + "f";
    final String sf = "%" + z + "s";
    final String df = "%"  + z + "d";
    final String dfmt = ff + df + ff + LS;
    final String sfmt = sf + sf + sf + LS;
    sb.append("Aux Detail").append(LS);
    sb.append(String.format(sfmt, "Item", "Weight", "NormRank"));
    final int totalCount = items.length;
    for (int i = 0; i < totalCount; i++) {
      final Row row = getRow(i);
      sb.append(String.format(dfmt, row.item, row.weight, row.normRank));
    }
    return sb.toString();
  }

}

