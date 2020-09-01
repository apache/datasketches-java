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

import org.apache.datasketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
class ReqAuxiliary {
  private float[] items;
  private long[] weights;
  private float[] normRanks;
  private boolean init = false;

  ReqAuxiliary(final ReqSketch sk) {
    buildAuxTable(sk);
  }

  private void buildAuxTable(final ReqSketch sk) {
    final int numComp = sk.compactors.size();
    final int totalItems = sk.size;
    final long N = sk.getN();
    items = new float[totalItems];
    weights = new long[totalItems];
    normRanks = new float[totalItems];
    int curCount = 0;
    for (int i = 0; i < numComp; i++) {
      final ReqCompactor c = sk.compactors.get(i);
      final int len = c.getBuffer().getLength();
      mergeSortIn(sk.compactors.get(i), curCount);
      curCount += len;
    }
    float sum = 0;
    for (int i = 0; i < totalItems; i++) {
      sum += weights[i];
      normRanks[i] = sum / N;
    }
    init = true;
  }

  /**
   * Gets the quantile of the largest normalized rank that is less than the given normalized rank,
   * which must be in the range [0.0, 1.0], inclusive, inclusive
   * @param normRank the given normalized rank
   * @param lteq the less-than or equal to criterion.
   * @return the largest quantile less than the given normalized rank.
   */
  float getQuantile(final float normRank, final boolean lteq) {
    if (!init) {
      throw new SketchesStateException("Aux structure not initialized.");
    }
    final int len = normRanks.length;
    final int index = ReqHelper.binarySearch(normRanks, 0, len - 1, normRank, lteq);
    if (index == -1) { return Float.NaN; }
    return items[index];
  }

  private void mergeSortIn(final ReqCompactor c, final int curCount) {
    final FloatBuffer buf = c.getBuffer();
    if (!buf.isSorted()) { buf.sort(); }
    final float[] arrIn = buf.getArray();
    final long wt = 1 << c.getLgWeight();
    int i = curCount;
    int j = buf.getLength();
    for (int k = i-- + j--; k-- > 0; ) {
      if ((i >= 0) && (j >= 0)) { //both valid
        if (items[i] >= arrIn[j]) {
          items[k] = items[i];
          weights[k] = weights[i--];
        } else {
          items[k] = arrIn[j--];
          weights[k] = wt;
        }
      } else if (i >= 0) { //i is valid
        items[k] = items[i];
        weights[k] = weights[i--];
      } else if (j >= 0) { //j is valid
        items[k] = arrIn[j--];
        weights[k] = wt;
      } else {
        break;
      }
    }
  }

  //used for testing

  Row getRow(final int index) {
    return new Row(items[index], weights[index], normRanks[index]);
  }

  class Row {
    float item;
    long weight;
    float normRank;

    Row(final float item, final long weight, final float normRank) {
      this.item = item;
      this.weight = weight;
      this.normRank = normRank;
    }
  }

}

