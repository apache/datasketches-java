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
  private int pos;
  private boolean init = false;

  ReqAuxiliary(final ReqSketch sk) {
    buildAuxTable(sk);
  }

  void buildAuxTable(final ReqSketch sk) {
    final int numComp = sk.compactors.size();
    final int totalItems = sk.size;
    final long N = sk.getN();
    items = new float[totalItems];
    weights = new long[totalItems];
    normRanks = new float[totalItems];
    pos = 0;

    for (int i = 0; i < numComp; i++) {
      mergeSortIn(sk.compactors.get(i));
    }
    float sum = 0;
    for (int i = 0; i < totalItems; i++) {
      sum += weights[i];
      normRanks[i] = sum / N;
    }
    init = true;
  }

  Row getRow(final int index) {
    return new Row(items[index], weights[index], normRanks[index]);
  }

  /**
   * Gets the quantile of the largest normalized rank that is less than the given normalized rank,
   * which must be in the range [0.0, 1.0], inclusive, inclusive
   * @param normRank the given normalized rank
   * @return the largest quantile less than the given normalized rank.
   */
  float getQuantile(final float normRank) {
    if (!init) {
      throw new SketchesStateException("Aux structure not initialized.");
    }
    final int len = normRanks.length;
    final int index = binarySearch(normRanks, 0, len - 1, normRank);
    if (index == -1) { return Float.NaN; }
    return items[index];
  }

  void mergeSortIn(final ReqCompactor c) {
    final FloatBuffer buf = c.getBuffer();
    if (!buf.isSorted()) { buf.sort(); }
    final float[] arrIn = buf.getArray();
    final long wt = 1 << c.getLgWeight();
    int i = pos;
    int j = buf.getItemCount();
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
    pos += buf.getItemCount();
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

  /**
   * Binary Search for the index of the highest value in the given range that is strictly less-than
   * the key. If -1 is returned there are no values in the range that are strictly less than the key.
   * If there are duplicates in the array and the key is one of those values, the index returned
   * will be the index of the next lower value prior to the sequence of duplicates.
   * @param arr the given array that must be sorted.
   * @param low the index of the lowest value in the range
   * @param high the index of the highest value in the range
   * @param key the value to search for.
   * @return the index of the highest value in the given range that is strictly less than the key
   */
  static int binarySearch(final float[] arr, final int low, final int high, final float key) {
    int lo = low;
    int mid = lo;
    int hi = high;
    while (lo <= hi) {
      mid = lo + ((hi - lo) / 2);
      if      (key < arr[mid]) { hi = mid - 1; }
      else if (key > arr[mid]) { lo = mid + 1; }
      else {
        //println("\nFound: lo: " + lo + " mid: " + mid + " hi: " + hi);
        while ((mid > lo) && (arr[mid - 1] == arr[mid])) { --mid; }
        return mid <= low ? -1 : --mid;
      }
    }
    //println("\nNot Found: lo: " + lo + " mid: " + mid + " hi: " + hi);
    return (hi < low) ? -1 : hi;
  }

  //static final void println(final Object o) { System.out.println(o.toString()); }
}
