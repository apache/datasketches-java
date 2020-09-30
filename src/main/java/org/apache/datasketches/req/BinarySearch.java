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

/**
 * Contains binary search algorithms
 * @author Lee Rhodes
 */
class BinarySearch {

  /**
   * Binary Search for the index of the double value in the given range that satisfies
   * the given comparison criterion.
   * If -1 is returned there are no values in the range that satisfy the criterion.
   *
   * @param arr the given array that must be sorted.
   * @param low the index of the lowest value in the range
   * @param high the index of the highest value in the range
   * @param v the value to search for.
   * @param crit one of LT, LE, GT, GE
   * @return the index of the value in the given range that satisfies the criterion
   */
  static int binarySearchDouble(final double[] arr, final int low, final int high, final double v,
      final Criteria crit) {
    int lo = low;
    int hi = high - 1;
    int ret;
    while (lo <= hi) {
      final int midA = lo + (hi - lo) / 2;
      ret = crit.compare(arr, midA, midA + 1, v);
      if (ret == -1 ) { hi = midA - 1; }
      else if (ret == 1) { lo = midA + 1; }
      else  { return crit.getIndex(midA, midA + 1); }
    }
    return crit.resolve(lo, hi, low, high);
  }

  /**
   * Binary Search for the index of the float value in the given range that satisfies
   * the given comparison criterion.
   * If -1 is returned there are no values in the range that satisfy the criterion.
   *
   * @param arr the given array that must be sorted.
   * @param low the index of the lowest value in the range
   * @param high the index of the highest value in the range
   * @param v the value to search for.
   * @param crit one of LT, LE, GT, GE
   * @return the index of the value in the given range that satisfies the criterion
   */
  static int binarySearchFloat(final float[] arr, final int low, final int high, final float v,
      final Criteria crit) {
    int lo = low;
    int hi = high - 1;
    int ret;
    while (lo <= hi) {
      final int midA = lo + (hi - lo) / 2;
      ret = crit.compare(arr, midA, midA + 1, v);
      if (ret == -1 ) { hi = midA - 1; }
      else if (ret == 1) { lo = midA + 1; }
      else  { return crit.getIndex(midA, midA + 1); }
    }
    return crit.resolve(lo, hi, low, high);
  }

}
