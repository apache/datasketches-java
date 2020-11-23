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

package org.apache.datasketches;

/**
 * Contains binary search algorithms for &lt;, &le;, ==, &ge;, and &gt;.
 *
 * @author Lee Rhodes
 */
public final class BinarySearch {

  /**
   * Binary Search for the index of the double value in the given search range that satisfies
   * the given comparison criterion.
   * If -1 is returned there are no values in the search range that satisfy the criterion.
   *
   * @param arr the given array that must be sorted.
   * @param low the index of the lowest value in the search range
   * @param high the index of the highest value in the search range
   * @param v the value to search for.
   * @param crit one of LT, LE, EQ, GT, GE
   * @return the index of the value in the given search range that satisfies the criterion
   */
  public static int find(final double[] arr, final int low, final int high,
      final double v, final Criteria crit) {
    int lo = low;
    int hi = high - 1;
    int ret;
    while (lo <= hi) {
      final int midA = lo + (hi - lo) / 2;
      ret = crit.compare(arr, midA, midA + 1, v);
      if (ret == -1 ) { hi = midA - 1; }
      else if (ret == 1) { lo = midA + 1; }
      else  { return crit.getIndex(arr, midA, midA + 1, v); }
    }
    return crit.resolve(lo, hi, low, high);
  }

  /**
   * Binary Search for the index of the float value in the given search range that satisfies
   * the given comparison criterion.
   * If -1 is returned there are no values in the search range that satisfy the criterion.
   *
   * @param arr the given array that must be sorted.
   * @param low the index of the lowest value in the search range
   * @param high the index of the highest value in the search range
   * @param v the value to search for.
   * @param crit one of LT, LE, EQ, GT, GE
   * @return the index of the value in the given search range that satisfies the criterion
   */
  public static int find(final float[] arr, final int low, final int high,
      final float v, final Criteria crit) {
    int lo = low;
    int hi = high - 1;
    int ret;
    while (lo <= hi) {
      final int mid = lo + (hi - lo) / 2;
      ret = crit.compare(arr, mid, mid + 1, v);
      if (ret == -1 ) { hi = mid - 1; }
      else if (ret == 1) { lo = mid + 1; }
      else  { return crit.getIndex(arr, mid, mid + 1, v); }
    }
    return crit.resolve(lo, hi, low, high);
  }

  /**
   * Binary Search for the index of the long value in the given search range that satisfies
   * the given comparison criterion.
   * If -1 is returned there are no values in the search range that satisfy the criterion.
   *
   * @param arr the given array that must be sorted.
   * @param low the index of the lowest value in the search range
   * @param high the index of the highest value in the search range
   * @param v the value to search for.
   * @param crit one of LT, LE, EQ, GT, GE
   * @return the index of the value in the given search range that satisfies the criterion
   */
  public static int find(final long[] arr, final int low, final int high,
      final long v, final Criteria crit) {
    int lo = low;
    int hi = high - 1;
    int ret;
    while (lo <= hi) {
      final int mid = lo + (hi - lo) / 2;
      ret = crit.compare(arr, mid, mid + 1, v);
      if (ret == -1 ) { hi = mid - 1; }
      else if (ret == 1) { lo = mid + 1; }
      else  { return crit.getIndex(arr, mid, mid + 1, v); }
    }
    return crit.resolve(lo, hi, low, high);
  }

  /**
   * Binary Search for the index of the exact float value in the given search range.
   * If -1 is returned there are no values in the search range that satisfy the criterion.
   * @param arr The given array to search.
   * @param low the index of the lowest value of the search range
   * @param high the index of the higest value of the search range
   * @param v the value to search for
   * @return return the index of the value, if found, otherwise, return -1;
   */
  public static int find(final float[] arr, final int low, final int high, final float v) {
    int lo = low;
    int hi = high;
    while (lo <= hi) {
      final int mid = lo + (hi - lo) / 2;
      if (v < arr[mid]) { hi = mid - 1; }
      else {
        if (v > arr[mid]) { lo = mid + 1; }
        else { return mid; }
      }
    }
    return -1;
  }

  /**
   * Binary Search for the index of the exact double value in the given search range.
   * If -1 is returned there are no values in the search range that satisfy the criterion.
   * @param arr The given array to search.
   * @param low the index of the lowest value of the search range
   * @param high the index of the higest value of the search range
   * @param v the value to search for
   * @return return the index of the value, if found, otherwise, return -1;
   */
  public static int find(final double[] arr, final int low, final int high, final double v) {
    int lo = low;
    int hi = high;
    while (lo <= hi) {
      final int mid = lo + (hi - lo) / 2;
      if (v < arr[mid]) { hi = mid - 1; }
      else {
        if (v > arr[mid]) { lo = mid + 1; }
        else { return mid; }
      }
    }
    return -1;
  }

}
