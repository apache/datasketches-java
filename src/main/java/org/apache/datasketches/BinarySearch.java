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
 * Contains common equality binary search algorithms.
 *
 * @author Lee Rhodes
 */
public final class BinarySearch {

  /**
   * Binary Search for the index of the exact float value in the given search range.
   * If -1 is returned there are no values in the search range that equals the given value.
   * @param arr The given ordered array to search.
   * @param low the index of the lowest value of the search range
   * @param high the index of the highest value of the search range
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
   * If -1 is returned there are no values in the search range that equals the given value.
   * @param arr The given ordered array to search.
   * @param low the index of the lowest value of the search range
   * @param high the index of the highest value of the search range
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

  /**
   * Binary Search for the index of the exact long value in the given search range.
   * If -1 is returned there are no values in the search range that equals the given value.
   * @param arr The given ordered array to search.
   * @param low the index of the lowest value of the search range
   * @param high the index of the highest value of the search range
   * @param v the value to search for
   * @return return the index of the value, if found, otherwise, return -1;
   */
  public static int find(final long[] arr, final int low, final int high, final long v) {
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
