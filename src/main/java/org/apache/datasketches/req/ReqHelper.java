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

import static java.lang.Math.round;

/**
 * @author Lee Rhodes
 */
class ReqHelper {

  /**
   * Binary Search for the index of the highest value in the given range that is either less-than or
   * less-than or equal to the key, depending on the state of lteq.
   * If -1 is returned there are no values in the range that are strictly less than or less-than or
   * equal to the key, depending on the state of lteq.
   * If there are duplicates in the array and the key is one of those values, the index returned
   * will be the index of the next lower value prior to the sequence of duplicates if the criteria
   * lteq = false, or the index of the highest value in the sequence of duplicates if the criteria
   * lteq = true.
   * @param arr the given array that must be sorted.
   * @param low the index of the lowest value in the range
   * @param high the index of the highest value in the range
   * @param key the value to search for.
   * @param lteq if true, the terminating criteria is less-than or equals, otherwise the criteria
   * is less-than.
   * @return the index of the highest value in the given range that is strictly less than or
   * less-than or equals to the key, based on the given lteq criteria.
   */
  static int binarySearch(final float[] arr, final int low, final int high, final float key,
      final boolean lteq) {
    int lo = low;
    int mid = lo;
    int hi = high;
    while (lo <= hi) {
      mid = lo + ((hi - lo) / 2);
      if      (key < arr[mid]) { hi = mid - 1; }
      else if (key > arr[mid]) { lo = mid + 1; }
      else { //found
        if (lteq) {
          if (mid == high) { return high; }
          while ((mid < high) && (arr[mid + 1] == arr[mid])) { ++mid; }
          return mid;
        }
        if (mid == low) { return -1; }
        while ((mid > low) && (arr[mid - 1] == arr[mid])) { --mid; }
        return --mid;
      }
    } //not found
    return (hi < low) ? -1 : hi;
  }

  /**
   * Returns the nearest even integer to the given value.
   * @param value the given value
   * @return the nearest even integer to the given value.
   */
  //also used by test
  static final int nearestEven(final double value) {
    return ((int) round(value / 2.0)) << 1;
  }

}
