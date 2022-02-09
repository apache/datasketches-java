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

import java.util.Comparator;
import java.util.Objects;

/**
 * This provides efficient, unique and unambiguous binary searching for inequalities
 * for ordered arrays of increasing values that may include duplicate values. These
 * inequalities include &lt;, &le;, ==, &ge;, &gt;. The same search method can be used for all
 * these inequalities.
 *
 * <p>In order to make the searching unique and unambiguous, we modified the traditional binary
 * search algorithm to search for adjacent pairs of values <i>{A, B}</i> in the values array
 * instead of just a single value, where <i>A</i> and <i>B</i> are the array indices of two
 * adjacent values in the array. We then define the searching criteria,
 * given an array of values <i>arr[]</i> and the search key value <i>v</i>.</p>
 *
 * @author Lee Rhodes
 */
public class GenericInequalitySearch {

  /**
   * The enumerator of inequalities
   */
  public enum Inequality {

    /**
     * <b>Less-Than:</b> Find the highest ranked adjacent ordered pair <i>{A, B}</i> such that:<br>
     * <i>arr[A] &lt; v &le; arr[B]</i> within the given range.<br>
     * Let <i>low</i> = lowest index of the lowest value in the range.<br>
     * Let <i>high</i> = highest index of the highest value in the range.
     *
     * <p>If <i>v</i> &gt; arr[high], return arr[high].<br>
     * If <i>v</i> &le; arr[low], return -1.<br>
     * Else return index of A.</p>
     */
    LT,

    /**
     * <b>Less-Than Or Equals:</b> Find the highest ranked adjacent ordered pair <i>{A, B}</i> such that:<br>
     * <i>arr[A] &le; v &lt; arr[B]</i>.<br>
     * Let <i>low</i> = lowest index of the lowest value in the range.<br>
     * Let <i>high</i> = highest index of the highest value in the range.
     *
     * <p>If <i>v</i> &ge; arr[high], return arr[high].<br>
     * If <i>v</i> &lt; arr[low], return -1.<br>
     * Else return index of A.</p>
     */
    LE,

    /**
     * <b>Equals:</b> Although not an inequality, it is included for completeness.
     * An index &ge; 0 is returned unless not found, then -1 is returned.
     */
    EQ,

    /**
     * <b>Greater-Than Or Equals:</b> Find the lowest ranked adjacent pair <i>{A, B}</i> such that:<br>
     * <i>arr[A] &lt; v &le; arr[B]</i>.<br>
     * Let <i>low</i> = lowest index of the lowest value in the range.<br>
     * Let <i>high</i> = highest index of the highest value in the range.
     *
     * <p>If <i>v</i> &le; arr[low], return arr[low].<br>
     * If <i>v</i> &gt; arr[high], return -1.<br>
     * Else return index of B.</p>
     */
    GE,

    /**
     * <b>Greater-Than:</b> Find the lowest ranked adjacent pair <i>{A, B}</i> such that:<br>
     * <i>arr[A] &le; v &lt; arr[B]</i>.<br>
     * Let <i>low</i> = lowest index of the lowest value in the range.<br>
     * Let <i>high</i> = highest index of the highest value in the range.
     *
     * <p>If <i>v</i> &lt; arr[low], return arr[low].<br>
     * If <i>v</i> &ge; arr[high], return -1.<br>
     * Else return index of B.</p>
     */
    GT
  }

  /**
   * Binary Search for the index of the generic value in the given search range that satisfies
   * the given inequality.
   * If -1 is returned there are no values in the search range that satisfy the inequality.
   *
   * @param arr the given array that must be sorted with increasing values, must not be null,
   * and must not contain null values in the given range {low, high} inclusive.
   * @param low the lowest index of the lowest value in the search range, inclusive.
   * @param high the highest index of the highest value in the search range, inclusive.
   * @param v the value to search for. It must not be null.
   * @param inequality one of LT, LE, EQ, GE, GT.  It must not be null.
   * @param comparator for the type T. It must not be null.
   * @param <T> The generic type of value to be used in the search process.
   * @return the index of the value in the given search range that satisfies the inequality.
   */
  public static <T> int find(final T[] arr, final int low, final int high, final T v,
      final Inequality inequality, final Comparator<T> comparator) {
    Objects.requireNonNull(arr, "Input arr must not be null");
    Objects.requireNonNull(v,"Input v must not be null");
    Objects.requireNonNull(inequality, "Input inequality must not be null");
    Objects.requireNonNull(comparator,"Input comparator must not be null");

    int lo = low;
    int hi = high - 1;
    int ret;
    while (lo <= hi) {
      final int midA = lo + (hi - lo) / 2;
      ret = compare(arr, midA, midA + 1, v, inequality, comparator);
      if (ret == -1 ) { hi = midA - 1; }
      else if (ret == 1) { lo = midA + 1; }
      else  { return getIndex(arr, midA, midA + 1, v, inequality, comparator); }
    }
    return resolve(lo, hi, low, high, inequality);
  }

  private static <T> int compare(final T[] arr, final int a, final int b, final T v,
      final Inequality inequality, final Comparator<T> comparator) {
    int result = 0;
    switch (inequality) {
      case GE:
      case LT: {
        result = comparator.compare(v, arr[a]) < 1 ? -1 : comparator.compare(arr[b], v) < 0 ? 1 : 0;
        break;
      }
      case GT:
      case LE: {
        result = comparator.compare(v, arr[a]) < 0 ? -1 : comparator.compare(arr[b], v) < 1 ? 1 : 0;
        break;
      }
      case EQ: {
        result = comparator.compare(v, arr[a]) < 0 ? -1 : comparator.compare(arr[b], v) < 0 ? 1 : 0;
        break;
      }
    }
    return result;
  }

  private static <T> int getIndex(final T[] arr, final int a, final int b, final T v,
      final Inequality inequality, final Comparator<T> comparator) {
    int result = 0;
    switch (inequality) {
      case LT:
      case LE: {
        result = a; break;
      }
      case GE:
      case GT: {
        result = b; break;
      }
      case EQ: {
        result = comparator.compare(v, arr[a]) == 0 ? a : comparator.compare(v, arr[b]) == 0 ? b : -1;
      }
    }
    return result;
  }

  private static int resolve(final int lo, final int hi, final int low, final int high,
      final Inequality inequality) {
    int result = 0;
    switch (inequality) {
      case LT: {
        result = lo >= high ? high : -1;
        break;
      }
      case LE: {
        result = lo >= high ? high : -1;
        break;
      }
      case EQ: {
        result = -1;
        break;
      }
      case GE: {
        result = hi <= low ? low : -1;
        break;
      }
      case GT: {
        result = hi <= low ? low : -1;
        break;
      }
    }
    return result;
  }

}
