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

package org.apache.datasketches.quantilescommon;

import java.util.Comparator;
import java.util.Objects;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * This provides efficient, unique and unambiguous binary searching for inequality comparison criteria
 * for ordered arrays of values that may include duplicate values. The inequality criteria include
 * &lt;, &le;, ==, &ge;, &gt;. All the inequality criteria use the same search algorithm.
 * (Although == is not an inequality, it is included for convenience.)
 *
 * <p>In order to make the searching unique and unambiguous, we modified the traditional binary
 * search algorithm to search for adjacent pairs of values <i>{A, B}</i> in the values array
 * instead of just a single value, where <i>a</i> and <i>b</i> are the array indices of two
 * adjacent values in the array. For all the search criteria, when the algorithm has narrowed the
 * search down to a single value or adjacent pair of values, the <i>resolve()</i> method provides the
 * final result of the search. If there is no valid value in the array that satisfies the search
 * criterion, the algorithm will return -1 to the caller.</p>
 *
 * <p>Given a sorted array of values <i>arr[]</i> and a search key value <i>v</i>, the algorithms for
 * the searching criteria are given with each enum criterion.</p>
 *
 * @see <a href="https://datasketches.apache.org/docs/Quantiles/SketchingQuantilesAndRanksTutorial.html">
 * Sketching Quantiles and Ranks Tutorial</a>
 * @author Lee Rhodes
 */
public final class GenericInequalitySearch {

  /**
   * The enumerator of inequalities
   */
  public enum Inequality {

    /**
     * Given a sorted array of increasing values <i>arr[]</i> and a key value <i>v</i>,
     * this criterion instructs the binary search algorithm to find the highest adjacent pair of
     * values <i>{A,B}</i> such that <i>A &lt; v &le; B</i>.<br>
     * Let <i>low</i> = index of the lowest value in the range.<br>
     * Let <i>high</i> = index of the highest value in the range.
     *
     * <p>If <i>v</i> &gt; arr[high], return arr[high].<br>
     * If <i>v</i> &le; arr[low], return -1.<br>
     * Else return index of A.</p>
     */
    LT,

    /**
     * Given a sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
     * this criterion instructs the binary search algorithm to find the highest adjacent pair of
     * values <i>{A,B}</i> such that <i>A &le; V &lt; B</i>.<br>
     * Let <i>low</i> = index of the lowest value in the range.<br>
     * Let <i>high</i> = index of the highest value in the range.
     *
     * <p>If <i>v</i> &ge; arr[high], return arr[high].<br>
     * If <i>v</i> &lt; arr[low], return -1.<br>
     * Else return index of A.</p>
     */
    LE,

    /**
     * Given a sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
     * this criterion instructs the binary search algorithm to find the adjacent pair of
     * values <i>{A,B}</i> such that <i>A &le; V &le; B</i>.
     * The returned value from the binary search algorithm will be the index of <i>A</i> or <i>B</i>,
     * if one of them is equal to <i>V</i>, or -1 if V is not equal to either one.
     */
    EQ,

    /**
     * Given a sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
     * this criterion instructs the binary search algorithm to find the lowest adjacent pair of
     * values <i>{A,B}</i> such that <i>A &lt; V &le; B</i>.<br>
     * Let <i>low</i> = index of the lowest value in the range.<br>
     * Let <i>high</i> = index of the highest value in the range.
     *
     * <p>If <i>v</i> &le; arr[low], return arr[low].<br>
     * If <i>v</i> &gt; arr[high], return -1.<br>
     * Else return index of B.</p>
     */
    GE,

    /**
     * Given a sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
     * this criterion instructs the binary search algorithm to find the lowest adjacent pair of
     * values <i>{A,B}</i> such that <i>A &le; V &lt; B</i>.<br>
     * Let <i>low</i> = index of the lowest value in the range.<br>
     * Let <i>high</i> = index of the highest value in the range.
     *
     * <p>If <i>v</i> &lt; arr[low], return arr[low].<br>
     * If <i>v</i> &ge; arr[high], return -1.<br>
     * Else return index of B.</p>
     */
    GT
  }

  /**
   * Binary Search for the index of the generic value in the given search range that satisfies
   * the given Inequality criterion.
   * If -1 is returned there are no values in the search range that satisfy the inequality.
   *
   * @param arr the given array of comparable values that must be sorted.
   * The array must not be null or empty and the values of the array must not be null (or NaN)
   * in the range [low, high].
   * @param low the lowest index of the lowest value in the search range, inclusive.
   * @param high the highest index of the highest value in the search range, inclusive.
   * @param v the value to search for. It must not be null (or NaN).
   * @param crit one of the Inequality criteria: LT, LE, EQ, GE, GT.  It must not be null.
   * @param comparator for the type T.
   * It must not be null. It must return: -1 if A &lt; B, 0 if A == B, and +1 if A &gt; B.
   * @param <T> The generic type of value to be used in the search process.
   * @return the index of the value in the given search range that satisfies the Inequality criterion.
   */
  public static <T> int find(final T[] arr, final int low, final int high, final T v,
      final Inequality crit, final Comparator<T> comparator) {
    Objects.requireNonNull(arr, "Input arr must not be null");
    Objects.requireNonNull(v,"Input v must not be null");
    Objects.requireNonNull(crit, "Input inequality must not be null");
    Objects.requireNonNull(comparator,"Input comparator must not be null");
    if (arr.length == 0) { throw new SketchesArgumentException("Input array must not be empty."); }

    int lo = low;
    int hi = high;
    while (lo <= hi) {
      if (hi - lo <= 1) {
        return resolve(arr, lo, hi, v, crit, comparator);
      }
      final int mid = lo + (hi - lo) / 2;
      final int ret = compare(arr, mid, mid + 1, v, crit, comparator);
      if (ret == -1 ) { hi = mid; }
      else if (ret == 1) { lo = mid + 1; }
      else  { return getIndex(arr, mid, mid + 1, v, crit, comparator); }
    }
    return -1; //should never return here
  }

  private static <T> int compare(final T[] arr, final int a, final int b, final T v,
      final Inequality crit, final Comparator<T> comparator) {
    int result = 0;
    switch (crit) {
      case GE:
      case LT: {
        result = comparator.compare(v, arr[a]) <= 0 ? -1 : comparator.compare(arr[b], v) < 0 ? 1 : 0;
        break;
      }
      case GT:
      case LE: {
        result = comparator.compare(v, arr[a]) < 0 ? -1 : comparator.compare(arr[b], v) <= 0 ? 1 : 0;
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
      final Inequality crit, final Comparator<T> comparator) {
    int result = 0;
    switch (crit) {
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

  private static <T> int resolve(final T[] arr, final int lo, final int hi, final T v,
      final Inequality crit, final Comparator<T> comparator) {
    int result = 0;
    switch (crit) {
      case LT: {
        result = (lo == hi)
            ? (comparator.compare(v, arr[lo]) > 0 ? lo : -1)
            : (comparator.compare(v, arr[hi]) > 0
                ? hi
                : (comparator.compare(v, arr[lo]) > 0 ? lo : -1));
        break;
      }
      case LE: {
        result = (lo == hi)
            ? (comparator.compare(v, arr[lo]) >= 0 ? lo : -1)
            : (comparator.compare(v, arr[hi]) >= 0
                ? hi
                : (comparator.compare(v, arr[lo]) >= 0 ? lo : -1));
        break;
      }
      case EQ: {
        result = (lo == hi)
            ? (comparator.compare(v, arr[lo]) == 0 ? lo : -1)
            : (comparator.compare(v, arr[hi]) == 0
                ? hi
                : (comparator.compare(v, arr[lo]) == 0 ? lo : -1));
        break;
      }
      case GE: {
        result = (lo == hi)
            ? (comparator.compare(v, arr[lo]) <= 0 ? lo : -1)
            : (comparator.compare(v, arr[lo]) <= 0
                ? lo
                : (comparator.compare(v, arr[hi]) <= 0 ? hi : -1));
        break;
      }
      case GT: {
        result = (lo == hi)
            ? (comparator.compare(v, arr[lo]) < 0 ? lo : -1)
            : (comparator.compare(v, arr[lo]) < 0
                ? lo
                : (comparator.compare(v, arr[hi]) < 0 ? hi : -1));
        break;
      }
    }
    return result;
  }

}
