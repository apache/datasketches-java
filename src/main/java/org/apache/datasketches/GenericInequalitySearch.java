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

/**
 * This provides efficient, unique and unambiguous binary searching for inequalities
 * for ordered arrays of values that may include duplicate values. These
 * inequalities include &lt;, &le;, ==, &ge;, &gt;. The same search method can be used for all
 * these inequalities.
 *
 * <p>In order to make the searching unique and unambiguous, we modified the traditional binary
 * search algorithm to search for adjacent pairs of values <i>{A, B}</i> in the values array
 * instead of just a single value, where <i>A</i> and <i>B</i> are the array indicies of two
 * adjacent values in the array. We then define the searching criteria,
 * given an array of values <i>arr[]</i> and the search key value <i>v</i>, as follows:</p>
 * <ul>
 * <li><b>LT:</b> Find the highest ranked adjacent pair <i>{A, B}</i> such that:<br>
 * <i>arr[A] < v <= arr[B]</i>. Normally we return the index <i>A</i>. However if the
 * search algorithm reaches the ends of the search range, the search algorithm calls the
 * <i>resolve()</i> method to determine what to return to the caller.
 * </li>
 * <li><b>LE:</b>  Find the highest ranked adjacent pair <i>{A, B}</i> such that:<br>
 * <i>arr[A] <= v < arr[B]</i>. Normally we return the index <i>A</i>. However if the
 * search algorithm reaches the ends of the search range, the search algorithm calls the
 * <i>resolve()</i> method to determine what to return to the caller.
 * </li>
 * <li><b>EQ:</b>  Find the adjacent pair <i>{A, B}</i> such that:<br>
 * <i>arr[A] <= v <= arr[B]</i>. We return the index <i>A</i> or <i>B</i> whichever
 * equals <i>v</i>, otherwise we return -1.
 * </li>
 * <li><b>GE:</b>  Find the lowest ranked adjacent pair <i>{A, B}</i> such that:<br>
 * <i>arr[A] < v <= arr[B]</i>. Normally we return the index <i>B</i>. However if the
 * search algorithm reaches the ends of the search range, the search algorithm calls the
 * <i>resolve()</i> method to determine what to return to the caller.
 * </li>
 * <li><b>GT:</b>  Find the lowest ranked adjacent pair <i>{A, B}</i> such that:<br>
 * <i>arr[A] <= v <= arr[B]</i>. Normally we return the index <i>B</i>. However if the
 * search algorithm reaches the ends of the search range, the search algorithm calls the
 * <i>resolve()</i> method to determine what to return to the caller.
 * </li>
 * </ul>
 *
 * @author Lee Rhodes
 */
public class GenericInequalitySearch {

  /**
   * The enumerator of inequalities
   */
  public enum Inequality {

    /**
     * Less-Than
     */
    LT,

    /**
     * Less-Than Or Equals
     */
    LE,

    /**
     * Equals. Although not an inequality, it is included for completeness.
     */
    EQ,

    /**
     * Greater-Than Or Equals
     */
    GE,

    /**
     * Greater-Than
     */
    GT
  }

  /**
   * Constructs this class
   */
  public GenericInequalitySearch() { }

  /**
   * Binary Search for the index of the generic value in the given search range that satisfies
   * the given inequality.
   * If -1 is returned there are no values in the search range that satisfy the inequality.
   *
   * @param arr the given array that must be sorted.
   * @param low the index of the lowest value in the search range
   * @param high the index of the highest value in the search range
   * @param v the value to search for.
   * @param inequality one of LT, LE, EQ, GE, GT
   * @param comparator for the type T
   * @param <T> The generic type of value to be used in the search process.
   * @return the index of the value in the given search range that satisfies the inequality.
   */
  public static <T> int find(final T[] arr, final int low, final int high, final T v,
      final Inequality inequality, final Comparator<T> comparator) {
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
