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

import java.util.Objects;

/**
 * This provides efficient, unique and unambiguous binary searching for inequality comparison criteria
 * for ordered arrays of values that may include duplicate values. The inequality criteria include
 * &lt;, &le;, ==, &ge;, &gt;. All the inequality criteria use the same search algorithm.
 * (Although == is not an inequality, it is included for convenience.)
 *
 * <p>In order to make the searching unique and unambiguous, we modified the traditional binary
 * search algorithm to search for adjacent pairs of values <i>{A, B}</i> in the values array
 * instead of just a single value, where <i>A</i> and <i>B</i> are the array indices of two
 * adjacent values in the array. For all the search criteria, if the algorithm reaches the ends of
 * the search range, the algorithm calls the <i>resolve()</i> method to determine what to
 * return to the caller. If the key value cannot be resolved, it returns a -1 to the caller.
 *
 * <p>Given a sorted array of values <i>arr[]</i> and the search key value <i>v</i>, the algorithms for
 * the searching criteria are given with each enum criterion.</p>
 *
 * @author Lee Rhodes
 */
public enum InequalitySearch {

  /**
   * Given a sorted array of increasing values <i>arr[]</i> and a key value <i>v</i>,
   * this criterion instructs the binary search algorithm to find the highest adjacent pair of
   * values <i>{A,B}</i> such that <i>A &lt; v &le; B</i>.<br>
   * Let <i>low</i> = lowest index of the lowest value in the range.<br>
   * Let <i>high</i> = highest index of the highest value in the range.
   *
   * <p>If <i>v</i> &gt; arr[high], return arr[high].<br>
   * If <i>v</i> &le; arr[low], return -1.<br>
   * Else return index of A.</p>
   */
  LT { //arr[A] < V <= arr[B], return A
    @Override
    int compare(final double[] arr, final int a, final int b, final double v) {
      return v <= arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int compare(final float[] arr, final int a, final int b, final float v) {
      return v <= arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int compare(final long[] arr, final int a, final int b, final long v) {
      return v <= arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int getIndex(final double[] arr, final int a, final int b, final double v) {
      return a;
    }

    @Override
    int getIndex(final float[] arr, final int a, final int b, final float v) {
      return a;
    }

    @Override
    int getIndex(final long[] arr, final int a, final int b, final long v) {
      return a;
    }

    @Override
    int resolve(final int lo, final int hi, final int low, final int high) {
      if (lo >= high) { return high; }
      return -1;
    }

    @Override
    String desc(final double[] arr, final int low, final int high, final double v, final int idx) {
      if (idx == -1) {
        return "LT: " + v + " <= arr[" + low + "]=" + arr[low] + "; return -1";
      }
      if (idx == high) {
        return "LT: " + v + " > arr[" + high + "]=" + arr[high]
            + "; return arr[" + high + "]=" + arr[high];
      } //idx < high
      return "LT: " + v
      + ": arr[" + idx + "]=" + arr[idx] + " < " + v + " <= arr[" + (idx + 1) + "]=" + arr[idx + 1]
      + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final float[] arr, final int low, final int high, final float v, final int idx) {
      if (idx == -1) {
        return "LT: " + v + " <= arr[" + low + "]=" + arr[low] + "; return -1";
      }
      if (idx == high) {
        return "LT: " + v + " > arr[" + high + "]=" + arr[high]
            + "; return arr[" + high + "]=" + arr[high];
      } //idx < high
      return "LT: " + v
      + ": arr[" + idx + "]=" + arr[idx] + " < " + v + " <= arr[" + (idx + 1) + "]=" + arr[idx + 1]
      + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final long[] arr, final int low, final int high, final long v, final int idx) {
      if (idx == -1) {
        return "LT: " + v + " <= arr[" + low + "]=" + arr[low] + "; return -1";
      }
      if (idx == high) {
        return "LT: " + v + " > arr[" + high + "]=" + arr[high]
            + "; return arr[" + high + "]=" + arr[high];
      } //idx < high
      return "LT: " + v
      + ": arr[" + idx + "]=" + arr[idx] + " < " + v + " <= arr[" + (idx + 1) + "]=" + arr[idx + 1]
      + "; return arr[" + idx + "]=" + arr[idx];
    }
  },

  /**
   * Given a sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
   * this criterion instructs the binary search algorithm to find the highest adjacent pair of
   * values <i>{A,B}</i> such that <i>A &le; V &lt; B</i>.<br>
   * Let <i>low</i> = lowest index of the lowest value in the range.<br>
   * Let <i>high</i> = highest index of the highest value in the range.
   *
   * <p>If <i>v</i> &ge; arr[high], return arr[high].<br>
   * If <i>v</i> &lt; arr[low], return -1.<br>
   * Else return index of A.</p>
   */
  LE { //arr[A] <= V < arr[B], return A
    @Override
    int compare(final double[] arr, final int a, final int b, final double v) {
      return v < arr[a] ? -1 : arr[b] <= v ? 1 : 0;
    }

    @Override
    int compare(final float[] arr, final int a, final int b, final float v) {
      return v < arr[a] ? -1 : arr[b] <= v ? 1 : 0;
    }

    @Override
    int compare(final long[] arr, final int a, final int b, final long v) {
      return v < arr[a] ? -1 : arr[b] <= v ? 1 : 0;
    }

    @Override
    int getIndex(final double[] arr, final int a, final int b, final double v) {
      return a;
    }

    @Override
    int getIndex(final float[] arr, final int a, final int b, final float v) {
      return a;
    }

    @Override
    int getIndex(final long[] arr, final int a, final int b, final long v) {
      return a;
    }

    @Override
    int resolve(final int lo, final int hi, final int low, final int high) {
      if (lo >= high) { return high; }
      return -1;
    }

    @Override
    String desc(final double[] arr, final int low, final int high, final double v, final int idx) {
      if (idx == -1) {
        return "LE: " + v + " < arr[" + low + "]=" + arr[low] + "; return -1";
      }
      if (idx == high) {
        return "LE: " + v + " >= arr[" + high + "]=" + arr[high]
            + "; return arr[" + high + "]=" + arr[high];
      }
      return "LE: " + v
      + ": arr[" + idx + "]=" + arr[idx] + " <= " + v + " < arr[" + (idx + 1) + "]=" + arr[idx + 1]
          + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final float[] arr, final int low, final int high, final float v, final int idx) {
      if (idx == -1) {
        return "LE: " + v + " < arr[" + low + "]=" + arr[low] + "; return -1";
      }
      if (idx == high) {
        return "LE: " + v + " >= arr[" + high + "]=" + arr[high]
            + "; return arr[" + high + "]=" + arr[high];
      }
      return "LE: " + v
      + ": arr[" + idx + "]=" + arr[idx] + " <= " + v + " < arr[" + (idx + 1) + "]=" + arr[idx + 1]
          + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final long[] arr, final int low, final int high, final long v, final int idx) {
      if (idx == -1) {
        return "LE: " + v + " < arr[" + low + "]=" + arr[low] + "; return -1";
      }
      if (idx == high) {
        return "LE: " + v + " >= arr[" + high + "]=" + arr[high]
            + "; return arr[" + high + "]=" + arr[high];
      }
      return "LE: " + v
      + ": arr[" + idx + "]=" + arr[idx] + " <= " + v + " < arr[" + (idx + 1) + "]=" + arr[idx + 1]
          + "; return arr[" + idx + "]=" + arr[idx];
    }
  },

  /**
   * Given a sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
   * this criterion instructs the binary search algorithm to find the adjacent pair of
   * values <i>{A,B}</i> such that <i>A &le; V &le; B</i>.
   * The returned value from the binary search algorithm will be the index of <i>A</i> or <i>B</i>,
   * if one of them is equal to <i>V</i>, or -1 if V is not equal to either one.
   */
  EQ { //arr[A] <= V <= arr[B], return A or B
    @Override
    int compare(final double[] arr, final int a, final int b, final double v) {
      return v < arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int compare(final float[] arr, final int a, final int b, final float v) {
      return v < arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int compare(final long[] arr, final int a, final int b, final long v) {
      return v < arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int getIndex(final double[] arr, final int a, final int b, final double v) {
      return v == arr[a] ? a : v == arr[b] ? b : -1;
    }

    @Override
    int getIndex(final float[] arr, final int a, final int b, final float v) {
      return v == arr[a] ? a : v == arr[b] ? b : -1;
    }

    @Override
    int getIndex(final long[] arr, final int a, final int b, final long v) {
      return v == arr[a] ? a : v == arr[b] ? b : -1;
    }

    @Override
    int resolve(final int lo, final int hi, final int low, final int high) {
      return -1;
    }

    @Override
    String desc(final double[] arr, final int low, final int high, final double v, final int idx) {
      if (idx == -1) {
        if (v > arr[high]) {
          return "EQ: " + v + " > arr[" + high + "]; return -1";
        }
        if (v < arr[low]) {
          return "EQ: " + v + " < arr[" + low + "]; return -1";
        }
        return "EQ: " + v + " Cannot be found within arr[" + low + "], arr[" + high + "]; return -1";
      }
      return "EQ: " + v + " == arr[" + idx + "]; return " + idx;
    }

    @Override
    String desc(final float[] arr, final int low, final int high, final float v, final int idx) {
      if (idx == -1) {
        if (v > arr[high]) {
          return "EQ: " + v + " > arr[" + high + "]; return -1";
        }
        if (v < arr[low]) {
          return "EQ: " + v + " < arr[" + low + "]; return -1";
        }
        return "EQ: " + v + " Cannot be found within arr[" + low + "], arr[" + high + "]; return -1";
      }
      return "EQ: " + v + " == arr[" + idx + "]; return " + idx;
    }

    @Override
    String desc(final long[] arr, final int low, final int high, final long v, final int idx) {
      if (idx == -1) {
        if (v > arr[high]) {
          return "EQ: " + v + " > arr[" + high + "]; return -1";
        }
        if (v < arr[low]) {
          return "EQ: " + v + " < arr[" + low + "]; return -1";
        }
        return "EQ: " + v + " Cannot be found within arr[" + low + "], arr[" + high + "]; return -1";
      }
      return "EQ: " + v + " == arr[" + idx + "]; return " + idx;
    }
  },

  /**
   * Given a sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
   * this criterion instructs the binary search algorithm to find the lowest adjacent pair of
   * values <i>{A,B}</i> such that <i>A &lt; V &le; B</i>.<br>
   * Let <i>low</i> = lowest index of the lowest value in the range.<br>
   * Let <i>high</i> = highest index of the highest value in the range.
   *
   * <p>If <i>v</i> &le; arr[low], return arr[low].<br>
   * If <i>v</i> &gt; arr[high], return -1.<br>
   * Else return index of B.</p>
   */
  GE { //arr[A] < V <= arr[B], return B
    @Override
    int compare(final double[] arr, final int a, final int b, final double v) {
      return v <= arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int compare(final float[] arr, final int a, final int b, final float v) {
      return v <= arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int compare(final long[] arr, final int a, final int b, final long v) {
      return v <= arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int getIndex(final double[] arr, final int a, final int b, final double v) {
      return b;
    }

    @Override
    int getIndex(final float[] arr, final int a, final int b, final float v) {
      return b;
    }

    @Override
    int getIndex(final long[] arr, final int a, final int b, final long v) {
      return b;
    }

    @Override
    int resolve(final int lo, final int hi, final int low, final int high) {
      if (hi <= low) { return low; }
      return -1;
    }

    @Override
    String desc(final double[] arr, final int low, final int high, final double v, final int idx) {
      if (idx == -1) {
        return "GE: " + v + " > arr[" + high + "]=" + arr[high] + "; return -1";
      }
      if (idx == low) {
        return "GE: " + v + " <= arr[" + low + "]=" + arr[low]
            + "; return arr[" + low + "]=" + arr[low];
      } //idx > low
      return "GE: " + v
      + ": arr[" + (idx - 1) + "]=" + arr[idx - 1] + " < " + v + " <= arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final float[] arr, final int low, final int high, final float v, final int idx) {
      if (idx == -1) {
        return "GE: " + v + " > arr[" + high + "]=" + arr[high] + "; return -1";
      }
      if (idx == low) {
        return "GE: " + v + " <= arr[" + low + "]=" + arr[low]
            + "; return arr[" + low + "]=" + arr[low];
      } //idx > low
      return "GE: " + v
      + ": arr[" + (idx - 1) + "]=" + arr[idx - 1] + " < " + v + " <= arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final long[] arr, final int low, final int high, final long v, final int idx) {
      if (idx == -1) {
        return "GE: " + v + " > arr[" + high + "]=" + arr[high] + "; return -1";
      }
      if (idx == low) {
        return "GE: " + v + " <= arr[" + low + "]=" + arr[low]
            + "; return arr[" + low + "]=" + arr[low];
      } //idx > low
      return "GE: " + v
      + ": arr[" + (idx - 1) + "]=" + arr[idx - 1] + " < " + v + " <= arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
    }
  },

  /**
   * Given a sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
   * this criterion instructs the binary search algorithm to find the lowest adjacent pair of
   * values <i>{A,B}</i> such that <i>A &le; V &lt; B</i>.<br>
   * Let <i>low</i> = lowest index of the lowest value in the range.<br>
   * Let <i>high</i> = highest index of the highest value in the range.
   *
   * <p>If <i>v</i> &lt; arr[low], return arr[low].<br>
   * If <i>v</i> &ge; arr[high], return -1.<br>
   * Else return index of B.</p>
   */
  GT { //arr[A] <= V < arr[B], return B
    @Override
    int compare(final double[] arr, final int a, final int b, final double v) {
      return v < arr[a] ? -1 : arr[b] <= v ? 1 : 0;
    }

    @Override
    int compare(final float[] arr, final int a, final int b, final float v) {
      return v < arr[a] ? -1 : arr[b] <= v ? 1 : 0;
    }

    @Override
    int compare(final long[] arr, final int a, final int b, final long v) {
      return v < arr[a] ? -1 : arr[b] <= v ? 1 : 0;
    }

    @Override
    int getIndex(final double[] arr, final int a, final int b, final double v) {
      return b;
    }

    @Override
    int getIndex(final float[] arr, final int a, final int b, final float v) {
      return b;
    }

    @Override
    int getIndex(final long[] arr, final int a, final int b, final long v) {
      return b;
    }

    @Override
    int resolve(final int lo, final int hi, final int low, final int high) {
      if (hi <= low) { return low; }
      return -1;
    }

    @Override
    String desc(final double[] arr, final int low, final int high, final double v, final int idx) {
      if (idx == -1) {
        return "GT: " + v + " >= arr[" + high + "]=" + arr[high] + "; return -1";
      }
      if (idx == low) {
        return "GT: " + v + " < arr[" + low + "]=" + arr[low]
            + "; return arr[" + low + "]=" + arr[low];
      } //idx > low
      return "GT: " + v
      + ": arr[" + (idx - 1) + "]=" + arr[idx - 1] + " <= " + v + " < arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final float[] arr, final int low, final int high, final float v, final int idx) {
      if (idx == -1) {
        return "GT: " + v + " >= arr[" + high + "]=" + arr[high] + "; return -1";
      }
      if (idx == low) {
        return "GT: " + v + " < arr[" + low + "]=" + arr[low]
            + "; return arr[" + low + "]=" + arr[low];
      } //idx > low
      return "GT: " + v
      + ": arr[" + (idx - 1) + "]=" + arr[idx - 1] + " <= " + v + " < arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final long[] arr, final int low, final int high, final long v, final int idx) {
      if (idx == -1) {
        return "GT: " + v + " >= arr[" + high + "]=" + arr[high] + "; return -1";
      }
      if (idx == low) {
        return "GT: " + v + " < arr[" + low + "]=" + arr[low]
            + "; return arr[" + low + "]=" + arr[low];
      } //idx > low
      return "GT: " + v
      + ": arr[" + (idx - 1) + "]=" + arr[idx - 1] + " <= " + v + " < arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
    }
  };

  /**
   * The call to compare index a and index b with the value v.
   * @param arr The underlying sorted array of double values
   * @param a the lower index of the current pair
   * @param b the higher index of the current pair
   * @param v the double value to search for
   * @return +1, which means we must search higher in the array, or -1, which means we must
   * search lower in the array, or 0, which means we have found the correct bounding pair.
   */
  abstract int compare(double[] arr, int a, int b, double v);

  /**
   * The call to compare index a and index b with the value v.
   * @param arr The underlying sorted array of float values
   * @param a the lower index of the current pair
   * @param b the higher index of the current pair
   * @param v the float value to search for
   * @return +1, which means we must search higher in the array, or -1, which means we must
   * search lower in the array, or 0, which means we have found the correct bounding pair.
   */
  abstract int compare(float[] arr, int a, int b, float v);

  /**
   * The call to compare index a and index b with the value v.
   * @param arr The underlying sorted array of long values
   * @param a the lower index of the current pair
   * @param b the higher index of the current pair
   * @param v the long value to search for
   * @return +1, which means we must search higher in the array, or -1, which means we must
   * search lower in the array, or 0, which means we have found the correct bounding pair.
   */
  abstract int compare(long[] arr, int a, int b, long v);

  /**
   * If the compare operation returns 0, which means "found", this returns the index of the
   * found value that satisfies the selected criteria.
   * @param arr the array being searched
   * @param a the lower index of the current pair
   * @param b the higher index of the current pair
   * @param v the value being searched for.
   * @return the index of the found value that satisfies the selected criteria.
   */
  abstract int getIndex(double[] arr, int a, int b, double v);

  /**
   * If the compare operation returns 0, which means "found", this returns the index of the
   * found value that satisfies the selected criteria.
   * @param arr the array being searched
   * @param a the lower index of the current pair
   * @param b the higher index of the current pair
   * @param v the value being searched for.
   * @return the index of the found value that satisfies the selected criteria.
   */
  abstract int getIndex(float[] arr, int a, int b, float v);

  /**
   * If the compare operation returns 0, which means "found", this returns the index of the
   * found value that satisfies the selected criteria.
   * @param arr the array being searched
   * @param a the lower index of the current pair
   * @param b the higher index of the current pair
   * @param v the value being searched for.
   * @return the index of the found value that satisfies the selected criteria.
   */
  abstract int getIndex(long[] arr, int a, int b, long v);

  /**
   * Called to resolve what to do if not found. In the search algorithm this occurs when the
   * <i>lo</i> and <i>hi</i> indices become inverted at the ends of the array.
   * This resolve method then determines what to do to resolve what to return based on the
   * criterion.
   * @param lo the current lo value
   * @param hi the current hi value
   * @param low the low index of the range
   * @param high the high index of the range
   * @return the index of the resolution or -1, if it cannot be resolved.
   */
  abstract int resolve(int lo, int hi, int low, int high);

  /**
   * Optional call that describes the details of the results of the search.
   * Used primarily for debugging.
   * @param arr The underlying sorted array of double values
   * @param low the low index of the range
   * @param high the high index of the range
   * @param v the double value to search for
   * @param idx the resolved index from the search
   * @return the descriptive string.
   */
  abstract String desc(double[] arr, int low, int high, double v, int idx);

  /**
   * Optional call that describes the details of the results of the search.
   * Used primarily for debugging.
   * @param arr The underlying sorted array of double values
   * @param low the low index of the range
   * @param high the high index of the range
   * @param v the double value to search for
   * @param idx the resolved index from the search
   * @return the descriptive string.
   */
  abstract String desc(float[] arr, int low, int high, float v, int idx);

  /**
   * Optional call that describes the details of the results of the search.
   * Used primarily for debugging.
   * @param arr The underlying sorted array of double values
   * @param low the low index of the range
   * @param high the high index of the range
   * @param v the double value to search for
   * @param idx the resolved index from the search
   * @return the descriptive string.
   */
  abstract String desc(long[] arr, int low, int high, long v, int idx);

  /**
   * Binary Search for the index of the double value in the given search range that satisfies
   * the given InequalitySearch criterion.
   * If -1 is returned there are no values in the search range that satisfy the criterion.
   *
   * @param arr the given array that must be sorted.
   * It must not be null and must not contain any NaN values in the range {low, high} inclusive.
   * @param low the lowest index of the lowest value in the search range, inclusive.
   * @param high the highest index of the highest value in the search range, inclusive.
   * @param v the value to search for. It must not be NaN.
   * @param crit one of LT, LE, EQ, GT, GE. It must not be null.
   * @return the index of the value in the given search range that satisfies the criterion
   */
  public static int find(final double[] arr, final int low, final int high,
      final double v, final InequalitySearch crit) {
    Objects.requireNonNull(arr, "Inpurt arr must not be null");
    Objects.requireNonNull(crit, "Input crit must not be null");
    if (Double.isNaN(v)) { throw new SketchesArgumentException("Input v must not be NaN."); }
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
   * the given InequalitySearch criterion.
   * If -1 is returned there are no values in the search range that satisfy the criterion.
   *
   * @param arr the given array that must be sorted.
   * It must not be null and must not contain any NaN values in the range {low, high} inclusive.
   * @param low the lowest index of the lowest value in the search range, inclusive.
   * @param high the highest index of the highest value in the search range, inclusive.
   * @param v the value to search for. It must not be NaN.
   * @param crit one of LT, LE, EQ, GT, GE
   * @return the index of the value in the given search range that satisfies the criterion
   */
  public static int find(final float[] arr, final int low, final int high,
      final float v, final InequalitySearch crit) {
    Objects.requireNonNull(arr, "Inpurt arr must not be null");
    Objects.requireNonNull(crit, "Input crit must not be null");
    if (Float.isNaN(v)) { throw new SketchesArgumentException("Input v must not be NaN."); }
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
   * the given InequalitySearch criterion.
   * If -1 is returned there are no values in the search range that satisfy the criterion.
   *
   * @param arr the given array that must be sorted.
   * @param low the lowest index of the lowest value in the search range, inclusive.
   * @param high the highest index of the highest value in the search range, inclusive.
   * @param v the value to search for.
   * @param crit one of LT, LE, EQ, GT, GE
   * @return the index of the value in the given search range that satisfies the criterion
   */
  public static int find(final long[] arr, final int low, final int high,
      final long v, final InequalitySearch crit) {
    Objects.requireNonNull(arr, "Inpurt arr must not be null");
    Objects.requireNonNull(crit, "Input crit must not be null");
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
} //End of enum
