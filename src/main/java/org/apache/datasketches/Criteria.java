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
 * This supports the BinarySearch class by providing efficient, unique and unambiguous searching for
 * comparison criteria for ordered arrays of values that may include duplicate values. These
 * searching criteria include &lt;, &le;, ==, &ge;, &gt;. We also would like to be able to use the
 * same search algorithm for all the criteria.
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
public enum Criteria {

  /**
   * Given an sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
   * this criterion instructs the binary search algorithm to find the highest adjacent pair of
   * values <i>{A,B}</i> such that <i>A &lt; V &le; B</i>.
   * The returned value from the binary search algorithm will be the index of <i>A</i>
   * or -1, if the value <i>V</i> &le; the the lowest value in the selected range of the array.
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
    int getIndex(final double[] arr, final int a, final int b, final double v) {
      return a;
    }

    @Override
    int getIndex(final float[] arr, final int a, final int b, final float v) {
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
  },

  /**
   * Given an sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
   * this criterion instructs the binary search algorithm to find the highest adjacent pair of
   * values <i>{A,B}</i> such that <i>A &le; V &lt; B</i>.
   * The returned value from the binary search algorithm will be the index of <i>A</i>
   * or -1, if the value <i>V</i> &lt; the the lowest value in the selected range of the array.
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
    int getIndex(final double[] arr, final int a, final int b, final double v) {
      return a;
    }

    @Override
    int getIndex(final float[] arr, final int a, final int b, final float v) {
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
  },

  /**
   * Given an sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
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
    int getIndex(final double[] arr, final int a, final int b, final double v) {
      return v == arr[a] ? a : v == arr[b] ? b : -1;
    }

    @Override
    int getIndex(final float[] arr, final int a, final int b, final float v) {
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
  },

  /**
   * Given an sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
   * this criterion instructs the binary search algorithm to find the lowest adjacent pair of
   * values <i>{A,B}</i> such that <i>A &lt; V &le; B</i>.
   * The returned value from the binary search algorithm will be the index of <i>B</i>
   * or -1, if the value <i>V</i> &gt; the the highest value in the selected range of the array.
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
    int getIndex(final double[] arr, final int a, final int b, final double v) {
      return b;
    }

    @Override
    int getIndex(final float[] arr, final int a, final int b, final float v) {
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
  },

  /**
   * Given an sorted array of increasing values <i>arr[]</i> and a key value <i>V</i>,
   * this criterion instructs the binary search algorithm to find the lowest adjacent pair of
   * values <i>{A,B}</i> such that <i>A &le; V &lt; B</i>.
   * The returned value from the binary search algorithm will be the index of <i>B</i>
   * or -1, if the value <i>V</i> &ge; the the highest value in the selected range of the array.
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
    int getIndex(final double[] arr, final int a, final int b, final double v) {
      return b;
    }

    @Override
    int getIndex(final float[] arr, final int a, final int b, final float v) {
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
}
