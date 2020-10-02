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
 * This supports the BinarySearch class.
 *
 * @author Lee Rhodes
 */
public enum Criteria {

  /**
   * Given an sorted array of increasing values and a value <i>V</i>, this criterion instructs the
   * binary search algorithm to find the highest adjacent pair of values <i>{A,B}</i> such that
   * <i>A &lt; V &le; B</i>.
   * The returned value from the binary search algorithm will be the index of <i>A</i>
   * or -1, if the value <i>V</i> &le; the the lowest value in the selected range of the array.
   */
  LT { //A < V <= B, return A
    @Override
    int compare(final double[] arr, final int a, final int b, final double v) {
      return v <= arr[a] ? -1 : arr[b] < v ? 1 : 0; //-1,+1, 0
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
    int resolve(final int loA, final int hiA, final int low, final int high) {
      if (loA == high) { return high; }
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
   * Given an sorted array of increasing values and a value <i>V</i>, this criterion instructs the
   * binary search algorithm to find the highest adjacent pair of values <i>{A,B}</i> such that
   * <i>A &le; V &lt; B</i>.
   * The returned value from the binary search algorithm will be the index of <i>A</i>
   * or -1, if the value <i>V</i> &lt; the the lowest value in the selected range of the array.
   */
  LE { //A <= V < B, return A
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
    int resolve(final int loA, final int hiA, final int low, final int high) {
      if (loA >= high) { return high; }
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
   * Given an sorted array of increasing values and a value <i>V</i>, this criterion instructs the
   * binary search algorithm to find the adjacent pair of values <i>{A,B}</i> such that
   * <i>A &le; V &le; B</i>.
   * The returned value from the binary search algorithm will be the index of <i>A</i> or <i>B</i>,
   * if one of them is equal to <i>V</i>, or -1 if V is not equal to either one.
   */
  EQ { //A <= V <= B, return A or B
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
    int resolve(final int loA, final int hiA, final int low, final int high) {
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
   * Given an sorted array of increasing values and a value <i>V</i>, this criterion instructs the
   * binary search algorithm to find the lowest adjacent pair of values <i>{A,B}</i> such that
   * <i>A &lt; V &le; B</i>.
   * The returned value from the binary search algorithm will be the index of <i>B</i>
   * or -1, if the value <i>V</i> &gt; the the highest value in the selected range of the array.
   */
  GE { //A < V <= B, return B
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
    int resolve(final int loA, final int hiA, final int low, final int high) {
      if (hiA <= low) { return low; }
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
   * Given an sorted array of increasing values and a value <i>V</i>, this criterion instructs the
   * binary search algorithm to find the lowest adjacent pair of values <i>{A,B}</i> such that
   * <i>A &le; V &lt; B</i>.
   * The returned value from the binary search algorithm will be the index of <i>B</i>
   * or -1, if the value <i>V</i> &ge; the the highest value in the selected range of the array.
   */
  GT { //A <= V < B, return B
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
    int resolve(final int loA, final int hiA, final int low, final int high) {
      if (hiA <= low) { return low; }
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
   * Called to resolve what to do if not found.
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
