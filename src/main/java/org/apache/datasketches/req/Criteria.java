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
 * <b>NOTE:</b> This is an internal class and is public to allow characterization testing from
 * another package. It is not intended to be used by normal users of the ReqSketch.
 *
 * @author Lee Rhodes
 */
public enum Criteria {
  /**
   * Given an increasing sorted array of values and a value <i>V</i>, this criterion instucts the
   * binary search algorithm to find the highest adjacent pair of values <i>{A,B}</i> such that
   * <i>A &lt; V &le; B</i>,
   * The returned value from the binary search algorithm will be the index of <i>A</i>
   * or -1, if the value <i>V</i> &le; the the lowest value in the selected range of the array.
   */
  LT { //A < V <= B, return A
    @Override
    int compare(final double[] arr, final int a, final int b, final double v) {
      return  v <= arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int compare(final float[] arr, final int a, final int b, final float v) {
      return  v <= arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override //only if compare == 0
    int getIndex(final int a, final int b) {
      return a;
    }

    @Override
    int resolve(final int loA, final int hiA, final int low, final int high) {
      if (loA == high) { return high; }
      return -1;
    }

    @Override
    String desc(final double[] arr, final int low, final int high, final int idx, final double v) {
      if (idx >= high) {
        return "arr[" + idx + "]=" + arr[idx] + " < " + v
            + "; return arr[" + idx + "]=" + arr[idx];
      }
      return "arr[" + idx + "]=" + arr[idx] + " < " + v
          + "  <= arr[" + (idx + 1) + "]=" + arr[idx + 1]
          + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final float[] arr, final int low, final int high, final int idx, final float v) {
      if (idx >= high) {
        return "arr[" + idx + "]=" + arr[idx] + " < " + v
            + "; return arr[" + idx + "]=" + arr[idx];
      }
      return "arr[" + idx + "]=" + arr[idx] + " < " + v
          + "  <= arr[" + (idx + 1) + "]=" + arr[idx + 1]
          + "; return arr[" + idx + "]=" + arr[idx];
    }
  },
  /**
   * Given an increasing sorted array of values and a value <i>V</i>, this criterion instucts the
   * binary search algorithm to find the highest adjacent pair of values <i>{A,B}</i> such that
   * <i>A &le; V &lt; B</i>,
   * The returned value from the binary search algorithm will be the index of <i>A</i>
   * or -1, if the value <i>V</i> &lt; the the lowest value in the selected range of the array.
   */
  LE { //A <= V < B, return A
    @Override
    int compare(final double[] arr, final int a, final int b, final double v) {
      final int ret = v < arr[a] ? -1 : arr[b] <= v ? 1 : 0;

      return ret;
    }

    @Override
    int compare(final float[] arr, final int a, final int b, final float v) {
      final int ret = v < arr[a] ? -1 : arr[b] <= v ? 1 : 0;

      return ret;
    }

    @Override
    int getIndex(final int a, final int b) {
      return a;
    }

    @Override //only if compare == -1
    int resolve(final int loA, final int hiA, final int low, final int high) {
      if (loA >= high) { return high; }
      return -1;
    }

    @Override
    String desc(final double[] arr, final int low, final int high, final int idx, final double v) {
      if (idx >= high) {
        return "arr[" + idx + "]=" + arr[idx] + " <= " + v
            + "; return arr[" + idx + "]=" + arr[idx];
      }
      return "arr[" + idx + "]=" + arr[idx] + " <= " + v
          + "  < arr[" + (idx + 1) + "]=" + arr[idx + 1]
          + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final float[] arr, final int low, final int high, final int idx, final float v) {
      if (idx >= high) {
        return "arr[" + idx + "]=" + arr[idx] + " <= " + v
            + "; return arr[" + idx + "]=" + arr[idx];
      }
      return "arr[" + idx + "]=" + arr[idx] + " <= " + v
          + "  < arr[" + (idx + 1) + "]=" + arr[idx + 1]
          + "; return arr[" + idx + "]=" + arr[idx];
    }
  },
  /**
   * Given an increasing sorted array of values and a value <i>V</i>, this criterion instucts the
   * binary search algorithm to find the lowest adjacent pair of values <i>{A,B}</i> such that
   * <i>A &le; V &lt; B</i>,
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
    int getIndex(final int a, final int b) {
      return b;
    }

    @Override
    int resolve(final int loA, final int hiA, final int low, final int high) {
      if (hiA <= low) { return low; }
      return -1;
    }

    @Override
    String desc(final double[] arr, final int low, final int high, final int idx, final double v) {
      if (idx <= low) {
        return v + " < arr[" + idx + "]=" + arr[idx]
                 + "; return arr[" + idx + "]=" + arr[idx];
      }
      return "arr[" + (idx - 1) + "]=" + arr[idx - 1] + " <= " + v
          + "  < arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final float[] arr, final int low, final int high, final int idx, final float v) {
      if (idx <= low) {
        return v + " < arr[" + idx + "]=" + arr[idx]
                 + "; return arr[" + idx + "]=" + arr[idx];
      }
      return "arr[" + (idx - 1) + "]=" + arr[idx - 1] + " <= " + v
          + "  < arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
    }
  },
  /**
   * Given an increasing sorted array of values and a value <i>V</i>, this criterion instucts the
   * binary search algorithm to find the lowest adjacent pair of values <i>{A,B}</i> such that
   * <i>A &lt; V &le; B</i>,
   * The returned value from the binary search algorithm will be the index of <i>B</i>
   * or -1, if the value <i>V</i> &gt; the the highest value in the selected range of the array.
   */
  GE { //A < B <= B, return B
    @Override
    int compare(final double[] arr, final int a, final int b, final double v) {
      return v <= arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int compare(final float[] arr, final int a, final int b, final float v) {
      return v <= arr[a] ? -1 : arr[b] < v ? 1 : 0;
    }

    @Override
    int getIndex(final int a, final int b) {
      return b;
    }

    @Override
    int resolve(final int loA, final int hiA, final int low, final int high) {
      if (hiA <= low) { return low; }
      return -1;
    }

    @Override
    String desc(final double[] arr, final int low, final int high, final int idx, final double v) {
      if (idx <= low) {
        return v + " < arr[" + idx + "]=" + arr[idx]
                 + "; return arr[" + idx + "]=" + arr[idx];
      }
      return "arr[" + (idx - 1) + "]=" + arr[idx - 1] + " < " + v
          + "  <= arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
    }

    @Override
    String desc(final float[] arr, final int low, final int high, final int idx, final float v) {
      if (idx <= low) {
        return v + " < arr[" + idx + "]=" + arr[idx]
                 + "; return arr[" + idx + "]=" + arr[idx];
      }
      return "arr[" + (idx - 1) + "]=" + arr[idx - 1] + " < " + v
          + "  <= arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
    }
  };

  /**
   * The call to compare index a and index b with the value v.
   * @param arr The underlying sorted array of double values
   * @param a the lower index of the current pair
   * @param b the higer index of the current pair
   * @param v the double value to search for
   * @return +1, which means we must search higher in the aray, or -1, whicn means we must
   * search lower in the array, or 0, which means we have found the correct bounding pair.
   */
  abstract int compare(double[] arr, int a, int b, double v);

  /**
   * The call to compare index a and index b with the value v.
   * @param arr The underlying sorted array of float values
   * @param a the lower index of the current pair
   * @param b the higer index of the current pair
   * @param v the float value to search for
   * @return +1, which means we must search higher in the aray, or -1, whicn means we must
   * search lower in the array, or 0, which means we have found the correct bounding pair.
   */
  abstract int compare(float[] arr, int a, int b, float v);

  /**
   * If the compare operation returns 0, which means "found", this returns the index of the
   * found value that satisfies the selected criteria.
   * @param a the lower index of the current pair
   * @param b the higer index of the current pair
   * @return the index of the found value that satisfies the selected criteria.
   */
  abstract int getIndex(int a, int b);

  /**
   * Called to resolve what to do at the ends of the array
   * @param loA the current loA value
   * @param hiA the current hiA value
   * @param low the low index of the full range
   * @param high the high index of the full range
   * @return the index of the resolution or -1, if it cannot be resolved.
   */
  abstract int resolve(int loA, int hiA, int low, int high);

  /**
   * Optional call that describes the details of the results of the search.
   * Used primarily for debugging.
   * @param arr The underlying sorted array of double values
   * @param low the low index of the full range
   * @param high the high index of the full range
   * @param idx the resolved index
   * @param v the double value to search for
   * @return the descriptive string.
   */
  abstract String desc(double[] arr, int low, int high, int idx, double v);

  /**
   * Optional call that describes the details of the results of the search.
   * Used primarily for debugging.
   * @param arr The underlying sorted array of double values
   * @param low the low index of the full range
   * @param high the high index of the full range
   * @param idx the resolved index
   * @param v the double value to search for
   * @return the descriptive string.
   */
  abstract String desc(float[] arr, int low, int high, int idx, float v);
}
