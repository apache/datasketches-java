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

package org.apache.datasketches.tdigest;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Specialized sorting algorithm that can sort one array and permute another array the same way
 */
public final class Sort {

  /**
   * Stable sort two arrays. The first array is sorted while the second array is permuted the same way.
   * @param keys array to be sorted
   * @param values array to be permuted the same way
   * @param n number of elements to sort from the beginning of the arrays
   */
  public static void stableSort(final double[] keys, final long[] values, final int n) {
    stableLimitedQuickSort(keys, values, 0, n, 64);
    stableLimitedInsertionSort(keys, values, 0, n, 64);
  }

  private static void stableLimitedQuickSort(final double[] keys, final long[] values, int start, int end, final int limit) {
    // the while loop implements tail-recursion to avoid excessive stack calls on nasty cases
    while (end - start > limit) {

      final int pivotIndex = start + ThreadLocalRandom.current().nextInt(end - start);
      double pivotValue = keys[pivotIndex];

      // move pivot to beginning of array
      swap(keys, start, pivotIndex);
      swap(values, start, pivotIndex);

      // use a three way partition because many duplicate values is an important case
      int low = start + 1;   // low points to first value not known to be equal to pivotValue
      int high = end;        // high points to first value > pivotValue
      int i = low;           // i scans the array
      while (i < high) {
        // invariant: values[k] == pivotValue for k in [0..low)
        // invariant: values[k] < pivotValue for k in [low..i)
        // invariant: values[k] > pivotValue for k in [high..end)
        // in-loop:  i < high
        // in-loop:  low < high
        // in-loop:  i >= low
        final double vi = keys[i];
        if (vi == pivotValue && i == pivotIndex) {
          if (low != i) {
            swap(keys, low, i);
            swap(values, low, i);
          } else {
            i++;
          }
          low++;
        } else if (vi > pivotValue || (vi == pivotValue && i > pivotIndex)) {
          high--;
          swap(keys, i, high);
          swap(values, i, high);
        } else {
          i++;
        }
      }
      // assert i == high || low == high therefore, we are done with partition
      // at this point, i==high, from [start,low) are == pivot, [low,high) are < and [high,end) are >
      // we have to move the values equal to the pivot into the middle. To do this, we swap pivot
      // values into the top end of the [low,high) range stopping when we run out of destinations
      // or when we run out of values to copy
      int from = start;
      int to = high - 1;
      for (i = 0; from < low && to >= low; i++) {
        swap(keys, from, to);
        swap(values, from++, to--);
      }
      if (from == low) {
        // ran out of things to copy. This means that the last destination is the boundary
        low = to + 1;
      } else {
        // ran out of places to copy to. This means that there are uncopied pivots and the
        // boundary is at the beginning of those
        low = from;
      }

      // now recurse, but arrange it to handle the longer limit by tail recursion
      // we have to sort the pivot values because they may have different weights
      // we can't do that, however until we know how much weight is in the left and right
      if (low - start < end - high) {
        // left side is smaller
        stableLimitedQuickSort(keys, values, start, low, limit);
        // this is really a way to do
        //    quickSort(keys, values, high, end, limit);
        start = high;
      } else {
        stableLimitedQuickSort(keys, values, high, end, limit);
        // this is really a way to do
        //    quickSort(keys, values, start, low, limit);
        end = low;
      }
    }
  }

  private static void stableLimitedInsertionSort(final double[] keys, final long[] values, int start, int n, final int limit) {
    for (int i = start + 1; i < n; i++) {
      final double k = keys[i];
      final long v = values[i];
      final int m = Math.max(i - limit, start);
      // values in [start, i) are ordered
      // scan backwards to find where to stick the current key
      for (int j = i; j >= m; j--) {
        if (j == 0 || keys[j - 1] <= k) {
          if (j < i) {
            System.arraycopy(keys, j, keys, j + 1, i - j);
            System.arraycopy(values, j, values, j + 1, i - j);
            keys[j] = k;
            values[j] = v;
          }
          break;
        }
      }
    }
  }
  
  private static void swap(final double[] values, final int i, final int j) {
    final double tmpValue = values[i];
    values[i] = values[j];
    values[j] = tmpValue;
  }

  private static void swap(final long[] values, final int i, final int j) {
    final long tmpValue = values[i];
    values[i] = values[j];
    values[j] = tmpValue;
  }

  public static void reverse(final double[] values, final int n) {
    for (int i = 0; i < n / 2; i++) {
      swap(values, i, n - i - 1);
    }
  }

  public static void reverse(final long[] values, final int n) {
    for (int i = 0; i < n / 2; i++) {
      swap(values, i, n - i - 1);
    }
  }
}
