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
 * Specialized sorting algorithm that can sort one array and permute another array the same way.
 */
public final class Sort {

  /**
   * No argument constructor.
   */
  public Sort() { }

  /**
   * Sort two arrays so that the result is deterministic.
   * The elements are ordered by key (ascending) and, if the keys are equal, by the paired value
   * (ascending). The first array is sorted while the second array is permuted the same way.
   *
   * <p>Because the pairs (key, value) are totally ordered, the output depends only on the
   * multiset of pairs and not on the initial order of the elements nor on the choice of pivots.
   *
   * @param keys array to be sorted
   * @param values array to be permuted the same way, also used to break ties between equal keys
   * @param n number of elements to sort from the beginning of the arrays
   */
  public static void stableSort(final double[] keys, final long[] values, final int n) {
    stableLimitedQuickSort(keys, values, 0, n, 64);
    stableLimitedInsertionSort(keys, values, 0, n, 64);
  }

  private static void stableLimitedQuickSort(final double[] keys, final long[] values,
      int start, int end, final int limit) {
    // the while loop implements tail-recursion to avoid excessive stack calls on nasty cases
    while (end - start > limit) {

      final int pivotIndex = start + ThreadLocalRandom.current().nextInt(end - start);
      final double pivotKey = keys[pivotIndex];
      final long pivotValue = values[pivotIndex];

      // use a three way partition because many duplicate pairs is an important case
      // invariant: pairs in [start, low) are less than the pivot
      // invariant: pairs in [low, i) are equal to the pivot
      // invariant: pairs in [high, end) are greater than the pivot
      // pairs in [i, high) are not yet classified
      int low = start;
      int high = end;
      int i = start;
      while (i < high) {
        final int c = compare(keys[i], values[i], pivotKey, pivotValue);
        if (c < 0) {
          swap(keys, low, i);
          swap(values, low, i);
          low++;
          i++;
        } else if (c > 0) {
          high--;
          swap(keys, i, high);
          swap(values, i, high);
        } else {
          i++;
        }
      }
      // at this point [low, high) holds pairs equal to the pivot (same key and same value),
      // which are indistinguishable, so they don't need any further sorting.

      // now recurse, but arrange it to handle the longer limit by tail recursion
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

  private static void stableLimitedInsertionSort(final double[] keys, final long[] values,
      final int start, final int n, final int limit) {
    for (int i = start + 1; i < n; i++) {
      final double k = keys[i];
      final long v = values[i];
      final int m = Math.max(i - limit, start);
      // values in [start, i) are ordered
      // scan backwards to find where to stick the current key
      for (int j = i; j >= m; j--) {
        if (j == 0 || compare(keys[j - 1], values[j - 1], k, v) <= 0) {
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

  /**
   * Compares two (key, value) pairs: first by key, then, if the keys are equal, by value.
   */
  private static int compare(final double key1, final long value1,
      final double key2, final long value2) {
    if (key1 < key2) { return -1; }
    if (key1 > key2) { return 1; }
    return Long.compare(value1, value2);
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

  /**
   * Reverse the first n elements of the given array.
   *
   * @param values array to be reversed
   * @param n number of elements to reverse
   */
  public static void reverse(final double[] values, final int n) {
    for (int i = 0; i < n / 2; i++) {
      swap(values, i, n - i - 1);
    }
  }

  /**
   * Reverse the first n elements of the given array.
   *
   * @param values array to be reversed
   * @param n number of elements to reverse
   */
  public static void reverse(final long[] values, final int n) {
    for (int i = 0; i < n / 2; i++) {
      swap(values, i, n - i - 1);
    }
  }
}
