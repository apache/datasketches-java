/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

/**
 * QuickSelect algorithm improved from Sedgewick. Gets the kth order value
 * (1-based or 0-based) from the array.
 * Warning! This changes the ordering of elements in the given array!<br>
 * Also see:<br>
 * blog.teamleadnet.com/2012/07/quick-select-algorithm-find-kth-element.html<br>
 * See QuickSelectTest for examples and testNG tests.
 *
 * @author Lee Rhodes
 */
public final class QuickSelect {

  private QuickSelect() {}

  /**
   * Gets the 0-based kth order statistic from the array. Warning! This changes the ordering
   * of elements in the given array!
   *
   * @param arr The array to be re-arranged.
   * @param lo The lowest 0-based index to be considered.
   * @param hi The highest 0-based index to be considered.
   * @param pivot The 0-based index of the value to pivot on.
   * @return The value of the smallest (n)th element where n is 0-based.
   */
  public static long select(final long[] arr, int lo, int hi, final int pivot) {
    while (hi > lo) {
      final int j = partition(arr, lo, hi);
      if (j == pivot) {
        return arr[pivot];
      }
      if (j > pivot) {
        hi = j - 1;
      }
      else {
        lo = j + 1;
      }
    }
    return arr[pivot];
  }

  /**
   * Gets the 1-based kth order statistic from the array including any zero values in the
   * array. Warning! This changes the ordering of elements in the given array!
   *
   * @param arr The hash array.
   * @param pivot The 1-based index of the value that is chosen as the pivot for the array.
   * After the operation all values below this 1-based index will be less than this value
   * and all values above this index will be greater. The 0-based index of the pivot will be
   * pivot-1.
   * @return The value of the smallest (N)th element including zeros, where N is 1-based.
   */
  public static long selectIncludingZeros(final long[] arr, final int pivot) {
    final int arrSize = arr.length;
    final int adj = pivot - 1;
    return select(arr, 0, arrSize - 1, adj);
  }

  /**
   * Gets the 1-based kth order statistic from the array excluding any zero values in the
   * array. Warning! This changes the ordering of elements in the given array!
   *
   * @param arr The hash array.
   * @param nonZeros The number of non-zero values in the array.
   * @param pivot The 1-based index of the value that is chosen as the pivot for the array.
   * After the operation all values below this 1-based index will be less than this value
   * and all values above this index will be greater. The 0-based index of the pivot will be
   * pivot+arr.length-nonZeros-1.
   * @return The value of the smallest (N)th element excluding zeros, where N is 1-based.
   */
  public static long selectExcludingZeros(final long[] arr, final int nonZeros, final int pivot) {
    if (pivot > nonZeros) {
      return 0L;
    }
    final int arrSize = arr.length;
    final int zeros = arrSize - nonZeros;
    final int adjK = (pivot + zeros) - 1;
    return select(arr, 0, arrSize - 1, adjK);
  }

  /**
   * Partition arr[] into arr[lo .. i-1], arr[i], arr[i+1,hi]
   *
   * @param arr The given array to partition
   * @param lo  the low index
   * @param hi  the high index
   * @return the next partition value.  Ultimately, the desired pivot.
   */
  private static int partition(final long[] arr, final int lo, final int hi) {
    int i = lo, j = hi + 1; //left and right scan indices
    final long v = arr[lo]; //partitioning item value
    while (true) {
      //Scan right, scan left, check for scan complete, and exchange
      while (arr[ ++i] < v) {
        if (i == hi) {
          break;
        }
      }
      while (v < arr[ --j]) {
        if (j == lo) {
          break;
        }
      }
      if (i >= j) {
        break;
      }
      final long x = arr[i];
      arr[i] = arr[j];
      arr[j] = x;
    }
    //put v=arr[j] into position with a[lo .. j-1] <= a[j] <= a[j+1 .. hi]
    final long x = arr[lo];
    arr[lo] = arr[j];
    arr[j] = x;
    return j;
  }

  //For double arrays

  /**
   * Gets the 0-based kth order statistic from the array. Warning! This changes the ordering
   * of elements in the given array!
   *
   * @param arr The array to be re-arranged.
   * @param lo The lowest 0-based index to be considered.
   * @param hi The highest 0-based index to be considered.
   * @param pivot The 0-based smallest value to pivot on.
   * @return The value of the smallest (n)th element where n is 0-based.
   */
  public static double select(final double[] arr, int lo, int hi, final int pivot) {
    while (hi > lo) {
      final int j = partition(arr, lo, hi);
      if (j == pivot) {
        return arr[pivot];
      }
      if (j > pivot) {
        hi = j - 1;
      }
      else {
        lo = j + 1;
      }
    }
    return arr[pivot];
  }

  /**
   * Gets the 1-based kth order statistic from the array including any zero values in the
   * array. Warning! This changes the ordering of elements in the given array!
   *
   * @param arr The hash array.
   * @param pivot The 1-based index of the value that is chosen as the pivot for the array.
   * After the operation all values below this 1-based index will be less than this value
   * and all values above this index will be greater. The 0-based index of the pivot will be
   * pivot-1.
   * @return The value of the smallest (N)th element including zeros, where N is 1-based.
   */
  public static double selectIncludingZeros(final double[] arr, final int pivot) {
    final int arrSize = arr.length;
    final int adj = pivot - 1;
    return select(arr, 0, arrSize - 1, adj);
  }

  /**
   * Gets the 1-based kth order statistic from the array excluding any zero values in the
   * array. Warning! This changes the ordering of elements in the given array!
   *
   * @param arr The hash array.
   * @param nonZeros The number of non-zero values in the array.
   * @param pivot The 1-based index of the value that is chosen as the pivot for the array.
   * After the operation all values below this 1-based index will be less than this value
   * and all values above this index will be greater. The 0-based index of the pivot will be
   * pivot+arr.length-nonZeros-1.
   * @return The value of the smallest (N)th element excluding zeros, where N is 1-based.
   */
  public static double selectExcludingZeros(final double[] arr, final int nonZeros, final int pivot) {
    if (pivot > nonZeros) {
      return 0L;
    }
    final int arrSize = arr.length;
    final int zeros = arrSize - nonZeros;
    final int adjK = (pivot + zeros) - 1;
    return select(arr, 0, arrSize - 1, adjK);
  }

  /**
   * Partition arr[] into arr[lo .. i-1], arr[i], arr[i+1,hi]
   *
   * @param arr The given array to partition
   * @param lo  the low index
   * @param hi  the high index
   * @return the next partition value.  Ultimately, the desired pivot.
   */
  private static int partition(final double[] arr, final int lo, final int hi) {
    int i = lo, j = hi + 1; //left and right scan indices
    final double v = arr[lo]; //partitioning item value
    while (true) {
      //Scan right, scan left, check for scan complete, and exchange
      while (arr[ ++i] < v) {
        if (i == hi) {
          break;
        }
      }
      while (v < arr[ --j]) {
        if (j == lo) {
          break;
        }
      }
      if (i >= j) {
        break;
      }
      final double x = arr[i];
      arr[i] = arr[j];
      arr[j] = x;
    }
    //put v=arr[j] into position with a[lo .. j-1] <= a[j] <= a[j+1 .. hi]
    final double x = arr[lo];
    arr[lo] = arr[j];
    arr[j] = x;
    return j;
  }

}
