/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

/**
 * Common static methods for quantiles sketches
 */
public class QuantilesHelper {

  /**
   * Convert the weights into totals of the weights preceding each item
   * @param array of weights
   * @return total weight
   */
  public static long convertToPrecedingCummulative(final long[] array) {
    long subtotal = 0;
    for (int i = 0; i < array.length; i++) {
      final long newSubtotal = subtotal + array[i];
      array[i] = subtotal;
      subtotal = newSubtotal;
    }
    return subtotal;
  }

  /**
   * Returns the zero-based index (position) of a value in the hypothetical sorted stream of
   * values of size n.
   * @param phi the fractional position where: 0 &le; &#966; &le; 1.0.
   * @param n the size of the stream
   * @return the index, a value between 0 and n-1.
   */
  public static long posOfPhi(final double phi, final long n) {
    final long pos = (long) Math.floor(phi * n);
    return (pos == n) ? n - 1 : pos;
  }

  /**
   * This is written in terms of a plain array to facilitate testing.
   * @param arr the chunk containing the position
   * @param pos the position
   * @return the index of the chunk containing the position
   */
  public static int chunkContainingPos(final long[] arr, final long pos) {
    final int nominalLength = arr.length - 1; /* remember, arr contains an "extra" position */
    assert nominalLength > 0;
    final long n = arr[nominalLength];
    assert 0 <= pos;
    assert pos < n;
    final int l = 0;
    final int r = nominalLength;
    // the following three asserts should probably be retained since they ensure
    // that the necessary invariants hold at the beginning of the search
    assert l < r;
    assert arr[l] <= pos;
    assert pos < arr[r];
    return searchForChunkContainingPos(arr, pos, l, r);
  }

  // Let m_i denote the minimum position of the length=n "full" sorted sequence
  //   that is represented in slot i of the length = n "chunked" sorted sequence.
  //
  // Note that m_i is the same thing as auxCumWtsArr_[i]
  //
  // Then the answer to a positional query 0 <= q < n is l, where 0 <= l < len,
  // A)  m_l <= q
  // B)   q  < m_r
  // C)   l+1 = r
  //
  // A) and B) provide the invariants for our binary search.
  // Observe that they are satisfied by the initial conditions:  l = 0 and r = len.
  private static int searchForChunkContainingPos(final long[] arr, final long pos, final int l, final int r) {
    // the following three asserts can probably go away eventually, since it is fairly clear
    // that if these invariants hold at the beginning of the search, they will be maintained
    assert l < r;
    assert arr[l] <= pos;
    assert pos < arr[r];
    if (l + 1 == r) {
      return l;
    }
    final int m = l + (r - l) / 2;
    if (arr[m] <= pos) {
      return searchForChunkContainingPos(arr, pos, m, r);
    }
    return searchForChunkContainingPos(arr, pos, l, m);
  }

  /**
   * Compute an array of evenly spaced normalized ranks from 0 to 1 inclusive.
   * A value of 1 will result in [0], 2 will result in [0, 1],
   * 3 will result in [0, .5, 1] and so on.
   * @param n number of ranks needed (must be greater than 0)
   * @return array of ranks
   */
  public static double[] getEvenlySpacedRanks(final int n) {
    if (n <= 0) {
      throw new SketchesArgumentException("n must be > 0");
    }
    final double[] fractions = new double[n];
    fractions[0] = 0.0;
    for (int i = 1; i < n; i++) {
      fractions[i] = (double) i / (n - 1);
    }
    if (n > 1) {
      fractions[n - 1] = 1.0;
    }
    return fractions;
  }

}
