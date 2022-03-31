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
 * Common static methods for classic quantiles and KLL sketches
 */
public class QuantilesHelper {

  /**
   * Convert the weights into totals of the weights preceding each item.
   * An array of {1,1,1,0} becomes {0,1,2,3}
   * @param array of weights where first element is zero
   * @return total weight
   */ //used by classic Quantiles and KLL
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
   * Returns the linear zero-based index (position) of a value in the hypothetical sorted stream of
   * values of size n.
   * @param rank the fractional position where: 0 &le; &#966; &le; 1.0.
   * @param n the size of the stream
   * @return the index, a value between 0 and n-1.
   */ //used by classic Quantiles and KLL
  public static long posOfRank(final double rank, final long n) {
    final long pos = (long) Math.floor(rank * n);
    return pos == n ? n - 1 : pos; //avoids ArrayIndexOutOfBoundException
  }

  /**
   * Returns the linear zero-based index (position) of a value in the hypothetical sorted stream of
   * values of size n.
   * @param rank the fractional position where: 0 &le; &#966; &le; 1.0.
   * @param n the size of the stream
   * @return the index, a value between 0 and n-1.
   * @deprecated use {@link #posOfRank(double, long)} instead. Version 3.2.0.
   */ //used by classic Quantiles and KLL
  @Deprecated
  public static long posOfPhi(final double rank, final long n) {
    return posOfRank(rank, n);
  }

  /**
   * This is written in terms of a plain array to facilitate testing.
   * @param wtArr the cumulative weights array consisting of chunks
   * @param pos the position
   * @return the index of the chunk containing the position
   */ //also used by KLL
  public static int chunkContainingPos(final long[] wtArr, final long pos) {
    final int nominalLength = wtArr.length - 1; /* remember, wtArr contains an "extra" position */
    assert nominalLength > 0;
    final long n = wtArr[nominalLength];
    assert 0 <= pos;
    assert pos < n;
    final int l = 0; //left
    final int r = nominalLength; //right
    // the following three asserts should probably be retained since they ensure
    // that the necessary invariants hold at the beginning of the search
    assert l < r;
    assert wtArr[l] <= pos;
    assert pos < wtArr[r];
    return searchForChunkContainingPos(wtArr, pos, l, r);
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

}
