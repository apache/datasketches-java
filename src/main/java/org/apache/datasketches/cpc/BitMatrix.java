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

package org.apache.datasketches.cpc;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.hash.MurmurHash3.hash;

import java.util.Arrays;

/**
 * Used only in test.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class BitMatrix {
  private final int lgK;
  private final long seed;
  private long numCoupons;
  private long[] bitMatrix;
  private boolean numCouponsInvalid; //only used if we allowed merges

  BitMatrix(final int lgK) {
    this(lgK, DEFAULT_UPDATE_SEED);
  }

  BitMatrix(final int lgK, final long seed) {
    this.lgK = lgK;
    this.seed = seed;
    bitMatrix = new long[1 << lgK];
    numCoupons = 0;
    numCouponsInvalid = false;
  }

  //leaves lgK and seed untouched
  void reset() {
    Arrays.fill(bitMatrix, 0);
    numCoupons = 0;
    numCouponsInvalid = false;
  }

  long getNumCoupons() {
    if (numCouponsInvalid) {
      numCoupons = countCoupons(bitMatrix);
      numCouponsInvalid = false;
    }
    return numCoupons;
  }

  long[] getMatrix() {
    return bitMatrix;
  }

  /**
   * Present the given long as a potential unique item.
   *
   * @param datum The given long datum.
   */
  public void update(final long datum) {
    final long[] data = { datum };
    final long[] harr = hash(data, seed);
    hashUpdate(harr[0], harr[1]);
  }

  private void hashUpdate(final long hash0, final long hash1) {
    int col = Long.numberOfLeadingZeros(hash1);
    if (col > 63) { col = 63; } // clip so that 0 <= col <= 63
    final long kMask = (1L << lgK) - 1L;
    int row = (int) (hash0 & kMask);
    final int rowCol = (row << 6) | col;

    // Avoid the hash table's "empty" value which is (2^26 -1, 63) (all ones) by changing it
    // to the pair (2^26 - 2, 63), which effectively merges the two cells.
    // This case is *extremely* unlikely, but we might as well handle it.
    // It can't happen at all if lgK (or maxLgK) < 26.
    if (rowCol == -1) { row ^= 1; } //set the LSB of row to 0

    final long oldPattern = bitMatrix[row];
    final long newPattern = oldPattern | (1L << col);
    if (newPattern != oldPattern) {
      numCoupons++;
      bitMatrix[row] = newPattern;
    }
  }

  static long countCoupons(final long[] bitMatrix) {
    long count = 0;
    final int len = bitMatrix.length;
    for (int i = 0; i < len; i++) { count += Long.bitCount(bitMatrix[i]); }
    return count;
  }

}
