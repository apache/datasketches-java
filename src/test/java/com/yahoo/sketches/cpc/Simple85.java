/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.Fm85.rowColFromTwoHashes;

import java.util.Arrays;

/**
 * @author Lee Rhodes
 */
public class Simple85 {
  long[] bitMatrix;
  long numCoupons;
  int lgK;

  Simple85(int lgK) {
    this.lgK = lgK;
    bitMatrix = new long[1 << lgK];
    numCoupons = 0;
  }

  void reset() {
    Arrays.fill(bitMatrix, 0);
    numCoupons = 0;
  }

  static void rowColUpdate(Simple85 sketch, int rowCol) {
    //println("SimpleRowCol=" + rowCol + ", Row=" + (rowCol >>> 6) + ", Col=" + (rowCol & 63));
    int col = rowCol & 63;
    int row = rowCol >>> 6;
    long oldPattern = sketch.bitMatrix[row];
    long newPattern = oldPattern | (1L << col);
    if (newPattern != oldPattern) { sketch.numCoupons++; }
    sketch.bitMatrix[row] = newPattern;
  }

  static void hashUpdate(Simple85 sketch, long hash0, long hash1) {
    int rowCol = rowColFromTwoHashes(hash0, hash1, sketch.lgK);
    rowColUpdate(sketch, rowCol);
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
}
