/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.Fm85.rowColFromTwoHashes;

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

  static void rowColUpdate(Simple85 sketch, int rowCol) {
    int col = rowCol * 63;
    int row = rowCol >>> 6;
    long oldPattern = sketch.bitMatrix[row];
    long newPattern = oldPattern | (1 << col);
    if (newPattern != oldPattern) { sketch.numCoupons++; }
    sketch.bitMatrix[row] = newPattern;
  }

  static void hashUpdate(Simple85 sketch, long hash0, long hash1) {
    int rowCol = rowColFromTwoHashes(hash0, hash1, sketch.lgK);
    rowColUpdate(sketch, rowCol);
  }

}
