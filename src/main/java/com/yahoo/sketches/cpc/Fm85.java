/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.Fm85Util.checkLgK;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class Fm85 {
  final byte lgK;
  boolean isCompressed;
  boolean mergeFlag;    // Is the sketch the result of merging?
  long numCoupons;      // The number of coupons collected so far.

  //The following variables occur in the updateable semi-compressed type.
  byte[] slidingWindow;
  int windowOffset;
  int[] surprisingValueTable;

  // The following variables occur in the non-updateable fully-compressed type.
  int[] compressedWindow;            //cwStream
  int cwLength; // The number of 32-bit words in this bitstream.
  int numCompressedSurprisingValues; //numSV
  int[] compressedSurprisingValues;  // csvStream
  int csvLength; // The number of 32-bit words in this bitstream.

  // Note that (as an optimization) the two bitstreams could be concatenated.

  byte firstInterestingColumn; // fiCol. This is part of a speed optimization.

  double kxp;                  //used with HIP
  double hipEstAccum;          //used with HIP
  double hipErrAccum;          //not currently used

  Fm85(final int lgK) {
    checkLgK(lgK);
    this.lgK = (byte) lgK;
    kxp = 1 << lgK;
    reset();
  }

  final void reset() {
    isCompressed = false;
    mergeFlag = false;
    numCoupons = 0;
    slidingWindow = null;
    windowOffset = 0;
    surprisingValueTable = null;
    compressedWindow = null;
    cwLength = 0;
    numCompressedSurprisingValues = 0;
    compressedSurprisingValues = null;
    csvLength = 0;
    firstInterestingColumn = (byte) 0;
    kxp = 1 << lgK;
    hipEstAccum = 0;
    hipErrAccum = 0;
  }

  static Flavor determineFlavor(final int lgK, final long c) {
    final long k = 1L << lgK;
    final long c2 = c << 1;
    final long c8 = c << 3;
    final long c32 = c << 5;
    if (c == 0)
     {
      return Flavor.EMPTY;    //    0  == C <    1
    }
    if (c32 < (3 * k))
     {
      return Flavor.SPARSE;   //    1  <= C <   3K/32
    }
    if (c2 < k)
     {
      return Flavor.HYBRID;   // 3K/32 <= C <   K/2
    }
    if (c8 < (27 * k)) {
      return Flavor.PINNED;   //   K/2 <= C < 27K/8
    }
    else {
      return Flavor.SLIDING;  // 27K/8 <= C
    }
  }

  static Flavor determineSketchFlavor(final Fm85 sketch) {
    return determineFlavor(sketch.lgK, sketch.numCoupons);
  }

  static long determineCorrectOffset(final long lgK, final long c) {
    final long k = (1L << lgK);
    final long tmp = (c << 3) - (19 * k);        // 8C - 19K
    if (tmp < 0) { return 0; }
    return tmp >> (lgK + 3L); // tmp / 8K
  }

  //Warning: this is called in several places, including during the
  //transitional moments during which sketch invariants involving
  //flavor and offset are out of whack and in fact we are re-imposing
  //them. Therefore it cannot rely on determineFlavor() or
  //determineCorrectOffset(). Instead it interprets the low level data
  //structures "as is".

  //This produces a full-size k-by-64 bit matrix from any Live sketch.

//  long[] bitMatrixOfSketch(final Fm85 sketch) {
//    assert (sketch.isCompressed == false);
//    final int k = (1 << sketch.lgK);
//    final int offset = sketch.windowOffset;
//    assert (offset >= 0) && (offset <= 56);
//
//    final long[] matrix = new long[k];
//
//    //Fill the matrix with default rows in which the "early zone" is filled with ones.
//    //This is essential for the routine's O(k) time cost (as opposed to O(C)).
//    final long defaultRow = (1L << offset) - 1L;
//    for (int i = 0; i < k; i++) { matrix[i] = defaultRow; }
//
//    if (sketch.numCoupons == 0) {
//      return (matrix); // Returning a matrix of zeros rather than NULL.
//    }
//
//    final byte[] window = sketch.slidingWindow;
//    if (window != null) { // In other words, we are in window mode, not sparse mode.
//      for (int i = 0; i < k; i++) { // set the window bits, trusting the sketch's current offset.
//        matrix[i] |= (window[i] << offset);
//      }
//    }
//
//    final U32Table table = sketch.surprisingValueTable;
//    assert (table != null);
//    final int[] slots = table.slots;
//    final long numSlots = (1L << table.lgSize);
//    for (int i = 0; i < numSlots; i++) {
//      final int rowCol = slots[i];
//      if (rowCol != ALL32BITS) {
//        final short col = (short) (rowCol & 63);
//        final int row = rowCol >> 6;
//        // Flip the specified matrix bit from its default value.
//        // In the "early" zone the bit changes from 1 to 0.
//        // In the "late" zone the bit changes from 0 to 1.
//        matrix[row] ^= (1L << col);
//      }
//    }
//
//    return (matrix);
//  }







}
