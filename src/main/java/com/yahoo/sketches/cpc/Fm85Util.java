/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class Fm85Util {
  static final long ALL64BITS = -1L;
  static final long ALL32BITS = (1L << 32) - 1L;

  //Place holder, to be eliminated
  static WritableMemory shallowCopy(final WritableMemory oldObject, final int numBytes) {
    if ((oldObject == null) || (numBytes == 0)) {
      throw new SketchesArgumentException("shallowCopyObject: bad arguments");
    }
    final WritableMemory newObject = WritableMemory.allocate(numBytes);
    oldObject.copyTo(0, newObject, 0, numBytes);
    return newObject;
  }

  //Place holder, to be eliminated, use Java builtin
  static int countLeadingZerosInUnsignedLong(final long theInput) {
    return Long.numberOfLeadingZeros(theInput);
  }

  static final byte[] byteTrailingZerosTable = new byte[256];

  private static void fillByteTrailingZerosTable() {
    byteTrailingZerosTable[0] = 8;
    for (int i = 1; i < 256; i++) {
      byteTrailingZerosTable[i] = (byte) Long.numberOfTrailingZeros(i);
    }
  }

  //Place holder, to be eliminated, use Java builtin
  static int countTrailingZerosInUnsignedLong(final long theInput) {
    return Long.numberOfTrailingZeros(theInput);
  }

  //Place holder, to be eliminated, use sketches.Util
  static double invPow2(final int e) {
    return com.yahoo.sketches.Util.invPow2(e);
  }

  static final double[] kxpByteLookup = new double[256];

  private static void fillKxpByteLookup() { //called from static initializer
    for (int b = 0; b < 256; b++) {
      double sum = 0;
      for (int col = 0; col < 8; col++) {
        final int bit = (b >>> col) & 1;
        if (bit == 0) { // note the inverted logic
          sum += invPow2(col + 1); //note the "+1"
        }
      }
      kxpByteLookup[b] = sum;
    }
  }

  static long divideLongsRoundingUp(final long x, final long y) {
    assert (x >= 0) && (y > 0);
    final long quotient = x / y;
    return ((quotient * y) == x) ?  quotient : quotient + 1;
  }

  static long longFloorLog2OfLong(final long x) {
    if (x < 1L) {
      throw new SketchesArgumentException("x must be > 0: " + x);
    }
    long p = 0;
    long y = 1;
    while (true) {
      if (y == x) { return (p); }
      if (y  > x) { return (p - 1); }
      p  += 1;
      y <<= 1;
    }
  }

  static long golombChooseNumberOfBaseBits(final long k, final long count) {
    assert k >= 1L;
    assert count >= 1L;
    final long quotient = (k - count) / count; // integer division
    return (quotient == 0) ? 0 : longFloorLog2OfLong(quotient);
  }

  static long countBitsSetInMatrix(final long[] array) {
    long count = 0;
    final int len = array.length;
    for (int i = 0; i < len; i++) { count += Long.bitCount(array[i]); }
    return count;
  }

  static void checkLgK(final int lgK) {
    if ((lgK < 4) || (lgK > 26)) {
      throw new SketchesArgumentException("LgK must be >= 4 and <= 26: " + lgK);
    }
  }

  static {
    fillKxpByteLookup();
    fillByteTrailingZerosTable();
  }
}
