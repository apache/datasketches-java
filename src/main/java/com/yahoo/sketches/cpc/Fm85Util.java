/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.invPow2;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class Fm85Util {
  static final int minLgK = 4;
  static final int maxLgK = 26;

  static final long LZ_MASK_56 = (1L << 56) - 1L;
  static final long LZ_MASK_48 = (1L << 48) - 1L;
  static final long LZ_MASK_40 = (1L << 40) - 1L;
  static final long LZ_MASK_32 = (1L << 32) - 1L;
  static final long LZ_MASK_24 = (1L << 24) - 1L;
  static final long LZ_MASK_16 = (1L << 16) - 1L;
  static final long LZ_MASK_08 = (1L <<  8) - 1L;

  static final byte[] byteTrailingZerosTable = new byte[256];
  static final byte[] byteLeadingZerosTable = new byte[256];
  static final double[] kxpByteLookup = new double[256];

  private static void fillByteTrailingZerosTable() {
    byteTrailingZerosTable[0] = 8;
    for (int i = 1; i < 256; i++) {
      byteTrailingZerosTable[i] = (byte) Integer.numberOfTrailingZeros(i);
    }
  }

  private static void fillByteLeadingZerosTable() {
    byteLeadingZerosTable[0] = 8;
    for (int i = 1; i < 256; i++) {
      byteLeadingZerosTable[i] = (byte) Integer.numberOfLeadingZeros(i << 24);
    }
  }

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

  static int countLeadingZeros(final long theInput) {
    return Long.numberOfLeadingZeros(theInput);
  }

  //Evaluate
  static int countLeadingZerosByByte(final long theInput) {
    if (theInput > LZ_MASK_56) { return 0 + byteLeadingZerosTable[(int) ((theInput >>> 56) & 0XFFL)]; }
    if (theInput > LZ_MASK_48) { return 8 + byteLeadingZerosTable[(int) ((theInput >>> 48) & 0XFFL)]; }
    if (theInput > LZ_MASK_40) { return 16 + byteLeadingZerosTable[(int) ((theInput >>> 40) & 0XFFL)]; }
    if (theInput > LZ_MASK_32) { return 24 + byteLeadingZerosTable[(int) ((theInput >>> 32) & 0XFFL)]; }
    if (theInput > LZ_MASK_24) { return 32 + byteLeadingZerosTable[(int) ((theInput >>> 24) & 0XFFL)]; }
    if (theInput > LZ_MASK_16) { return 40 + byteLeadingZerosTable[(int) ((theInput >>> 16) & 0XFFL)]; }
    if (theInput > LZ_MASK_08) { return 48 + byteLeadingZerosTable[(int) ((theInput >>>  8) & 0XFFL)]; }
    return 56 + byteLeadingZerosTable[(int) (theInput & 0XFFL)];
  }

  static int countTrailingZeros(final long theInput) {
    return Long.numberOfTrailingZeros(theInput);
  }

  static int countTrailingZerosByByte(final long theInput) {
    long tmp = theInput;
    for (int j = 0; j < 8; j++) {
      final int aByte = (int) (tmp & 0XFFL);
      if (aByte != 0) { return (j << 3) + byteTrailingZerosTable[aByte]; }
      tmp >>>= 8;
    }
    return 64;
  }

  //Place holder, to be eliminated
  //  static WritableMemory shallowCopy(final WritableMemory oldObject, final int numBytes) {
  //    if ((oldObject == null) || (numBytes == 0)) {
  //      throw new SketchesArgumentException("shallowCopyObject: bad arguments");
  //    }
  //    final WritableMemory newObject = WritableMemory.allocate(numBytes);
  //    oldObject.copyTo(0, newObject, 0, numBytes);
  //    return newObject;
  //  }

  static long divideLongsRoundingUp(final long x, final long y) {
    assert (x >= 0) && (y > 0);
    final long quotient = x / y;
    return ((quotient * y) == x) ?  quotient : quotient + 1;
  }

  /**
   * Returns the floor of Log2(x)
   * @param x the given x
   * @return  the floor of Log2(x)
   */
  static long floorLog2ofX(final long x) {
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


  static int golombChooseNumberOfBaseBits(final int k, final long count) {
    assert k >= 1L;
    assert count >= 1L;
    final long quotient = (k - count) / count; // integer division
    return (quotient == 0) ? 0 : (int) floorLog2ofX(quotient);
  }

  static long countBitsSetInMatrix(final long[] array) {
    long count = 0;
    final int len = array.length;
    for (int i = 0; i < len; i++) { count += Long.bitCount(array[i]); }
    return count;
  }

  static void checkLgK(final int lgK) {
    if ((lgK < minLgK) || (lgK > maxLgK)) {
      throw new SketchesArgumentException("LgK must be >= 4 and <= 26: " + lgK);
    }
  }

  static {
    fillKxpByteLookup();
    fillByteTrailingZerosTable();
    fillByteLeadingZerosTable();
  }

}
