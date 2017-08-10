/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * This class provides a compact representation of reservoir size by encoding it into a
 * fixed-point 16-bit value.
 * <p>The value itself is a fractional power of 2, with 5 bits of exponent and 11 bits of
 * mantissa. The exponent allows a choice of anywhere from 0-30, and there are 2048 possible
 * reservoir size values within each octave. Because reservoir size must be an integer, this
 * means that some sizes below 2048 may have multiple valid encodings.</p>
 *
 * <p>Reservoir sizes can be specified exactly to 4096, and may be off by up to 0.03% thereafter.
 * The value returned is always at least as large as the requested size, but may be larger.
 * </p>
 *
 * <p>NOTE: Numerical instability may cause an off-by-one error on reservoir size, causing a
 * slight increase in storage over the optimal value.</p>
 *
 * @author Jon Malkin
 */
final class ReservoirSize {
  /**
   * Number of bins per power of two.
   */
  static final int BINS_PER_OCTAVE = 2048;

  /**
   * Precomputed inverse values for efficiency
   */
  private static final double INV_BINS_PER_OCTAVE = 1.0 / BINS_PER_OCTAVE;
  private static final double INV_LN_2 = 1.0 / Math.log(2.0);

  /**
   * Values for encoding/decoding
   */
  private static final int EXPONENT_MASK = 0x1F;
  private static final int EXPONENT_SHIFT = 11;
  private static final int INDEX_MASK = 0x07FF;
  private static final int OUTPUT_MASK = 0xFFFF;
  private static final int MAX_ABS_VALUE = 2146959360;
  private static final int MAX_ENC_VALUE = 0xF7FF; // p=30, i=2047

  private ReservoirSize() {}

  /**
   * Given target reservoir size k, computes the smallest representable reservoir size that can
   * hold k entries and returns it in a 16-bit fixed-point format as a <tt>short</tt>.
   *
   * @param k target reservoir size
   * @return reservoir size as 16-bit encoded value
   */
  public static short computeSize(final int k) {
    if ((k < 1) || (k > MAX_ABS_VALUE)) {
      throw new SketchesArgumentException("Can only encode strictly positive sketch sizes "
              + "less than " + MAX_ABS_VALUE + ", found: " + k);
    }

    final int p = Util.toLog2(Util.floorPowerOf2(k), "computeSize: p");

    // because of floor() + 1 below, need to check power of 2 here
    if (Util.isPowerOf2(k)) {
      return (short) (((p & EXPONENT_MASK) << EXPONENT_SHIFT) & OUTPUT_MASK);
    }

    // mantissa is scalar in range [1,2); can reconstruct k as m * 2^p
    final double m = Math.pow(2.0, (Math.log(k) * INV_LN_2) - p);

    // Convert to index offset: ceil(m * BPO) - BPO
    // Typically in range range 0-(BINS_PER_OCTAVE-1) (but see note below)
    final int i = ((int) Math.floor(m * BINS_PER_OCTAVE) - BINS_PER_OCTAVE) + 1;

    // Due to ceiling, possible to overflow BINS_PER_OCTAVE
    // E.g., if BPO = 2048 then for k=32767 we have p=14. Except that 32767 > decodeValue
    // (p=14, i=2047)=32756, so we encode and return p+1
    if (i == BINS_PER_OCTAVE) {
      return (short) ((((p + 1) & EXPONENT_MASK) << EXPONENT_SHIFT) & OUTPUT_MASK);
    }

    return (short) (((p & EXPONENT_MASK) << EXPONENT_SHIFT) | ((i & INDEX_MASK) & OUTPUT_MASK));
  }

  /**
   * Decodes the 16-bit reservoir size value into an int.
   *
   * @param encodedSize Encoded 16-bit value
   * @return int represented by <tt>encodedSize</tt>
   */
  public static int decodeValue(final short encodedSize) {
    final int value = encodedSize & 0xFFFF;

    if (value > MAX_ENC_VALUE) {
      throw new SketchesArgumentException("Maximum valid encoded value is "
              + Integer.toHexString(MAX_ENC_VALUE) + ", found: " + value);
    }

    final int p = (value >>> EXPONENT_SHIFT) & EXPONENT_MASK;
    final int i = value & INDEX_MASK;

    return (int) ((1 << p) * ((i * INV_BINS_PER_OCTAVE) + 1.0));
  }
}
