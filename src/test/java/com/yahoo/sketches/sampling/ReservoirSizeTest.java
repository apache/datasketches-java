/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;

public class ReservoirSizeTest {
  @Test
  public void checkComputeSize() {
    short enc;
    enc = ReservoirSize.computeSize(1);
    assertEquals(enc, (short) 0x0000);

    enc = ReservoirSize.computeSize(128);
    assertEquals(enc, (short) 0x3800);

    enc = ReservoirSize.computeSize(200);
    assertEquals(enc, (short) 0x3C80);

    enc = ReservoirSize.computeSize(4097);
    assertEquals(enc, (short) 0x6001);

    // NOTE: 0x61C4 is exact but Java seems to have numerical precision issues
    enc = ReservoirSize.computeSize(5000);
    assertEquals(enc, (short) 0x61C5);

    enc = ReservoirSize.computeSize(25000);
    assertEquals(enc, (short) 0x7436);

    // Encoding cannot represent 32767 with an exponent of 14, so need to go to the next power of 2
    enc = ReservoirSize.computeSize(32767);
    assertEquals(enc, (short) 0x7800);

    enc = ReservoirSize.computeSize(95342);
    assertEquals(enc, (short) 0x83A4);

    try {
      ReservoirSize.computeSize(-1);
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().startsWith("Can only encode strictly positive sketch sizes less than"));
    }
  }

  @Test
  public void checkDecodeValue() {
    int dec;

    dec = ReservoirSize.decodeValue((short) 0x0000);
    assertEquals(dec, 1);

    dec = ReservoirSize.decodeValue((short) 0x3800);
    assertEquals(dec, 128);

    dec = ReservoirSize.decodeValue((short) 0x3C80);
    assertEquals(dec, 200);

    dec = ReservoirSize.decodeValue((short) 0x6001);
    assertEquals(dec, 4098);

    dec = ReservoirSize.decodeValue((short) 0x61C4);
    assertEquals(dec, 5000);

    dec = ReservoirSize.decodeValue((short) 0x7435);
    assertEquals(dec, 25000);

    dec = ReservoirSize.decodeValue((short) 0x83A4);
    assertEquals(dec, 95360);

    try {
      ReservoirSize.decodeValue((short) -1);
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().startsWith("Maximum valid encoded value is "));
    }
  }

  @Test
  public void checkRelativeError() {
    // Generate some random values and ensure the relative error of the decoded result is
    // within epsilon (eps) of the target.
    // This condition should always hold regardless of the random seed used.
    final double eps = 1.0 / ReservoirSize.BINS_PER_OCTAVE;
    final int maxValue = 2146959359; // based on MAX_ABS_VALUE
    final int numIters = 100;

    for (int i = 0; i < numIters; ++i) {
      final int input = SamplingUtil.rand.nextInt(maxValue) + 1;
      final int result = ReservoirSize.decodeValue(ReservoirSize.computeSize(input));

      // result must be no smaller than input
      assertTrue(result >= input, "encoded/decoded result < input: " + result + " vs "
              + input + " (iter " + i + ")");

      // cap on max error
      final double relativeError = ((double) result - input) / input;
      assertTrue(relativeError <= eps, "Error exceeds tolerance. Expected relative error <= "
              + eps + "; " + "found " + relativeError);
    }
  }
}
