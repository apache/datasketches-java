/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.invPow2;

/**
 */
final class HipHllSketch extends HllSketch {
  // derived using some formulas from Ting's paper
  private static final double HIP_REL_ERROR_NUMER = 0.836083874576235;

  private double invPow2Sum;
  private double hipEstAccum;

  public HipHllSketch(final Fields fields) {
    super(fields);

    invPow2Sum = numBuckets();
    hipEstAccum = 0d;

    setUpdateCallback(
        new Fields.UpdateCallback() {
          private int numBuckets = fields.getPreamble().getConfigK();

          @Override
          public void bucketUpdated(final int bucket, final byte oldVal, final byte newVal) {
            final double oneOverQ = oneOverQ();
            hipEstAccum += oneOverQ;
            // subtraction before addition is intentional, in order to avoid overflow
            invPow2Sum -= invPow2(oldVal);
            invPow2Sum += invPow2(newVal);
          }

          private double oneOverQ() {
            return numBuckets / invPow2Sum;
          }
        }
    );
  }

  @Override
  public HllSketch union(final HllSketch that) {
    throw new UnsupportedOperationException(
        "HipHllSketches cannot handle merges, use a normal HllSketch");
  }

  @Override
  public double getUpperBound(final double numStdDevs) {
    return hipEstAccum / (1.0 - eps(numStdDevs));
  }

  @Override
  public double getLowerBound(final double numStdDevs) {
    final double lowerBound = hipEstAccum / (1.0 + eps(numStdDevs));
    final int numBuckets = numBuckets();
    if (lowerBound < numBuckets) {
      final double numNonZeros = numBuckets - numBucketsAtZero();
      if (lowerBound < numNonZeros) {
        return numNonZeros;
      }
    }
    return lowerBound;
  }

  @Override
  public double getEstimate() {
    return hipEstAccum;
  }

  @Override
  protected double inversePowerOf2Sum() {
    return invPow2Sum;
  }

  private double eps(final double numStdDevs) {
    return (numStdDevs * HIP_REL_ERROR_NUMER) / Math.sqrt(numBuckets());
  }

  @Override
  public byte[] toByteArray() {
    final byte[] retVal = super.toByteArray();

    // Override isHip flag on preamble
    retVal[5] |= 0x1;

    return retVal;
  }
}
