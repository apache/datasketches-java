/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

/**
 * @author Kevin Lang
 */
class HipHllSketch extends HllSketch {
  // derived using some formulas from Ting's paper
  private static final double HIP_REL_ERROR_NUMER = 0.836083874576235;

  private double invPow2Sum;
  private double hipEstAccum;

  public HipHllSketch(final Fields fields) {
    super(fields);

    this.invPow2Sum = numBuckets();
    this.hipEstAccum = 0d;

    setUpdateCallback(
        new Fields.UpdateCallback() {
          private int numBuckets = fields.getPreamble().getConfigK();

          @Override
          public void bucketUpdated(int bucket, byte oldVal, byte newVal) {
            double oneOverQ = oneOverQ();
            hipEstAccum += oneOverQ;
            // subtraction before addition is intentional, in order to avoid overflow
            invPow2Sum -= HllUtils.invPow2(oldVal);
            invPow2Sum += HllUtils.invPow2(newVal);
          }

          private double oneOverQ() {
            return numBuckets / invPow2Sum;
          }
        }
    );
  }

  @Override
  public HllSketch union(HllSketch that) {
    throw new UnsupportedOperationException(
        "HipHllSketches cannot handle merges, use a normal HllSketch");
  }

  @Override
  public double getUpperBound(double numStdDevs) {
    return hipEstAccum / (1.0 - eps(numStdDevs));
  }

  @Override
  public double getLowerBound(double numStdDevs) {
    double lowerBound = hipEstAccum / (1.0 + eps(numStdDevs));
    int numBuckets = numBuckets();
    if (lowerBound < numBuckets) {
      double numNonZeros = numBuckets - numBucketsAtZero();
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

  private double eps(double numStdDevs) {
    return numStdDevs * HIP_REL_ERROR_NUMER / Math.sqrt(numBuckets());
  }
}
