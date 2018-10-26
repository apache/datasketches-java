/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.CpcConfidence.getHipConfidenceLB;
import static com.yahoo.sketches.cpc.CpcConfidence.getHipConfidenceUB;
import static com.yahoo.sketches.cpc.CpcConfidence.getIconConfidenceLB;
import static com.yahoo.sketches.cpc.CpcConfidence.getIconConfidenceUB;
import static com.yahoo.sketches.cpc.IconEstimator.getIconEstimate;
import static com.yahoo.sketches.cpc.PreambleUtil.checkLoPreamble;
import static com.yahoo.sketches.cpc.PreambleUtil.getHipAccum;
import static com.yahoo.sketches.cpc.PreambleUtil.getNumCoupons;
import static com.yahoo.sketches.cpc.PreambleUtil.hasHip;
import static com.yahoo.sketches.cpc.PreambleUtil.isCompressed;
import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssert;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;

/**
 * This provides a read-only view of a serialized image of a CpcSketch, which can be
 * on-heap or off-heap represented as a Memory object, or on-heap represented as a byte array.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public final class CpcWrapper {
  Memory mem;

  /**
   * Construct a read-only view of the given Memory that contains a CpcSketch
   * @param mem the given Memory
   */
  public CpcWrapper(final Memory mem) {
    this.mem = mem;
    checkLoPreamble(mem);
    rtAssert(isCompressed(mem));

  }

  /**
   * Construct a read-only view of the given byte array that contains a CpcSketch.
   * @param byteArray the given byte array
   */
  public CpcWrapper(final byte[] byteArray) {
    this(Memory.wrap(byteArray));
  }

  /**
   * Returns the best estimate of the cardinality of the sketch.
   * @return the best estimate of the cardinality of the sketch.
   */
  public double getEstimate() {
    if (!hasHip(mem)) {
      return getIconEstimate(PreambleUtil.getLgK(mem), getNumCoupons(mem));
    }
    return getHipAccum(mem);
  }

  /**
   * Return the DataSketches identifier for this CPC family of sketches.
   * @return the DataSketches identifier for this CPC family of sketches.
   */
  public static Family getFamily() {
    return Family.CPC;
  }

  /**
   * Returns the configured Log_base2 of K of this sketch.
   * @return the configured Log_base2 of K of this sketch.
   */
  public int getLgK() {
    return PreambleUtil.getLgK(mem);
  }

  /**
   * Returns the best estimate of the lower bound of the confidence interval given <i>kappa</i>,
   * the number of standard deviations from the mean.
   * @param kappa the given number of standard deviations from the mean: 1, 2 or 3.
   * @return the best estimate of the lower bound of the confidence interval given <i>kappa</i>.
   */
  public double getLowerBound(final int kappa) {
    if (!hasHip(mem)) {
      return getIconConfidenceLB(PreambleUtil.getLgK(mem), getNumCoupons(mem), kappa);
    }
    return getHipConfidenceLB(PreambleUtil.getLgK(mem), getNumCoupons(mem), getHipAccum(mem), kappa);
  }

  /**
   * Returns the best estimate of the upper bound of the confidence interval given <i>kappa</i>,
   * the number of standard deviations from the mean.
   * @param kappa the given number of standard deviations from the mean: 1, 2 or 3.
   * @return the best estimate of the upper bound of the confidence interval given <i>kappa</i>.
   */
  public double getUpperBound(final int kappa) {
    if (!hasHip(mem)) {
      return getIconConfidenceUB(PreambleUtil.getLgK(mem), getNumCoupons(mem), kappa);
    }
    return getHipConfidenceUB(PreambleUtil.getLgK(mem), getNumCoupons(mem), getHipAccum(mem), kappa);
  }

}
