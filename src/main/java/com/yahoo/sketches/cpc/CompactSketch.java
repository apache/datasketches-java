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
import static com.yahoo.sketches.cpc.PreambleUtil.getLgK;
import static com.yahoo.sketches.cpc.PreambleUtil.getNumCoupons;
import static com.yahoo.sketches.cpc.PreambleUtil.hasHip;

import com.yahoo.memory.Memory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public final class CompactSketch {
  Memory mem;

  public CompactSketch(final Memory mem) {
    this.mem = mem;
    checkLoPreamble(mem);
  }

  public CompactSketch(final byte[] byteArray) {
    this(Memory.wrap(byteArray));
  }

  /**
   * Returns the best estimate of the cardinality of the sketch.
   * @return the best estimate of the cardinality of the sketch.
   */
  public double getEstimate() {
    if (!hasHip(mem)) {
      return getIconEstimate(getLgK(mem), getNumCoupons(mem));
    }
    return getHipAccum(mem);
  }

  /**
   * Returns the best estimate of the upper bound of the confidence interval given <i>kappa</i>,
   * the number of standard deviations from the mean.
   * @param kappa the given number of standard deviations from the mean: 1, 2 or 3.
   * @return the best estimate of the upper bound of the confidence interval given <i>kappa</i>.
   */
  public double getUpperBound(final int kappa) {
    if (!hasHip(mem)) {
      return getIconConfidenceUB(getLgK(mem), getNumCoupons(mem), kappa);
    }
    return getHipConfidenceUB(getLgK(mem), getNumCoupons(mem), getHipAccum(mem), kappa);
  }

  /**
   * Returns the best estimate of the lower bound of the confidence interval given <i>kappa</i>,
   * the number of standard deviations from the mean.
   * @param kappa the given number of standard deviations from the mean: 1, 2 or 3.
   * @return the best estimate of the lower bound of the confidence interval given <i>kappa</i>.
   */
  public double getLowerBound(final int kappa) {
    if (!hasHip(mem)) {
      return getIconConfidenceLB(getLgK(mem), getNumCoupons(mem), kappa);
    }
    return getHipConfidenceLB(getLgK(mem), getNumCoupons(mem), getHipAccum(mem), kappa);
  }

}
