/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.BoundsOnBinomialProportions.approximateLowerBoundOnP;
import static com.yahoo.sketches.BoundsOnBinomialProportions.approximateUpperBoundOnP;

import java.util.Random;

/**
 * Common utility functions for the sampling family of sketches.
 *
 * @author Jon Malkin
 */
final class SamplingUtil {

  /**
   * Number of standard deviations to use for subset sum error bounds
   */
  private static final double DEFAULT_KAPPA = 2.0;

  public static final Random rand = new Random();

  private SamplingUtil() {}

  /**
   * Checks if target sampling allocation is more than 50% of max sampling size. If so, returns
   * max sampling size, otherwise passes through the target size.
   *
   * @param maxSize      Maximum allowed reservoir size, as from getK()
   * @param resizeTarget Next size based on a pure ResizeFactor scaling
   * @return <code>(reservoirSize_ &lt; 2*resizeTarget ? reservoirSize_ : resizeTarget)</code>
   */
  static int getAdjustedSize(final int maxSize, final int resizeTarget) {
    if (maxSize - (resizeTarget << 1) < 0L) {
      return maxSize;
    }
    return resizeTarget;
  }

  static double nextDoubleExcludeZero() {
    double r = rand.nextDouble();
    while (r == 0.0) {
      r = rand.nextDouble();
    }
    return r;
  }

  static int startingSubMultiple(final int lgTarget, final int lgRf, final int lgMin) {
    return (lgTarget <= lgMin)
            ? lgMin : (lgRf == 0) ? lgTarget
            : (lgTarget - lgMin) % lgRf + lgMin;
  }

  static double pseudoHypergeometricUBonP(final long n, final int k, final double samplingRate) {
    final double adjustedKappa = DEFAULT_KAPPA * Math.sqrt(1 - samplingRate);
    return approximateUpperBoundOnP(n, k, adjustedKappa);
  }

  static double pseudoHypergeometricLBonP(final long n, final int k, final double samplingRate) {
    final double adjustedKappa = DEFAULT_KAPPA * Math.sqrt(1 - samplingRate);
    return approximateLowerBoundOnP(n, k, adjustedKappa);
  }
}
