/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.sampling;

import static org.apache.datasketches.BoundsOnBinomialProportions.approximateLowerBoundOnP;
import static org.apache.datasketches.BoundsOnBinomialProportions.approximateUpperBoundOnP;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

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
    double r = rand().nextDouble();
    while (r == 0.0) {
      r = rand().nextDouble();
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

  public static Random rand() {
    return ThreadLocalRandom.current();
  }
}
