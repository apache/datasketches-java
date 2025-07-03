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

package org.apache.datasketches.hll2;

import static org.apache.datasketches.hll2.HllUtil.MIN_LOG_K;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class HllEstimators {

  //HLL UPPER AND LOWER BOUNDS

  /*
   * The upper and lower bounds are not symmetric and thus are treated slightly differently.
   * For the lower bound, when the unique count is <= k, LB >= numNonZeros, where
   * numNonZeros = k - numAtCurMin AND curMin == 0.
   *
   * For HLL6 and HLL8, curMin is always 0 and numAtCurMin is initialized to k and is decremented
   * down for each valid update until it reaches 0, where it stays. Thus, for these two
   * isomorphs, when numAtCurMin = 0, means the true curMin is > 0 and the unique count must be
   * greater than k.
   *
   * HLL4 always maintains both curMin and numAtCurMin dynamically. Nonetheless, the rules for
   * the very small values <= k where curMin = 0 still apply.
   */

  static final double hllLowerBound(final AbstractHllArray absHllArr, final int numStdDev) {
    final int lgConfigK = absHllArr.lgConfigK;
    final int configK = 1 << lgConfigK;
    final double numNonZeros =
        (absHllArr.getCurMin() == 0) ? configK - absHllArr.getNumAtCurMin() : configK;
    final double estimate = absHllArr.getEstimate();
    final boolean oooFlag = absHllArr.isOutOfOrder();
    final double relErr = BaseHllSketch.getRelErr(false, oooFlag, lgConfigK, numStdDev);
    return Math.max(estimate / (1.0 + relErr), numNonZeros);
  }

  static final double hllUpperBound(final AbstractHllArray absHllArr, final int numStdDev) {
    final int lgConfigK = absHllArr.lgConfigK;
    final double estimate = absHllArr.getEstimate();
    final boolean oooFlag = absHllArr.isOutOfOrder();
    final double relErr = BaseHllSketch.getRelErr(true, oooFlag, lgConfigK, numStdDev);
    return estimate / (1.0 - relErr);
  }

  //THE HLL COMPOSITE ESTIMATOR

  /**
   * This is the (non-HIP) estimator.
   * It is called "composite" because multiple estimators are pasted together.
   * @param absHllArr an instance of the AbstractHllArray class.
   * @return the composite estimate
   */
  //In C: again-two-registers.c hhb_get_composite_estimate L1489
  static final double hllCompositeEstimate(final AbstractHllArray absHllArr) {
    final int lgConfigK = absHllArr.getLgConfigK();
    final double rawEst = getHllRawEstimate(lgConfigK, absHllArr.getKxQ0() + absHllArr.getKxQ1());

    final double[] xArr = CompositeInterpolationXTable.xArrs[lgConfigK - MIN_LOG_K];
    final double yStride = CompositeInterpolationXTable.yStrides[lgConfigK - MIN_LOG_K];
    final int xArrLen = xArr.length;

    if (rawEst < xArr[0]) { return 0; }

    final int xArrLenM1 = xArrLen - 1;

    if (rawEst > xArr[xArrLenM1]) {
      final double finalY = yStride * (xArrLenM1);
      final double factor = finalY / xArr[xArrLenM1];
      return rawEst * factor;
    }

    final double adjEst =
        CubicInterpolation.usingXArrAndYStride(xArr, yStride, rawEst);

    // We need to completely avoid the linear_counting estimator if it might have a crazy value.
    // Empirical evidence suggests that the threshold 3*k will keep us safe if 2^4 <= k <= 2^21.

    if (adjEst > (3 << lgConfigK)) { return adjEst; }
    //Alternate call
    //if ((adjEst > (3 << lgConfigK)) || ((curMin != 0) || (numAtCurMin == 0)) ) { return adjEst; }

    final double linEst =
        getHllBitMapEstimate(lgConfigK, absHllArr.getCurMin(), absHllArr.getNumAtCurMin());

    // Bias is created when the value of an estimator is compared with a threshold to decide whether
    // to use that estimator or a different one.
    // We conjecture that less bias is created when the average of the two estimators
    // is compared with the threshold. Empirical measurements support this conjecture.

    final double avgEst = (adjEst + linEst) / 2.0;

    // The following constants comes from empirical measurements of the crossover point
    // between the average error of the linear estimator and the adjusted HLL estimator
    double crossOver = 0.64;
    if (lgConfigK == 4)      { crossOver = 0.718; }
    else if (lgConfigK == 5) { crossOver = 0.672; }

    return (avgEst > (crossOver * (1 << lgConfigK))) ? adjEst : linEst;
  }

  /**
   * Estimator when N is small, roughly less than k log(k).
   * Refer to Wikipedia: Coupon Collector Problem
   * @param lgConfigK the current configured lgK of the sketch
   * @param curMin the current minimum value of the HLL window
   * @param numAtCurMin the current number of slots with the value curMin
   * @return the very low range estimate
   */
  //In C: again-two-registers.c hhb_get_improved_linear_counting_estimate L1274
  private static final double getHllBitMapEstimate(
      final int lgConfigK, final int curMin, final int numAtCurMin) {
    final int configK = 1 << lgConfigK;
    final int numUnhitBuckets =  (curMin == 0) ? numAtCurMin : 0;

    //This will eventually go away.
    if (numUnhitBuckets == 0) {
      return configK * Math.log(configK / 0.5);
    }

    final int numHitBuckets = configK - numUnhitBuckets;
    return HarmonicNumbers.getBitMapEstimate(configK, numHitBuckets);
  }

  //In C: again-two-registers.c hhb_get_raw_estimate L1167
  //This algorithm is from Flajolet's, et al, 2007 HLL paper, Fig 3.
  private static final double getHllRawEstimate(final int lgConfigK, final double kxqSum) {
    final int configK = 1 << lgConfigK;
    final double correctionFactor;
    if (lgConfigK == 4) { correctionFactor = 0.673; }
    else if (lgConfigK == 5) { correctionFactor = 0.697; }
    else if (lgConfigK == 6) { correctionFactor = 0.709; }
    else { correctionFactor = 0.7213 / (1.0 + (1.079 / configK)); }
    return (correctionFactor * configK * configK) / kxqSum;
  }

}
