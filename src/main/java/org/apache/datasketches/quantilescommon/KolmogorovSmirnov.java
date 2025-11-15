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

package org.apache.datasketches.quantilescommon;

import static java.lang.Math.abs;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.sqrt;
import static org.apache.datasketches.quantilescommon.QuantilesAPI.UNSUPPORTED_MSG;

import org.apache.datasketches.req.ReqSketch;

/**
 * Kolmogorov-Smirnov Test
 * See <a href="https://en.wikipedia.org/wiki/Kolmogorov-Smirnov_test">Kolmogorov–Smirnov Test</a>
 */
public final class KolmogorovSmirnov {

  /**
   * No argument constructor.
   */
  public KolmogorovSmirnov() { }

  /**
   * Computes the raw delta between two QuantilesDoublesAPI sketches for the <i>kolmogorovSmirnovTest(...)</i> method.
   * @param sketch1 first Input QuantilesDoublesAPI
   * @param sketch2 second Input QuantilesDoublesAPI
   * @return the raw delta area between two QuantilesDoublesAPI sketches
   */
  public static double computeKSDelta(final QuantilesDoublesAPI sketch1, final QuantilesDoublesAPI sketch2) {
    final DoublesSortedView p = sketch1.getSortedView();
    final DoublesSortedView q = sketch2.getSortedView();

    final double[] pSamplesArr = p.getQuantiles();
    final double[] qSamplesArr = q.getQuantiles();
    final long[] pCumWtsArr = p.getCumulativeWeights();
    final long[] qCumWtsArr = q.getCumulativeWeights();
    final int pSamplesArrLen = pSamplesArr.length;
    final int qSamplesArrLen = qSamplesArr.length;

    final double n1 = sketch1.getN();
    final double n2 = sketch2.getN();

    double deltaHeight = 0;
    int i = 0;
    int j = 0;

    while ((i < pSamplesArrLen - 1) && (j < qSamplesArrLen - 1)) {
      deltaHeight = max(deltaHeight, abs(pCumWtsArr[i] / n1 - qCumWtsArr[j] / n2));
      if (pSamplesArr[i] < qSamplesArr[j]) {
        i++;
      } else if (qSamplesArr[j] < pSamplesArr[i]) {
        j++;
      } else {
        i++;
        j++;
      }
    }

    deltaHeight = max(deltaHeight, abs(pCumWtsArr[i] / n1 - qCumWtsArr[j] / n2));
    return deltaHeight;
  }

  /**
   * Computes the raw delta between two QuantilesFloatsAPI sketches for the <i>kolmogorovSmirnovTest(...)</i> method.
   * method.
   * @param sketch1 first Input QuantilesFloatsAPI sketch
   * @param sketch2 second Input QuantilesFloatsAPI sketch
   * @return the raw delta area between two QuantilesFloatsAPI sketches
   */
  public static double computeKSDelta(final QuantilesFloatsAPI sketch1, final QuantilesFloatsAPI sketch2) {
    final FloatsSortedView p = sketch1.getSortedView();
    final FloatsSortedView q = sketch2.getSortedView();

    final float[] pSamplesArr = p.getQuantiles();
    final float[] qSamplesArr = q.getQuantiles();
    final long[] pCumWtsArr = p.getCumulativeWeights();
    final long[] qCumWtsArr = q.getCumulativeWeights();
    final int pSamplesArrLen = pSamplesArr.length;
    final int qSamplesArrLen = qSamplesArr.length;

    final double n1 = sketch1.getN();
    final double n2 = sketch2.getN();

    double deltaHeight = 0;
    int i = 0;
    int j = 0;

    while ((i < pSamplesArrLen - 1) && (j < qSamplesArrLen - 1)) {
      deltaHeight = max(deltaHeight, abs(pCumWtsArr[i] / n1 - qCumWtsArr[j] / n2));
      if (pSamplesArr[i] < qSamplesArr[j]) {
        i++;
      } else if (qSamplesArr[j] < pSamplesArr[i]) {
        j++;
      } else {
        i++;
        j++;
      }
    }

    deltaHeight = max(deltaHeight, abs(pCumWtsArr[i] / n1 - qCumWtsArr[j] / n2));
    return deltaHeight;
  }

  /**
   * Computes the adjusted delta height threshold for the <i>kolmogorovSmirnovTest(...)</i> method.
   * This adjusts the computed threshold by the error epsilons of the two given sketches.
   * The two sketches must be of the same primitive type, double or float.
   * This will not work with the REQ sketch.
   * @param sketch1 first Input QuantilesAPI sketch
   * @param sketch2 second Input QuantilesAPI sketch
   * @param tgtPvalue Target p-value. Typically .001 to .1, e.g., .05.
   * @return the adjusted threshold to be compared with the raw delta area.
   */
  public static double computeKSThreshold(final QuantilesAPI sketch1,
                                          final QuantilesAPI sketch2,
                                          final double tgtPvalue) {
    final double r1 = sketch1.getNumRetained();
    final double r2 = sketch2.getNumRetained();
    final double alpha = tgtPvalue;
    final double alphaFactor = sqrt(-0.5 * log(0.5 * alpha));
    final double deltaAreaThreshold = alphaFactor * sqrt((r1 + r2) / (r1 * r2));
    final double eps1 = sketch1.getNormalizedRankError(false);
    final double eps2 = sketch2.getNormalizedRankError(false);
    return deltaAreaThreshold + eps1 + eps2;
  }

  /**
   * Performs the Kolmogorov-Smirnov Test between two QuantilesAPI sketches.
   * Note: if the given sketches have insufficient data or if the sketch sizes are too small,
   * this will return false. The two sketches must be of the same primitive type, double or float.
   * This will not work with the REQ sketch.
   * @param sketch1 first Input QuantilesAPI
   * @param sketch2 second Input QuantilesAPI
   * @param tgtPvalue Target p-value. Typically .001 to .1, e.g., .05.
   * @return Boolean indicating whether we can reject the null hypothesis (that the sketches
   * reflect the same underlying distribution) using the provided tgtPValue.
   */
  public static boolean kolmogorovSmirnovTest(final QuantilesAPI sketch1,
      final QuantilesAPI sketch2, final double tgtPvalue) {

    final double delta = isDoubleType(sketch1, sketch2)
        ? computeKSDelta((QuantilesDoublesAPI)sketch1, (QuantilesDoublesAPI)sketch2)
        : computeKSDelta((QuantilesFloatsAPI)sketch1, (QuantilesFloatsAPI)sketch2);
    final double thresh = computeKSThreshold(sketch1, sketch2, tgtPvalue);
    return delta > thresh;
  }

  private static boolean isDoubleType(final Object sk1, final Object sk2) {
    if (sk1 instanceof ReqSketch || sk2 instanceof ReqSketch) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    final boolean isDbl = (sk1 instanceof QuantilesDoublesAPI && sk2 instanceof QuantilesDoublesAPI);
    final boolean isFlt = (sk1 instanceof QuantilesFloatsAPI && sk2 instanceof QuantilesFloatsAPI);
    if (isDbl ^ isFlt) { return isDbl; }
    else { throw new UnsupportedOperationException(UNSUPPORTED_MSG); }
  }

}
