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

package org.apache.datasketches.quantiles;

/**
 * Kolmogorov-Smirnov Test
 * See <a href="https://en.wikipedia.org/wiki/Kolmogorov-Smirnov_test">Kolmogorov–Smirnov Test</a>
 */
final class KolmogorovSmirnov {

  /**
   * Computes the raw delta area between two quantile sketches for the
   * <i>kolmogorovSmirnovTest(DoublesSketch, DoublesSketch, double)</i>
   * method.
   * @param sketch1 Input DoubleSketch 1
   * @param sketch2 Input DoubleSketch 2
   * @return the raw delta area between two quantile sketches
   */
  public static double computeKSDelta(final DoublesSketch sketch1, final DoublesSketch sketch2) {
    final DoublesAuxiliary p = new DoublesAuxiliary(sketch1);
    final DoublesAuxiliary q = new DoublesAuxiliary(sketch2);

    final double[] pSamplesArr = p.auxSamplesArr_;
    final double[] qSamplesArr = q.auxSamplesArr_;
    final long[] pCumWtsArr = p.auxCumWtsArr_;
    final long[] qCumWtsArr = q.auxCumWtsArr_;
    final int pSamplesArrLen = pSamplesArr.length;
    final int qSamplesArrLen = qSamplesArr.length;

    final double n1 = sketch1.getN();
    final double n2 = sketch2.getN();

    double deltaArea = 0;
    int i = 0;
    int j = 0;

    while ((i < pSamplesArrLen) && (j < qSamplesArrLen)) {
      deltaArea = Math.max(deltaArea, Math.abs(pCumWtsArr[i] / n1 - qCumWtsArr[j] / n2));
      if (pSamplesArr[i] < qSamplesArr[j]) {
        i++;
      } else if (qSamplesArr[j] < pSamplesArr[i]) {
        j++;
      } else {
        i++;
        j++;
      }
    }

    deltaArea = Math.max(deltaArea, Math.abs(pCumWtsArr[i] / n1 - qCumWtsArr[j] / n2));
    return deltaArea;
  }

  /**
   * Computes the adjusted delta area threshold for the
   * <i>kolmogorovSmirnovTest(DoublesSketch, DoublesSketch, double)</i>
   * method.
   * This adjusts the computed threshold by the error epsilons of the two given sketches.
   * @param sketch1 Input DoubleSketch 1
   * @param sketch2 Input DoubleSketch 2
   * @param tgtPvalue Target p-value. Typically .001 to .1, e.g., .05.
   * @return the adjusted threshold to be compared with the raw delta area.
   */
  public static double computeKSThreshold(final DoublesSketch sketch1,
                                          final DoublesSketch sketch2,
                                          final double tgtPvalue) {
    final double r1 = sketch1.getRetainedItems();
    final double r2 = sketch2.getRetainedItems();
    final double alpha = tgtPvalue;
    final double alphaFactor = Math.sqrt(-0.5 * Math.log(0.5 * alpha));
    final double deltaAreaThreshold = alphaFactor * Math.sqrt((r1 + r2) / (r1 * r2));
    final double eps1 = sketch1.getNormalizedRankError(false);
    final double eps2 = sketch2.getNormalizedRankError(false);
    return deltaAreaThreshold + eps1 + eps2;
  }

  /**
   * Performs the Kolmogorov-Smirnov Test between two quantiles sketches.
   * Note: if the given sketches have insufficient data or if the sketch sizes are too small,
   * this will return false.
   * @param sketch1 Input DoubleSketch 1
   * @param sketch2 Input DoubleSketch 2
   * @param tgtPvalue Target p-value. Typically .001 to .1, e.g., .05.
   * @return Boolean indicating whether we can reject the null hypothesis (that the sketches
   * reflect the same underlying distribution) using the provided tgtPValue.
   */
  public static boolean kolmogorovSmirnovTest(final DoublesSketch sketch1,
      final DoublesSketch sketch2, final double tgtPvalue) {
    final double delta = computeKSDelta(sketch1, sketch2);
    final double thresh = computeKSThreshold(sketch1, sketch2, tgtPvalue);
    return delta > thresh;
  }

}
