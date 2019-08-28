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

package org.apache.datasketches.cpc;

import static org.apache.datasketches.cpc.CpcUtil.maxLgK;
import static org.apache.datasketches.cpc.CpcUtil.minLgK;
import static org.apache.datasketches.cpc.IconPolynomialCoefficients.iconPolynomialCoefficents;
import static org.apache.datasketches.cpc.IconPolynomialCoefficients.iconPolynomialNumCoefficients;

/**
 * The ICON estimator for CPC sketches is defined by the arXiv paper.
 *
 * <p>The current file provides exact and approximate implementations of this estimator.
 *
 * <p>The exact version works for any value of K, but is quite slow.
 *
 * <p>The much faster approximate version works for K values that are powers of two
 * ranging from 2^4 to 2^32.
 *
 * <p>At a high-level, this approximation can be described as using an
 * exponential approximation when C &gt; K * (5.6 or 5.7), while smaller
 * values of C are handled by a degree-19 polynomial approximation of
 * a pre-conditioned version of the true ICON mapping from C to N_hat.
 *
 * <p>This file also provides a validation procedure that compares its approximate
 * and exact implementations of the CPC ICON estimator.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class IconEstimator {

  static double evaluatePolynomial(final double[] coefficients, final int start, final int num,
      final double x) {
    final int end = (start + num) - 1;
    double total = coefficients[end];
    for (int j = end - 1; j >= start; j--) {
      total *= x;
      total += coefficients[j];
    }
    return total;
  }

  static double iconExponentialApproximation(final double k, final double c) {
    return (0.7940236163830469 * k * Math.pow(2.0, c / k));
  }

  static double getIconEstimate(final int lgK, final long c) {
    assert lgK >= minLgK;
    assert lgK <= maxLgK;
    if (c < 2L) { return ((c == 0L) ? 0.0 : 1.0); }
    final int k = 1 << lgK;
    final double doubleK = k;
    final double doubleC = c;
    // Differing thresholds ensure that the approximated estimator is monotonically increasing.
    final double thresholdFactor = ((lgK < 14) ? 5.7 : 5.6);
    if (doubleC > (thresholdFactor * doubleK)) {
      return (iconExponentialApproximation(doubleK, doubleC));
    }
    final double factor = evaluatePolynomial(iconPolynomialCoefficents,
        iconPolynomialNumCoefficients * (lgK - minLgK),
        iconPolynomialNumCoefficients,
        // The constant 2.0 is baked into the table iconPolynomialCoefficents[].
        // This factor, although somewhat arbitrary, is based on extensive characterization studies
        // and is considered a safe conservative factor.
        doubleC / (2.0 * doubleK));
    final double ratio = doubleC / doubleK;
    // The constant 66.774757 is baked into the table iconPolynomialCoefficents[].
    // This factor, although somewhat arbitrary, is based on extensive characterization studies
    // and is considered a safe conservative factor.
    final double term = 1.0 + ((ratio * ratio * ratio) / 66.774757);
    final double result = doubleC * factor * term;
    return (result >= doubleC) ? result : doubleC;
  }

}
