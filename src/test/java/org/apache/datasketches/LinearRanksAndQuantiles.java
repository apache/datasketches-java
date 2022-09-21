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

package org.apache.datasketches;

import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.fail;

public class LinearRanksAndQuantiles {

  /**
   * Gets the quantile based on the given normalized rank.
   * <ul><li><b>getQuantile(rank, INCLUSIVE) or q(r, GE)</b><br>
   * := Given r, return the quantile, q, of the smallest rank that is strictly Greater than or Equal to r.</li>
   * <li><br>getQuantile(rank, EXCLUSIVE) or q(r, GT)</b><br>
   * := Given r, return the quantile, q, of the smallest rank that is strictly Greater Than r.</li>
   * </ul>
   *
   * @param cumWeights the given natural cumulative weights. The last value must be N.
   * @param quantiles the given quantile array
   * @param givenNormR the given normalized rank, which must be in the range [0.0, 1.0], inclusive.
   * @param inclusive determines the search criterion used.
   * @return the quantile
   */
  public static float getTrueFloatQuantile(
      final long[] cumWeights,
      final float[] quantiles,
      final double givenNormR,
      final QuantileSearchCriteria inclusive) {
    final int len = cumWeights.length;
    final long N = cumWeights[len - 1];
    float result = Float.NaN;

    for (int i = 0; i < len; i++) {
      if (i == len - 1) { //at top or single element array
        double topR = (double)cumWeights[i] / N;
        float topQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (givenNormR <= topR) { result = topQ; break; }
          fail("normRank > 1.0");
        }
        //EXCLUSIVE
        if (givenNormR < topR ) { result = topQ; break; }
        if (givenNormR > 1.0) { fail("normRank > 1.0"); }
        result =topQ; // R == 1.0
        break;
      }
      else { //always at least two valid entries
        double loR = (double)cumWeights[i] / N;
        double hiR = (double)cumWeights[i + 1] / N;
        float loQ = quantiles[i];
        float hiQ = quantiles[i + 1];
        if (inclusive == INCLUSIVE) {
          if (i == 0) { //at bottom, starting up
            if (givenNormR <= loR) { result = loQ; break; }
          }
          if (loR < givenNormR && givenNormR <= hiR) { result = hiQ; break; }
          continue;
        }
        //EXCLUSIVE
        if (i == 0) { //at bottom, starting up
          if (givenNormR < loR) { result = loQ; break; }
        }
        if (loR <= givenNormR && givenNormR < hiR) { result = hiQ; break; }
        continue;
      }
    }
    return result;
  }

  /**
   * Gets the quantile based on the given normalized rank.
   * <ul><li><b>getQuantile(rank, INCLUSIVE) or q(r, GE)</b><br>
   * := Given r, return the quantile, q, of the smallest rank that is strictly Greater than or Equal to r.</li>
   * <li><br>getQuantile(rank, EXCLUSIVE) or q(r, GT)</b><br>
   * := Given r, return the quantile, q, of the smallest rank that is strictly Greater Than r.</li>
   * </ul>
   *
   * @param cumWeights the given natural cumulative weights. The last value must be N.
   * @param quantiles the given quantile array
   * @param givenNormR the given normalized rank, which must be in the range [0.0, 1.0], inclusive.
   * @param inclusive determines the search criterion used.
   * @return the quantile
   */
  public static double getTrueDoubleQuantile(
      final long[] cumWeights,
      final double[] quantiles,
      final double givenNormR,
      final QuantileSearchCriteria inclusive) {
    final int len = cumWeights.length;
    final long N = cumWeights[len - 1];
    double result = Double.NaN;

    for (int i = 0; i < len; i++) {
      if (i == len - 1) { //at top or single element array
        double topR = (double)cumWeights[i] / N;
        double topQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (givenNormR <= topR) { result = topQ; break; }
          fail("normRank > 1.0");
        }
        //EXCLUSIVE
        if (givenNormR < topR ) { result = topQ; break; }
        if (givenNormR > 1.0) { fail("normRank > 1.0"); }
        result = topQ; // R == 1.0
        break;
      }
      else { //always at least two valid entries
        double loR = (double)cumWeights[i] / N;
        double hiR = (double)cumWeights[i + 1] / N;
        double loQ = quantiles[i];
        double hiQ = quantiles[i + 1];
        if (inclusive == INCLUSIVE) {
          if (i == 0) { //at bottom, starting up
            if (givenNormR <= loR) { result = loQ; break; }
          }
          if (loR < givenNormR && givenNormR <= hiR) { result = hiQ; break; }
          continue;
        }
        //EXCLUSIVE) {
        if (i == 0) { //at bottom, starting up
          if (givenNormR < loR) { result = loQ; break; }
        }
        if (loR <= givenNormR && givenNormR < hiR) { result = hiQ; break; }
        continue;
      }
    }
    return result;
  }

  /**
   * Gets the normalized rank based on the given value.
   * <ul><li><b>getRank(quantile, INCLUSIVE) or r(q, LE)</b><br>
   * := Given q, return the rank, r, of the largest quantile that is less than or equal to q.</li>
   * <li><b>getRank(quantile, EXCLUSIVE) or r(q, LT)</b>
   * := Given q, return the rank, r, of the largest quantile that is strictly Less Than q.</li>
   * </ul>
   *
   * @param cumWeights the given cumulative weights
   * @param quantiles the given quantile array
   * @param givenQ the given quantile
   * @param inclusive determines the search criterion used.
   * @return the normalized rank
   */
  public static double getTrueFloatRank(
      final long[] cumWeights,
      final float[] quantiles,
      final float givenQ,
      final QuantileSearchCriteria inclusive) {
    final int len = quantiles.length;
    final long N = cumWeights[len -1];
    double result = Double.NaN;

    for (int i = len; i-- > 0; ) {
      if (i == 0) { //at bottom or single element array
        double bottomR = (double)cumWeights[i] / N;
        float bottomQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (givenQ <  bottomQ) { result = 0; break; }
          result = bottomR;
          break;
        }
        //EXCLUSIVE
        if (givenQ <= bottomQ) { result = 0; break; }
        if (bottomQ < givenQ) { result = bottomR; break; }
      }
      else { //always at least two valid entries
        double loR = (double)cumWeights[i - 1] / N;
        //double hiR = (double)cumWeights[i] / N;
        float loQ = quantiles[i - 1];
        float hiQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (i == len - 1) { //at top, starting down
            if (hiQ <= givenQ) { result = 1.0; break; }
          }
          if (loQ <= givenQ && givenQ < hiQ) { result = loR; break; }
          continue;
        }
        //EXCLUSIVE
        if (i == len - 1) { //at top, starting down
          if (hiQ < givenQ) { result = 1.0; break; }
        }
        if (loQ < givenQ && givenQ <= hiQ) { result = loR; break; }
        continue;
      }
    }
    return result;
  }

  /**
   * Gets the normalized rank based on the given value.
   * <ul><li><b>getRank(quantile, INCLUSIVE) or r(q, LE)</b><br>
   * := Given q, return the rank, r, of the largest quantile that is less than or equal to q.</li>
   * <li><b>getRank(quantile, EXCLUSIVE) or r(q, LT)</b>
   * := Given q, return the rank, r, of the largest quantile that is strictly Less Than q.</li>
   * </ul>
   *
   * @param cumWeights the given cumulative weights
   * @param quantiles the given quantile array
   * @param givenQ the given quantile
   * @param inclusive determines the search criterion used.
   * @return the normalized rank
   */
  public static double getTrueDoubleRank(
      final long[] cumWeights,
      final double[] quantiles,
      final double givenQ,
      final QuantileSearchCriteria inclusive) {
    final int len = quantiles.length;
    final long N = cumWeights[len -1];
    double result = Double.NaN;

    for (int i = len; i-- > 0; ) {
      if (i == 0) { //at bottom or single element array
        double bottomR = (double)cumWeights[i] / N;
        double bottomQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (givenQ <  bottomQ) { result = 0; break; }
          result = bottomR;
          break;
        }
        //EXCLUSIVE
        if (givenQ <= bottomQ) { result = 0; break; }
        if (bottomQ < givenQ) { result = bottomR; break; }
      }
      else { //always at least two valid entries
        double loR = (double)cumWeights[i - 1] / N;
        //double hiR = (double)cumWeights[i] / N;
        double loQ = quantiles[i - 1];
        double hiQ = quantiles[i];
        if (inclusive == INCLUSIVE) {
          if (i == len - 1) { //at top, starting down
            if (hiQ <= givenQ) { result = 1.0; break; }
          }
          if (loQ <= givenQ && givenQ < hiQ) { result = loR; break; }
          continue;
        }
        //EXCLUSIVE
        if (i == len - 1) { //at top, starting down
          if (hiQ < givenQ) { result = 1.0; break; }
        }
        if (loQ < givenQ && givenQ <= hiQ) { result = loR; break; }
        continue;
      }
    }
    return result;
  }

}

