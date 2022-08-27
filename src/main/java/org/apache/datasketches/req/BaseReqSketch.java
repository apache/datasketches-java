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

package org.apache.datasketches.req;

import org.apache.datasketches.FloatsSortedView;
import org.apache.datasketches.QuantileSearchCriteria;
import org.apache.datasketches.QuantilesFloatsAPI;
import org.apache.datasketches.QuantilesFloatsSketchIterator;

/**
 * This abstract class provides a single place to define and document the public API
 * for the Relative Error Quantiles Sketch.
 *
 * @see <a href="https://datasketches.apache.org/docs/Quantiles/SketchingQuantilesAndRanksTutorial.html">
 * Sketching Quantiles and Ranks Tutorial</a>
 *
 * @author Lee Rhodes
 */
abstract class BaseReqSketch implements QuantilesFloatsAPI {

  @Override
  public abstract double[] getCDF(float[] splitPoints, QuantileSearchCriteria searchCrit);

  /**
   * If true, the high ranks are prioritized for better accuracy. Otherwise
   * the low ranks are prioritized for better accuracy.  This state is chosen during sketch
   * construction.
   * @return the high ranks accuracy state.
   */
  public abstract boolean getHighRankAccuracyMode();

  @Override
  public abstract int getK();

  @Override
  public abstract float getMaxQuantile();

  @Override
  public abstract float getMinQuantile();

  /**
   * Returns an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]).
   * Derived from Lemma 12 in https://arxiv.org/abs/2004.01668v2, but the constant factors were
   * modified based on empirical measurements.
   *
   * @param k the given value of k
   * @param rank the given normalized rank, a number in [0,1].
   * @param hra if true High Rank Accuracy mode is being selected, otherwise, Low Rank Accuracy.
   * @param totalN an estimate of the total number of values submitted to the sketch.
   * @return an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]).
   */
  public abstract double getRSE(int k, double rank, boolean hra, long totalN);

  @Override
  public abstract long getN();

  @Override
  public abstract double[] getPMF(float[] splitPoints, QuantileSearchCriteria searchCrit);

  @Override
  public abstract float getQuantile(double rank, QuantileSearchCriteria searchCrit);

  @Override
  public abstract float[] getQuantiles(double[] normRanks, QuantileSearchCriteria searchCrit);

  @Override
  public float[] getQuantiles(int numEvenlySpaced, QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { return null; }
    return getQuantiles(org.apache.datasketches.Util.evenlySpaced(0.0, 1.0, numEvenlySpaced), searchCrit);
  }

  @Override
  public abstract double getRank(float quantile, QuantileSearchCriteria searchCrit);

  /**
   * returns an approximate lower bound rank of the given normalized rank.
   * @param rank the given rank, a value between 0 and 1.0.
   * @param numStdDev the number of standard deviations. Must be 1, 2, or 3.
   * @return an approximate lower bound rank.
   */
  public abstract double getRankLowerBound(double rank, int numStdDev);

  @Override
  public abstract double[] getRanks(float[] quantiles, QuantileSearchCriteria searchCrit);

  /**
   * Returns an approximate upper bound rank of the given rank.
   * @param rank the given rank, a value between 0 and 1.0.
   * @param numStdDev the number of standard deviations. Must be 1, 2, or 3.
   * @return an approximate upper bound rank.
   */
  public abstract double getRankUpperBound(double rank, int numStdDev);

  @Override
  public abstract int getNumRetained();

  @Override
  public abstract int getSerializedSizeBytes();

  @Override
  public abstract FloatsSortedView getSortedView();

  @Override
  public boolean hasMemory() {
    return false;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public abstract boolean isEmpty();

  @Override
  public abstract boolean isEstimationMode();

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public abstract QuantilesFloatsSketchIterator iterator();

  /**
   * Merge other sketch into this one. The other sketch is not modified.
   * @param other sketch to be merged into this one.
   * @return this
   */
  public abstract ReqSketch merge(final ReqSketch other);

  /**
   * {@inheritDoc}
   * <p>The parameters k, highRankAccuracy, and reqDebug will not change.</p>
   */
  @Override
  public abstract void reset();


  @Override
  public abstract byte[] toByteArray();

  @Override
  public abstract String toString();

  @Override
  public abstract void update(final float value);

  /**
   * A detailed, human readable view of the sketch compactors and their data.
   * Each compactor string is prepended by the compactor lgWeight, the current number of retained
   * values of the compactor and the current nominal capacity of the compactor.
   * @param fmt the format string for the data values; example: "%4.0f".
   * @param allData all the retained values for the sketch will be output by
   * compactor level.  Otherwise, just a summary will be output.
   * @return a detailed view of the compactors and their data
   */
  public abstract String viewCompactorDetail(String fmt, boolean allData);

}
