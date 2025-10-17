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

package org.apache.datasketches.tuple;

import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * For building a new generic tuple UpdatableTupleSketch
 * @param <U> Type of update value
 * @param <S> Type of Summary
 */
public class UpdatableTupleSketchBuilder<U, S extends UpdatableSummary<U>> {

  private int nomEntries_;
  private ResizeFactor resizeFactor_;
  private float samplingProbability_;
  private final SummaryFactory<S> summaryFactory_;

  private static final float DEFAULT_SAMPLING_PROBABILITY = 1;
  private static final ResizeFactor DEFAULT_RESIZE_FACTOR = ResizeFactor.X8;

  /**
   * Creates an instance of UpdatableTupleSketchBuilder with default parameters
   * @param summaryFactory An instance of SummaryFactory.
   */
  public UpdatableTupleSketchBuilder(final SummaryFactory<S> summaryFactory) {
    nomEntries_ = ThetaUtil.DEFAULT_NOMINAL_ENTRIES;
    resizeFactor_ = DEFAULT_RESIZE_FACTOR;
    samplingProbability_ = DEFAULT_SAMPLING_PROBABILITY;
    summaryFactory_ = summaryFactory;
  }

  /**
   * This is to set the nominal number of entries.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * or equal to the given value.
   * @return this UpdatableTupleSketchBuilder
   */
  public UpdatableTupleSketchBuilder<U, S> setNominalEntries(final int nomEntries) {
    nomEntries_ = 1 << ThetaUtil.checkNomLongs(nomEntries);
    return this;
  }

  /**
   * This is to set the resize factor.
   * Value of X1 means that the maximum capacity is allocated from the start.
   * Default resize factor is X8.
   * @param resizeFactor value of X1, X2, X4 or X8
   * @return this UpdatableTupleSketchBuilder
   */
  public UpdatableTupleSketchBuilder<U, S> setResizeFactor(final ResizeFactor resizeFactor) {
    resizeFactor_ = resizeFactor;
    return this;
  }

  /**
   * This is to set sampling probability.
   * Default probability is 1.
   * @param samplingProbability sampling probability from 0 to 1
   * @return this UpdatableTupleSketchBuilder
   */
  public UpdatableTupleSketchBuilder<U, S> setSamplingProbability(final float samplingProbability) {
    if ((samplingProbability < 0) || (samplingProbability > 1f)) {
      throw new SketchesArgumentException("sampling probability must be between 0 and 1");
    }
    samplingProbability_ = samplingProbability;
    return this;
  }

  /**
   * Returns an UpdatableTupleSketch with the current configuration of this Builder.
   * @return an UpdatableTupleSketch
   */
  public UpdatableTupleSketch<U, S> build() {
    return new UpdatableTupleSketch<>(nomEntries_, resizeFactor_.lg(), samplingProbability_,
        summaryFactory_);
  }

  /**
   * Resets the Nominal Entries, Resize Factor and Sampling Probability to their default values.
   * The assignment of <i>U</i> and <i>S</i> remain the same.
   */
  public void reset() {
    nomEntries_ = ThetaUtil.DEFAULT_NOMINAL_ENTRIES;
    resizeFactor_ = DEFAULT_RESIZE_FACTOR;
    samplingProbability_ = DEFAULT_SAMPLING_PROBABILITY;
  }
}
