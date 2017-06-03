/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;

import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * For building a new generic tuple UpdatableSketch
 * @param <U> Type of update value
 * @param <S> Type of Summary
 */
public class UpdatableSketchBuilder<U, S extends UpdatableSummary<U>> {

  private int nomEntries_;
  private ResizeFactor resizeFactor_;
  private float samplingProbability_;
  private final SummaryFactory<S> summaryFactory_;

  private static final float DEFAULT_SAMPLING_PROBABILITY = 1;
  private static final ResizeFactor DEFAULT_RESIZE_FACTOR = ResizeFactor.X8;

  /**
   * Creates an instance of UpdatableSketchBuilder with default parameters
   * @param summaryFactory An instance of SummaryFactory.
   */
  public UpdatableSketchBuilder(final SummaryFactory<S> summaryFactory) {
    nomEntries_ = DEFAULT_NOMINAL_ENTRIES;
    resizeFactor_ = DEFAULT_RESIZE_FACTOR;
    samplingProbability_ = DEFAULT_SAMPLING_PROBABILITY;
    summaryFactory_ = summaryFactory;
  }

  /**
   * This is to set the nominal number of entries.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than 
   * given value.
   * @return this UpdatableSketchBuilder
   */
  public UpdatableSketchBuilder<U, S> setNominalEntries(final int nomEntries) {
    nomEntries_ = nomEntries;
    return this;
  }

  /**
   * This is to set the resize factor.
   * Value of X1 means that the maximum capacity is allocated from the start.
   * Default resize factor is X8.
   * @param resizeFactor value of X1, X2, X4 or X8
   * @return this UpdatableSketchBuilder
   */
  public UpdatableSketchBuilder<U, S> setResizeFactor(final ResizeFactor resizeFactor) {
    resizeFactor_ = resizeFactor;
    return this;
  }

  /**
   * This is to set sampling probability.
   * Default probability is 1.
   * @param samplingProbability sampling probability from 0 to 1
   * @return this UpdatableSketchBuilder
   */
  public UpdatableSketchBuilder<U, S> setSamplingProbability(final float samplingProbability) {
    if (samplingProbability < 0 || samplingProbability > 1f) {
      throw new SketchesArgumentException("sampling probability must be between 0 and 1");
    }
    samplingProbability_ = samplingProbability;
    return this;
  }

  /**
   * Returns an UpdatableSketch with the current configuration of this Builder.
   * @return an UpdatableSketch
   */
  public UpdatableSketch<U, S> build() {
    return new UpdatableSketch<U, S>(nomEntries_, resizeFactor_.lg(), samplingProbability_, 
        summaryFactory_);
  }

}
