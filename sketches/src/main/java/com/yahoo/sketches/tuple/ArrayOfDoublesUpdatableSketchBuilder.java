/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * For building a new ArrayOfDoublesUpdatableSketch
 */
public class ArrayOfDoublesUpdatableSketchBuilder {

  private int nomEntries_;
  private ResizeFactor resizeFactor_;
  private int numValues_;
  private float samplingProbability_;
  private long seed_;

  private static final int DEFAULT_NUMBER_OF_VALUES = 1;
  private static final float DEFAULT_SAMPLING_PROBABILITY = 1;
  private static final ResizeFactor DEFAULT_RESIZE_FACTOR = ResizeFactor.X8;

  /**
   * Creates an instance of builder with default parameters
   */
  public ArrayOfDoublesUpdatableSketchBuilder() {
    nomEntries_ = DEFAULT_NOMINAL_ENTRIES;
    resizeFactor_ = DEFAULT_RESIZE_FACTOR;
    numValues_ = DEFAULT_NUMBER_OF_VALUES;
    samplingProbability_ = DEFAULT_SAMPLING_PROBABILITY;
    seed_ = DEFAULT_UPDATE_SEED;
  }

  /**
   * This is to set the nominal number of entries.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than 
   * given value.
   * @return this builder
   */
  public ArrayOfDoublesUpdatableSketchBuilder setNominalEntries(final int nomEntries) {
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
  public ArrayOfDoublesUpdatableSketchBuilder setResizeFactor(final ResizeFactor resizeFactor) {
    resizeFactor_ = resizeFactor;
    return this;
  }

  /**
   * This is to set sampling probability.
   * Default probability is 1.
   * @param samplingProbability sampling probability from 0 to 1
   * @return this builder
   */
  public ArrayOfDoublesUpdatableSketchBuilder 
        setSamplingProbability(final float samplingProbability) {
    if (samplingProbability < 0 || samplingProbability > 1f) {
      throw new SketchesArgumentException("sampling probability must be between 0 and 1");
    }
    samplingProbability_ = samplingProbability;
    return this;
  }

  /**
   * This is to set the number of double values associated with each key
   * @param numValues number of double values
   * @return this builder
   */
  public ArrayOfDoublesUpdatableSketchBuilder setNumberOfValues(final int numValues) {
    numValues_ = numValues;
    return this;
  }

  /**
   * Sets the long seed value that is required by the hashing function.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this builder
   */
  public ArrayOfDoublesUpdatableSketchBuilder setSeed(final long seed) {
    seed_ = seed;
    return this;
  }

  /**
   * Returns an ArrayOfDoublesUpdatableSketch with the current configuration of this Builder.
   * @return an ArrayOfDoublesUpdatableSketch
   */
  public ArrayOfDoublesUpdatableSketch build() {
      return new HeapArrayOfDoublesQuickSelectSketch(nomEntries_, resizeFactor_.lg(), 
          samplingProbability_, numValues_, seed_);
  }

  /**
   * Returns an ArrayOfDoublesUpdatableSketch with the current configuration of this Builder.
   * @param dstMem instance of Memory to be used by the sketch
   * @return an ArrayOfDoublesUpdatableSketch
   */
  public ArrayOfDoublesUpdatableSketch build(final WritableMemory dstMem) {
    return new DirectArrayOfDoublesQuickSelectSketch(nomEntries_, resizeFactor_.lg(), 
        samplingProbability_, numValues_, seed_, dstMem);
  }

}
