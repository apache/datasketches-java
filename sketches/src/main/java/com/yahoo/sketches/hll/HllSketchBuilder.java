/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.*;

/**
 * @author Kevin Lang
 */
public class HllSketchBuilder { //TODO will need to add seed and Memory, etc.
  private Preamble preamble = null;
  private boolean compressedDense = false;
  private boolean denseMode = false;
  private boolean hipEstimator = false;
  
  /**
   * Default constructor using default nominal entries (4096).
   */
  public HllSketchBuilder() {
    preamble = Preamble.fromLogK(Integer.numberOfTrailingZeros(DEFAULT_NOMINAL_ENTRIES));
  }
  
  /**
   * Copy constructor
   * @return a copy of this sketch
   */
  public HllSketchBuilder copy() {
    HllSketchBuilder retVal = new HllSketchBuilder();
    retVal.preamble = preamble;
    retVal.compressedDense = compressedDense;
    retVal.denseMode = denseMode;
    retVal.hipEstimator = hipEstimator;

    return retVal;
  }
  
  /**
   * Sets the number of buckets (k) from the log_base2 of the desired value.
   * @param logBuckets the given log_base2 of the desired number of buckets
   * @return this Builder
   */
  public HllSketchBuilder setLogBuckets(int logBuckets) {
    this.preamble = Preamble.fromLogK((byte) logBuckets);
    return this;
  }
  
  /**
   * Gets the currently configured log_base2 of the number of buckets (k)
   * @return the currently configured log_base2 of the number of buckets (k)
   */
  public int getLogBuckets() {
    return preamble.getLogConfigK();
  }
  
  /**
   * Sets the Preamble
   * @param preamble the given Preamble
   * @return this builder
   */
  public HllSketchBuilder setPreamble(Preamble preamble) {
    this.preamble = preamble;
    return this;
  }
  
  /**
   * Gets the configured Preamble
   * @return the configured Preamble
   */
  public Preamble getPreamble() {
    return preamble;
  }
  
  /**
   * Sets the Dense Mode flag
   * @param denseMode the state of dense mode
   * @return this builder
   */
  public HllSketchBuilder setDenseMode(boolean denseMode) {
    this.denseMode = denseMode;
    return this;
  }

  /**
   * Gets the Dense Mode flag
   * @return the Dense Mode flag
   */
  public boolean isDenseMode() {
    return denseMode;
  }

  /**
   * Sets the Compressed Dense flag
   * @param compressedDense the state of Compressed Dense
   * @return this builder
   */
  public HllSketchBuilder setCompressedDense(boolean compressedDense) {
    this.compressedDense = compressedDense;
    return this;
  }

  /**
   * Gets the state of Compressed Dense
   * @return the state of Compressed Dense
   */
  public boolean isCompressedDense() {
    return compressedDense;
  }

  /**
   * Sets the Hip Estimator option
   * @param hipEstimator true if the Hip Estimater option is to be used
   * @return this builder
   */
  public HllSketchBuilder setHipEstimator(boolean hipEstimator) {
    this.hipEstimator = hipEstimator;
    return this;
  }
  
  /**
   * Gets the state of the Hip Estimator option
   * @return the state of the Hip Estimator option
   */
  public boolean isHipEstimator() {
    return hipEstimator;
  }
  
  /**
   * Build a new HllSketch
   * @return a new HllSketch
   */
  public HllSketch build() {
    final FieldsFactory denseFactory;
    if (compressedDense) {
      denseFactory = new DenseCompressedFieldsFactory();
    } else {
      denseFactory = new DenseFieldsFactory();
    }

    final Fields fields;
    if (denseMode) {
      fields = denseFactory.make(preamble);
    } else {
      fields = new OnHeapHashFields(preamble, 16, HashUtils.getMaxHashSize(preamble.getLogConfigK()), denseFactory);
    }
    
    if (hipEstimator) {
      return new HipHllSketch(fields);
    } else {
      return new HllSketch(fields);
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("HllSketchBuilder configuration:").append(LS)
      .append("LgK:").append(TAB).append(preamble.getLogConfigK()).append(LS)
      .append("K:").append(TAB).append(preamble.getConfigK()).append(LS)
      .append("DenseMode:").append(TAB).append(denseMode).append(LS)
      .append("HIP Estimator:").append(TAB).append(hipEstimator).append(LS)
      .append("Compressed Dense:").append(TAB).append(compressedDense).append(LS);
    
    return sb.toString();
  }
  
}
