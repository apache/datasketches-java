/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.TAB;

/**
 * @author Kevin Lang
 */
public class HllSketchBuilder { //TODO will need to add seed and Memory, etc.
  private Preamble preamble = null;
  private boolean compressedDense = false;
  private boolean denseMode = false;
  private boolean hipEstimator = false;
  
  public HllSketchBuilder() {
    preamble = Preamble.fromLogK(Integer.numberOfTrailingZeros(DEFAULT_NOMINAL_ENTRIES));
  }
  
  public HllSketchBuilder copy() {  //not used.  Do we need this?
    HllSketchBuilder retVal = new HllSketchBuilder();

    retVal.preamble = preamble;
    retVal.compressedDense = compressedDense;
    retVal.denseMode = denseMode;
    retVal.hipEstimator = hipEstimator;

    return retVal;
  }
  
  public HllSketchBuilder setLogBuckets(int logBuckets) {
    this.preamble = Preamble.fromLogK((byte) logBuckets);
    return this;
  }
  
  public int getLogBuckets() {
    return preamble.getLogConfigK();
  }
  
  public HllSketchBuilder setPreamble(Preamble preamble) {
    this.preamble = preamble;
    return this;
  }
  
  public Preamble getPreamble() {
    return preamble;
  }
  
  public HllSketchBuilder setDenseMode(boolean denseMode) {
    this.denseMode = denseMode;
    return this;
  }

  public boolean isDenseMode() {
    return denseMode;
  }

  public HllSketchBuilder setCompressedDense(boolean compressedDense) {
    this.compressedDense = compressedDense;
    return this;
  }

  public boolean isCompressedDense() {
    return compressedDense;
  }

  public HllSketchBuilder setHipEstimator(boolean hipEstimator) {
    this.hipEstimator = hipEstimator;
    return this;
  }
  
  public boolean isHipEstimator() {
    return hipEstimator;
  }
  
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
    sb.append("HllSketchBuilder configuration:").append(LS).
       append("LgK:").append(TAB).append(preamble.getLogConfigK()).append(LS).
       append("K:").append(TAB).append(preamble.getConfigK()).append(LS).
       append("DenseMode:").append(TAB).append(denseMode).append(LS).
       append("HIP Estimator:").append(TAB).append(hipEstimator).append(LS).
       append("Compressed Dense:").append(TAB).append(compressedDense).append(LS);
    
    return sb.toString();
  }
  
}
