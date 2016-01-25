/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.LS;
import static com.yahoo.sketches.quantiles.Util.TAB;
import static com.yahoo.sketches.quantiles.QuantilesSketch.MIN_BASE_BUF_SIZE;

import com.yahoo.sketches.memory.Memory;

/**
 * For building a new QuantilesSketch.
 * 
 * @author Lee Rhodes 
 */
public class QuantilesSketchBuilder {
  private int bK = MIN_BASE_BUF_SIZE/2;
  private Memory bDstMem = null;
  private short bSeed = 0;
  
  public QuantilesSketchBuilder() {
    bK = 227; //default for ~1% rank accuracy
    bDstMem = null;
  }
  
  public QuantilesSketchBuilder setK(int k) {
    QuantilesSketch.checkK(k);
    bK = k;
    return this;
  }
  
  public QuantilesSketchBuilder initMemory(Memory dstMem) {
    bDstMem = dstMem;
    return this;
  }
  
  public QuantilesSketchBuilder setSeed(short seed) {
    bSeed = seed;
    return this;
  }
  
  public QuantilesSketch build() {
    if (bDstMem != null) {
      throw new IllegalArgumentException("DirectQuantilesSketch not implemented.");
      //sketch = DirectQuantilesSketch.getInstance(bK, bDstMem);
    } 
    return HeapQuantilesSketch.getInstance(bK, bSeed);
  }
  
  public QuantilesSketch build(int k) {
    bK = k;
    return build();
  }
  
  public int getK() {
    return bK;
  }
  
  public Memory getMemory() {
    return bDstMem;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("QuantileSketchBuilder configuration:").append(LS);
    sb.append("K:").append(TAB).append(bK).append(LS);
    sb.append("DstMemory:").append(TAB).append(bDstMem != null).append(LS);
    return sb.toString();
  }
}
