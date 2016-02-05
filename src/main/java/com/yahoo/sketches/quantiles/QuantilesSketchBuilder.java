/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.LS;
import static com.yahoo.sketches.quantiles.Util.TAB;
import static com.yahoo.sketches.quantiles.Util.DEFAULT_K;

import com.yahoo.sketches.memory.Memory;

/**
 * For building a new QuantilesSketch.
 * 
 * @author Lee Rhodes 
 */
public class QuantilesSketchBuilder {
  private int bK;
  private Memory bDstMem;
  private short bSeed;
  
  /**
   * Constructor for building a new QuantilesSketch. The default configuration is 
   * <ul>
   * <li>k: {@value com.yahoo.sketches.quantiles.Util#DEFAULT_K} 
   * This produces a normalized rank error of about 1.7%</li>
   * <li>Seed: 0</li>
   * <li>Memory: null</li>
   * </ul>
   */
  public QuantilesSketchBuilder() {
    bK = DEFAULT_K;
    bDstMem = null;
    bSeed = 0;
  }
  
  /**
   * Sets the parameter <i>k</i> that determines the accuracy and size of the sketch
   * @param k determines the accuracy and size of the sketch.  
   * <i>k</i> must be greater than 0 and less than 65536.
   * It is recommended that <i>k</i> be a power of 2 to enable merging of sketches with
   * different values of <i>k</i>. However, in this case it is only possible to merge from 
   * larger values of <i>k</i> to smaller values.
   * @return this builder
   */
  public QuantilesSketchBuilder setK(int k) {
    QuantilesSketch.checkK(k);
    bK = k;
    return this;
  }
  
  /**
   * Gets the current configured value of <i>k</i>
   * @return the current configured value of <i>k</i>
   */
  public int getK() {
    return bK;
  }
  
  /**
   * Setting the seed makes the results of the sketch deterministic if the input values are
   * received in exactly the same order. This is only useful when performing test comparisons,
   * otherwise is not recommended.
   * @param seed Any value other than zero will be used as the seed in the internal random number 
   * generator.
   * @return this builder
   */
  public QuantilesSketchBuilder setSeed(short seed) {
    bSeed = seed;
    return this;
  }
  
  /**
   * Returns the current configured seed
   * @return the current configured seed
   */
  public int getSeed() {
    return bSeed;
  }
  
  /**
   * Initialize the specified backing destination Memory store for use as the destination of
   * the sketch data structure.
   * @param dstMem The destination Memory.
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   * @return this builder
   */
  public QuantilesSketchBuilder initMemory(Memory dstMem) {
    bDstMem = dstMem;
    return this;
  }
  
  /**
   * Returns the current configured Destination Memory.
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   * @return the current configured Destination Memory
   */
  public Memory getMemory() {
    return bDstMem;
  }
  
  /**
   * Returns a QuantilesSketch with the current configuration of this Builder.
   * @return a QuantilesSketch
   */
  public QuantilesSketch build() {
    if (bDstMem != null) {
      throw new IllegalArgumentException("DirectQuantilesSketch not implemented.");
      //sketch = DirectQuantilesSketch.getInstance(bK, bDstMem);
    } 
    return HeapQuantilesSketch.getInstance(bK, bSeed);
  }
  
  /**
   * Returns a QuantilesSketch with the current configuration of this Builder and the
   * given parameter <i>k</i>.
   * @param k determines the accuracy and size of the sketch.  
   * <i>k</i> must be greater than 0 and less than 65536. 
   * It is recommended that <i>k</i> be a power of 2 to enable merging of sketches with
   * different values of <i>k</i>. However, in this case it is only possible to merge from 
   * larger values of <i>k</i> to smaller values.
   * 
   * @return a QuantilesSketch
   */
  public QuantilesSketch build(int k) {
    setK(k);
    return build();
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("QuantileSketchBuilder configuration:").append(LS);
    sb.append("K:").append(TAB).append(bK).append(LS);
    sb.append("Seed:").append(TAB).append(bSeed).append(LS);
    sb.append("DstMemory:").append(TAB).append(bDstMem != null).append(LS);
    return sb.toString();
  }
}
