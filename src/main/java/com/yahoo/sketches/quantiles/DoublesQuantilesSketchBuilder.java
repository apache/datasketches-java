/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.LS;
import static com.yahoo.sketches.quantiles.Util.TAB;

/**
 * For building a new QuantilesSketch.
 * 
 * @author Lee Rhodes 
 */
public class DoublesQuantilesSketchBuilder {
  private int bK;
  
  /**
   * Constructor for building a new QuantilesSketch. The default configuration is 
   * <ul>
   * <li>k: {@value com.yahoo.sketches.quantiles.DoublesQuantilesSketch#DEFAULT_K} 
   * This produces a normalized rank error of about 1.7%</li>
   * <li>Seed: 0</li>
   * <li>Memory: null</li>
   * </ul>
   */
  public DoublesQuantilesSketchBuilder() {
    bK = DoublesQuantilesSketch.DEFAULT_K;
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
  public DoublesQuantilesSketchBuilder setK(int k) {
    Util.checkK(k);
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
   * Returns a QuantilesSketch with the current configuration of this Builder.
   * @return a QuantilesSketch
   */
  public DoublesQuantilesSketch build() {
    return HeapDoublesQuantilesSketch.getInstance(bK);
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
  public DoublesQuantilesSketch build(int k) {
    setK(k);
    return build();
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("QuantileSketchBuilder configuration:").append(LS);
    sb.append("K:").append(TAB).append(bK).append(LS);
    return sb.toString();
  }

}
