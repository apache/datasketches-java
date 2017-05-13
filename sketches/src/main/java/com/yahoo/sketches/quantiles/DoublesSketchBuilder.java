/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.LS;
import static com.yahoo.sketches.quantiles.Util.TAB;

import com.yahoo.memory.WritableMemory;

/**
 * For building a new quantiles DoublesSketch.
 *
 * @author Lee Rhodes
 */
public class DoublesSketchBuilder {
  private int bK = PreambleUtil.DEFAULT_K;

  /**
   * Constructor for a new DoublesSketchBuilder. The default configuration is
   * <ul>
   * <li>k: 128. This produces a normalized rank error of about 1.7%</li>
   * <li>Memory: null</li>
   * </ul>
   */
  public DoublesSketchBuilder() {}

  /**
   * Sets the parameter <i>k</i> that determines the accuracy and size of the sketch.
   * @param k determines the accuracy and size of the sketch.
   * It is recommended that <i>k</i> be a power of 2 to enable unioning of sketches with
   * different values of <i>k</i>. It is only possible to union from
   * larger values of <i>k</i> to smaller values.
   * @return this builder
   */
  public DoublesSketchBuilder setK(final int k) {
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
   * Returns an UpdateDoublesSketch with the current configuration of this Builder.
   * @return a UpdateDoublesSketch
   */
  public UpdateDoublesSketch build() {
    return HeapUpdateDoublesSketch.newInstance(bK);
  }

  /**
   * Returns a quantiles UpdateDoublesSketch with the current configuration of this builder
   * and the specified backing destination Memory store.
   * @param dstMem destination memory for use by the sketch
   * @return an UpdateDoublesSketch
   */
  public UpdateDoublesSketch build(final WritableMemory dstMem) {
    return DirectUpdateDoublesSketch.newInstance(bK, dstMem);
  }

  /**
   * Creates a human readable string that describes the current configuration of this builder.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("QuantileSketchBuilder configuration:").append(LS);
    sb.append("K     : ").append(TAB).append(bK).append(LS);
    return sb.toString();
  }

}
