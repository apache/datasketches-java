/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.LS;
import static com.yahoo.sketches.quantiles.Util.TAB;

import com.yahoo.memory.Memory;

/**
 * For building a new quantiles DoublesSketch.
 *
 * @author Lee Rhodes
 */
public class DoublesSketchBuilder {
  private int bK = PreambleUtil.DEFAULT_K;
  private Memory bMem = null;

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
   * Specifies the Memory to be initialized for a new off-heap version of the sketch.
   * @param mem the given Memory.
   * @return this builder
   */
  public DoublesSketchBuilder initMemory(final Memory mem) {
    bMem = mem;
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
   * Gets the configured Memory to be initialized by the sketch for off-heap use.
   * @return the configured Memory.
   */
  public Memory getMemory() {
    return bMem;
  }

  /**
   * Returns a DoublesSketch with the current configuration of this Builder.
   * @return a DoublesSketch
   */
  public UpdateDoublesSketch build() {
    return (bMem == null) ? HeapUpdateDoublesSketch.newInstance(bK)
        : DirectDoublesSketch.newInstance(bK, bMem);
  }

  /**
   * Returns a quantiles DoublesSketch with the current configuration of this builder and the
   * given parameter <i>k</i>.
   * @param k determines the accuracy and size of the sketch.
   * <i>k</i> must be greater than 1 and less than 65536.
   * It is recommended that <i>k</i> be a power of 2 to enable merging of sketches with
   * different values of <i>k</i>. However, in this case it is only possible to merge from
   * larger values of <i>k</i> to smaller values.
   *
   * @return a DoublesSketch
   */
  public UpdateDoublesSketch build(final int k) {
    return (bMem == null) ? HeapUpdateDoublesSketch.newInstance(k)
        : DirectDoublesSketch.newInstance(k, bMem);
  }

  /**
   * Creates a human readable string that describes the current configuration of this builder.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("QuantileSketchBuilder configuration:").append(LS);
    sb.append("K     : ").append(TAB).append(bK).append(LS);
    final String memStr = (bMem == null) ? "null" : "valid";
    sb.append("Memory: ").append(TAB).append(memStr);
    return sb.toString();
  }

}
