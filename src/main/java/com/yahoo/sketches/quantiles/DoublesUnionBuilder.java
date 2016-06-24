/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import com.yahoo.sketches.memory.Memory;

/**
 * For building a new QuantilesSketch Union operation.
 * 
 * @author Lee Rhodes 
 */
public class DoublesUnionBuilder {

  private int k_;

  /**
   * Constructor for building a new UnionBuilder.
   */
  public DoublesUnionBuilder() {
    k_ = DoublesSketch.DEFAULT_K;
  }

  /**
   * Sets the parameter <i>k</i> that determines the accuracy and size of the sketch
   * @param k determines the accuracy and size of the sketch.  
   * @return this builder
   */
  public DoublesUnionBuilder setK(final int k) {
    Util.checkK(k);
    k_ = k;
    return this;
  }

  /**
   * Returns a virgin Union object
   * @return a virgin Union object
   */
  public DoublesUnion build() {
    return new HeapDoublesUnion(k_);
  }

  /**
   * Returns a Union object that has been initialized with the given sketch to be used as a union 
   * target and will be modified. If you do not want the given sketch to be modified use the 
   * {@link #copyBuild(DoublesSketch)}.
   * 
   * @param sketch a QuantilesSketch that will be used as a target of subsequent union operations. 
   * @return a Union object
   */
  public DoublesUnion build(final DoublesSketch sketch) {
    return new HeapDoublesUnion(sketch);
  }

  /**
   * Returns a Union object that has been initialized with the data from the given Memory image
   * of a QuantilesSketch. A reference to this Memory image is not retained.
   * 
   * @param srcMem a Memory image of a QuantilesSketch
   * @return a Union object
   */
  public DoublesUnion build(Memory srcMem) {
    return new HeapDoublesUnion(srcMem);
  }

  /**
   * Returns a Union object that has been initialized with the data from the given sketch.
   * 
   * @param sketch A QuantilesSketch to be used as a source of data, but will not be modified.
   * @return a Union object
   */
  public DoublesUnion copyBuild(final DoublesSketch sketch) {
    return new HeapDoublesUnion(HeapDoublesSketch.copy(sketch));
  }

}
