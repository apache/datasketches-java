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
public class UnionBuilder {

  private int k_;
  private short seed_;

  /**
   * Constructor for building a new UnionBuilder. The default configuration is 
   * <ul>
   * <li>k: {@value com.yahoo.sketches.quantiles.QuantilesSketch#DEFAULT_K} 
   * This produces a normalized rank error of about 1.7%</li>
   * <li>Seed: 0</li>
   * </ul>
   */
  public UnionBuilder() {
    k_ = QuantilesSketch.DEFAULT_K;
    seed_ = 0;
  }

  /**
   * Sets the parameter <i>k</i> that determines the accuracy and size of the sketch
   * @param k determines the accuracy and size of the sketch.  
   * @return this builder
   */
  public UnionBuilder setK(final int k) {
    Util.checkK(k);
    k_ = k;
    return this;
  }

  /**
   * Setting the seed makes the results of the sketch deterministic if the input values are
   * received in exactly the same order. This is only useful when performing test comparisons,
   * otherwise is not recommended.
   * @param seed Any value other than zero will be used as the seed in the internal random number 
   * generator.
   * @return this builder
   */
  public UnionBuilder setSeed(final short seed) {
    seed_ = seed;
    return this;
  }

  /**
   * Returns a virgin Union object
   * @return a virgin Union object
   */
  public Union build() {
    return new HeapUnion(k_, seed_);
  }

  /**
   * Returns a Union object that has been initialized with the given sketch to be used as a union 
   * target and will be modified. If you do not want the given sketch to be modified use the 
   * {@link #copyBuild(QuantilesSketch)}.
   * 
   * @param sketch a QuantilesSketch that will be used as a target of subsequent union operations. 
   * @return a Union object
   */
  public Union build(QuantilesSketch sketch) {
    return new HeapUnion(sketch);
  }
  
  /**
   * Returns a Union object that has been initialized with the data from the given Memory image
   * of a QuantilesSketch. A reference to this Memory image is not retained.
   * 
   * @param srcMem a Memory image of a QuantilesSketch
   * @return a Union object
   */
  public Union build(Memory srcMem) {
    return new HeapUnion(srcMem);
  }
  
  /**
   * Returns a Union object that has been initialized with the data from the given sketch.
   * 
   * @param sketch A QuantilesSketch to be used as a source of data, but will not be modified.
   * @return a Union object
   */
  public Union copyBuild(QuantilesSketch sketch) {
    HeapQuantilesSketch copy = HeapQuantilesSketch.copy(sketch);
    return new HeapUnion(copy);
  }
  
}
