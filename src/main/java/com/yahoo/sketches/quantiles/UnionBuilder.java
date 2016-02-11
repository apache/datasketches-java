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
  
  /**
   * Returns a virgin Union object
   * @return a virgin Union object
   */
  public Union build() {
    return new HeapUnion();
  }
  
  /**
   * Returns a Union object that has been initilized with the given sketch to be used as a union 
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
   * Returns a Union object that has been initilized with the data from the given sketch.
   * 
   * @param sketch A QuantilesSketch to be used as a source of data, but will not be modified.
   * @return a Union object
   */
  public Union copyBuild(QuantilesSketch sketch) {
    HeapQuantilesSketch copy = HeapQuantilesSketch.copy(sketch);
    return new HeapUnion(copy);
  }
  
}
