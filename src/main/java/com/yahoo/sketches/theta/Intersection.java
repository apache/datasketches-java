/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import com.yahoo.sketches.memory.Memory;

/**
 * The API for intersection operations
 * 
 * @author Lee Rhodes
 */
public interface Intersection {

  /**
   * Intersect the given sketch with the internal state. 
   * This method can be repeatedly called.
   * If the given sketch is null the internal state becomes the empty sketch.  
   * Theta will become the minimum of thetas seen so far.
   * @param sketchIn the given sketch
   * @return the return state of the intersection
   */
  SetOpReturnState update(Sketch sketchIn);
  
  /**
   * Returns the result of a call to update(Sketch) as a new CompactSketch.
   * @param dstOrdered if true, the result of a SetOperation will be in ordered, compact form
   * @param dstMem the destination Memory
   * @return the result CompactSketch.
   */
  CompactSketch getResult(boolean dstOrdered, Memory dstMem);
  
  
  /**
   * Returns true if there is an intersection result available
   * @return true if there is an intersection result available
   */
  boolean hasResult();
  
  /**
   * Serialize this intersection to a byte array form. 
   * @return byte array of this intersection
   */
  byte[] toByteArray();
  
  /**
   * Resets this Intersection. The seed remains intact, otherwise reverts to 
   * the Universal Set, theta of 1.0 and empty = false.
   */
  void reset();
}