/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import com.yahoo.sketches.memory.Memory;

/**
 * The API for Union operations
 * 
 * @author Lee Rhodes
 */
public interface Union {

  /**
   * Union the given sketch with the internal state.
   * This method can be repeatedly called.
   * If the given sketch is null it is interpreted as an empty sketch.
   * 
   * @param sketchIn The incoming sketch.
   * @return the return state of the union
   */  
  SetOpReturnState update(Sketch sketchIn);
  
  /**
   * Get the result of the Union operations
   * @param dstOrdered true if output array must be sorted
   * @param dstMem the destination Memory
   * @return the Union result
   */
  CompactSketch getResult(boolean dstOrdered, Memory dstMem);
  
  /**
   * Returns a byte array image of this Union object
   * @return a byte array image of this Union object
   */
  byte[] toByteArray();
  
  /**
   * Resets this Union. The seed remains intact, otherwise reverts back to its virgin state.
   */
  void reset();
}