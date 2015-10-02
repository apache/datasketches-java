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
   */  
  void update(Sketch sketchIn);
  
  /**
   * Union the given sketch with the internal state.
   * This method can be repeatedly called. This method only works with Memory sketch objects
   * that are in Compact and Ordered form.
   * If the given sketch is null it is interpreted as an empty sketch.
   * @param mem Memory image of sketch to be merged
   */
  void update(Memory mem);
  
  /**
   * Gets the result of this operation as a CompactSketch
   * @param dstOrdered 
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>
   * 
   * @param dstMem 
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   * 
   * @return the result as a CompactSketch
   */
  CompactSketch getResult(boolean dstOrdered, Memory dstMem);
  
  /**
   * Gets the result of this operation as an ordered CompactSketch on the Java heap
   * @return the result of this operation as an ordered CompactSketch on the Java heap
   */
  CompactSketch getResult();
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