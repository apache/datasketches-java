/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.theta.SetOperation.SetReturnState;

/**
 * The API for AnotB operations
 * 
 * @author Lee Rhodes
 */
public interface AnotB {

  /**
   * Perform A-and-not-B set operation on the two given sketches.
   * A null sketch is interpreted as an empty sketch.
   * 
   * @param a The incoming sketch for the first argument
   * @param b The incoming sketch for the second argument
   * @return the return state of the AnotB
   */  
  SetReturnState update(Sketch a, Sketch b);
  
  /**
   * Returns the result as a new CompactSketch.
   * @param dstOrdered if true, the result of a SetOperation will be in ordered, compact form
   * @param dstMem the destination Memory
   * @return the result CompactSketch.
   */
  CompactSketch getResult(boolean dstOrdered, Memory dstMem);
  
}