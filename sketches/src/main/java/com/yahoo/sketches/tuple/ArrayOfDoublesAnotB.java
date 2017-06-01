/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import com.yahoo.memory.WritableMemory;

/**
 * Computes a set difference of two tuple sketches of type ArrayOfDoubles
 */
public abstract class ArrayOfDoublesAnotB {

  ArrayOfDoublesAnotB() {}

  /**
   * Perform A-and-not-B set operation on the two given sketches.
   * A null sketch is interpreted as an empty sketch.
   * This is not an accumulating update. Calling update() more than once
   * without calling getResult() will discard the result of previous update()
   * 
   * @param a The incoming sketch for the first argument
   * @param b The incoming sketch for the second argument
   */  
  public abstract void update(ArrayOfDoublesSketch a, ArrayOfDoublesSketch b);

  /**
   * Gets the result of this operation in the form of a ArrayOfDoublesCompactSketch
   * @return compact sketch representing the result of the operation
   */
  public abstract ArrayOfDoublesCompactSketch getResult();

  /**
   * Gets the result of this operation in the form of a ArrayOfDoublesCompactSketch
   * @param mem memory for the result (can be null)
   * @return compact sketch representing the result of the operation (off-heap if memory is 
   * provided)
   */
  public abstract ArrayOfDoublesCompactSketch getResult(WritableMemory mem);

}
