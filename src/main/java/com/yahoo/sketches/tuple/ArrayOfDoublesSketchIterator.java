/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

/**
 * Interface for iterating over tuple sketches of type ArrayOfDoubles
 */
public interface ArrayOfDoublesSketchIterator {
  /**
   * Advancing the iterator and checking existence of the next entry
   * is combined here for efficiency. This results in an undefined
   * state of the iterator before the first call of this method.
   * @return true if the next element exists
   */
  public boolean next();

  /**
   * Gets a key from the current entry in the sketch, which is a hash
   * of the original key passed to update(). The original keys are not
   * retained. Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return hash key from the current entry
   */
  public long getKey();

  /**
   * Gets an array of values from the current entry in the sketch.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return array of double values for the current entry (may or may not be a copy)
   */
  public double[] getValues();
}
