/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

/**
 * On-heap implementation of intersection set operation for tuple sketches of type
 * ArrayOfDoubles.
 */
final class HeapArrayOfDoublesIntersection extends ArrayOfDoublesIntersection {

  /**
   * Creates an instance of a HeapArrayOfDoublesIntersection with a custom update seed
   * @param numValues number of double values associated with each key
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  HeapArrayOfDoublesIntersection(final int numValues, final long seed) {
    super(numValues, seed);
  }

  @Override
  protected ArrayOfDoublesQuickSelectSketch createSketch(final int size, final int numValues, 
      final long seed) {
    return new HeapArrayOfDoublesQuickSelectSketch(size, 0, 1f, numValues, seed);
  }

}
