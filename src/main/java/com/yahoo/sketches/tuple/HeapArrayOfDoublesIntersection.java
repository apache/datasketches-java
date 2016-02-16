/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

/**
 * On-heap implementation
 */
public class HeapArrayOfDoublesIntersection extends ArrayOfDoublesIntersection {

  /**
   * Creates an instance of a HeapArrayOfDoublesIntersection
   * @param numValues number of double values associated with each key
   */
  public HeapArrayOfDoublesIntersection(int numValues) {
    this(numValues, DEFAULT_UPDATE_SEED);
  }

  /**
   * Creates an instance of a HeapArrayOfDoublesIntersection with a custom update seed
   * @param numValues number of double values associated with each key
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  public HeapArrayOfDoublesIntersection(int numValues, long seed) {
    super(numValues, seed);
  }

  @Override
  protected ArrayOfDoublesQuickSelectSketch createSketch(int size, int numValues, long seed) {
    return new HeapArrayOfDoublesQuickSelectSketch(size, 0, 1f, numValues, seed);
  }

}
