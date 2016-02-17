/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.sketches.memory.Memory;

/**
 * This implementation keeps data in a given memory.
 * The purpose is to avoid garbage collection.
 */
public class DirectArrayOfDoublesIntersection extends ArrayOfDoublesIntersection {

  private Memory mem_;

  /**
   * Creates an instance of a DirectArrayOfDoublesIntersection
   * @param numValues number of double values associated with each key
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  public DirectArrayOfDoublesIntersection(int numValues, Memory mem) {
    this(numValues, DEFAULT_UPDATE_SEED, mem);
  }

  /**
   * Creates an instance of a DirectArrayOfDoublesIntersection with a custom update seed
   * @param numValues number of double values associated with each key
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  public DirectArrayOfDoublesIntersection(int numValues, long seed, Memory mem) {
    super(numValues, seed);
    mem_ = mem;
  }

  @Override
  protected ArrayOfDoublesQuickSelectSketch createSketch(int size, int numValues, long seed) {
    return new DirectArrayOfDoublesQuickSelectSketch(size, 0, 1f, numValues, seed, mem_);
  }

}
