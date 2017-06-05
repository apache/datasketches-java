/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import com.yahoo.memory.WritableMemory;

/**
 * Direct Intersection operation for tuple sketches of type ArrayOfDoubles.
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
final class DirectArrayOfDoublesIntersection extends ArrayOfDoublesIntersection {

  private WritableMemory mem_;

  /**
   * Creates an instance of a DirectArrayOfDoublesIntersection with a custom update seed
   * @param numValues number of double values associated with each key
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param dstMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  DirectArrayOfDoublesIntersection(final int numValues, final long seed, final WritableMemory dstMem) {
    super(numValues, seed);
    mem_ = dstMem;
  }

  @Override
  protected ArrayOfDoublesQuickSelectSketch createSketch(final int size, final int numValues, 
      final long seed) {
    return new DirectArrayOfDoublesQuickSelectSketch(size, 0, 1f, numValues, seed, mem_);
  }

}
