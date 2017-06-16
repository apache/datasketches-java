/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import com.yahoo.memory.WritableMemory;

/**
 * Direct Union operation for tuple sketches of type ArrayOfDoubles.
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
class DirectArrayOfDoublesUnion extends ArrayOfDoublesUnion {

  final WritableMemory mem_;

  /**
   * Creates an instance of DirectArrayOfDoublesUnion
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than 
   * given value.
   * @param numValues Number of double values to keep for each key.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param dstMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  DirectArrayOfDoublesUnion(final int nomEntries, final int numValues, final long seed, 
      final WritableMemory dstMem) {
    super(new DirectArrayOfDoublesQuickSelectSketch(nomEntries, 3, 1f, numValues, seed, dstMem));
    mem_ = dstMem;
  }

  DirectArrayOfDoublesUnion(final ArrayOfDoublesQuickSelectSketch sketch, final WritableMemory mem) {
    super(sketch);
    mem_ = mem;
  }

  @Override
  public void reset() {
    sketch_ = new DirectArrayOfDoublesQuickSelectSketch(nomEntries_, 3, 1f, numValues_, seed_, mem_);
    setThetaLong(sketch_.getThetaLong());
  }

  @Override
  void setThetaLong(final long theta) {
    super.setThetaLong(theta);
    mem_.putLong(THETA_LONG, theta);
  }

}
