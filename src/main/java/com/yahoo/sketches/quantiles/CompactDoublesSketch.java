/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import com.yahoo.memory.Memory;

/**
 * @author Jon Malkin
 */
public abstract class CompactDoublesSketch extends DoublesSketch {
  CompactDoublesSketch(final int k) {
    super(k);
  }

  public static CompactDoublesSketch heapify(final Memory srcMem) {
    return HeapCompactDoublesSketch.heapifyInstance(srcMem);
  }

  @Override
  boolean isCompact() {
    return true;
  }

}
