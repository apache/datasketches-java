package com.yahoo.sketches.quantiles;

import com.yahoo.memory.Memory;

/**
 * @author Jon Malkin
 */
public abstract class CompactDoublesSketch extends DoublesSketch {
  CompactDoublesSketch(final int k) {
    super(k);
  }

  @Override
  boolean isCompact() {
    return true;
  }

  public static CompactDoublesSketch heapify(final Memory srcMem) {
    return HeapCompactDoublesSketch.heapifyInstance(srcMem);
  }
}
