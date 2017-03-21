package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;

import com.yahoo.memory.Memory;

import com.yahoo.sketches.SketchesArgumentException;

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
