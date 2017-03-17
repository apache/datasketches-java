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


  // Checks used at instantiation

  static void checkCompact(final int serVer, final int flags) {
    final boolean compact
            = (serVer == 2) | ((flags & (COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK)) > 0);
    if (!compact) {
      throw new SketchesArgumentException("CompactDoublesSketch must wrap a compact Memory");
    }
  }

}
