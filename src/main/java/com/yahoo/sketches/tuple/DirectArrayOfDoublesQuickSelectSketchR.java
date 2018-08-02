package com.yahoo.sketches.tuple;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesReadOnlyException;

final class DirectArrayOfDoublesQuickSelectSketchR extends DirectArrayOfDoublesQuickSelectSketch {

  DirectArrayOfDoublesQuickSelectSketchR(final Memory mem, final long seed) {
    super((WritableMemory) mem, seed);
  }

  @Override
  void insertOrIgnore(final long key, final double[] values) {
    throw new SketchesReadOnlyException();
  }

  @Override
  public void trim() {
    throw new SketchesReadOnlyException();
  }

}
