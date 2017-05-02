package com.yahoo.sketches.tuple;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesReadOnlyException;

final class DirectArrayOfDoublesQuickSelectSketchR extends DirectArrayOfDoublesQuickSelectSketch {

  DirectArrayOfDoublesQuickSelectSketchR(int nomEntries, int lgResizeFactor, float samplingProbability, int numValues,
      long seed, WritableMemory dstMem) {
    super(nomEntries, lgResizeFactor, samplingProbability, numValues, seed, dstMem);
  }

  DirectArrayOfDoublesQuickSelectSketchR(final WritableMemory mem, final long seed) {
    super(mem, seed);
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
