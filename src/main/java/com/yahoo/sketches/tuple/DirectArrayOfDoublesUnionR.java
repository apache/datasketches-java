package com.yahoo.sketches.tuple;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesReadOnlyException;

final class DirectArrayOfDoublesUnionR extends DirectArrayOfDoublesUnion {

  DirectArrayOfDoublesUnionR(final int nomEntries, final int numValues, final long seed,
      final WritableMemory dstMem) {
    super(nomEntries, numValues, seed, dstMem);
  }

  /**
   * Wraps the given Memory.
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  DirectArrayOfDoublesUnionR(final ArrayOfDoublesQuickSelectSketch sketch, final WritableMemory mem) {
    super(sketch, mem);
  }

  @Override
  public void update(final ArrayOfDoublesSketch sketchIn) {
    throw new SketchesReadOnlyException();
  }

  @Override
  public void reset() {
    throw new SketchesReadOnlyException();
  }

}
