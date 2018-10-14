package com.yahoo.sketches.tuple;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesReadOnlyException;

final class DirectArrayOfDoublesUnionR extends DirectArrayOfDoublesUnion {

  /**
   * Wraps the given Memory.
   * @param sketch the ArrayOfDoublesQuickSelectSketch
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
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
