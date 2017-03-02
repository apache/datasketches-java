package com.yahoo.sketches.quantiles;

/**
 * Created by jmalkin on 2/27/17.
 */
public abstract class CompactDoublesSketch extends DoublesSketch {
  CompactDoublesSketch(final int k) {
    super(k);
  }

  @Override
  boolean isCompact() {
    return true;
  }
}
