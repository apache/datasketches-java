package com.yahoo.sketches.quantiles;

/**
 * Created by jmalkin on 2/27/17.
 */
public abstract class UpdateDoublesSketch extends DoublesSketch {
  UpdateDoublesSketch(final int k) {
    super(k);
  }

  /**
   * Updates this sketch with the given double data item
   * @param dataItem an item from a stream of items.  NaNs are ignored.
   */
  public abstract void update(double dataItem);

  @Override
  boolean isCompact() {
    return false;
  }
}
