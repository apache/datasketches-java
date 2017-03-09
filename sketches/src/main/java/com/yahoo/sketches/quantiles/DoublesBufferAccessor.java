package com.yahoo.sketches.quantiles;

/**
 * @author Jon Malkin
 */
abstract class DoublesBufferAccessor {
  public abstract double[] getArray(int fromIdx, int numItems);

  public abstract void putArray(double[] srcArray, int srcIndex,
                                int dstIndex, int numItems);

  public abstract void sort();

  // These next 3 come from AbstractList, just included here to be explicit
  public abstract double get(final int index);

  public abstract double set(final int index, final Double value);

  public abstract int size();
}
